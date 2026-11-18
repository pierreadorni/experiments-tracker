"""Bounded, read-only C7 research tools served through the existing controller bridge.

``cluster_info`` reads only the executor's already-cached cluster observation and the
owner's configured resource caps. ``job_status`` and ``job_logs`` reuse the existing
experiment-scoped job lookup and local log reader. None refreshes discovery, submits
work or saves a receipt: repeating the same UUID is the same read.

It also holds the shared pre-dispatch gate for the contract-4 write tools: the exact
live research turn must own the action and the executor must not be in maintenance.
"""

from pathlib import Path
from uuid import UUID

from pydantic import Field

from .. import artifacts
from ..config import StrictModel
from ..maintenance import blocked

RESEARCH_PHASE = "research"
MAX_FIELD_CHARS = 4096
MAX_ERROR_CHARS = 500
MAX_ERRORS = 8
MAX_JOB_INTENTS = 32
DEFAULT_LOG_TAIL = 4000
MAX_LOG_TAIL = 65536
_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})
_SNAPSHOT_TEXT_FIELDS = ("source", "nodes", "partitions", "allocations", "associations")


class ClusterInfo(StrictModel):
    """One read-only capability request; the UUID makes a replay the same read."""

    request_id: UUID


class JobStatus(StrictModel):
    """This experiment's saved jobs only, scoped to at most 32 exact intent IDs."""

    request_id: UUID
    intent_ids: list[UUID] | None = Field(default=None, max_length=MAX_JOB_INTENTS)
    summary_only: bool = Field(default=False, strict=True)


class JobLogs(StrictModel):
    """One saved job's bounded local log tail."""

    request_id: UUID
    intent_id: UUID
    tail: int = Field(default=DEFAULT_LOG_TAIL, ge=1, le=MAX_LOG_TAIL, strict=True)


def _text(value, limit=MAX_FIELD_CHARS):
    if not isinstance(value, str) or len(value) <= limit:
        return value
    return value[:limit] + " [truncated]"


def _capabilities(snapshot):
    if not snapshot:
        return None
    result = {"observed_at": snapshot.get("observed_at")}
    for name in _SNAPSHOT_TEXT_FIELDS:
        if name in snapshot:
            result[name] = _text(snapshot[name])
    errors = snapshot.get("errors")
    if isinstance(errors, dict):
        result["errors"] = {
            str(name): _text(str(value), MAX_ERROR_CHARS) for name, value in list(errors.items())[:MAX_ERRORS]
        }
    return result


def cluster_info(workflow, experiment, request):
    """Return cached bounded capabilities and owner caps without a scheduler call."""
    snapshot = workflow.cluster_for(experiment)
    return {
        "request_id": str(request.request_id),
        "cached": snapshot is not None,
        "capabilities": _capabilities(snapshot),
        "owner_caps": workflow.config.slurm.limits.model_dump(mode="json"),
    }


def log_tail(root: Path, job, limit: int):
    """Read at most ``limit`` bytes of each log from the saved attempt directory.

    The path is contained inside ``root``, symbolic links are refused and only the
    final bytes are read. Remote logs are available only after the existing transfer
    has placed them here; absent files stay unavailable instead of opening SSH.
    """
    logs = {}
    if not job.get("job_id"):
        return logs
    for suffix in ("out", "err"):
        path = root / f"et-{job['intent_id']}-{job['job_id']}.{suffix}"
        if path.is_file() and not path.is_symlink():
            path = artifacts.allowed_path(path, root, max_bytes=None)
            with path.open("rb") as stream:
                stream.seek(max(0, path.stat().st_size - limit))
                logs[suffix] = stream.read(limit).decode(errors="replace")
    return logs


def _owned_intents(workflow, experiment):
    return {
        row["intent_id"]
        for row in workflow.store.rows(
            "SELECT intent_id FROM jobs WHERE experiment_id=?", (experiment["id"],)
        )
    }


def job_status(workflow, experiment, request):
    """Read only this experiment's saved jobs, optionally narrowed to owned intents."""
    intent_ids = None
    if request.intent_ids is not None:
        intent_ids = {str(value) for value in request.intent_ids}
        if intent_ids - _owned_intents(workflow, experiment):
            raise ValueError("intent_ids must select this experiment's saved jobs")
    jobs = workflow.controller.status(
        experiment["id"],
        intent_ids=intent_ids,
        summary_only=request.summary_only,
        limit=None if intent_ids is not None else MAX_JOB_INTENTS,
    )
    result = {"request_id": str(request.request_id), "jobs": jobs}
    if intent_ids is None:
        total = workflow.store.one(
            "SELECT COUNT(*) AS total FROM jobs WHERE experiment_id=?", (experiment["id"],)
        )["total"]
        if total > len(jobs):
            result["omitted"] = total - len(jobs)
    return result


def job_logs(workflow, experiment, request):
    """Return one owned job's bounded local log tail and its availability."""
    job = workflow.store.one(
        "SELECT * FROM jobs WHERE experiment_id=? AND intent_id=?",
        (experiment["id"], str(request.intent_id)),
    )
    if not job:
        raise ValueError("intent_id is not this experiment's saved job")
    root = Path(job["execution_root"]) if job.get("execution_root") else workflow.root(experiment["id"])
    logs = log_tail(root, job, request.tail)
    return {
        "request_id": str(request.request_id),
        "intent_id": job["intent_id"],
        "job_id": job["job_id"],
        "state": job["state"],
        "tail_bytes": request.tail,
        "available": bool(logs),
        "logs": logs,
    }


def require_research_write(workflow, experiment, action):
    """Refuse a contract-4 write outside its exact live turn or during maintenance.

    The write tools repeat their own saved-identity checks inside the transaction;
    this shared gate runs before either tool can touch the store. A release handover
    must never start a new durable research write.
    """
    require_live_research(experiment, action)
    if blocked(workflow.store):
        raise ValueError("maintenance forbids research writes")


def checkpoint_receipt(saved):
    """Return the save_checkpoint receipt; the private Markdown stays in its event."""
    return {field: saved[field] for field in ("request_id", "experiment_id", "action_id", "version", "bytes")}


def require_live_research(experiment, action):
    """Refuse this tool outside the exact live research turn of its own experiment."""
    contract = experiment.get("contract") or {}
    if contract.get("execution_contract") != 4:
        raise ValueError("this tool requires execution contract 4")
    if action.get("experiment_id") != experiment["id"] or action.get("phase") != RESEARCH_PHASE:
        raise ValueError("this tool requires this experiment's active research turn")
    if action.get("state") not in ("dispatching", "running"):
        raise ValueError("this tool requires this experiment's active research turn")
    if (
        experiment.get("deleted_at") is not None
        or experiment["phase"] in _TERMINAL_PHASES
        or experiment.get("pause_agent")
    ):
        raise ValueError("this tool requires a live research experiment")
