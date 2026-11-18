"""Bounded, read-only C7 research tools served through the existing controller bridge.

``cluster_info`` reads only the executor's already-cached cluster observation and the
owner's configured resource caps. It never refreshes discovery, submits work or saves a
receipt: repeating the same UUID is the same read.
"""

from uuid import UUID

from ..config import StrictModel

RESEARCH_PHASE = "research"
MAX_FIELD_CHARS = 4096
MAX_ERROR_CHARS = 500
MAX_ERRORS = 8
_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})
_SNAPSHOT_TEXT_FIELDS = ("source", "nodes", "partitions", "allocations", "associations")


class ClusterInfo(StrictModel):
    """One read-only capability request; the UUID makes a replay the same read."""

    request_id: UUID


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


def require_live_research(experiment, action):
    """Refuse this tool outside the exact live research turn of its own experiment."""
    contract = experiment.get("contract") or {}
    if contract.get("execution_contract") != 4:
        raise ValueError("cluster_info requires execution contract 4")
    if action.get("experiment_id") != experiment["id"] or action.get("phase") != RESEARCH_PHASE:
        raise ValueError("cluster_info requires this experiment's active research turn")
    if action.get("state") not in ("dispatching", "running"):
        raise ValueError("cluster_info requires this experiment's active research turn")
    if (
        experiment.get("deleted_at") is not None
        or experiment["phase"] in _TERMINAL_PHASES
        or experiment.get("pause_agent")
    ):
        raise ValueError("cluster_info requires a live research experiment")
