"""Inert, bounded reads of a result from its exact saved producing job."""

import json
from pathlib import Path
from uuid import UUID

from pydantic import Field, field_validator

from .. import artifacts
from ..config import StrictModel
from ..core import action_obligations
from ..maintenance import blocked
from ..models import ExperimentResult
from ..store import Conflict
from . import tools
from .result_path import MAX_RESULT_PATH_CHARS, validate_result_path

EVENT_KIND = "research_result_registered"
STAGE = "research"


class RegisterResult(StrictModel):
    """Read one result file from an experiment's successful producing job."""

    request_id: UUID
    intent_id: UUID
    path: str = Field(min_length=1, max_length=MAX_RESULT_PATH_CHARS)

    @field_validator("path")
    @classmethod
    def bounded_relative_path(cls, value):
        return validate_result_path(value)


def register_result(workflow, experiment, action, request):
    """Register bounded metrics and return the immutable artifact receipt."""
    request = RegisterResult.model_validate(request)
    eid = experiment["id"]
    store = workflow.store
    with store.tx() as db:
        _, saved_action, job = _saved_context(store, db, eid, action, request.intent_id)
        replay = _replay(db, request.request_id, eid, saved_action, str(request.intent_id), request.path)
        if replay is not None:
            return replay
    job = dict(job) if job is not None else None
    if (
        not job
        or job["state"] != "complete"
        or not job.get("job_id")
        or job.get("ended_at") is None
        or json.loads(job.get("execution_manifest") or "{}").get("executor") == "agent"
    ):
        raise ValueError("result intent must identify this experiment's successful Slurm job")
    saved_request = json.loads(job.get("execution_manifest") or "{}").get("research_request") or {}
    declared_result = saved_request.get("result_file")
    if declared_result is not None and request.path != declared_result:
        raise ValueError("result path differs from the exact job intent declaration")
    root_value = job.get("execution_root")
    if not root_value:
        raise ValueError("result job has no saved attempt output root")
    root = Path(root_value)
    if not root.is_absolute() or root.is_symlink():
        raise ValueError("result job attempt root must be absolute and not a symlink")
    base_root = Path(workflow.projects.base_root(eid)).resolve()
    expected_root = base_root / "attempts" / str(request.intent_id)
    if not root.is_dir() or root.resolve() != expected_root or root != expected_root:
        raise ValueError("result job attempt root must be contained under its exact experiment attempt")
    path = artifacts.allowed_path(root / request.path, root, max_bytes=artifacts.MAX_ARTIFACT_BYTES)
    with path.open("rb") as stream:
        data = stream.read(artifacts.MAX_ARTIFACT_BYTES + 1)
    if len(data) > artifacts.MAX_ARTIFACT_BYTES:
        raise ValueError("result exceeds 10 MiB limit")
    value = json.loads(
        data,
        parse_constant=lambda item: (_ for _ in ()).throw(ValueError(f"non-finite JSON constant: {item}")),
    )
    result = ExperimentResult.model_validate(value)
    result_value = result.model_dump(mode="json")
    source = request.path
    output_root = base_root
    name = f"result-{request.request_id}.json"
    target = output_root / name
    output_root.mkdir(parents=True, exist_ok=True)
    data = json.dumps(result_value, sort_keys=True, separators=(",", ":")).encode()
    if len(data) > artifacts.MAX_ARTIFACT_BYTES:
        raise ValueError("published result exceeds 10 MiB limit")
    try:
        with target.open("xb") as stream:
            stream.write(data)
    except FileExistsError:
        with artifacts.allowed_path(target, output_root).open("rb") as stream:
            if stream.read(len(data) + 1) != data:
                raise ValueError("published result copy cannot change; choose a new request") from None
    target.chmod(0o444)

    with store.tx() as db:
        saved, saved_action, current_job = _saved_context(store, db, eid, action, request.intent_id)
        if not _same_producer(current_job, job):
            raise ValueError("result producer identity changed during registration")
        replay = _replay(db, request.request_id, eid, saved_action, job["intent_id"], source)
        if replay is not None:
            return replay
        artifacts.register(store, eid, STAGE, output_root, [name], _db=db)
        artifact = db.execute(
            "SELECT id,sha256,size,media_type FROM artifacts WHERE experiment_id=? AND name=? "
            "AND evidence_revision=?",
            (eid, name, saved["evidence_revision"]),
        ).fetchone()
        receipt = {
            "request_id": str(request.request_id),
            "experiment_id": eid,
            "action_id": saved_action["id"],
            "intent_id": job["intent_id"],
            "artifact_id": artifact["id"],
            "name": name,
            "source": source,
            "sha256": artifact["sha256"],
            "size": artifact["size"],
            "media_type": artifact["media_type"],
        }
        store.emit(db, eid, EVENT_KIND, receipt)
        return receipt


def _saved_context(store, db, eid, action, intent_id):
    saved = store.experiment(eid)
    saved_action = action_obligations.expected_saved(db, action)
    tools.require_live_research(saved, saved_action)
    if not saved_action.get("thread_id") or saved.get("thread_id") != saved_action["thread_id"]:
        raise ValueError("register_result requires the exact saved research thread")
    if blocked(store):
        raise ValueError("maintenance forbids result reads")
    job = db.execute(
        "SELECT * FROM jobs WHERE experiment_id=? AND intent_id=?", (eid, str(intent_id))
    ).fetchone()
    return saved, saved_action, job


def _same_producer(current, initial):
    if current is None:
        return False
    current = dict(current)
    return all(
        current.get(key) == initial.get(key)
        for key in (
            "intent_id",
            "experiment_id",
            "stage_id",
            "attempt",
            "state",
            "job_id",
            "execution_root",
            "execution_manifest",
            "ended_at",
        )
    )


def _replay(db, request_id, eid, action, intent_id, source):
    row = db.execute(
        "SELECT experiment_id,payload FROM events WHERE kind=? AND "
        "json_extract(payload,'$.request_id')=? LIMIT 1",
        (EVENT_KIND, str(request_id)),
    ).fetchone()
    if row is None:
        return None
    prior = json.loads(row["payload"])
    if (
        row["experiment_id"] != eid
        or prior.get("action_id") != action["id"]
        or prior.get("intent_id") != intent_id
        or prior.get("source") != source
    ):
        raise Conflict("result request identity already used with different metadata or scope")
    return prior
