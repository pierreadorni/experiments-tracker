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
from . import tools

MAX_RESULT_PATH_CHARS = 1024


class RegisterResult(StrictModel):
    """Read one result file from an experiment's successful producing job."""

    request_id: UUID
    intent_id: UUID
    path: str = Field(min_length=1, max_length=MAX_RESULT_PATH_CHARS)

    @field_validator("path")
    @classmethod
    def bounded_relative_path(cls, value):
        path = Path(value)
        if not value.strip() or path.is_absolute() or ".." in path.parts:
            raise ValueError("result path must be a bounded relative path without traversal")
        return value


def register_result(workflow, experiment, action, request):
    """Return the validated result only when exact saved ownership checks pass."""
    request = RegisterResult.model_validate(request)
    eid = experiment["id"]
    store = workflow.store
    with store.tx() as db:
        saved = store.experiment(eid)
        saved_action = action_obligations.expected_saved(db, action)
        tools.require_live_research(saved, saved_action)
        if not saved_action["thread_id"] or saved.get("thread_id") != saved_action["thread_id"]:
            raise ValueError("register_result requires the exact saved research thread")
        if blocked(store):
            raise ValueError("maintenance forbids result reads")
        job = db.execute(
            "SELECT * FROM jobs WHERE experiment_id=? AND intent_id=?",
            (eid, str(request.intent_id)),
        ).fetchone()
    job = dict(job) if job is not None else None
    if (
        not job
        or job["state"] != "complete"
        or not job.get("job_id")
        or job.get("ended_at") is None
        or json.loads(job.get("execution_manifest") or "{}").get("executor") == "agent"
    ):
        raise ValueError("result intent must identify this experiment's successful Slurm job")
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
    return {
        "request_id": str(request.request_id),
        "intent_id": job["intent_id"],
        "result": result.model_dump(mode="json"),
    }
