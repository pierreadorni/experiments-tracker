"""Validate manual contract-4 job admission without performing external effects."""

from typing import Annotated
from uuid import UUID

from pydantic import Field, field_validator

from ..config import JobResources, StrictModel
from ..core import action_obligations
from ..maintenance import blocked
from . import approval
from .result_path import MAX_RESULT_PATH_CHARS, validate_result_path
from .tools import require_live_research


class SubmitJobRequest(StrictModel):
    request_id: UUID
    approval_request_id: UUID
    objective: str = Field(min_length=1, max_length=2000)
    name: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    command: list[Annotated[str, Field(max_length=4096)]] = Field(min_length=1, max_length=128)
    resources: JobResources
    result_file: str | None = Field(default=None, max_length=MAX_RESULT_PATH_CHARS)

    @field_validator("result_file")
    @classmethod
    def valid_result_file(cls, value):
        return None if value is None else validate_result_path(value)

    def saved_request(self):
        # Omit only the new field when absent; historical receipts included resource defaults.
        return self.model_dump(mode="json", exclude={"result_file"} if self.result_file is None else set())

    @field_validator("command")
    @classmethod
    def valid_argv(cls, values):
        if not values[0].strip() or any("\0" in value for value in values):
            raise ValueError("command must be argv with a nonblank executable and no NUL bytes")
        if sum(len(value.encode()) + 1 for value in values) > 16384:
            raise ValueError("command argv exceeds 16 KiB")
        return values


def preflight(workflow, experiment_id, action, request, *, db=None, remote_source=False):
    """Check saved admission facts in a new or caller-owned transaction."""
    request = SubmitJobRequest.model_validate(request)
    if db is not None:
        _check(workflow, experiment_id, action, request, db, remote_source=remote_source)
        return request
    with workflow.store.tx() as transaction:
        _check(workflow, experiment_id, action, request, transaction, remote_source=remote_source)
    return request


def _check(workflow, experiment_id, action, request, db, *, remote_source=False):
    store = workflow.store
    experiment = store.experiment(experiment_id)
    saved = action_obligations.expected_saved(db, action)
    require_live_research(experiment, saved)
    if (
        saved["state"] != "running"
        or not saved["turn_id"]
        or not saved["thread_id"]
        or saved["thread_id"] != experiment["thread_id"]
    ):
        raise ValueError("submission requires the exact running research turn")
    if blocked(store) or experiment["pause_compute"]:
        raise ValueError("new compute is paused or maintenance is active")
    autopilot_hold = workflow.research.service.authority(experiment_id, phase="compute")
    if autopilot_hold:
        raise ValueError(f"Autopilot compute authority refused submission: {autopilot_hold}")
    contract = experiment["contract"]
    context = contract.get("execution", {}).get("context", "acacia")
    if not contract.get("branch_workspace") or (context != "acacia" and not (remote_source and context == "jean-zay")):
        raise ValueError("submission requires a local named research workspace")
    latest = db.execute(
        "SELECT json_extract(payload,'$.request_id') FROM events "
        "WHERE experiment_id=? AND kind=? ORDER BY seq DESC LIMIT 1",
        (experiment_id, approval.REQUEST_EVENT),
    ).fetchone()
    if (
        latest is None
        or latest[0] != str(request.approval_request_id)
        or not approval.approved(store, experiment_id, request.approval_request_id, request.objective, db=db)
    ):
        raise ValueError("exact owner approval required")
    if db.execute(
        "SELECT 1 FROM jobs WHERE experiment_id=? AND state='submitting' LIMIT 1", (experiment_id,)
    ).fetchone():
        raise ValueError("an existing job intent has uncertain outcome")
    limits = workflow.config.slurm
    limits.limits.validate_request(request.resources)
    if request.resources.gpus_per_job:
        raise ValueError("research submission is CPU-only until GPU budget accounting is wired")
    if (
        limits.max_jobs_per_experiment is not None
        and db.execute("SELECT COUNT(*) FROM jobs WHERE experiment_id=?", (experiment_id,)).fetchone()[0]
        >= limits.max_jobs_per_experiment
    ):
        raise ValueError("experiment job limit reached")
    if (
        limits.max_concurrent_jobs is not None
        and db.execute("SELECT COUNT(*) FROM jobs WHERE state IN ('submitting','submitted')").fetchone()[0]
        >= limits.max_concurrent_jobs
    ):
        raise ValueError("executor concurrent job limit reached")
