"""Validate manual contract-4 job admission without performing external effects."""

import json
from typing import Annotated
from uuid import UUID

from pydantic import Field, field_validator

from ..config import JobResources, StrictModel
from ..core import action_obligations
from ..maintenance import blocked
from ..models import TensorBoardDisplay
from . import approval
from .result_path import MAX_RESULT_PATH_CHARS, validate_remote_result_path, validate_result_path
from .tools import require_live_research

MAX_DEPENDENCIES = 32
MAX_INPUTS = 16
MAX_INPUT_BYTES = 10 * 1024 * 1024
MAX_INPUTS_BYTES = 16 * 1024 * 1024


class SubmitJobRequest(StrictModel):
    request_id: UUID
    approval_request_id: UUID
    objective: str = Field(min_length=1, max_length=2000)
    name: str = Field(pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,99}$")
    command: list[Annotated[str, Field(max_length=4096)]] = Field(min_length=1, max_length=128)
    resources: JobResources
    result_file: str | None = Field(default=None, max_length=MAX_RESULT_PATH_CHARS)
    tensorboard: TensorBoardDisplay | None = None
    depends_on: list[UUID] = Field(default_factory=list, max_length=MAX_DEPENDENCIES)
    inputs: list[UUID] = Field(default_factory=list, max_length=MAX_INPUTS)

    @field_validator("depends_on")
    @classmethod
    def unique_dependencies(cls, values):
        if len(set(values)) != len(values):
            raise ValueError("depends_on must contain unique job intent UUIDs")
        return values

    @field_validator("result_file")
    @classmethod
    def valid_result_file(cls, value):
        return None if value is None else validate_result_path(value)

    @field_validator("inputs")
    @classmethod
    def unique_inputs(cls, values):
        if len(set(values)) != len(values):
            raise ValueError("inputs must contain unique register_result request UUIDs")
        return values

    def saved_request(self):
        # Keep historical receipts stable when newer optional fields are absent.
        absent = {name for name in ("result_file", "tensorboard") if getattr(self, name) is None}
        saved = self.model_dump(mode="json", exclude=absent)
        for name in ("depends_on", "inputs"):
            if not saved[name]:
                saved.pop(name)
        return saved

    @field_validator("command")
    @classmethod
    def valid_argv(cls, values):
        if not values[0].strip() or any("\0" in value for value in values):
            raise ValueError("command must be argv with a nonblank executable and no NUL bytes")
        if sum(len(value.encode()) + 1 for value in values) > 16384:
            raise ValueError("command argv exceeds 16 KiB")
        return values


def preflight(
    workflow, experiment_id, action, request, *, db=None, remote_source=False, existing_intent=None
):
    """Check saved admission facts in a new or caller-owned transaction."""
    request = SubmitJobRequest.model_validate(request)
    if db is not None:
        _check(
            workflow,
            experiment_id,
            action,
            request,
            db,
            remote_source=remote_source,
            existing_intent=existing_intent,
        )
        return request
    with workflow.store.tx() as transaction:
        _check(
            workflow,
            experiment_id,
            action,
            request,
            transaction,
            remote_source=remote_source,
            existing_intent=existing_intent,
        )
    return request


def _check(workflow, experiment_id, action, request, db, *, remote_source=False, existing_intent=None):
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
    for dependency in request.depends_on:
        predecessor = db.execute(
            "SELECT experiment_id,state FROM jobs WHERE intent_id=?", (str(dependency),)
        ).fetchone()
        if (
            predecessor is None
            or predecessor["experiment_id"] != experiment_id
            or predecessor["state"] != "complete"
        ):
            raise ValueError(
                "dependencies must be saved complete jobs; wait for the terminal wake, then request"
            )
    if request.inputs and remote_source:
        raise ValueError("registered-result inputs are not supported for remote submissions")
    total = 0
    for input_id in request.inputs:
        event = db.execute(
            "SELECT experiment_id,payload FROM events WHERE kind='research_result_registered' "
            "AND json_extract(payload,'$.request_id')=? LIMIT 1",
            (str(input_id),),
        ).fetchone()
        if event is None or event["experiment_id"] != experiment_id:
            raise ValueError("inputs must name registered results from this experiment")
        receipt = json.loads(event["payload"])
        producer = db.execute(
            "SELECT state,job_id,ended_at,execution_manifest FROM jobs WHERE intent_id=? AND experiment_id=?",
            (receipt.get("intent_id"), experiment_id),
        ).fetchone()
        artifact = db.execute(
            "SELECT experiment_id,name,size,sha256 FROM artifacts WHERE id=?",
            (receipt.get("artifact_id"),),
        ).fetchone()
        if (
            producer is None
            or producer["state"] != "complete"
            or not producer["job_id"]
            or producer["ended_at"] is None
            or json.loads(producer["execution_manifest"] or "{}").get("executor") == "agent"
            or artifact is None
            or artifact["experiment_id"] != experiment_id
            or artifact["name"] != receipt.get("name")
            or artifact["size"] != receipt.get("size")
            or artifact["sha256"] != receipt.get("sha256")
        ):
            raise ValueError("inputs must name immutable results from completed Slurm intents")
        if artifact["size"] > MAX_INPUT_BYTES:
            raise ValueError("registered-result input exceeds 10 MiB limit")
        total += artifact["size"]
    if total > MAX_INPUTS_BYTES:
        raise ValueError("registered-result inputs exceed 16 MiB total")
    if blocked(store) or experiment["pause_compute"]:
        raise ValueError("new compute is paused or maintenance is active")
    autopilot_hold = workflow.research.service.authority(experiment_id, phase="compute")
    if autopilot_hold:
        raise ValueError(f"Autopilot compute authority refused submission: {autopilot_hold}")
    contract = experiment["contract"]
    context = contract.get("execution", {}).get("context", "acacia")
    if not contract.get("branch_workspace") or (
        context != "acacia" and not (remote_source and context == "jean-zay")
    ):
        raise ValueError("submission requires a local named research workspace")
    if remote_source:
        if request.tensorboard is not None:
            raise ValueError("TensorBoard previews are not supported for Jean Zay submissions")
        if request.result_file is None:
            raise ValueError("remote submission requires a declared result_file")
        validate_remote_result_path(request.result_file)
    latest = db.execute(
        "SELECT json_extract(payload,'$.request_id') FROM events "
        "WHERE experiment_id=? AND kind=? ORDER BY seq DESC LIMIT 1",
        (experiment_id, approval.REQUEST_EVENT),
    ).fetchone()
    owner_approval = approval.approved(
        store, experiment_id, request.approval_request_id, request.objective, db=db
    )
    orchestrator_approval = None
    if not owner_approval:
        from . import autopilot_approval

        orchestrator_approval = autopilot_approval.approved(
            store,
            experiment_id,
            request.approval_request_id,
            request.objective,
            db=db,
            attributed_action_id=saved["id"],
        )
    if (
        latest is None
        or latest[0] != str(request.approval_request_id)
        or not (owner_approval or orchestrator_approval)
    ):
        raise ValueError("exact owner approval or attributed orchestrator approval required")
    if db.execute(
        "SELECT 1 FROM jobs WHERE experiment_id=? AND state='submitting' "
        "AND (? IS NULL OR intent_id!=?) LIMIT 1",
        (experiment_id, existing_intent, existing_intent),
    ).fetchone():
        raise ValueError("an existing job intent has uncertain outcome")
    limits = workflow.config.slurm
    limits.limits.validate_request(request.resources)
    if request.resources.gpus_per_job:
        raise ValueError("research submission is CPU-only until GPU budget accounting is wired")
    if (
        limits.max_jobs_per_experiment is not None
        and db.execute(
            "SELECT COUNT(*) FROM jobs WHERE experiment_id=? AND (? IS NULL OR intent_id!=?)",
            (experiment_id, existing_intent, existing_intent),
        ).fetchone()[0]
        >= limits.max_jobs_per_experiment
    ):
        raise ValueError("experiment job limit reached")
    if (
        limits.max_concurrent_jobs is not None
        and db.execute(
            "SELECT COUNT(*) FROM jobs WHERE state IN ('submitting','submitted') "
            "AND (? IS NULL OR intent_id!=?)",
            (existing_intent, existing_intent),
        ).fetchone()[0]
        >= limits.max_concurrent_jobs
    ):
        raise ValueError("executor concurrent job limit reached")
