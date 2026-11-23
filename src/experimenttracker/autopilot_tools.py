"""The Autopilot lead's project tools (C8). Authority comes from the lead's saved agent action.

Each write carries an idempotent ``request_id``: a repeat with the same arguments
replays the saved receipt. The lead also gets the shared contract 4 tools
``save_checkpoint``, ``update_report``, ``ask_owner`` and ``cluster_info``; it is
never served compute tools.
"""

from typing import Literal
from uuid import UUID

from pydantic import Field

from .config import StrictModel

PURPOSES = Literal["exploration", "ablation", "control", "correctness", "essential_result", "analysis"]


class Request(StrictModel):
    request_id: UUID


class ProjectStatus(Request):
    document: str | None = Field(default=None, max_length=100)


class CreateExperiment(Request):
    brief: str = Field(min_length=1, max_length=3000)
    purpose: PURPOSES
    estimated_finish_at: float


class MessageExperiment(Request):
    experiment_id: UUID
    text: str = Field(min_length=1, max_length=1000)


class ControlExperiment(Request):
    experiment_id: UUID
    operation: Literal["defer", "resume", "abandon"]
    reason: str = Field(min_length=1, max_length=4000)


class RequestIntegration(Request):
    experiment_id: UUID
    reason: str = Field(min_length=1, max_length=2000)


class Source(StrictModel):
    kind: Literal["experiment", "output", "artifact", "report", "literature"]
    id: str = Field(min_length=1, max_length=2000)
    version: int | None = Field(default=None, ge=1)
    retrieved_at: float | None = None
    access: Literal["metadata", "abstract", "full_text", "unavailable"] | None = None
    title: str | None = Field(default=None, max_length=2000)


class UpdateDocument(Request):
    name: str = Field(max_length=100)
    base_version: int = Field(ge=0)
    text: str = Field(min_length=1, max_length=200000)
    sources: list[Source] = Field(default_factory=list, max_length=100)


class ApproveObjective(Request):
    experiment_id: UUID
    approval_request_id: UUID
    objective: str = Field(min_length=1, max_length=2000)


MODELS = {
    "project_status": ProjectStatus,
    "create_experiment": CreateExperiment,
    "message_experiment": MessageExperiment,
    "control_experiment": ControlExperiment,
    "request_integration": RequestIntegration,
    "update_document": UpdateDocument,
    "approve_experiment_objective": ApproveObjective,
}
DESCRIPTIONS = {
    "project_status": "Read the run, calendar, members, pending objectives, document versions and member report versions; optionally one document's latest text. Repeating a request_id is the same read.",
    "create_experiment": "Delegate one question to a new member experiment within the calendar, open-experiment limit and writing-phase purpose; never submit compute yourself.",
    "message_experiment": "Send bounded steering to one member experiment; it reaches the member's next turn.",
    "control_experiment": "Defer or resume a member separately from owner pauses, or abandon it by exact cancellation when the policy allows.",
    "request_integration": "Queue a member's saved branch for the integration worker; the worker owns any approval and the merge.",
    "update_document": "Publish the next version of one project document from its current base_version, with source references. Numbers need actual project evidence; missing results stay TODO.",
    "approve_experiment_objective": "Record project-orchestrator approval for a member's exact pending objective; this is not an owner answer.",
}
TOOLS = [
    {"name": name, "description": DESCRIPTIONS[name], "inputSchema": model.model_json_schema()}
    for name, model in MODELS.items()
]
