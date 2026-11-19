"""Project-only tool schemas. Authority and action identity come from the local session."""

from typing import Literal
from uuid import UUID

from pydantic import Field

from .config import StrictModel


class Read(StrictModel):
    pass


class ExperimentRead(StrictModel):
    experiment_id: UUID


class DocumentRead(StrictModel):
    name: str = Field(max_length=100)
    version: int | None = Field(default=None, ge=1)


class Mutation(StrictModel):
    request_id: UUID
    expected_revision: int = Field(ge=0)


class BacklogItem(StrictModel):
    id: str = Field(pattern=r"^[a-zA-Z0-9_-]{1,80}$")
    question: str = Field(min_length=1, max_length=4000)
    purpose: Literal["exploration", "ablation", "control", "correctness", "essential_result", "analysis"]
    decision_if_positive: str = Field(min_length=1, max_length=2000)
    decision_if_negative: str = Field(min_length=1, max_length=2000)
    stop_condition: str = Field(min_length=1, max_length=2000)
    review_at: float
    max_attempts: int = Field(ge=1, le=100)
    priority: int = Field(default=0, ge=-100, le=100)
    protocol_document: str = "protocol"
    status: Literal["proposed", "active", "deferred", "stopped"] = "proposed"
    resource_estimate: str = Field(min_length=1, max_length=2000)
    dependencies: list[str] = Field(default_factory=list, max_length=30)


class Backlog(Mutation):
    item: BacklogItem
    item_revision: int = Field(ge=0)


class Decision(Mutation):
    rationale: str = Field(min_length=1, max_length=8000)
    outcome: Literal["continue", "narrow", "pivot", "stop", "borrow"]
    backlog_id: str | None = Field(default=None, max_length=80)
    next_review_at: float | None = None
    next_max_attempts: int | None = Field(default=None, ge=1, le=100)
    borrow_percent: float = Field(default=0, ge=0, le=30)
    dropped_work: str = Field(default="", max_length=4000)
    remaining_ablations: str = Field(default="", max_length=4000)


class Create(Mutation):
    brief: str = Field(min_length=1, max_length=3000)
    backlog_id: str = Field(max_length=80)
    scope_rationale: str = Field(min_length=1, max_length=4000)
    estimated_finish_at: float


class Review(Mutation):
    experiment_id: UUID
    plan_revision: int = Field(ge=1)
    scope_rationale: str = Field(min_length=1, max_length=4000)
    comparable_protocol: str = Field(min_length=1, max_length=4000)
    estimated_finish_at: float


class ApproveObjective(Mutation):
    experiment_id: UUID
    approval_request_id: UUID
    objective: str = Field(min_length=1, max_length=2000)


class Control(Mutation):
    experiment_id: UUID
    operation: Literal["defer", "resume", "abandon", "steer"]
    reason: str = Field(min_length=1, max_length=4000)
    stage_ids: list[str] = Field(default_factory=list, max_length=100)


class Source(StrictModel):
    kind: Literal["experiment", "output", "artifact", "report", "literature"]
    id: str = Field(min_length=1, max_length=2000)
    version: int | None = Field(default=None, ge=1)
    retrieved_at: float | None = None
    access: Literal["metadata", "abstract", "full_text", "unavailable"] | None = None
    title: str | None = Field(default=None, max_length=2000)


class Document(StrictModel):
    name: str = Field(max_length=100)
    base_version: int = Field(ge=0)
    content: str = Field(min_length=1, max_length=200000)
    sources: list[Source] = Field(default_factory=list, max_length=100)


class Publish(Mutation):
    documents: list[Document] = Field(min_length=1, max_length=10)


class Foundation(Mutation):
    state: Literal["in_progress", "blocked", "ready"]
    gaps: str = Field(min_length=1, max_length=4000)


class Schedule(Mutation):
    due_at: float
    reason: str = Field(min_length=1, max_length=2000)


class Input(Mutation):
    question: str = Field(min_length=1, max_length=4000)


class Priorities(Mutation):
    priorities: dict[str, int] = Field(max_length=100)


class Handoff(Mutation):
    ready: bool
    summary: str = Field(min_length=1, max_length=8000)
    known_issues: str = Field(min_length=1, max_length=8000)


class Compile(Mutation):
    pass


MODELS = {
    "get_project_context": Read,
    "get_experiment": ExperimentRead,
    "get_results": ExperimentRead,
    "get_document": DocumentRead,
    "create_experiment": Create,
    "review_experiment_plan": Review,
    "approve_experiment_objective": ApproveObjective,
    "control_experiment": Control,
    "update_backlog": Backlog,
    "record_decision": Decision,
    "publish_documents": Publish,
    "set_foundation": Foundation,
    "set_priorities": Priorities,
    "schedule_review": Schedule,
    "request_researcher_input": Input,
    "prepare_handoff": Handoff,
    "compile_manuscript": Compile,
}
DESCRIPTIONS = {
    "get_project_context": "Read current scope, policy, revision, phase, budget, backlog and changed inputs.",
    "get_experiment": "Read a project experiment's saved status, plan and exact output references.",
    "get_results": "Read bounded project experiment measurements, reports and direct outputs; never invent evidence.",
    "get_document": "Read a registered immutable document version. Literature must distinguish verified full text and abstract access.",
    "create_experiment": "Delegate a question to existing experiment agents within saved authority; never submit compute yourself.",
    "review_experiment_plan": "Approve a linked plan with attributed scientific judgment inside the saved envelope; questions still need human review.",
    "approve_experiment_objective": "Record project-orchestrator approval for the exact pending C4 objective; this is not an owner answer.",
    "control_experiment": "Defer/resume scoped work separately from user pauses, abandon using exact cancellation, or steer through the existing child agent.",
    "update_backlog": "Save a stable investigation question and concrete stop/review point before spending; do not rename questions to evade reviews.",
    "record_decision": "Record continue/narrow/pivot/stop or bounded borrowing, with rationale and dropped work.",
    "publish_documents": "Publish immutable bounded document versions with source references. Numbers need actual compatible research evidence; missing results stay TODO.",
    "set_foundation": "Record substantive initial review and working documents; ready requires literature, plan, protocol, claims, backlog and manuscript.",
    "set_priorities": "Reorder linked work; priority cannot enlarge budgets or override holds.",
    "schedule_review": "Save one future useful research checkpoint; do not poll unchanged jobs with recurring model turns.",
    "request_researcher_input": "Create a durable project inbox question; does not send email or external messages.",
    "prepare_handoff": "Register the latest paper and honest gaps for researcher review. Readiness is scientific judgment, not certification.",
    "compile_manuscript": "Compile registered paper.tex/references.bib with no shell escape, bounded time/output. Missing TeX remains a prerequisite.",
}
TOOLS = [
    {"name": name, "description": DESCRIPTIONS[name], "inputSchema": model.model_json_schema()}
    for name, model in MODELS.items()
]
