import json
from typing import Literal
from uuid import UUID

from pydantic import Field, FiniteFloat, model_validator

from .config import JobResources, StrictModel
from .execution_settings import ExecutionSettings

RECIPE_ID = "toy_compare_v1"
# Detailed results include per-seed/per-area metrics and pairwise comparisons.
# Artifact ingestion also enforces the independent 10 MiB file limit.
MAX_METRICS = 10_000
MAX_VALIDATION_CHECKS = 10_000
STAGES = (
    "prepare",
    "train_linear",
    "train_quadratic",
    "evaluate_and_plot",
    "validate",
    "interpret",
    "publish",
)


class CreateExperiment(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    brief: str = Field(min_length=10, max_length=4000)
    recipe_id: Literal["toy_compare_v1", "agent_experiment_v1"] = "agent_experiment_v1"
    project_id: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,63}$")
    workspace_mode: Literal["auto", "shared", "worktree"] = "auto"
    execution: ExecutionSettings | None = None
    reference_experiment_id: UUID | None = None
    model: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$")
    reasoning_effort: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,29}$")


class CreateProject(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    name: str = Field(min_length=1, max_length=80, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9 _.-]*$")
    mode: Literal["new", "existing"]
    folder: str | None = Field(default=None, min_length=1, max_length=1000)
    allow_edits: Literal[True]
    instructions: str = Field(default="", max_length=8000)

    @model_validator(mode="after")
    def valid_folder(self):
        if self.mode == "existing" and not self.folder:
            raise ValueError("select an existing folder")
        if self.mode == "new" and self.folder:
            raise ValueError("new folders are created in the configured projects directory")
        return self


class UpdateProjectSettings(StrictModel):
    delete: bool = False
    schema_version: Literal[1] = 1
    request_id: UUID
    instructions: str = Field(max_length=8000)
    execution: ExecutionSettings | None = None


class PlanReviewCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    decision: Literal["approve", "revise", "cancel"]
    plan_revision: int = Field(ge=1, strict=True)
    feedback: str | None = Field(default=None, max_length=4000)

    @model_validator(mode="after")
    def revision_feedback(self):
        if self.decision == "revise" and not (self.feedback or "").strip():
            raise ValueError("revision feedback is required")
        return self


class SteerCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    text: str = Field(min_length=1, max_length=50000)
    mode: Literal["later", "restart"] = "later"

    @model_validator(mode="after")
    def nonempty(self):
        if not self.text.strip():
            raise ValueError("steering text is required")
        return self


class ReserveCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    five_hour_percent: int = Field(ge=0, le=90, strict=True)
    weekly_percent: int = Field(ge=0, le=90, strict=True)


class DeleteCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID


class NoteCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    text: str = Field(min_length=1, max_length=10000)


class DiscussCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    text: str = Field(min_length=1, max_length=4000)
    model: str | None = Field(default=None, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9._-]{0,99}$")
    reasoning_effort: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,29}$")

    @model_validator(mode="after")
    def nonempty(self):
        if not self.text.strip():
            raise ValueError("message must contain text")
        return self


class ActionCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    action: Literal[
        "pause_agent",
        "resume_agent",
        "pause_new_compute",
        "resume_new_compute",
        "summarize_title",
        "retry_agent",
    ]


class AnswerCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    pending_request_id: str = Field(min_length=1, max_length=200)
    answer: str = Field(min_length=1, max_length=4000)


def request_answer(pending, answer):
    method = pending["method"]
    if method == "item/tool/requestUserInput":
        questions = pending["params"].get("questions", [])
        if not questions:
            raise ValueError("input request has no questions")
        if len(questions) == 1:
            values = {questions[0]["id"]: [answer]}
        else:
            try:
                values = json.loads(answer)
            except (ValueError, TypeError) as exc:
                raise ValueError("answer multiple questions with a JSON object keyed by question ID") from exc
            if not isinstance(values, dict) or set(values) != {q["id"] for q in questions}:
                raise ValueError("answer every question ID exactly once")
            values = {key: [value] if isinstance(value, str) else value for key, value in values.items()}
        if any(
            not isinstance(value, list)
            or not value
            or any(not isinstance(text, str) or not text.strip() for text in value)
            for value in values.values()
        ):
            raise ValueError("each question needs a non-empty string or list of strings")
        return {"answers": {key: {"answers": value} for key, value in values.items()}}
    if method not in ("item/commandExecution/requestApproval", "item/fileChange/requestApproval"):
        raise ValueError("unsupported request; no automatic approval")
    if answer not in ("accept", "decline", "cancel"):
        raise ValueError("approval answer must be accept, decline, or cancel")
    return {"decision": answer}


class Preparation(StrictModel):
    schema_version: Literal[1] = 1
    recipe_id: Literal["toy_compare_v1"] = RECIPE_ID
    snapshot_hash: str
    parameters_hash: str
    summary: str = Field(min_length=1, max_length=4000)
    scope_status: Literal["supported", "unsupported"] | None = None
    scope_reason: str | None = Field(default=None, min_length=1, max_length=4000)


class Observation(StrictModel):
    text: str = Field(min_length=1, max_length=4000)
    evidence_ids: list[str] = Field(min_length=1, max_length=10)


class Analysis(StrictModel):
    schema_version: Literal[1] = 1
    title: str = Field(min_length=1, max_length=200)
    summary: str = Field(min_length=1, max_length=4000)
    observations: list[Observation] = Field(min_length=1, max_length=10)
    limitations: list[str] = Field(min_length=1, max_length=10)
    next_ideas: list[str] = Field(max_length=5)
    scientific_outcome: Literal["supported", "unsupported", "inconclusive"]
    presentation: "ReportDocument | None" = None
    analysis_files: list[str] = Field(
        default_factory=list,
        max_length=20,
        description="New figures/data written in the writable analysis directory; safe flat filenames.",
    )


class TensorBoardDisplay(StrictModel):
    log_dir: str = Field(
        pattern=r"^tensorboard/[a-zA-Z0-9_-]+$",
        description="Stage-specific event directory relative to EXPERIMENT_ROOT (e.g. tensorboard/train).",
    )
    scalar_tag: str | None = Field(default=None, min_length=1, max_length=200)
    image_tag: str | None = Field(default=None, min_length=1, max_length=200)


class ComputeStage(StrictModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,47}$")
    title: str = Field(min_length=1, max_length=200)
    outputs: list[str] = Field(default_factory=list, max_length=100)
    resources: JobResources | None = None
    script: str | None = None
    local_outputs: list[str] = Field(default_factory=list, max_length=100)
    temporary_outputs: list[str] = Field(
        default_factory=list,
        max_length=100,
        description="Regenerable caches/intermediates under temporary/ relative to EXPERIMENT_ROOT. Not hashed or published; deleted only after the experiment's report is ready. Retained during repair.",
    )
    needs: list[str] | None = Field(default=None, max_length=256)
    tensorboard: TensorBoardDisplay | None = None
    requested: bool = True
    result_file: str | None = None
    input_output_ids: list[str] | None = Field(default=None, max_length=1000)
    expected_metrics: list[str] = Field(default_factory=list, max_length=1000)
    sample_count: int | None = Field(default=None, ge=1, strict=True)


class PlannedStage(StrictModel):
    id: str = Field(pattern=r"^[a-z][a-z0-9_]{0,47}$")
    title: str = Field(min_length=1, max_length=200)
    summary: str = Field(min_length=1, max_length=1000)
    resources: JobResources
    needs: list[str] | None = Field(default=None, max_length=256)


def stage_dependencies(stages, schema_version):
    """Return the frozen graph; version 1 keeps its historical ordered chain."""
    ids = [stage.id for stage in stages]
    if len(ids) != len(set(ids)) or set(ids) & {"plan", "prepare", "validate", "interpret", "publish"}:
        raise ValueError("stage identities must be unique and non-reserved")
    if schema_version == 1:
        if any(stage.needs is not None for stage in stages):
            raise ValueError("version 1 stages cannot declare dependencies")
        return {stage: ids[index - 1 : index] for index, stage in enumerate(ids)}
    if any(stage.needs is None for stage in stages):
        raise ValueError(f"version {schema_version} stages must declare needs, including [] for roots")
    graph = {stage.id: stage.needs for stage in stages}
    for stage, needs in graph.items():
        if len(needs) != len(set(needs)) or stage in needs or set(needs) - set(ids):
            raise ValueError(f"invalid dependencies for stage {stage}")
    visited = set()
    active = set()

    def visit(stage):
        if stage in active:
            raise ValueError("stage dependencies contain a cycle")
        if stage in visited:
            return
        active.add(stage)
        for dependency in graph[stage]:
            visit(dependency)
        active.remove(stage)
        visited.add(stage)

    if schema_version == 3:
        for identity in ids:
            visit(identity)
        return graph
    visit(ids[-1])
    if visited != set(ids):
        raise ValueError("the final result stage must depend on every other stage")
    return graph


class ResearchPlan(StrictModel):
    schema_version: Literal[1, 2, 3] = 1
    title: str = Field(min_length=1, max_length=100)
    summary: str = Field(min_length=1, max_length=4000)
    protocol: str = Field(min_length=1, max_length=8000)
    questions: list[str] = Field(default_factory=list, max_length=8)
    stages: list[PlannedStage] = Field(default_factory=list, max_length=256)

    @model_validator(mode="before")
    @classmethod
    def namespace_compute_ids(cls, value):
        # Planning has not dispatched code. Resolve cosmetic collisions with
        # executor stages before review, preserving edges and the raw turn result.
        if not isinstance(value, dict) or not isinstance(value.get("stages"), list):
            return value
        stages = value["stages"]
        occupied = {s.get("id") for s in stages if isinstance(s, dict)}
        aliases = {}
        for identity in sorted(occupied & {"plan", "prepare", "validate", "interpret", "publish"}):
            replacement = "compute_" + identity
            suffix = 1
            while replacement in occupied:
                replacement = f"compute_{identity}_{suffix}"
                suffix += 1
            aliases[identity] = replacement
            occupied.add(replacement)
        if not aliases:
            return value
        return {
            **value,
            "stages": [
                {
                    **stage,
                    "id": aliases.get(stage.get("id"), stage.get("id")),
                    **(
                        {"needs": [aliases.get(n, n) for n in stage["needs"]]}
                        if isinstance(stage.get("needs"), list)
                        else {}
                    ),
                }
                if isinstance(stage, dict)
                else stage
                for stage in stages
            ],
        }

    @model_validator(mode="after")
    def unique_stages(self):
        if self.questions:
            if self.stages or any(not q.strip() or len(q) > 2000 for q in self.questions):
                raise ValueError("clarification requires nonempty questions and no compute stages")
            return self
        if not self.stages:
            raise ValueError("a complete plan requires at least one stage")
        stage_dependencies(self.stages, self.schema_version)
        return self


class Implementation(StrictModel):
    schema_version: Literal[1, 2, 3] = 1
    title: str | None = Field(default=None, min_length=1, max_length=100)
    summary: str = Field(min_length=1, max_length=4000)
    protocol: str = Field(min_length=1, max_length=8000)
    synthetic_demo: bool
    parameters_json: str = Field(max_length=10000)
    files: list[str] = Field(min_length=1, max_length=256)
    stages: list[ComputeStage] = Field(min_length=1, max_length=256)
    figures: list[str] = Field(max_length=20)
    python_executable: str | None = None
    project_files: list[str] = Field(default_factory=list, max_length=256)
    reuse_stages: list[str] | None = Field(
        default=None,
        max_length=256,
        description="During repair, explicitly reuse checked successful stages. Null chooses unchanged stages. Never reuse a failed stage or its descendants.",
    )
    reuse_rationale: str | None = Field(default=None, max_length=4000)
    objective_change: str | None = Field(
        default=None,
        max_length=4000,
        description="Explain a material change to the approved scientific objective that needs user review. Routine code, graph and resource fixes leave this null.",
    )

    @model_validator(mode="after")
    def check_plan(self):
        import re

        parameters = json.loads(self.parameters_json)
        if not isinstance(parameters, dict):
            raise ValueError("parameters_json must encode an object")
        if any(not stage.script for stage in self.stages) and "recipe.py" not in self.files:
            raise ValueError("stages without a shell script require recipe.py")
        if any(
            stage.script and (stage.script not in self.files or not stage.script.endswith((".sh", ".sbatch")))
            for stage in self.stages
        ):
            raise ValueError("stage scripts must be declared .sh or .sbatch source files")
        if self.python_executable and (
            not self.python_executable.startswith("/") or "\n" in self.python_executable
        ):
            raise ValueError("Python executable must be an absolute path")
        stage_dependencies(self.stages, self.schema_version)
        outputs = [name for stage in self.stages for name in stage.outputs]
        log_dirs = [stage.tensorboard.log_dir for stage in self.stages if stage.tensorboard]
        if len(log_dirs) != len(set(log_dirs)):
            raise ValueError("TensorBoard event directories must be unique per stage")
        names = self.files + outputs
        if len(names) != len(set(names)):
            raise ValueError("source/output names must be distinct and bounded")
        from pathlib import PurePosixPath

        for name in self.files:
            path = PurePosixPath(name)
            if (
                str(path) != name
                or path.is_absolute()
                or not path.parts
                or any(
                    not re.fullmatch(r"[a-zA-Z0-9_][a-zA-Z0-9._-]{0,149}", part) or part in (".", "..")
                    for part in path.parts
                )
            ):
                raise ValueError("source files need safe relative package paths")
        if any(not re.fullmatch(r"[a-zA-Z0-9][a-zA-Z0-9._-]{0,149}", name) for name in outputs):
            raise ValueError("published outputs need safe flat filenames")
        if any(re.match(r"report-v[0-9]+(?:\.|-)", name) for name in names):
            raise ValueError("report version filenames are reserved for presentation")
        if any(name.startswith(("local-output-", "project-source-")) for name in names):
            raise ValueError("local-output and project-source filenames are reserved")
        local_names = [name for stage in self.stages for name in stage.local_outputs]
        temporary_names = [name for stage in self.stages for name in stage.temporary_outputs]
        for name in local_names + temporary_names + self.project_files:
            path = PurePosixPath(name)
            if (
                not path.parts
                or str(path) != name
                or path.is_absolute()
                or any(
                    not re.fullmatch(r"[a-zA-Z0-9_][a-zA-Z0-9._-]*", part) or part in (".", "..")
                    for part in path.parts
                )
            ):
                raise ValueError("local outputs and project files need safe relative paths")
        # A dedicated scratch namespace protects executor-owned paths. Also
        # reject overlap with any user-declared source or retained evidence.
        if (
            any(not name.startswith("temporary/") for name in temporary_names)
            or len(temporary_names) != len(set(temporary_names))
            or any(
                left == right or left.startswith(right + "/") or right.startswith(left + "/")
                for index, left in enumerate(temporary_names)
                for right in names + local_names + temporary_names[index + 1 :]
            )
        ):
            raise ValueError(
                "temporary output paths must be unique and separate from sources, evidence and executor paths"
            )
        if len(local_names) != len(set(local_names)) or set(local_names) & set(names):
            raise ValueError("local outputs must be unique and separate from published evidence")
        if any(
            left.startswith(right + "/") or right.startswith(left + "/")
            for index, left in enumerate(local_names)
            for right in local_names[index + 1 :]
        ):
            raise ValueError("local output paths cannot overlap")
        if len(self.project_files) != len(set(self.project_files)):
            raise ValueError("project files must be unique")
        if self.reuse_stages is not None and (
            len(self.reuse_stages) != len(set(self.reuse_stages))
            or set(self.reuse_stages) - {s.id for s in self.stages}
        ):
            raise ValueError("reuse_stages must contain unique declared stage IDs")
        if set(names) & {
            "manifest.json",
            "preparation.json",
            "analysis.json",
            "report.html",
            "requirements.txt",
        }:
            raise ValueError("reserved evidence filename")
        if self.schema_version == 3:
            if any(
                name in ("snapshot", "inputs.json", "remote-worker.py")
                or name.startswith(("snapshot/", "job-source-"))
                for name in outputs + local_names
            ):
                raise ValueError("declared outputs overlap protected attempt source/control paths")
            results = [stage for stage in self.stages if stage.result_file]
            if len(results) > 1 and "results.json" in outputs:
                raise ValueError(
                    "multiple evaluations need distinct result_file names; results.json is the aggregate"
                )
            if not results:
                raise ValueError("version 3 requires an evaluation stage with result_file")
            if any(stage.result_file not in stage.outputs for stage in results):
                raise ValueError("result_file must be a declared small output")
        if self.schema_version != 3 and not {"results.json", "validation.json"} <= set(
            self.stages[-1].outputs
        ):
            raise ValueError("final compute stage must produce results.json and validation.json")
        if len(self.figures) != len(set(self.figures)) or any(
            name not in outputs or not name.endswith((".svg", ".png", ".jpg", ".jpeg"))
            for name in self.figures
        ):
            raise ValueError("figures must be declared image outputs")
        return self


class Metric(StrictModel):
    name: str = Field(min_length=1, max_length=200)
    variant: str = Field(min_length=1, max_length=200)
    value: FiniteFloat
    unit: str = Field(max_length=100)


class ExperimentResult(StrictModel):
    schema_version: Literal[1] = 1
    protocol: str = Field(min_length=1, max_length=8000)
    metrics: list[Metric] = Field(min_length=1, max_length=MAX_METRICS)
    sample_count: int | None = Field(default=None, ge=1, strict=True)


class ValidationCheck(StrictModel):
    name: str = Field(min_length=1, max_length=200)
    passed: bool
    detail: str = Field(min_length=1, max_length=4000)


class ExperimentValidation(StrictModel):
    schema_version: Literal[1] = 1
    passed: bool
    checks: list[ValidationCheck] = Field(min_length=1, max_length=MAX_VALIDATION_CHECKS)


class MetricReference(StrictModel):
    name: str = Field(min_length=1, max_length=200)
    variant: str = Field(min_length=1, max_length=200)


class ReportCell(StrictModel):
    text: str | None = Field(default=None, max_length=200)
    metric: MetricReference | None = None
    uncertainty: MetricReference | None = None
    decimals: int = Field(default=4, ge=0, le=8)

    @model_validator(mode="after")
    def check_cell(self):
        if (self.text is None) == (self.metric is None) or (self.uncertainty and not self.metric):
            raise ValueError("a table cell needs either text or a metric reference")
        return self


class PlotPoint(StrictModel):
    x: FiniteFloat
    metric: MetricReference
    uncertainty: MetricReference | None = None


class PlotSeries(StrictModel):
    label: str = Field(min_length=1, max_length=100)
    points: list[PlotPoint] = Field(min_length=1, max_length=100)
    individual_points: list[PlotPoint] = Field(default_factory=list, max_length=100)


class CurveSeries(StrictModel):
    label: str = Field(min_length=1, max_length=100)
    match_field: str | None = Field(default=None, max_length=100)
    match_value: str | int | float | None = None
    y_field: str = Field(min_length=1, max_length=100)


class ReportBlock(StrictModel):
    kind: Literal["heading", "paragraph", "table", "figure", "metric_plot", "curve_plot"]
    text: str | None = Field(default=None, max_length=6000)
    evidence_ids: list[str] = Field(default_factory=list, max_length=10)
    caption: str | None = Field(default=None, max_length=1500)
    alt: str | None = Field(default=None, max_length=500)
    artifact_id: str | None = None
    columns: list[str] | None = Field(default=None, min_length=1, max_length=6)
    rows: list[list[ReportCell]] | None = Field(default=None, min_length=1, max_length=12)
    x_label: str | None = Field(default=None, max_length=200)
    y_label: str | None = Field(default=None, max_length=200)
    series: list[PlotSeries] | None = Field(default=None, min_length=1, max_length=6)
    error_description: str | None = Field(default=None, max_length=500)
    records_key: str | None = Field(default=None, max_length=100)
    x_field: str | None = Field(default=None, max_length=100)
    target_field: str | None = Field(default=None, max_length=100)
    curves: list[CurveSeries] | None = Field(default=None, min_length=1, max_length=6)
    average_repetitions: bool = False
    order: Literal["x", "source"] = "x"

    @model_validator(mode="after")
    def check_block(self):
        if self.kind in ("heading", "paragraph") and not (self.text and self.text.strip()):
            raise ValueError("text block needs text")
        if self.kind in ("table", "figure", "metric_plot", "curve_plot") and not self.caption:
            raise ValueError("visual block needs a caption")
        if self.kind == "table" and (
            not self.columns or not self.rows or any(len(row) != len(self.columns) for row in self.rows)
        ):
            raise ValueError("table rows must match the columns")
        if self.kind == "figure" and not (self.artifact_id and self.alt):
            raise ValueError("figure needs an artifact and alt text")
        if self.kind in ("metric_plot", "curve_plot") and not (self.x_label and self.y_label and self.alt):
            raise ValueError("plot needs axis labels and alt text")
        if self.kind == "metric_plot":
            if not self.series:
                raise ValueError("metric plot needs series")
            if any(p.uncertainty for s in self.series for p in s.points) and not self.error_description:
                raise ValueError("error bars need an explicit description")
        if self.kind == "curve_plot" and not (
            self.artifact_id and self.records_key and self.x_field and self.curves
        ):
            raise ValueError("curve plot needs a source, record fields and curves")
        return self


class ReportDocument(StrictModel):
    schema_version: Literal[1] = 1
    title: str = Field(min_length=1, max_length=200)
    standfirst: str = Field(min_length=1, max_length=2000)
    blocks: list[ReportBlock] = Field(min_length=1)


class PresentCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    document: ReportDocument


class DiscussionArtifact(StrictModel):
    """A script-produced file and its exact bytes, resolved by the executor."""

    name: str = Field(min_length=1, max_length=100, pattern=r"^[a-zA-Z0-9][a-zA-Z0-9_.-]*$")
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    evidence_ids: list[str] = Field(default_factory=list, max_length=10)


class FollowupExperiment(StrictModel):
    brief: str = Field(min_length=10, max_length=4000)


class DiscussionReply(StrictModel):
    schema_version: Literal[1] = 1
    reply: str = Field(min_length=1, max_length=8000)
    evidence_ids: list[str] = Field(default_factory=list, max_length=10)
    presentation: ReportDocument | None = None
    generated_artifacts: list[DiscussionArtifact] = Field(default_factory=list)
    new_experiment: FollowupExperiment | None = None


Analysis.model_rebuild()
