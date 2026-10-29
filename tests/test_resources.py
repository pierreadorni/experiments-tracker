"""Resource selection, durable GPU intents, and cluster-only training evidence."""

import asyncio
import copy
import json
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_generic import PLAN, ImplementationAgent, approve_plan

from experimenttracker import artifacts, slurm
from experimenttracker.audit import verify_experiment
from experimenttracker.config import JobResources, ProjectConfig, ResourceLimits, SlurmConfig
from experimenttracker.models import Implementation
from experimenttracker.slurm import (
    DemoScheduler,
    ResourceUnavailable,
    SlurmScheduler,
    SubmissionUncertain,
    wrapper,
)
from experimenttracker.workflow import Workflow

GPU = JobResources(
    partition="longrun",
    account="obelix",
    cpus_per_job=8,
    memory_mb_per_job=98304,
    walltime_minutes_per_job=720,
    gpus_per_job=1,
    gpu_type="a100",
)


@pytest.mark.parametrize(
    "change",
    [
        {"partition": "longrun --array=0-9"},
        {"gpu_type": "a100;secret"},
        {"gpus_per_job": -1},
        {"cpus_per_job": True},
        {"walltime_minutes_per_job": 0},
        {"nodes": 2, "gpus_per_job": 1},
        {"array": "0-100"},
        {"job_name": "other-job"},
    ],
)
def test_unsafe_or_unmanaged_requests_are_not_scheduler_arguments(change):
    with pytest.raises(ValidationError):
        JobResources.model_validate({**GPU.model_dump(), **change})


def test_cluster_limits_replace_demo_maxima_and_optional_owner_limits_apply():
    assert SlurmConfig(**GPU.model_dump()).walltime_minutes_per_job == 720
    ResourceLimits().validate_request(GPU)
    with pytest.raises(ValueError, match="partition"):
        ResourceLimits(partitions=["shortrun"]).validate_request(GPU)
    with pytest.raises(ValueError, match="gpus_per_job"):
        ResourceLimits(gpus_per_job=0).validate_request(GPU)


async def test_gpu_preflight_and_submission_use_identical_resources_and_backend_identity(
    tmp_path, monkeypatch
):
    calls = []

    async def command(*args, **kwargs):
        calls.append((args, kwargs))
        return "" if "--test-only" in args else "123456;acacia"

    monkeypatch.setattr(slurm, "command", command)
    scheduler = SlurmScheduler(SlurmConfig())
    script = tmp_path / "managed.sh"
    script.write_text("#!/bin/sh\nexit 0\n")
    intent = str(uuid4())
    await scheduler.preflight(intent, script, tmp_path, GPU)
    assert await scheduler.submit(intent, script, tmp_path, resources=GPU) == "123456"
    dry, submit = [c[0] for c in calls]
    assert tuple(a for a in dry if a != "--test-only") == submit
    assert submit[submit.index("--gres") + 1] == "gpu:a100:1"
    assert submit[submit.index("--time") + 1] == "720"
    assert submit[submit.index("--mem") + 1] == "98304"
    assert submit[submit.index("--comment") + 1] == intent
    assert submit[submit.index("--job-name") + 1] == "et-" + intent
    assert all(c[1]["clean_slurm"] for c in calls)


async def test_preflight_failure_is_safe_but_lost_submission_remains_uncertain(tmp_path, monkeypatch):
    async def failure(*args, **kwargs):
        raise TimeoutError("connection lost")

    monkeypatch.setattr(slurm, "command", failure)
    scheduler = SlurmScheduler(SlurmConfig())
    with pytest.raises(ResourceUnavailable):
        await scheduler.preflight("intent", tmp_path / "job.sh", tmp_path, GPU)
    with pytest.raises(SubmissionUncertain):
        await scheduler.submit("intent", tmp_path / "job.sh", tmp_path, resources=GPU)


async def test_cancel_checks_the_saved_scheduler_comment(monkeypatch):
    calls = []

    async def fake_command(*args, **kwargs):
        calls.append(args)
        return "12345|someone-elses-intent\n" if args[0] == "squeue" else ""

    monkeypatch.setattr(slurm, "command", fake_command)
    scheduler = SlurmScheduler(SlurmConfig())
    with pytest.raises(ResourceUnavailable, match="no longer matches"):
        await scheduler.cancel("12345", "our-intent")
    assert all(args[0] != "scancel" for args in calls)


async def test_discovery_preserves_node_state_and_reports_missing_associations(monkeypatch):
    async def command(*args, **kwargs):
        if args[0] == "sinfo":
            return "sn5|longrun|drain|72|256000|gpu:a100:2|a100"
        if args[0] == "scontrol":
            return "PartitionName=longrun MaxTime=7-00:00:00"
        raise RuntimeError("accounting unavailable")

    monkeypatch.setattr(slurm, "command", command)
    snapshot = await SlurmScheduler(SlurmConfig()).discover()
    assert "drain" in snapshot["nodes"]
    assert snapshot["errors"] == {"associations": "accounting unavailable"}
    assert snapshot["observed_at"] > 0


class RecordedScheduler(DemoScheduler):
    """GPU scheduling is mocked. Only the tiny synthetic CPU fixture executes locally."""

    def __init__(self, directory, store):
        super().__init__(directory)
        self.store = store
        self.preflights = 0
        self.lost_response = True

    async def discover(self):
        return {
            "observed_at": 1,
            "source": "test fixture",
            "nodes": "fixture a100",
            "partitions": "fixture longrun",
            "associations": "fixture",
            "errors": {},
        }

    async def preflight(self, intent, script, root, resources):
        self.preflights += 1

    async def submit(self, intent, script, root, *, resources=None):
        saved = self.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
        assert saved["state"] == "submitting"
        assert json.loads(saved["resources"]) == resources.model_dump()
        result = await super().submit(intent, script, root)
        if self.lost_response:
            self.lost_response = False
            raise SubmissionUncertain("test fixture lost the scheduler response")
        return result


async def test_gpu_plan_persists_before_dispatch_and_restart_never_resubmits(runtime, monkeypatch):
    store, _, _, old = runtime
    plan = copy.deepcopy(PLAN)
    for stage in plan["stages"]:
        stage["resources"] = GPU.model_dump()
    monkeypatch.setattr("test_generic.PLAN", plan)
    config = old.config.model_copy(update={"mode": "live"})
    scheduler = RecordedScheduler(config.state_dir / "resource-fixture", store)
    agent = ImplementationAgent()
    workflow = Workflow(config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if scheduler.submissions:
            break
    assert scheduler.submissions == 1
    first = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert first["state"] == "submitting" and first["job_id"] is None
    assert json.loads(first["resources"])["gpu_type"] == "a100"
    await workflow.close()
    # A changed fallback configuration cannot rewrite the frozen stage request.
    config = config.model_copy(
        update={"slurm": SlurmConfig(partition="shortrun", walltime_minutes_per_job=5)}
    )
    workflow = Workflow(config, store, scheduler, agent)
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    assert store.experiment(eid)["phase"] == "complete", store.experiment(eid)
    assert scheduler.submissions == 2
    assert scheduler.preflights == 2
    assert store.one("SELECT * FROM jobs WHERE intent_id=?", (first["intent_id"],))["job_id"]
    assert all(
        json.loads(j["resources"])["walltime_minutes_per_job"] == 720
        for j in store.rows("SELECT resources FROM jobs WHERE experiment_id=?", (eid,))
    )
    await workflow.close()


async def test_unavailable_preflight_creates_no_submission_intent_and_no_duplicate_notices(
    runtime, monkeypatch
):
    store, _, _, old = runtime
    plan = copy.deepcopy(PLAN)
    for stage in plan["stages"]:
        stage["resources"] = GPU.model_dump()
    monkeypatch.setattr("test_generic.PLAN", plan)
    scheduler = RecordedScheduler(old.config.state_dir / "unavailable-fixture", store)

    async def unavailable(*args):
        raise ResourceUnavailable("requested GPU is drained")

    scheduler.preflight = unavailable
    workflow = Workflow(
        old.config.model_copy(update={"mode": "live"}), store, scheduler, ImplementationAgent()
    )
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["hold_reason"] == "resources_unavailable":
            break
    assert store.experiment(eid)["hold_reason"] == "resources_unavailable"
    for _ in range(3):
        workflow.last_preflight.clear()
        await workflow.tick(force_observe=True)
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert scheduler.submissions == 0
    assert (
        len(store.rows("SELECT * FROM inbox WHERE experiment_id=? AND text LIKE 'Slurm cannot%'", (eid,)))
        == 1
    )
    await workflow.close()


def test_shell_stages_freeze_project_sources_and_large_checkpoints_stay_local(tmp_path):
    project = tmp_path / "project"
    project.mkdir()
    (project / "train.py").write_text("print('training source')\n")
    root = tmp_path / "experiment"
    work = root / "work"
    work.mkdir(parents=True)
    (work / "train.sh").write_text(
        "#!/bin/bash\n#SBATC" + "H --array=0-999\nprintf '%s' \"$EXPERIMENT_STAGE\"\n"
    )
    plan = Implementation.model_validate(
        {
            **PLAN,
            "files": ["train.sh"],
            "project_files": ["train.py"],
            "figures": [],
            "stages": [
                {
                    "id": "fit",
                    "title": "Train",
                    "script": "train.sh",
                    "resources": GPU.model_dump(),
                    "outputs": ["results.json", "validation.json"],
                    "local_outputs": ["checkpoints/model.pth"],
                }
            ],
        }
    )
    manifest = artifacts.freeze_implementation(root, plan, "", project=project)
    artifacts.verify_project_sources(root, manifest, project)
    script = wrapper(
        root, "fit", "intent", "/usr/bin/python3", script="train.sh", project=project, resources=GPU
    )
    assert "#SBATCH" not in script.read_text()
    assert "OMP_NUM_THREADS=8" in script.read_text()
    (root / "checkpoints").mkdir()
    checkpoint = root / "checkpoints/model.pth"
    with checkpoint.open("wb") as stream:
        stream.truncate(artifacts.MAX_ARTIFACT_BYTES + 1)
    names = artifacts.seal_local_outputs(root, "fit", manifest)
    assert names == ["local-output-fit.json"]
    provenance = artifacts.read_json(root / names[0], root)
    assert provenance["outputs"][0]["size"] == artifacts.MAX_ARTIFACT_BYTES + 1
    assert artifacts.seal_local_outputs(root, "fit", manifest) == names
    checkpoint.write_bytes(b"changed")
    with pytest.raises(ValueError, match="checkpoint changed"):
        artifacts.seal_local_outputs(root, "fit", manifest)
    (project / "train.py").write_text("print('modified')\n")
    with pytest.raises(ValueError, match="source changed"):
        artifacts.verify_project_sources(root, manifest, project)


def test_plan_can_exceed_three_stages_and_owner_gpu_hour_budget_is_enforced(runtime):
    store, _, _, workflow = runtime
    stages = [
        {"id": f"fit_{i}", "title": "Fit", "outputs": [f"raw_{i}.json"], "resources": GPU.model_dump()}
        for i in range(5)
    ]
    stages[-1]["outputs"] = ["results.json", "validation.json"]
    plan = Implementation.model_validate({**PLAN, "figures": [], "stages": stages})
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    workflow.config = workflow.config.model_copy(
        update={"mode": "live", "slurm": SlurmConfig(limits=ResourceLimits(gpu_hours_per_experiment=24))}
    )
    with pytest.raises(ValueError, match="GPU-hour budget"):
        workflow.validate_resource_plan(store.experiment(eid), plan)
    workflow.config = workflow.config.model_copy(update={"slurm": SlurmConfig()})
    workflow.validate_resource_plan(store.experiment(eid), plan)


@pytest.mark.parametrize("local_output", ["checkpoints/model.pth", "checkpoints"])
async def test_shell_experiment_reuses_project_environment_and_publishes_checkpoint_identity(
    runtime, tmp_path, local_output
):
    store, scheduler, _, old = runtime
    project = tmp_path / "research"
    (project / ".venv/bin").mkdir(parents=True)
    interpreter = project / ".venv/bin/python"
    interpreter.symlink_to("/usr/bin/python3")
    library = project / "train_library.py"
    library.write_text("def measurement():\n    return 6 * 7\n")
    config = old.config.model_copy(
        update={
            "projects": [
                *old.config.projects,
                ProjectConfig(id="research", name="Research", checkout=project),
            ]
        }
    )
    plan = Implementation.model_validate(
        {
            **PLAN,
            "files": ["train.sh"],
            "python_executable": str(interpreter),
            "project_files": ["train_library.py"],
            "figures": [],
            "stages": [
                {
                    "id": "fit",
                    "title": "Synthetic shell measurement",
                    "script": "train.sh",
                    "resources": JobResources().model_dump(),
                    "outputs": ["results.json", "validation.json"],
                    "local_outputs": [local_output],
                }
            ],
        }
    )
    script = """#!/bin/bash
set -euo pipefail
cd "$EXPERIMENT_PROJECT"
"$EXPERIMENT_PYTHON" - <<'PY'
import os, json
from pathlib import Path
from train_library import measurement
r=Path(os.environ['EXPERIMENT_ROOT'])
(r/'checkpoints').mkdir()
with (r/'checkpoints/model.pth').open('wb') as f: f.truncate(11*1024*1024)
(r/'results.json').write_text(json.dumps({'schema_version':1, 'protocol':PROTOCOL, 'metrics':[{'name':'measurement','variant':'requested method','value':measurement(),'unit':''}]}))
(r/'validation.json').write_text(json.dumps({'schema_version':1,'passed':True,'checks':[{'name':'independent sum','passed':measurement()==sum([7]*6),'detail':'sum and product agree'}]}))
PY
""".replace("PROTOCOL", repr(plan.protocol))

    class ShellAgent(ImplementationAgent):
        async def begin(self, thread, prompt, schema, *, workspace=None, **kwargs):
            payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
            if payload["phase"] == "plan":
                self.calls += 1
                turn = f"shell-plan-{self.calls}"
                self.results[(thread, turn)] = {
                    "schema_version": 1,
                    "title": "Shell measurement",
                    "summary": plan.summary,
                    "protocol": plan.protocol,
                    "stages": [
                        {
                            "id": stage.id,
                            "title": stage.title,
                            "summary": "Compute and independently check the result.",
                            "resources": stage.resources.model_dump(),
                        }
                        for stage in plan.stages
                    ],
                }
                return turn
            if payload["phase"] != "prepare":
                return await super().begin(thread, prompt, schema, workspace=workspace)
            self.calls += 1
            turn = f"shell-{self.calls}"
            (workspace / "train.sh").write_text(script)
            self.results[(thread, turn)] = plan.model_dump()
            return turn

    agent = ShellAgent()
    workflow = Workflow(config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
            "project_id": "research",
        },
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    for _ in range(150):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    exp = store.experiment(eid)
    assert exp["phase"] == "complete", exp
    assert workflow.experiment_python(exp) == interpreter
    assert library.read_text() == "def measurement():\n    return 6 * 7\n"
    rows = store.rows("SELECT name,size FROM artifacts WHERE experiment_id=?", (eid,))
    assert "local-output-fit.json" in {r["name"] for r in rows}
    assert "model.pth" not in {r["name"] for r in rows}
    assert all(r["size"] <= artifacts.MAX_ARTIFACT_BYTES for r in rows)
    assert verify_experiment(store, eid, workflow.root(eid))["scientific_checks"] == "passed"
    assert scheduler.submissions == 1 and agent.calls == 3
    await workflow.close()


async def test_dependency_timeout_retains_snapshot_and_retries_without_repair(runtime, monkeypatch):
    from unittest.mock import AsyncMock

    from test_workflow import until

    store, scheduler, _, old = runtime
    workflow = Workflow(old.config, store, scheduler, ImplementationAgent())
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    await until(workflow, store, eid, lambda e: e["snapshot"] is not None)
    exp = store.experiment(eid)
    original = workflow.dependencies
    monkeypatch.setattr(workflow, "dependencies", AsyncMock(side_effect=TimeoutError()))
    outcome = await workflow.submit_prepared_stage(exp, "z_measure")
    assert outcome == {"state": "held", "reason": "runtime_check_pending"}
    held = store.experiment(eid)
    assert held["snapshot"] == exp["snapshot"]
    assert held["evidence_revision"] == exp["evidence_revision"]
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    monkeypatch.setattr(workflow, "dependencies", original)
    await workflow.submit_prepared_stage(held, "z_measure")
    assert len(store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))) == 1
    await workflow.close()
