"""Resource selection, durable GPU intents, and cluster-only training evidence."""

import copy
import json
from uuid import uuid4

import pytest
from helpers import IterationAgent, seed_saved_contract3, wait_for_experiment, wait_until
from pydantic import ValidationError
from test_generic import approve_plan

from experimenttracker import slurm
from experimenttracker.config import JobResources, ResourceLimits, SlurmConfig
from experimenttracker.models import ResearchPlan
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


class GPUAgent(IterationAgent):
    """Two GPU stages keep the helper's train/evaluate source and synthetic metric."""

    async def session(self, thread_id, workspace, *, writable=False, **kwargs):
        self.writable = writable
        return await super().session(thread_id, workspace, **kwargs)

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
        result = copy.deepcopy(self.results[(thread_id, turn)])
        if payload["phase"] in ("plan", "prepare"):
            result["stages"] = [
                {**stage, "resources": GPU.model_dump()}
                for stage in result["stages"]
                if stage["id"] in ("train", "evaluate")
            ]
            if payload["phase"] == "plan":
                assert not self.writable
            else:
                assert self.writable and workspace == self.workspace
        if payload["phase"] == "prepare":
            result["figures"] = []
            if payload.get("repair_context"):
                # A refused admission ran no stage, so no producer can be reused.
                result["reuse_stages"] = []
                result["reuse_rationale"] = "No stage ran before the refused admission; retry both stages."
                result["repair_decisions"] = [
                    {
                        "stage_id": stage["id"],
                        "decision": "retry",
                        "rationale": "Nothing ran under the refused admission; retry the same work.",
                    }
                    for stage in result["stages"]
                ]
        self.results[(thread_id, turn)] = result
        return turn


@pytest.mark.usefixtures("contract_three_engine")
async def test_gpu_plan_persists_before_dispatch_and_restart_never_resubmits(runtime):
    store, _, _, old = runtime
    config = old.config.model_copy(update={"mode": "live"})
    scheduler = RecordedScheduler(config.state_dir / "resource-fixture", store)
    agent = GPUAgent()
    workflow = Workflow(config, store, scheduler, agent)
    eid = seed_saved_contract3(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_plan(workflow, store, eid)
    assert store.experiment(eid)["plan_review"]["schema_version"] == 3
    await wait_until(
        lambda: bool(scheduler.submissions),
        ticks=lambda: workflow.tick(force_observe=True),
        describe=lambda: store.experiment(eid),
    )
    assert scheduler.submissions == 1
    first = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert first["stage_id"] == "train"
    assert first["state"] == "submitting" and first["job_id"] is None
    assert json.loads(first["resources"]) == GPU.model_dump()
    await workflow.close()
    # A changed fallback configuration cannot rewrite the frozen stage request.
    config = config.model_copy(
        update={"slurm": SlurmConfig(partition="shortrun", walltime_minutes_per_job=5)}
    )
    workflow = Workflow(config, store, scheduler, agent)
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
    assert store.experiment(eid)["phase"] == "complete", store.experiment(eid)
    assert scheduler.submissions == 2
    assert scheduler.preflights == 2
    adopted = store.one("SELECT * FROM jobs WHERE intent_id=?", (first["intent_id"],))
    assert adopted["job_id"] == f"demo-{first['intent_id']}"
    jobs = store.rows("SELECT resources FROM jobs WHERE experiment_id=?", (eid,))
    assert len(jobs) == 2
    assert all(json.loads(job["resources"]) == GPU.model_dump() for job in jobs)
    await workflow.close()


@pytest.mark.usefixtures("contract_three_engine")
async def test_unavailable_preflight_creates_no_submission_intent_and_no_duplicate_notices(runtime):
    store, _, _, old = runtime
    config = old.config.model_copy(update={"mode": "live"})
    scheduler = RecordedScheduler(config.state_dir / "unavailable-fixture", store)
    attempts = []

    async def unavailable(intent, script, root, resources):
        attempts.append(resources)
        raise ResourceUnavailable("requested GPU is drained")

    scheduler.preflight = unavailable
    agent = GPUAgent()
    workflow = Workflow(config, store, scheduler, agent)
    eid = seed_saved_contract3(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_plan(workflow, store, eid)
    await wait_for_experiment(workflow, store, eid, lambda e: e["hold_reason"] == "resources_unavailable")
    for expected in (2, 3, 4):
        workflow.last_preflight.clear()
        await wait_until(
            lambda value=expected: len(attempts) == value,
            ticks=lambda: workflow.tick(force_observe=True),
            describe=lambda: store.experiment(eid),
        )
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert not store.rows("SELECT * FROM events WHERE experiment_id=? AND kind='submission_intent'", (eid,))
    assert scheduler.submissions == 0
    assert all(resources == GPU for resources in attempts)
    assert (
        len(store.rows("SELECT * FROM inbox WHERE experiment_id=? AND text LIKE 'Slurm cannot%'", (eid,)))
        == 1
    )
    exp = store.experiment(eid)
    assert exp["phase"] == "preparing" and exp["hold_reason"] == "resources_unavailable"
    assert exp["error"] == "requested GPU is drained"
    saved_plan = exp["snapshot"]["plan"]
    assert [stage["id"] for stage in saved_plan["stages"]] == ["train", "evaluate"]
    assert saved_plan["figures"] == []
    assert saved_plan["reuse_stages"] == [] and len(saved_plan["repair_decisions"]) == 2
    repairs = len(
        store.rows("SELECT * FROM events WHERE experiment_id=? AND kind='automatic_repair_started'", (eid,))
    )
    assert (repairs, agent.calls) == (3, 5)
    await workflow.close()


def test_shell_wrapper_derives_threads_from_resources_and_launches_the_declared_script(tmp_path):
    root = tmp_path / "experiment"
    (root / "snapshot").mkdir(parents=True)
    (root / "snapshot/train.sh").write_text("#!/bin/bash\nprintf '%s' \"$EXPERIMENT_STAGE\"\n")
    project = tmp_path / "project"
    project.mkdir()
    script = wrapper(
        root, "fit", "intent", "/usr/bin/python3", script="train.sh", project=project, resources=GPU
    ).read_text()
    assert "#SBATCH" not in script
    assert "OMP_NUM_THREADS=8" in script
    assert "export EXPERIMENT_PROJECT=" + str(project) in script
    assert "export EXPERIMENT_STAGE=fit" in script
    assert script.rstrip().endswith(f"exec /bin/bash {root}/snapshot/train.sh")


async def test_plan_can_exceed_three_stages_and_owner_gpu_hour_budget_is_enforced(runtime):
    store, scheduler, _, workflow = runtime
    plan = ResearchPlan.model_validate(
        {
            "schema_version": 3,
            "title": "Five GPU fits",
            "summary": "Fit five synthetic GPU stages with an explicit dependency chain.",
            "protocol": "Synthetic mechanism check, not scientific research.",
            "stages": [
                {
                    "id": f"fit_{index}",
                    "title": f"Synthetic fit {index}",
                    "summary": f"Measure synthetic stage {index} on its declared GPU allocation.",
                    "resources": GPU.model_dump(),
                    "needs": [f"fit_{index - 1}"] if index else [],
                }
                for index in range(5)
            ],
        }
    )
    eid = seed_saved_contract3(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    workflow.config = workflow.config.model_copy(
        update={"mode": "live", "slurm": SlurmConfig(limits=ResourceLimits(gpu_hours_per_experiment=24))}
    )
    with pytest.raises(ValueError, match="GPU-hour budget"):
        workflow.validate_resource_plan(store.experiment(eid), plan)
    workflow.config = workflow.config.model_copy(update={"slurm": SlurmConfig()})
    workflow.validate_resource_plan(store.experiment(eid), plan)
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert scheduler.submissions == 0 and workflow.agent.calls == 0
