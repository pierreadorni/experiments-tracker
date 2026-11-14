"""Concurrent state machines retain a single model slot and durable job limits."""

import asyncio
import json
from uuid import uuid4

import pytest
from helpers import IterationAgent, approve_plan, until

from experimenttracker.config import ProjectConfig
from experimenttracker.slurm import Observation
from experimenttracker.workflow import Workflow


def create(store, **extra):
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Exercise synthetic iteration mechanisms.", "recipe_id": "agent_experiment_v1", **extra},
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    return eid


@pytest.mark.parametrize("limit,submitted", [(1, 1), (2, 2), (None, 3)])
async def test_job_limit_across_experiments_and_restart(runtime, tmp_path, limit, submitted):
    store, scheduler, _, old = runtime
    old.config.slurm.max_concurrent_jobs = limit
    agent = IterationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    original_observe = scheduler.observe

    async def running(job_id):
        return Observation("RUNNING", None)

    scheduler.observe = running
    # The global cap needs independent projects; same-project exclusion is
    # asserted separately below without the editor lock being bypassed.
    experiments = []
    for index in range(3):
        project_id = f"cap-{index}"
        checkout = tmp_path / project_id
        checkout.mkdir()
        project = ProjectConfig(id=project_id, name=project_id, checkout=checkout, writable=False)
        old.config.projects.append(project)
        store.projects[project.id] = project
        experiments.append(create(store, project_id=project_id))
    for experiment in experiments:
        await approve_plan(workflow, store, experiment)
    for experiment in experiments:
        await until(
            workflow,
            store,
            experiment,
            lambda e, experiment=experiment: bool(
                store.one(
                    "SELECT 1 FROM stages WHERE experiment_id=? AND id='prepare' AND state='complete'",
                    (experiment,),
                )
            ),
        )
    assert agent.calls == 6

    def submitted_jobs():
        return store.rows("SELECT intent_id,job_id FROM jobs WHERE state='submitted'")

    await until(workflow, store, experiments[-1], lambda e: len(submitted_jobs()) == submitted)
    jobs = submitted_jobs()
    assert len(jobs) == submitted
    if limit == 1:
        assert store.experiment(experiments[1])["hold_reason"] == "compute_busy"
    assert workflow.status(None)["compute_policy"]["max_concurrent_jobs"] == limit
    await workflow.close()
    resumed = Workflow(workflow.config, store, scheduler, agent)
    await resumed.tick(force_observe=True)
    assert store.rows("SELECT intent_id,job_id FROM jobs WHERE state='submitted'") == jobs
    assert scheduler.submissions == submitted
    scheduler.observe = original_observe
    for experiment in experiments:
        await until(resumed, store, experiment, lambda e: e["phase"] == "complete")
    assert scheduler.submissions == 9 and agent.calls == 9
    assert len(store.rows("SELECT * FROM inbox WHERE kind='result_ready'")) == 3
    await resumed.close()


async def test_readonly_planning_during_training_and_project_write_hold(runtime):
    store, scheduler, _, old = runtime

    class PermissionAgent(IterationAgent):
        async def session(self, thread_id, workspace, **kwargs):
            self.writable = kwargs.get("writable", False)
            return await super().session(thread_id, workspace, **kwargs)

        async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
            phase = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"]
            if phase == "plan":
                assert not self.writable
            elif phase == "prepare":
                assert self.writable and workspace == self.workspace
            return await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)

    agent = PermissionAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    first = create(store, workspace_mode="shared", brief="Test an entirely new method and show its frontier.")
    original_observe = scheduler.observe

    async def running(job_id):
        row = store.one("SELECT experiment_id FROM jobs WHERE job_id=?", (job_id,))
        if row and row["experiment_id"] == first:
            return Observation("RUNNING", None)
        return await original_observe(job_id)

    scheduler.observe = running
    await approve_plan(workflow, store, first)
    await until(workflow, store, first, lambda e: e["phase"] == "training")
    second = create(
        store, workspace_mode="shared", brief="Test an entirely new method and show its frontier."
    )
    await approve_plan(workflow, store, second)
    await until(workflow, store, second, lambda e: e["hold_reason"] == "project_busy")
    assert store.experiment(first)["phase"] == "training"
    assert not (workflow.root(second) / "work/recipe.py").exists()
    assert (
        store.one("SELECT state FROM agent_actions WHERE experiment_id=? AND phase='plan'", (second,))[
            "state"
        ]
        == "completed"
    )
    assert (
        store.one("SELECT state FROM agent_actions WHERE experiment_id=? AND phase='prepare'", (second,))[
            "state"
        ]
        == "pending"
    )
    # A terminal workflow with a still-running saved job remains protected.
    store.update(first, phase="failed")
    await workflow.tick(force_observe=True)
    assert store.experiment(second)["hold_reason"] == "project_busy"
    # Confirming that saved job ended releases implementation and compute.
    with store.tx() as db:
        db.execute("UPDATE jobs SET state='failed' WHERE experiment_id=?", (first,))
    await workflow.tick(force_observe=True)
    assert store.one("SELECT state FROM jobs WHERE experiment_id=?", (first,))["state"] == "submitted"
    assert store.experiment(second)["hold_reason"] == "project_busy"
    scheduler.observe = original_observe
    await until(workflow, store, second, lambda e: e["phase"] == "training")
    assert (workflow.root(second) / "work/recipe.py").exists()
    await workflow.close()


async def test_model_turns_serialized_and_uncertainty_blocks_new_dispatch(runtime):
    store, scheduler, _, old = runtime
    agent = IterationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    release = asyncio.Event()
    original_finish = agent.finish

    async def blocked(thread, turn):
        await release.wait()
        return await original_finish(thread, turn)

    agent.finish = blocked
    first, second = create(store), create(store)
    await until(workflow, store, second, lambda e: e["hold_reason"] == "agent_busy")
    for _ in range(4):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.001)
    assert agent.calls == 1
    assert len(store.rows("SELECT * FROM agent_actions WHERE state='running'")) == 1
    release.set()
    await approve_plan(workflow, store, first)
    await approve_plan(workflow, store, second)
    await until(workflow, store, first, lambda e: e["phase"] == "complete")
    await until(workflow, store, second, lambda e: e["phase"] == "complete")
    third = create(store)
    saved = store.one("SELECT id FROM agent_actions WHERE experiment_id=? LIMIT 1", (first,))
    workflow._action_state(saved["id"], "uncertain", error="saved outcome unknown")
    calls = agent.calls
    await until(workflow, store, third, lambda e: e["hold_reason"] == "agent_busy")
    assert agent.calls == calls
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (third,))
    await workflow.close()


async def test_paused_oldest_experiment_does_not_block_another(runtime):
    store, scheduler, _, old = runtime
    agent = IterationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    first = create(store)
    store.command(uuid4(), "action", {"action": "pause_agent"}, eid=first)
    second = create(store)
    await approve_plan(workflow, store, second)
    await until(workflow, store, second, lambda e: e["phase"] == "complete")
    assert store.experiment(first)["hold_reason"] == "agent_paused"
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (first,))
    assert scheduler.submissions == 3
    await workflow.close()
