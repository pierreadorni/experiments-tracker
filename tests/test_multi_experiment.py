"""Concurrent state machines retain a single model slot and durable job limits."""

import asyncio
from uuid import uuid4

import pytest
from test_generic import ImplementationAgent, approve_plan
from test_workflow import submit, until

from experimenttracker.slurm import Observation
from experimenttracker.workflow import Workflow


@pytest.mark.parametrize("limit,submitted", [(1, 1), (2, 2), (None, 3)])
async def test_job_limit_across_experiments_and_restart(runtime, limit, submitted):
    store, scheduler, agent, workflow = runtime
    workflow.config.slurm.max_concurrent_jobs = limit
    original_observe = scheduler.observe

    async def running(job_id):
        return Observation("RUNNING", None)

    scheduler.observe = running
    experiments = [submit(store)]
    await until(workflow, store, experiments[0], lambda e: e["phase"] == "training")
    for _ in range(2):
        experiment = submit(store)
        experiments.append(experiment)
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
    for _ in range(5):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.001)
    jobs = store.rows("SELECT intent_id,job_id FROM jobs WHERE state='submitted'")
    assert len(jobs) == submitted
    assert agent.calls == 3
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
    assert scheduler.submissions == 9 and agent.calls == 6
    assert len(store.rows("SELECT * FROM inbox WHERE kind='result_ready'")) == 3
    await resumed.close()


async def test_readonly_planning_during_training_and_project_write_hold(runtime):
    store, scheduler, _, old = runtime
    workflow = Workflow(old.config, store, scheduler, ImplementationAgent())

    async def running(job_id):
        return Observation("RUNNING", None)

    scheduler.observe = running

    def create():
        return store.command(
            uuid4(),
            "create",
            {
                "brief": "Test an entirely new method and show its frontier.",
                "recipe_id": "agent_experiment_v1",
                "workspace_mode": "shared",
            },
        )["experiment_id"]

    first = create()
    await approve_plan(workflow, store, first)
    await until(workflow, store, first, lambda e: e["phase"] == "training")
    second = create()
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
    await until(workflow, store, second, lambda e: e["phase"] == "training")
    assert (workflow.root(second) / "work/recipe.py").exists()
    await workflow.close()


async def test_model_turns_serialized_and_uncertainty_blocks_new_dispatch(runtime):
    store, _, agent, workflow = runtime
    release = asyncio.Event()
    original_finish = agent.finish

    async def blocked(thread, turn):
        await release.wait()
        return await original_finish(thread, turn)

    agent.finish = blocked
    first, second = submit(store), submit(store)
    await until(workflow, store, second, lambda e: e["hold_reason"] == "agent_busy")
    for _ in range(4):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.001)
    assert agent.calls == 1
    assert len(store.rows("SELECT * FROM agent_actions WHERE state='running'")) == 1
    release.set()
    await until(workflow, store, second, lambda e: e["phase"] == "complete")
    await until(workflow, store, first, lambda e: e["phase"] == "complete")
    third = submit(store)
    saved = store.one("SELECT id FROM agent_actions WHERE experiment_id=? LIMIT 1", (first,))
    workflow._action_state(saved["id"], "uncertain", error="saved outcome unknown")
    calls = agent.calls
    await until(workflow, store, third, lambda e: e["hold_reason"] == "agent_busy")
    assert agent.calls == calls
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (third,))
    await workflow.close()


async def test_paused_oldest_experiment_does_not_block_another(runtime):
    store, scheduler, _, workflow = runtime
    first = submit(store)
    store.command(uuid4(), "action", {"action": "pause_agent"}, eid=first)
    second = submit(store)
    await until(workflow, store, second, lambda e: e["phase"] == "complete")
    assert store.experiment(first)["hold_reason"] == "agent_paused"
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (first,))
    assert scheduler.submissions == 3
    await workflow.close()
