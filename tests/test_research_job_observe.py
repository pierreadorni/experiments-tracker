"""Focused contract 4 scheduler observation tests using manually saved jobs."""

import json
from types import SimpleNamespace

import pytest

from experimenttracker.research import job_observe, wakeups
from experimenttracker.store import encode


class FakeScheduler:
    def __init__(self, observed):
        self.observed = observed
        self.calls = []

    async def observe(self, job_id):
        self.calls.append(job_id)
        if isinstance(self.observed, Exception):
            raise self.observed
        return self.observed


def saved_job(store):
    eid = store.command(
        "manual-observe-create", "create", {"brief": "synthetic observer", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    exp = store.experiment(eid)
    contract = dict(exp["contract"], execution_contract=4)
    intent = "manual-observe-job"
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
        db.execute("INSERT INTO stages(experiment_id,id,updated_at) VALUES(?,'research',1)", (eid,))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_manifest) VALUES(?,?,?,1,'submitted','42','manual','{}','synthetic',1,?)",
            (intent, eid, "research", encode({"executor": "slurm"})),
        )
        from experimenttracker.core import job_obligations

        job_obligations.sync_job(db, intent)
    return store.experiment(eid), store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))


@pytest.mark.asyncio
async def test_terminal_observation_saves_job_obligation_and_wakeup_atomically(runtime):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    scheduler = FakeScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    workflow.scheduler_for = lambda *_: scheduler

    result = await job_observe.observe(workflow, exp, job)

    assert scheduler.calls == ["42"]
    assert result["state"] == "complete"
    assert result["scheduler_state"] == "COMPLETED"
    wake = store.one("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))
    assert json.loads(wake["reason"]) == {"event": wakeups.JOB_TERMINAL, "source_id": job["intent_id"]}
    with store.tx() as db:
        assert (
            db.execute(
                "SELECT state FROM obligations WHERE id LIKE ?", (f"job:{job['intent_id']}:%",)
            ).fetchone()[0]
            == "satisfied"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observed",
    [
        SimpleNamespace(state="RUNNING", exit_code=None, ended_at=None),
        SimpleNamespace(state="COMPLETED", exit_code=None, ended_at=None),
    ],
)
async def test_healthy_or_accounting_pending_job_does_not_wake(runtime, observed):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    workflow.scheduler_for = lambda *_: FakeScheduler(observed)

    assert await job_observe.observe(workflow, exp, job) is None
    assert store.rows("SELECT * FROM agent_wakeups") == []
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "submitted"


@pytest.mark.asyncio
async def test_workflow_routes_contract4_saved_jobs_before_contract3_logic(runtime, monkeypatch):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    scheduler = FakeScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    workflow.scheduler_for = lambda *_: scheduler
    monkeypatch.setattr("experimenttracker.iteration.enabled", lambda *_: pytest.fail("C3 logic reached"))

    await workflow.observe_jobs()
    await workflow.observe_jobs()

    assert scheduler.calls == ["42"]
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "complete"
    assert len(store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


@pytest.mark.asyncio
async def test_workflow_routes_healthy_contract4_without_wakeup(runtime):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    workflow.scheduler_for = lambda *_: FakeScheduler(
        SimpleNamespace(state="RUNNING", exit_code=None, ended_at=None)
    )

    await workflow.observe_jobs()

    assert store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],)) == []
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "submitted"


async def test_scheduler_uncertainty_is_not_a_terminal_retry(runtime):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    workflow.scheduler_for = lambda *_: FakeScheduler(TimeoutError("unreadable"))

    with pytest.raises(TimeoutError):
        await job_observe.observe(workflow, exp, job)
    assert store.rows("SELECT * FROM agent_wakeups") == []
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "submitted"
