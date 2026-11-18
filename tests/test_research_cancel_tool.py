"""Contract-4 cancel_job saves one exact selective cancellation for the owned turn.

These tests prove the strict input model, the exact owned research-turn and Slurm-job
checks, the pause/terminal/maintenance gates inside the saving transaction, the UUID
replay, and that the adapter never calls the scheduler: the existing cancellation
reconciler proceeds on a later tick with the saved identity.
"""

import json
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_research_tools import add_action, saved_contract4
from test_tick_steps import block_maintenance

from experimenttracker import early_repair
from experimenttracker.research.cancel_tool import CancelJob, cancel_job
from experimenttracker.slurm import Observation
from experimenttracker.store import encode


def saved_job(store, eid, *, stage_id="prepare", executor=None, job_id="101", state="submitted"):
    """Record one saved Slurm intent for an experiment (no scheduler contact)."""
    intent_id = str(uuid4())
    manifest = json.dumps({"executor": executor}) if executor else None
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,"
            "resources,snapshot_hash,created_at,execution_manifest) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (intent_id, eid, stage_id, 1, state, job_id, "hash", "{}", "snapshot", 0, manifest),
        )
    return store.one("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))


def live_action(store, eid, *, phase="research", state="running"):
    identity = add_action(store, eid, phase=phase, state=state)
    return store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))


def request(intent_id, reason="wrong recipe"):
    return CancelJob(request_id=uuid4(), intent_id=intent_id, reason=reason)


def cancellations(store, eid):
    return store.experiment(eid)["contract"].get("job_cancellations", {})


def test_cancel_job_input_requires_uuids_and_a_bounded_reason():
    request_id, intent_id = uuid4(), uuid4()
    parsed = CancelJob.model_validate(
        {"request_id": str(request_id), "intent_id": str(intent_id), "reason": "stop"}
    )
    assert (parsed.request_id, parsed.intent_id, parsed.reason) == (request_id, intent_id, "stop")
    for payload in (
        {"intent_id": str(intent_id), "reason": "stop"},
        {"request_id": str(request_id), "reason": "stop"},
        {"request_id": str(request_id), "intent_id": str(intent_id)},
        {"request_id": "not-a-uuid", "intent_id": str(intent_id), "reason": "stop"},
        {"request_id": str(request_id), "intent_id": "not-a-uuid", "reason": "stop"},
        {"request_id": str(request_id), "intent_id": str(intent_id), "reason": ""},
        {"request_id": str(request_id), "intent_id": str(intent_id), "reason": "   "},
        {"request_id": str(request_id), "intent_id": str(intent_id), "reason": "r" * 2001},
        {"request_id": str(request_id), "intent_id": str(intent_id), "reason": "stop", "extra": 1},
    ):
        with pytest.raises(ValidationError):
            CancelJob.model_validate(payload)


async def test_cancel_job_saves_the_exact_owned_request_without_scheduling(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    action = live_action(store, eid)
    job = saved_job(store, eid)
    payload = request(job["intent_id"])

    def forbidden(*_args, **_kwargs):
        raise AssertionError("cancel_job must not reach the scheduler")

    monkeypatch.setattr(workflow, "scheduler_for", forbidden)
    result = cancel_job(store, store.experiment(eid), action, payload)

    assert result == {
        "request_id": str(payload.request_id),
        "intent_id": job["intent_id"],
        "state": "pending",
        "reason": "wrong recipe",
    }
    assert cancellations(store, eid) == {
        str(payload.request_id): {
            "intent_id": job["intent_id"],
            "reason": "wrong recipe",
            "state": "pending",
        }
    }
    kinds = [
        row["kind"]
        for row in store.rows(
            "SELECT kind FROM events WHERE experiment_id=? AND kind LIKE 'selective_cancellation%'",
            (eid,),
        )
    ]
    assert kinds == ["selective_cancellation_requested"]
    # Saving is the whole effect: the reconciler throttle was never touched.
    assert workflow.last_cancellation == {}


async def test_saved_cancellation_is_reconciled_later_with_the_exact_identity(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    action = live_action(store, eid)
    job = saved_job(store, eid)
    payload = request(job["intent_id"])
    cancel_job(store, store.experiment(eid), action, payload)

    class FakeScheduler:
        def __init__(self):
            self.cancelled = []

        async def reconcile(self, intent_id):
            assert intent_id == job["intent_id"]
            return job["job_id"]

        async def observe(self, identity):
            assert identity == job["job_id"]
            return Observation("RUNNING", None)

        async def cancel(self, job_id, intent_id):
            self.cancelled.append((job_id, intent_id))

    scheduler = FakeScheduler()
    monkeypatch.setattr(workflow, "scheduler_for", lambda _exp, _job: scheduler)
    await early_repair.reconcile_cancellations(workflow, eid)

    record = cancellations(store, eid)[str(payload.request_id)]
    assert record == {
        "intent_id": job["intent_id"],
        "reason": "wrong recipe",
        "state": "dispatching",
        "job_id": job["job_id"],
        "scheduler_state": "RUNNING",
        "error": None,
    }
    assert scheduler.cancelled == [(job["job_id"], job["intent_id"])]


async def test_repeating_the_request_uuid_replays_the_saved_decision(runtime):
    store, _, _, _ = runtime
    eid = saved_contract4(store)
    action = live_action(store, eid)
    job = saved_job(store, eid)
    payload = request(job["intent_id"])

    first = cancel_job(store, store.experiment(eid), action, payload)
    second = cancel_job(store, store.experiment(eid), action, payload)

    assert second == first
    assert len(cancellations(store, eid)) == 1
    with pytest.raises(ValueError, match="another decision"):
        cancel_job(
            store,
            store.experiment(eid),
            action,
            CancelJob(request_id=payload.request_id, intent_id=job["intent_id"], reason="other"),
        )


@pytest.mark.parametrize(
    "phase,state,pause_agent,contract,message",
    [
        ("plan", "running", False, 4, "active research turn"),
        ("research", "pending", False, 4, "active research turn"),
        ("research", "running", True, 4, "live research experiment"),
        ("complete", "running", False, 4, "live research experiment"),
        ("research", "running", False, 3, "execution contract 4"),
    ],
)
async def test_cancel_job_requires_the_owned_live_research_turn(
    runtime, phase, state, pause_agent, contract, message
):
    store = runtime[0]
    eid = saved_contract4(store, phase=phase, pause_agent=pause_agent)
    if contract != 4:
        with store.tx() as db:
            value = {**store.experiment(eid)["contract"], "execution_contract": contract}
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(value), eid))
    action = live_action(
        store, eid, phase=phase if phase in ("plan", "research") else "research", state=state
    )
    job = saved_job(store, eid)

    with pytest.raises(ValueError, match=message):
        cancel_job(store, store.experiment(eid), action, request(job["intent_id"]))
    assert cancellations(store, eid) == {}


async def test_cancel_job_requires_an_idle_installation(runtime):
    store = runtime[0]
    eid = saved_contract4(store)
    action = live_action(store, eid)
    job = saved_job(store, eid)
    block_maintenance(store)

    with pytest.raises(ValueError, match="idle installation"):
        cancel_job(store, store.experiment(eid), action, request(job["intent_id"]))
    assert cancellations(store, eid) == {}


async def test_cancel_job_requires_this_experiments_own_slurm_intent(runtime):
    store = runtime[0]
    eid = saved_contract4(store)
    action = live_action(store, eid)
    other = saved_contract4(store)
    foreign = saved_job(store, other)
    agent_job = saved_job(store, eid, stage_id="interpret", executor="agent")

    for intent_id in (str(uuid4()), foreign["intent_id"], agent_job["intent_id"]):
        with pytest.raises(ValueError, match="exact Slurm intent"):
            cancel_job(store, store.experiment(eid), action, request(intent_id))
    assert cancellations(store, eid) == {}
