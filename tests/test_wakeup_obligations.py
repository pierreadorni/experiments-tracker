"""Saved controller wakeups own one backend timer obligation per episode.

These use saved synthetic wakeups and stubbed diagnostics; no turn is dispatched.
"""

import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from helpers import role_step
from test_controller import create

from experimenttracker.core import wakeup_obligations


def timer(store, wakeup_id):
    return store.one("SELECT * FROM obligations WHERE id=?", (f"wakeup:{wakeup_id}:0",))


def due(store, wakeup_id):
    with store.tx() as db:
        db.execute("UPDATE agent_wakeups SET due_at=0 WHERE id=?", (wakeup_id,))


def test_schedule_opens_backend_timer_with_the_saved_due_time(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    request = uuid4()
    wake = wf.controller.schedule(eid, request, 300, "Backend compute check")
    saved = store.one("SELECT * FROM agent_wakeups WHERE id=?", (wake["id"],))
    opened = timer(store, wake["id"])
    assert opened["state"] == "open"
    assert (opened["experiment_id"], opened["project_id"]) == (
        eid,
        store.experiment(eid)["contract"]["project"]["id"],
    )
    assert (opened["kind"], opened["owner"], opened["target"]) == ("timer", "backend", wake["id"])
    assert opened["wait"] == f"wakeup:{wake['id']}"
    assert opened["detail"] == f"reason={saved['reason']}"
    assert opened["due_at"] == saved["due_at"]
    events = store.rows(
        "SELECT * FROM events WHERE experiment_id=? AND kind='agent_wakeup_scheduled'", (eid,)
    )
    assert wf.controller.schedule(eid, request, 300, "Backend compute check") == wake
    replay = store.rows(
        "SELECT * FROM events WHERE experiment_id=? AND kind='agent_wakeup_scheduled'", (eid,)
    )
    assert replay == events and timer(store, wake["id"])["created_at"] == opened["created_at"]


def test_replacing_pending_request_abandons_its_timer(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    first = wf.controller.schedule(eid, uuid4(), 60, "Initial check")
    second = wf.controller.schedule(eid, uuid4(), 60, "Updated check")
    assert store.one("SELECT state FROM agent_wakeups WHERE id=?", (first["id"],))["state"] == "superseded"
    assert timer(store, first["id"])["state"] == "abandoned"
    assert timer(store, second["id"])["state"] == "open"


async def test_queueing_the_saved_action_satisfies_its_timer_in_that_transaction(runtime, monkeypatch):
    store, _, _, wf = runtime
    eid = create(store)
    store.update(eid, phase="training")
    wake = wf.controller.schedule(eid, uuid4(), 60, "Synthetic telemetry gap")
    due(store, wake["id"])
    wf.controller.server = object()
    wf.agent_ready = True
    monkeypatch.setattr(wf.controller, "diagnostic_jobs", lambda _: [{"intent_id": "synthetic-job"}])
    monkeypatch.setattr(wf.gate, "decide", lambda: SimpleNamespace(admitted=True))

    async def no_dispatch(_action):
        return None

    monkeypatch.setattr(wf, "dispatch", no_dispatch)
    sync = wakeup_obligations.sync_wakeup

    def fail(*args):
        raise RuntimeError("injected wakeup obligation failure")

    monkeypatch.setattr(wakeup_obligations, "sync_wakeup", fail)
    try:
        with pytest.raises(RuntimeError, match="injected wakeup obligation failure"):
            await role_step(wf.controller)
        monkeypatch.setattr(wakeup_obligations, "sync_wakeup", sync)
        # The timer and the action it queues commit or roll back together.
        assert store.one("SELECT state FROM agent_wakeups WHERE id=?", (wake["id"],))["state"] == "pending"
        assert timer(store, wake["id"])["state"] == "open"
        assert not store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
        assert not store.rows("SELECT * FROM obligations WHERE id LIKE 'agent-action:%'")
        assert not store.rows("SELECT * FROM events WHERE kind='agent_wakeup_queued'")

        await role_step(wf.controller)
        action = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
        assert store.one("SELECT state FROM agent_wakeups WHERE id=?", (wake["id"],))["state"] == "queued"
        assert timer(store, wake["id"])["state"] == "satisfied"
        queued = store.one("SELECT state FROM obligations WHERE id=?", (f"agent-action:{action['id']}:0",))
        assert queued["state"] == "open"
        assert store.one("SELECT 1 FROM events WHERE kind='agent_wakeup_queued'")
        if wf.agent_task:
            await wf.agent_task
        wf.agent_task = wf.active_action = None
    finally:
        wf.controller.server = None
        await wf.close()


async def test_failed_monitor_action_abandons_its_timer(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    action_id = str(uuid4())
    wake = wf.controller.schedule(eid, uuid4(), 60, "Synthetic telemetry gap")
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,error,attempts,created_at,updated_at) "
            "VALUES(?,?,'monitor',1,'failed','prompt','turn failed',1,0,0)",
            (action_id, eid),
        )
        db.execute("UPDATE agent_wakeups SET state='queued',action_id=? WHERE id=?", (action_id, wake["id"]))
    wf.controller.server = object()
    try:
        await role_step(wf.controller)
        assert store.one("SELECT state FROM agent_wakeups WHERE id=?", (wake["id"],))["state"] == "failed"
        assert timer(store, wake["id"])["state"] == "abandoned"
        assert store.one("SELECT 1 FROM events WHERE kind='agent_wakeup_failed'")
    finally:
        wf.controller.server = None
        await wf.close()


def test_saved_wakeup_and_timer_obligation_roll_back_together(runtime):
    store = runtime[0]
    eid = create(store)
    identity = str(uuid4())
    with pytest.raises(RuntimeError, match="rollback"):
        with store.tx() as db:
            db.execute(
                "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,created_at) VALUES(?,?,?,?,0)",
                (identity, eid, json.dumps({"delay_seconds": 60, "reason": "Synthetic"}), 120.0),
            )
            wakeup_obligations.sync_wakeup(db, identity)
            raise RuntimeError("rollback")
    assert not store.one("SELECT 1 FROM agent_wakeups WHERE id=?", (identity,))
    assert timer(store, identity) is None


def test_live_pending_wakeup_conflicting_with_terminal_history_is_diagnosed(runtime):
    store = runtime[0]
    eid = create(store)
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,created_at) VALUES(?,?,'{}',120.0,0)",
            (identity, eid),
        )
        opened = wakeup_obligations.sync_wakeup(db, identity)
        db.execute("UPDATE agent_wakeups SET state='superseded' WHERE id=?", (identity,))
        abandoned = wakeup_obligations.sync_wakeup(db, identity)
        assert (opened["state"], abandoned["state"]) == ("open", "abandoned")
        db.execute("UPDATE agent_wakeups SET state='pending' WHERE id=?", (identity,))
        with pytest.raises(ValueError, match="conflicts with terminal obligation history"):
            wakeup_obligations.sync_wakeup(db, identity)
    assert timer(store, identity)["state"] == "abandoned"
