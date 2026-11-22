"""Saved controller wakeups own one backend timer obligation per episode.

These use saved synthetic wakeups and stubbed diagnostics; no turn is dispatched.
"""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3 as create

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
