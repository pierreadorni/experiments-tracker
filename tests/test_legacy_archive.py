import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker import clock


def legacy(store, contract="missing", phase="training", deleted=False):
    eid = seed_saved_contract3(store, "saved run")
    with store.tx() as db:
        row = db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()
        value = json.loads(row[0])
        if contract == "missing":
            value.pop("execution_contract", None)
        else:
            value["execution_contract"] = int(contract)
        db.execute(
            "UPDATE experiments SET contract=?,phase=?,pending_request=?,deleted_at=? WHERE id=?",
            (
                json.dumps(value),
                phase,
                json.dumps({"identity": "saved-rpc"}),
                clock.now() if deleted else None,
                eid,
            ),
        )
    return eid


@pytest.mark.usefixtures("contract_three_engine")
def test_archive_sweeps_only_unfinished_non_current_contracts(runtime):
    store, *_ = runtime
    old = [legacy(store, contract) for contract in ("missing", 1, 2)]
    current = legacy(store, 3)
    terminal = [legacy(store, 2, phase) for phase in ("complete", "failed", "cancelled")]
    deleted = legacy(store, 2, deleted=True)
    store.archive_legacy_experiments()
    assert [store.experiment(eid)["hold_reason"] for eid in old] == ["legacy_contract_archived"] * 3
    assert store.experiment(current)["hold_reason"] is None
    assert all(store.experiment(eid)["hold_reason"] is None for eid in terminal + [deleted])
    assert all(store.experiment(eid)["pending_request"] == {"identity": "saved-rpc"} for eid in old)
    assert len(store.rows("SELECT * FROM inbox WHERE kind='required_decision'")) == 3


def test_archive_is_replay_safe_and_preserves_saved_identities(runtime):
    store, *_ = runtime
    eid = legacy(store, 1)
    now = clock.now()
    action = str(uuid4())
    stage = store.one("SELECT id FROM stages WHERE experiment_id=? LIMIT 1", (eid,))["id"]
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,"
            "thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,'uncertain','thread-saved','turn-saved','prompt',?,?)",
            (action, eid, stage, now, now),
        )
    store.archive_legacy_experiments()
    before = (
        store.experiment(eid)["revision"],
        store.rows("SELECT id,kind,revision FROM events WHERE experiment_id=?", (eid,)),
    )
    store.archive_legacy_experiments()
    after = (
        store.experiment(eid)["revision"],
        store.rows("SELECT id,kind,revision FROM events WHERE experiment_id=?", (eid,)),
    )
    assert after == before
    saved = store.one("SELECT state,thread_id,turn_id FROM agent_actions WHERE id=?", (action,))
    assert saved == {"state": "uncertain", "thread_id": "thread-saved", "turn_id": "turn-saved"}


def test_archive_rolls_back_hold_notice_and_events_on_emit_error(runtime, monkeypatch):
    store, *_ = runtime
    legacy(store, 2)
    original = store.emit

    def fail(db, target, kind, payload=None):
        if kind == "notification":
            raise RuntimeError("injected")
        original(db, target, kind, payload)

    monkeypatch.setattr(store, "emit", fail)
    tables = ("experiments", "stages", "agent_actions", "jobs", "events", "inbox", "outbox")
    before = {table: store.rows(f"SELECT * FROM {table}") for table in tables}
    with pytest.raises(RuntimeError, match="injected"):
        store.archive_legacy_experiments()
    assert {table: store.rows(f"SELECT * FROM {table}") for table in tables} == before
