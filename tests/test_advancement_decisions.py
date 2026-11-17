"""Inert c1a advancement decision validation and replay behavior."""

from uuid import uuid4

import pytest

from experimenttracker import clock
from experimenttracker.core import advancement_decisions


def experiment(store):
    return store.command(
        uuid4(), "create", {"brief": "Synthetic decision", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]


def save(db, eid, exp, reason=None, wait="none"):
    return advancement_decisions.set_decision(
        db,
        eid,
        phase=exp["phase"],
        evidence_revision=exp["evidence_revision"],
        reason=reason,
        wait=wait,
    )


def test_source_less_pairs_are_closed_and_replay_keeps_timestamps(runtime, monkeypatch):
    store = runtime[0]
    eid = experiment(store)
    exp = store.experiment(eid)
    with pytest.raises(RuntimeError, match="active transaction"):
        advancement_decisions.read(store.db, eid)

    monkeypatch.setattr(clock, "now", lambda: 10.0)
    with store.tx() as db:
        row = save(db, eid, exp)
        assert row["id"] == f"advancement:{eid}:0"
        assert (row["owner"], row["kind"], row["wait"]) == ("backend", "reconcile", "none")
        assert advancement_decisions.read(db, eid) == {
            "version": 1,
            "phase": exp["phase"],
            "evidence_revision": exp["evidence_revision"],
            "reason": None,
            "wait": "none",
        }

    monkeypatch.setattr(clock, "now", lambda: 20.0)
    with store.tx() as db:
        replay = save(db, eid, exp)
    assert replay == row

    with store.tx() as db:
        for reason, wait in (
            ("agent_busy", "admission:agent_busy"),
            ("quota_hold", "admission:quota"),
            ("workspace_unavailable", "backend:workspace_unavailable"),
        ):
            saved = save(db, eid, exp, reason, wait)
            assert advancement_decisions.read(db, eid)["reason"] == reason
            assert saved["id"] == row["id"]
        with pytest.raises(ValueError, match="unsupported advancement reason"):
            save(db, eid, exp, "needs_attention", "source:any")
        with pytest.raises(ValueError, match="unsupported advancement reason"):
            save(db, eid, exp, "quota_hold", "admission:other")


@pytest.mark.parametrize(
    "payload",
    [
        "not-json",
        '{"version":true,"phase":"preparing","evidence_revision":1,"reason":null,"wait":"none"}',
        '{"version":1,"phase":"preparing","evidence_revision":true,"reason":null,"wait":"none"}',
        '{"version":1,"phase":"preparing","evidence_revision":1,"reason":null,"wait":"none","extra":0}',
    ],
)
def test_read_rejects_malformed_saved_payload(runtime, payload):
    store = runtime[0]
    eid = experiment(store)
    exp = store.experiment(eid)
    with store.tx() as db:
        row = save(db, eid, exp)
        db.execute("UPDATE obligations SET detail=? WHERE id=?", (payload, row["id"]))
        with pytest.raises(ValueError, match="payload|version|evidence"):
            advancement_decisions.read(db, eid)


def test_read_rejects_poisoned_scope_wait_and_generation(runtime):
    store = runtime[0]
    eid = experiment(store)
    exp = store.experiment(eid)
    with store.tx() as db:
        row = save(db, eid, exp, "agent_busy", "admission:agent_busy")

    with store.tx() as db:
        db.execute("UPDATE obligations SET wait='admission:other' WHERE id=?", (row["id"],))
        with pytest.raises(ValueError, match="wait does not match"):
            advancement_decisions.read(db, eid)
    with store.tx() as db:
        db.execute("UPDATE obligations SET wait=? WHERE id=?", ("admission:agent_busy", row["id"]))
        db.execute("UPDATE obligations SET project_id='wrong' WHERE id=?", (row["id"],))
        with pytest.raises(ValueError, match="scope"):
            advancement_decisions.read(db, eid)
    with store.tx() as db:
        db.execute("UPDATE obligations SET project_id=? WHERE id=?", (row["project_id"], row["id"]))
        db.execute("UPDATE experiments SET phase='running' WHERE id=?", (eid,))
        with pytest.raises(ValueError, match="phase and evidence"):
            advancement_decisions.read(db, eid)


def test_absent_row_read_deleted_and_terminal_guards(runtime):
    store = runtime[0]
    eid = experiment(store)
    exp = store.experiment(eid)
    with store.tx() as db:
        assert advancement_decisions.read(db, eid) is None
        db.execute("UPDATE experiments SET deleted_at=1 WHERE id=?", (eid,))
        with pytest.raises(ValueError, match="deleted experiments"):
            advancement_decisions.read(db, eid)
        with pytest.raises(ValueError, match="deleted experiments"):
            save(db, eid, exp)

    eid2 = experiment(store)
    exp2 = store.experiment(eid2)
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='complete' WHERE id=?", (eid2,))
        assert advancement_decisions.read(db, eid2) is None
        with pytest.raises(ValueError, match="terminal experiments"):
            save(db, eid2, exp2)


def test_write_rolls_back_and_terminal_history_is_not_reopened(runtime):
    store = runtime[0]
    eid = experiment(store)
    exp = store.experiment(eid)
    with pytest.raises(RuntimeError, match="rollback"):
        with store.tx() as db:
            save(db, eid, exp, "quota_hold", "admission:quota")
            raise RuntimeError("rollback")
    lifetime = store.rows("SELECT * FROM obligations WHERE id LIKE ?", (f"advancement:{eid}:%",))
    assert [(row["id"], row["state"], row["detail"]) for row in lifetime] == [
        (f"advancement:{eid}:0", "open", None)
    ]

    with store.tx() as db:
        row = save(db, eid, exp)
        db.execute("UPDATE obligations SET state='satisfied' WHERE id=?", (row["id"],))
        with pytest.raises(ValueError, match="terminal advancement lifetime"):
            save(db, eid, exp, "agent_busy", "admission:agent_busy")
