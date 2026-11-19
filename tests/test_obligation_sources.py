"""Episode identities for saved obligation sources; no runtime wiring yet."""

import pytest
from helpers import seed_saved_contract3

from experimenttracker import clock
from experimenttracker.core import obligation_sources as sources
from experimenttracker.core import obligations


def responsibility(eid, *, owner="backend", wait="admission"):
    return {
        "experiment_id": eid,
        "kind": "agent_turn",
        "target": "action-1",
        "owner": owner,
        "wait": wait,
        "detail": "saved action",
    }


def rows(store, source_type="action", saved_id="action-1"):
    source_key = f"{source_type}:{saved_id}"
    prefix = source_key + ":"
    candidates = store.rows(
        "SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix)
    )
    return [row for row in candidates if row["id"].rsplit(":", 1)[0] == source_key]


def test_episode_replay_preserves_timestamps_and_requires_transition_for_rebind(runtime, monkeypatch):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    monkeypatch.setattr(clock, "now", lambda: 10.0)
    with store.tx() as db:
        first = sources.open_episode(db, "action", "action-1", **responsibility(eid))
    assert first["id"] == "action:action-1:0"
    assert (first["created_at"], first["updated_at"]) == (10.0, 10.0)

    monkeypatch.setattr(clock, "now", lambda: 20.0)
    with store.tx() as db:
        replay = sources.open_episode(db, "action", "action-1", **responsibility(eid))
    assert replay["updated_at"] == 10.0
    with pytest.raises(ValueError, match="proven source transition"):
        with store.tx() as db:
            sources.open_episode(db, "action", "action-1", **responsibility(eid, owner="agent"))
    with store.tx() as db:
        second = sources.open_episode(
            db,
            "action",
            "action-1",
            source_transition=True,
            **responsibility(eid, owner="agent", wait="turn:turn-1"),
        )
    assert second["id"] == "action:action-1:1" and second["state"] == "open"
    assert [(r["id"], r["state"]) for r in rows(store)] == [
        ("action:action-1:0", "abandoned"),
        ("action:action-1:1", "open"),
    ]


def test_transition_validation_is_atomic_and_source_target_cannot_rebind(runtime):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    with store.tx() as db:
        first = sources.open_episode(db, "action", "action-1", **responsibility(eid))
    with pytest.raises(ValueError, match="unknown obligation owner"):
        with store.tx() as db:
            sources.open_episode(
                db,
                "action",
                "action-1",
                source_transition=True,
                **responsibility(eid, owner="invalid"),
            )
    assert rows(store)[0]["state"] == "open"
    with pytest.raises(ValueError, match="target cannot change"):
        with store.tx() as db:
            sources.open_episode(
                db,
                "action",
                "action-1",
                source_transition=True,
                **{**responsibility(eid, owner="agent"), "target": "other-action"},
            )
    assert rows(store)[0]["state"] == "open"
    assert first["id"] == "action:action-1:0"


def test_colon_saved_ids_are_distinct_from_prefix_siblings(runtime):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    parent_id = f"initial-report:{eid}"
    with store.tx() as db:
        parent = sources.open_episode(db, "publication", parent_id, **responsibility(eid, owner="backend"))
        sources.open_episode(db, "publication", parent_id + ":child", **responsibility(eid, owner="backend"))
    assert parent["id"] == f"publication:{parent_id}:0"
    assert len(rows(store, "publication", parent_id)) == 1


def test_running_uncertain_running_cycles_never_reopen_terminal_ids(runtime):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    with store.tx() as db:
        running = sources.open_episode(
            db, "action", "action-1", **responsibility(eid, owner="agent", wait="turn:turn-1")
        )
    with store.tx() as db:
        uncertain = sources.open_episode(
            db,
            "action",
            "action-1",
            source_transition=True,
            **responsibility(eid, wait="reconcile:action-1"),
        )
    with store.tx() as db:
        running_again = sources.open_episode(
            db,
            "action",
            "action-1",
            source_transition=True,
            **responsibility(eid, owner="agent", wait="turn:turn-1"),
        )
    with store.tx() as db:
        sources.abandon_episode(db, "action", "action-1", 2)
        terminal_replay = sources.open_episode(
            db,
            "action",
            "action-1",
            **responsibility(eid, owner="agent", wait="turn:turn-1"),
        )
    with store.tx() as db:
        running_third = sources.open_episode(
            db,
            "action",
            "action-1",
            source_transition=True,
            **responsibility(eid, owner="agent", wait="turn:turn-1"),
        )

    assert (running["id"], uncertain["id"], running_again["id"]) == (
        "action:action-1:0",
        "action:action-1:1",
        "action:action-1:2",
    )
    assert terminal_replay["state"] == "abandoned"
    assert running_third["id"] == "action:action-1:3"
    assert [row["state"] for row in rows(store)] == ["abandoned", "abandoned", "abandoned", "open"]


def test_terminal_helpers_are_idempotent_and_conflicting_history_is_diagnostic(runtime):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    with store.tx() as db:
        first = sources.open_episode(db, "action", "action-1", **responsibility(eid))
        assert sources.satisfy_episode(db, "action", "action-1", 0)["state"] == "satisfied"
        assert sources.abandon_episode(db, "action", "action-1", 0)["state"] == "satisfied"
    saved = rows(store)[0]
    with pytest.raises(ValueError, match="terminal obligation history"):
        with store.tx() as db:
            sources.open_episode(db, "action", "action-1", **responsibility(eid, owner="agent"))
    assert rows(store)[0] == saved
    with store.tx() as db:
        next_episode = sources.open_episode(
            db,
            "action",
            "action-1",
            source_transition=True,
            **responsibility(eid, owner="agent"),
        )
    assert next_episode["id"] == "action:action-1:1"
    assert sources.episode_id("action", "action-1", 1) == next_episode["id"]
    with pytest.raises(ValueError):
        sources.episode_id("action", "action-1", True)
    assert first["id"] == "action:action-1:0"


def test_transaction_rollback_couples_source_event_and_episode(runtime):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    before = {
        "phase": store.experiment(eid)["phase"],
        "events": store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)),
        "outbox": store.rows(
            "SELECT o.* FROM outbox o JOIN events e ON e.id=o.id WHERE e.experiment_id=?", (eid,)
        ),
        "obligations": rows(store),
    }
    with pytest.raises(RuntimeError, match="injected"):
        with store.tx() as db:
            db.execute("UPDATE experiments SET phase='training' WHERE id=?", (eid,))
            sources.open_episode(db, "action", "action-1", **responsibility(eid))
            store.emit(db, eid, "state_changed", {"phase": "training"})
            raise RuntimeError("injected rollback")
    assert store.experiment(eid)["phase"] == before["phase"]
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == before["events"]
    assert (
        store.rows("SELECT o.* FROM outbox o JOIN events e ON e.id=o.id WHERE e.experiment_id=?", (eid,))
        == before["outbox"]
    )
    assert rows(store) == before["obligations"]


def test_episode_helpers_reject_inactive_transaction_and_corrupt_history(runtime):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    with pytest.raises(RuntimeError):
        sources.open_episode(store.db, "action", "action-1", **responsibility(eid))
    assert rows(store) == []
    with store.tx() as db:
        obligations.open(db, "action:action-1:0", **responsibility(eid))
        obligations.abandon(db, "action:action-1:0")
        obligations.open(db, "action:action-1:2", **responsibility(eid))
    with pytest.raises(ValueError, match="gapped obligation episode history"):
        with store.tx() as db:
            sources.open_episode(db, "action", "action-1", **responsibility(eid))


def test_latest_episode_is_read_only_for_missing_open_and_terminal_rows(runtime):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    with store.tx() as db:
        assert sources.latest_episode(db, "action", "missing") is None
    assert rows(store, "action", "missing") == []

    with store.tx() as db:
        opened = sources.open_episode(db, "action", "action-1", **responsibility(eid))
    saved_open = rows(store)
    with store.tx() as db:
        found = sources.latest_episode(db, "action", "action-1")
    assert found == opened == saved_open[0]
    assert rows(store) == saved_open

    with store.tx() as db:
        sources.satisfy_episode(db, "action", "action-1", 0)
    saved_terminal = rows(store)
    with store.tx() as db:
        terminal = sources.latest_episode(db, "action", "action-1")
    assert terminal == saved_terminal[0] and terminal["state"] == "satisfied"
    assert rows(store) == saved_terminal


@pytest.mark.parametrize("bad_id", ["action:action-1:x", "action:action-1:3"])
def test_latest_episode_reports_malformed_or_gapped_history_without_writes(runtime, bad_id):
    store = runtime[0]
    eid = seed_saved_contract3(store, "Synthetic")
    with store.tx() as db:
        obligations.open(db, "action:action-1:0", **responsibility(eid))
        obligations.open(db, bad_id, **responsibility(eid))
    before = rows(store)
    message = "invalid" if bad_id.endswith(":x") else "gapped"
    with pytest.raises(ValueError, match=f"{message} obligation episode history"):
        with store.tx() as db:
            sources.latest_episode(db, "action", "action-1")
    assert rows(store) == before
    with pytest.raises(RuntimeError):
        sources.latest_episode(store.db, "action", "action-1")
