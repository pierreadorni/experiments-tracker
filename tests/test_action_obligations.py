"""Action obligations follow saved action state inside its source transaction."""

from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker import clock
from experimenttracker.codex_client import NeedsAttention
from experimenttracker.core import action_obligations, obligation_sources
from experimenttracker.store import encode


def seed_action(store, *, state="pending", phase="prepare", thread_id=None, turn_id=None, legacy=False):
    eid = seed_saved_contract3(store, "Synthetic obligation mapping")
    if legacy:
        contract = store.experiment(eid)["contract"]
        contract.pop("execution_contract", None)
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,?,?,?,'synthetic',1,1)",
            (action_id, eid, phase, state, thread_id, turn_id),
        )
        store.emit(db, eid, "agent_action_changed", {"action_id": action_id, "state": state})
    return eid, action_id


def action_row(db, action_id):
    row = db.execute("SELECT * FROM agent_actions WHERE id=?", (action_id,)).fetchone()
    return dict(row)


def sync_transition(
    store, eid, action_id, *, state=None, thread_id=None, turn_id=None, error=None, result=None
):
    with store.tx() as db:
        before = action_row(db, action_id)
        db.execute(
            "UPDATE agent_actions SET state=?,thread_id=?,turn_id=?,error=?,result=?,updated_at=? WHERE id=?",
            (
                state if state is not None else before["state"],
                thread_id if thread_id is not None else before["thread_id"],
                turn_id if turn_id is not None else before["turn_id"],
                error,
                result,
                before["updated_at"] + 1,
                action_id,
            ),
        )
        current = action_row(db, action_id)
        store.emit(db, eid, "agent_action_changed", {"action_id": action_id, "state": current["state"]})
        return action_obligations.sync_action(db, action_id, before=before)


def obligation_rows(store, action_id):
    prefix = f"agent-action:{action_id}:"
    rows = store.rows("SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix))
    return [row for row in rows if row["id"].rsplit(":", 1)[0] == f"agent-action:{action_id}"]


def test_pending_admission_metadata_replay_preserves_episode_and_timestamps(runtime, monkeypatch):
    store = runtime[0]
    eid, action_id = seed_action(store)
    monkeypatch.setattr(clock, "now", lambda: 10.0)
    with store.tx() as db:
        first = action_obligations.sync_action(db, action_id)
    assert (first["id"], first["owner"], first["wait"], first["target"]) == (
        f"agent-action:{action_id}:0",
        "backend",
        "admission",
        action_id,
    )
    assert first["project_id"] == store.experiment(eid)["contract"]["project"]["id"]

    monkeypatch.setattr(clock, "now", lambda: 20.0)
    with store.tx() as db:
        before = action_row(db, action_id)
        db.execute("UPDATE agent_actions SET error='metadata',result='metadata' WHERE id=?", (action_id,))
        replay = action_obligations.sync_action(db, action_id, before=before)
        assert action_row(db, action_id)["error"] == "metadata"
    assert replay == first
    assert obligation_rows(store, action_id) == [first]


def test_transition_refuses_stale_expected_snapshot(runtime):
    store = runtime[0]
    eid, action_id = seed_action(store)
    stale = action_row(store.db, action_id)
    sync_transition(store, eid, action_id, state="dispatching")
    with pytest.raises(NeedsAttention, match="saved action changed during reconciliation"):
        with store.tx() as db:
            action_obligations.transition(db, action_id, {"state": "running"}, expected=stale)
    assert action_row(store.db, action_id)["state"] == "dispatching"


def test_transition_and_its_obligation_roll_back_together(runtime):
    store = runtime[0]
    eid, action_id = seed_action(store)
    before = action_row(store.db, action_id)
    events = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    with pytest.raises(RuntimeError, match="injected rollback"):
        with store.tx() as db:
            action_obligations.transition(db, action_id, action_obligations.dispatch_changes("thread"))
            raise RuntimeError("injected rollback")
    assert action_row(store.db, action_id) == before
    assert obligation_rows(store, action_id) == []
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events


def test_transition_records_dispatch_attempt_then_running_turn(runtime):
    store = runtime[0]
    _, action_id = seed_action(store)
    with store.tx() as db:
        saved = action_obligations.transition(
            db, action_id, action_obligations.dispatch_changes("thread"), before=action_row(db, action_id)
        )
    assert (saved["state"], saved["thread_id"], saved["attempts"]) == ("dispatching", "thread", 1)
    assert saved == action_row(store.db, action_id)
    with store.tx() as db:
        saved = action_obligations.transition(
            db, action_id, action_obligations.turn_changes("turn"), expected=saved
        )
    assert (saved["state"], saved["turn_id"]) == ("running", "turn")
    assert [(row["owner"], row["wait"], row["state"]) for row in obligation_rows(store, action_id)] == [
        ("backend", f"reconcile:{action_id}", "abandoned"),
        ("agent", "turn:turn", "open"),
    ]


def test_workflow_action_state_closes_obligation_with_existing_event(runtime):
    store, _, _, workflow = runtime[:4]
    eid, action_id = seed_action(store)
    with store.tx() as db:
        action_obligations.sync_action(db, action_id)

    workflow._action_state(action_id, "completed", result={"ok": True})

    obligation = obligation_rows(store, action_id)[0]
    event = store.one("SELECT kind FROM events WHERE experiment_id=? ORDER BY seq DESC LIMIT 1", (eid,))
    assert obligation["state"] == "satisfied"
    assert event["kind"] == "agent_action_changed"


def test_running_uncertain_running_changes_responsibility_without_reopening_ids(runtime):
    store = runtime[0]
    eid, action_id = seed_action(store, state="running", thread_id="thread-1", turn_id="turn-1")
    with store.tx() as db:
        running = action_obligations.sync_action(db, action_id)
    uncertain = sync_transition(store, eid, action_id, state="uncertain")
    running_again = sync_transition(store, eid, action_id, state="running")
    assert [
        (row["id"], row["owner"], row["kind"], row["wait"]) for row in (running, uncertain, running_again)
    ] == [
        (f"agent-action:{action_id}:0", "agent", "agent_turn", "turn:turn-1"),
        (f"agent-action:{action_id}:1", "backend", "reconcile", f"reconcile:{action_id}"),
        (f"agent-action:{action_id}:2", "agent", "agent_turn", "turn:turn-1"),
    ]
    assert [row["state"] for row in obligation_rows(store, action_id)] == ["abandoned", "abandoned", "open"]


def test_running_without_both_exact_turn_ids_waits_for_reconciliation(runtime):
    store = runtime[0]
    _, action_id = seed_action(store, state="running", thread_id="thread-1")
    with store.tx() as db:
        obligation = action_obligations.sync_action(db, action_id)
    assert (obligation["owner"], obligation["kind"], obligation["wait"]) == (
        "backend",
        "reconcile",
        f"reconcile:{action_id}",
    )


@pytest.mark.parametrize(
    "state,thread_id,turn_id,kind,owner,wait",
    [
        ("pending", None, None, "agent_turn", "backend", "admission"),
        ("dispatching", None, None, "reconcile", "backend", "reconcile"),
        ("uncertain", None, None, "reconcile", "backend", "reconcile"),
        ("running", None, "turn", "reconcile", "backend", "reconcile"),
        ("running", "thread", "turn", "agent_turn", "agent", "turn:turn"),
    ],
)
def test_state_selects_responsibility_kind_and_exact_wait(
    runtime, state, thread_id, turn_id, kind, owner, wait
):
    store = runtime[0]
    _, action_id = seed_action(store, state=state, thread_id=thread_id, turn_id=turn_id)
    with store.tx() as db:
        row = action_obligations.sync_action(db, action_id)
    assert (row["kind"], row["owner"], row["wait"]) == (
        kind,
        owner,
        wait if wait != "reconcile" else f"reconcile:{action_id}",
    )


@pytest.mark.parametrize(
    "state,terminal", [("completed", "satisfied"), ("failed", "abandoned"), ("cancelled", "abandoned")]
)
def test_terminal_action_closes_only_existing_history(runtime, state, terminal):
    store = runtime[0]
    _, action_id = seed_action(store, state=state)
    with store.tx() as db:
        assert action_obligations.sync_action(db, action_id) is None
    assert obligation_rows(store, action_id) == []

    eid, action_id = seed_action(store)
    with store.tx() as db:
        action_obligations.sync_action(db, action_id)
    result = sync_transition(store, eid, action_id, state=state)
    assert result["state"] == terminal
    assert len(obligation_rows(store, action_id)) == 1


def test_terminal_history_requires_proven_transition_for_active_source(runtime):
    store = runtime[0]
    _, action_id = seed_action(store)
    with store.tx() as db:
        action_obligations.sync_action(db, action_id)
        obligation_sources.abandon_episode(db, "agent-action", action_id, 0)
    with pytest.raises(ValueError, match="terminal obligation history"):
        with store.tx() as db:
            action_obligations.sync_action(db, action_id)
    assert len(obligation_rows(store, action_id)) == 1

    with store.tx() as db:
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id='thread',turn_id='turn' WHERE id=?",
            (action_id,),
        )
    with pytest.raises(ValueError, match="transition proof required"):
        with store.tx() as db:
            action_obligations.sync_action(db, action_id)
    assert len(obligation_rows(store, action_id)) == 1


def test_terminal_history_advances_only_with_same_transaction_source_transition(runtime):
    store = runtime[0]
    _, action_id = seed_action(store)
    with store.tx() as db:
        old = action_obligations.sync_action(db, action_id)
        obligation_sources.abandon_episode(db, "agent-action", action_id, 0)
    new = sync_transition(store, old["experiment_id"], action_id, state="dispatching")
    assert (new["id"], new["kind"], new["owner"]) == (f"agent-action:{action_id}:1", "reconcile", "backend")


@pytest.mark.parametrize("owner,kind", [("backend", "job"), ("agent", "reconcile")])
def test_unrelated_saved_ledger_responsibility_is_diagnostic_without_effects(runtime, owner, kind):
    store = runtime[0]
    eid, action_id = seed_action(store)
    with store.tx() as db:
        saved = action_obligations.sync_action(db, action_id)
        db.execute("UPDATE obligations SET owner=?,kind=? WHERE id=?", (owner, kind, saved["id"]))
    action_before = action_row(store.db, action_id)
    events_before = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    obligation_before = obligation_rows(store, action_id)
    with pytest.raises(ValueError, match="owner/kind pair"):
        with store.tx() as db:
            action_obligations.sync_action(db, action_id)
    assert action_row(store.db, action_id) == action_before
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events_before
    assert obligation_rows(store, action_id) == obligation_before


@pytest.mark.parametrize(
    "phase,legacy,deleted,cancelled,expected",
    [
        ("prepare", True, False, False, False),
        ("discuss:report", True, False, False, True),
        ("prepare", False, True, False, False),
        ("prepare", False, False, True, False),
    ],
)
def test_pending_admission_respects_archive_and_terminal_experiment(
    runtime, phase, legacy, deleted, cancelled, expected
):
    store = runtime[0]
    eid, action_id = seed_action(store, phase=phase, legacy=legacy)
    with store.tx() as db:
        if deleted:
            db.execute("UPDATE experiments SET deleted_at=1 WHERE id=?", (eid,))
        if cancelled:
            db.execute("UPDATE experiments SET phase='cancelled' WHERE id=?", (eid,))
        result = action_obligations.sync_action(db, action_id)
    assert (result is not None) is expected
    assert len(obligation_rows(store, action_id)) == int(expected)


def test_pending_skipped_admission_abandons_existing_open_episode(runtime):
    store = runtime[0]
    eid, action_id = seed_action(store)
    with store.tx() as db:
        first = action_obligations.sync_action(db, action_id)
        db.execute("UPDATE experiments SET phase='cancelled' WHERE id=?", (eid,))
        skipped = action_obligations.sync_action(db, action_id)
    assert skipped["id"] == first["id"] and skipped["state"] == "abandoned"
    assert len(obligation_rows(store, action_id)) == 1


def test_legacy_exact_running_and_uncertain_actions_remain_trackable(runtime):
    store = runtime[0]
    _, running_id = seed_action(store, state="running", thread_id="thread", turn_id="turn", legacy=True)
    _, uncertain_id = seed_action(store, state="uncertain", legacy=True)
    with store.tx() as db:
        running = action_obligations.sync_action(db, running_id)
        uncertain = action_obligations.sync_action(db, uncertain_id)
    assert (running["owner"], running["wait"]) == ("agent", "turn:turn")
    assert (uncertain["owner"], uncertain["wait"]) == ("backend", f"reconcile:{uncertain_id}")


def test_scope_phase_and_evidence_identity_conflicts_roll_back(runtime):
    store = runtime[0]
    eid, action_id = seed_action(store)
    with store.tx() as db:
        original = action_obligations.sync_action(db, action_id)
    contract = store.experiment(eid)["contract"]
    contract["project"]["id"] = "different-project"
    with pytest.raises(ValueError, match="scope, phase, or evidence"):
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
            action_obligations.sync_action(db, action_id)
    with pytest.raises(ValueError, match="phase or evidence identity"):
        with store.tx() as db:
            before = action_row(db, action_id)
            db.execute("UPDATE agent_actions SET evidence_revision=2 WHERE id=?", (action_id,))
            action_obligations.sync_action(db, action_id, before=before)
    assert store.experiment(eid)["contract"]["project"]["id"] == original["project_id"]
    assert action_row(store.db, action_id)["evidence_revision"] == 1
    assert obligation_rows(store, action_id) == [original]


def test_source_event_and_obligation_roll_back_together(runtime):
    store = runtime[0]
    eid, action_id = seed_action(store)
    events_before = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    with pytest.raises(RuntimeError, match="injected rollback"):
        with store.tx() as db:
            before = action_row(db, action_id)
            db.execute("UPDATE agent_actions SET state='dispatching' WHERE id=?", (action_id,))
            store.emit(db, eid, "agent_action_changed", {"action_id": action_id, "state": "dispatching"})
            action_obligations.sync_action(db, action_id, before=before)
            raise RuntimeError("injected rollback")
    assert action_row(store.db, action_id)["state"] == "pending"
    assert obligation_rows(store, action_id) == []
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events_before
