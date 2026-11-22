from uuid import uuid4

import pytest
from helpers import seed_saved_contract4

from experimenttracker.codex_client import NeedsAttention
from experimenttracker.core import action_obligations


def obligations_for(store, action_id):
    prefix = f"agent-action:{action_id}:"
    rows = store.rows("SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix))
    return [row for row in rows if row["id"].rsplit(":", 1)[0] == f"agent-action:{action_id}"]


def seed_pending_action(store, eid):
    """A saved pending research action; its transitions are driven through ``_action_state``."""
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
            "VALUES(?,?,'research',1,'pending','saved',1,1)",
            (action_id, eid),
        )
        action_obligations.sync_action(db, action_id)
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


@pytest.mark.asyncio
async def test_exact_completed_callback_replay_keeps_event_and_obligation(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = seed_saved_contract4(store, "Replay exact completed turn")
    action = seed_pending_action(store, eid)
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action["id"],)).fetchone())
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id='saved-thread',turn_id='saved-turn' WHERE id=?",
            (action["id"],),
        )
        action_obligations.sync_action(db, action["id"], before=before)
    expected = dict(store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],)))
    result = {"schema_version": 1, "title": "Same exact completion"}

    workflow._action_state(action["id"], "completed", result=result, expected=expected)
    events = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    obligations = obligations_for(store, action["id"])
    workflow._action_state(action["id"], "completed", result=result, expected=expected)
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events
    assert obligations_for(store, action["id"]) == obligations

    wrong_turn = {**expected, "turn_id": "different-turn"}
    with pytest.raises(NeedsAttention, match="saved action changed during reconciliation"):
        workflow._action_state(action["id"], "completed", result=result, expected=wrong_turn)
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events
    assert obligations_for(store, action["id"]) == obligations

    wrong_action = {**expected, "id": "another-action"}
    with pytest.raises(NeedsAttention, match="saved action changed during reconciliation"):
        workflow._action_state(action["id"], "completed", result=result, expected=wrong_action)
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events
    assert obligations_for(store, action["id"]) == obligations


@pytest.mark.asyncio
async def test_action_source_event_and_obligation_rollback_together(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = seed_saved_contract4(store, "Rollback a saved action")
    action = seed_pending_action(store, eid)
    before_events = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    before_obligations = obligations_for(store, action["id"])
    sync = action_obligations.sync_action

    def fail_after_sync(db, action_id, *, before=None):
        sync(db, action_id, before=before)
        raise RuntimeError("injected sync failure")

    monkeypatch.setattr(action_obligations, "sync_action", fail_after_sync)
    with pytest.raises(RuntimeError, match="injected sync failure"):
        workflow._action_state(action["id"], "dispatching")
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action["id"],))["state"] == "pending"
    assert obligations_for(store, action["id"]) == before_obligations
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == before_events


@pytest.mark.asyncio
async def test_exact_discussion_reconciliation_satisfies_existing_episode(runtime, monkeypatch):
    store, _, agent, workflow = runtime
    eid = seed_saved_contract4(store, "Keep a completed reader")
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,'discuss:1',1,'uncertain','reader-thread','reader-turn','saved',1,1)",
            (action_id, eid),
        )
        action_obligations.sync_action(db, action_id)

    async def read_turn(thread_id, turn_id):
        assert (thread_id, turn_id) == ("reader-thread", "reader-turn")
        return {"reply": "Historical conversation result"}

    monkeypatch.setattr(agent, "read_turn", read_turn)
    result = await workflow.reconcile_action(action_id)
    assert result["state"] == "completed"
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action_id,))["state"] == "completed"
    saved = obligations_for(store, action_id)
    assert [(row["owner"], row["kind"], row["state"]) for row in saved] == [
        ("backend", "reconcile", "satisfied")
    ]
