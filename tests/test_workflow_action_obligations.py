import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker.codex_client import NeedsAttention
from experimenttracker.core import action_obligations
from experimenttracker.workflow import Workflow


def obligations_for(store, action_id):
    prefix = f"agent-action:{action_id}:"
    rows = store.rows("SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix))
    return [row for row in rows if row["id"].rsplit(":", 1)[0] == f"agent-action:{action_id}"]


async def make_title_action(store, workflow, monkeypatch, eid):
    store.update(eid, phase="preparing", plan_review={"state": "awaiting_review", "revision": 1})
    workflow.agent_ready = True
    workflow.gate.decide = lambda: SimpleNamespace(admitted=True)
    monkeypatch.setattr(
        "experimenttracker.workflow.luna_title_model", lambda models: ("deterministic-demo", "low")
    )

    async def waiting(_action, _slot):
        await asyncio.Event().wait()

    monkeypatch.setattr(workflow, "dispatch_title", waiting)
    await workflow.advance_title()
    workflow.agent_pool.admit()
    task = workflow.agent_task
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=? AND phase='title'", (eid,))
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    workflow.agent_task = None
    return action


@pytest.mark.asyncio
async def test_workflow_action_creation_opens_admission_obligation(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = seed_saved_contract3(store, "A new brief")
    monkeypatch.setattr(workflow, "prompt", lambda exp, phase: f"saved {phase} prompt")
    monkeypatch.setattr(workflow.research.service, "authority", lambda *args: "quota_hold")
    await workflow.agent_stage(store.experiment(eid), "plan")
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=? AND phase='plan'", (eid,))
    rows = obligations_for(store, action["id"])
    assert [(row["owner"], row["kind"], row["wait"], row["state"]) for row in rows] == [
        ("backend", "agent_turn", "admission", "open")
    ]
    queued_events = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_action_queued'", (eid,)
    )
    assert any(
        (payload := json.loads(event["payload"])).get("action_id") == action["id"]
        and payload.get("phase") == "plan"
        for event in queued_events
    )


@pytest.mark.asyncio
async def test_title_dispatch_records_handoffs_terminal_result_and_replay(runtime, monkeypatch):
    store, _, agent, workflow = runtime
    eid = seed_saved_contract3(store, "A concise research title")
    action = await make_title_action(store, workflow, monkeypatch, eid)
    assert obligations_for(store, action["id"])[0]["wait"] == "admission"
    monkeypatch.setattr("experimenttracker.workflow.blocked", lambda store: False)
    monkeypatch.setattr(
        workflow.research.service,
        "quota",
        lambda gate, eid=None: SimpleNamespace(admitted=True),
    )
    monkeypatch.setattr(workflow.research.service, "authority", lambda *args: None)
    monkeypatch.setattr(
        workflow,
        "refresh_quota",
        lambda _slot=None: asyncio.sleep(0, result=SimpleNamespace(admitted=True)),
    )
    turns = []

    async def begin(thread_id, prompt, schema, **kwargs):
        turns.append((thread_id, prompt))
        return "title-turn"

    async def finish(thread_id, turn_id):
        return {"schema_version": 1, "title": "Saved title"}

    monkeypatch.setattr(agent, "begin", begin)
    monkeypatch.setattr(agent, "finish", finish)
    await Workflow.dispatch_title(workflow, action, workflow.agent_pool.primary)
    rows = obligations_for(store, action["id"])
    assert [(row["owner"], row["kind"], row["state"]) for row in rows] == [
        ("backend", "agent_turn", "abandoned"),
        ("backend", "reconcile", "abandoned"),
        ("agent", "agent_turn", "satisfied"),
    ]
    saved = store.one("SELECT state,thread_id,turn_id FROM agent_actions WHERE id=?", (action["id"],))
    assert (saved["state"], saved["turn_id"]) == ("completed", "title-turn")
    assert turns and saved["thread_id"] == turns[0][0]
    events_before = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    obligations_before = obligations_for(store, action["id"])
    workflow._action_state(action["id"], "completed", result={"schema_version": 1, "title": "Saved title"})
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events_before
    assert obligations_for(store, action["id"]) == obligations_before


@pytest.mark.asyncio
async def test_completion_after_await_cannot_settle_replaced_saved_turn(runtime, monkeypatch):
    store, _, agent, workflow = runtime
    eid = seed_saved_contract3(store, "Reject stale turn completion")
    action = await make_title_action(store, workflow, monkeypatch, eid)
    monkeypatch.setattr("experimenttracker.workflow.blocked", lambda store: False)
    monkeypatch.setattr(
        workflow.research.service,
        "quota",
        lambda gate, eid=None: SimpleNamespace(admitted=True),
    )
    monkeypatch.setattr(workflow.research.service, "authority", lambda *args: None)
    monkeypatch.setattr(
        workflow,
        "refresh_quota",
        lambda _slot=None: asyncio.sleep(0, result=SimpleNamespace(admitted=True)),
    )

    async def begin(*args, **kwargs):
        return "saved-turn"

    async def finish(thread_id, turn_id):
        assert turn_id == "saved-turn"
        with store.tx() as db:
            before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action["id"],)).fetchone())
            db.execute("UPDATE agent_actions SET turn_id='replacement-turn' WHERE id=?", (action["id"],))
            action_obligations.sync_action(db, action["id"], before=before)
        return {"schema_version": 1, "title": "Stale result"}

    monkeypatch.setattr(agent, "begin", begin)
    monkeypatch.setattr(agent, "finish", finish)
    with pytest.raises(NeedsAttention, match="saved action changed during reconciliation"):
        await Workflow.dispatch_title(workflow, action, workflow.agent_pool.primary)

    saved = store.one("SELECT state,turn_id FROM agent_actions WHERE id=?", (action["id"],))
    assert (saved["state"], saved["turn_id"]) == ("running", "replacement-turn")
    rows = obligations_for(store, action["id"])
    assert rows[-1]["state"] == "open" and rows[-1]["wait"] == "turn:replacement-turn"
    assert not any(
        event["kind"] == "agent_action_changed"
        for event in store.rows("SELECT kind FROM events WHERE experiment_id=?", (eid,))
    )


@pytest.mark.asyncio
async def test_exact_completed_callback_replay_keeps_event_and_obligation(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = seed_saved_contract3(store, "Replay exact completed turn")
    action = await make_title_action(store, workflow, monkeypatch, eid)
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
    eid = seed_saved_contract3(store, "Rollback a saved action")
    action = await make_title_action(store, workflow, monkeypatch, eid)
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
    eid = seed_saved_contract3(store, "Keep a completed reader")
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
