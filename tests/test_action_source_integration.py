"""Saved action writers update their obligation in the same transaction."""

from uuid import uuid4

import pytest

from experimenttracker.core import action_obligations


def seed_experiment(store, brief):
    return store.command(uuid4(), "create", {"brief": brief, "recipe_id": "agent_experiment_v1"})[
        "experiment_id"
    ]


def obligations(store, action_id):
    prefix = f"agent-action:{action_id}:"
    return store.rows("SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix))


def failed_action(store, eid, *, phase="prepare", error="known failure"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,error,created_at,updated_at) "
            "VALUES(?,?,?,1,'failed','saved prompt',?,1,1)",
            (identity, eid, phase, error),
        )
    return store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))


def test_continuation_obligation_event_and_replay_are_atomic(runtime, monkeypatch):
    store = runtime[0]
    eid = seed_experiment(store, "Synthetic continuation transaction")
    failed = failed_action(store, eid)
    events_before = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    sync = action_obligations.sync_action

    def fail(*args, **kwargs):
        raise RuntimeError("injected obligation failure")

    monkeypatch.setattr(action_obligations, "sync_action", fail)
    with pytest.raises(RuntimeError, match="injected obligation failure"):
        store.continue_action(failed)
    monkeypatch.setattr(action_obligations, "sync_action", sync)
    assert not store.rows("SELECT * FROM agent_actions WHERE previous_action_id=?", (failed["id"],))
    assert not store.rows(
        "SELECT * FROM obligations WHERE target IN (SELECT id FROM agent_actions WHERE previous_action_id=?)",
        (failed["id"],),
    )
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events_before

    child = store.continue_action(failed)
    created_events = store.rows(
        "SELECT * FROM events WHERE experiment_id=? AND kind='agent_continuation_queued'", (eid,)
    )
    assert len(created_events) == 1
    assert obligations(store, child["id"])[0]["wait"] == "admission"
    assert store.continue_action(failed, request_id=str(uuid4())) == child
    assert (
        store.rows("SELECT * FROM events WHERE experiment_id=? AND kind='agent_continuation_queued'", (eid,))
        == created_events
    )
    assert len(obligations(store, child["id"])) == 1


def test_session_retry_and_title_summary_write_admission_obligations_atomically(runtime, monkeypatch):
    store = runtime[0]
    eid = seed_experiment(store, "Synthetic session retry")
    failure = failed_action(store, eid, phase="plan", error="session resume rejected: expired")
    store.update(eid, phase="interpreting", hold_reason="agent_session_unavailable")
    events_before = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    request = uuid4()
    sync = action_obligations.sync_action

    def fail(*args, **kwargs):
        raise RuntimeError("injected obligation failure")

    monkeypatch.setattr(action_obligations, "sync_action", fail)
    with pytest.raises(RuntimeError, match="injected obligation failure"):
        store.command(request, "action", {"action": "retry_agent"}, eid=eid)
    monkeypatch.setattr(action_obligations, "sync_action", sync)
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (failure["id"],))["state"] == "failed"
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events_before
    assert not store.one("SELECT 1 FROM commands WHERE request_id=?", (str(request),))

    store.command(request, "action", {"action": "retry_agent"}, eid=eid)
    resumed = store.one("SELECT * FROM agent_actions WHERE id=?", (failure["id"],))
    assert resumed["state"] == "pending"
    assert obligations(store, failure["id"])[0]["wait"] == "admission"

    title_eid = seed_experiment(store, "Synthetic title summary")
    title_request = uuid4()
    store.command(title_request, "action", {"action": "summarize_title"}, eid=title_eid)
    title_action = store.one("SELECT * FROM agent_actions WHERE id=?", (str(title_request),))
    assert title_action["state"] == "pending"
    assert obligations(store, title_action["id"])[0]["wait"] == "admission"


def test_discussion_correction_rolls_back_then_admits_one_continuation(runtime, monkeypatch):
    from legacy_fixture import load_legacy_completed

    store, _, _, workflow = runtime
    eid = load_legacy_completed(store, workflow)
    message_id = store.command(uuid4(), "discuss", {"text": "Explain the result"}, eid=eid)["request_id"]
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,'running','thread','turn',?,1,1)",
            (action_id, eid, "discuss:" + message_id, "instruction\nPAYLOAD:\n{}"),
        )
        action_obligations.sync_action(db, action_id)
        db.execute("UPDATE discussions SET action_id=?,state='running' WHERE id=?", (action_id, message_id))
    workflow._action_state(action_id, "completed", result={"raw": "invalid publication"})
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
    message = store.one("SELECT * FROM discussions WHERE id=?", (message_id,))
    events = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    episodes = obligations(store, action_id)
    sync = action_obligations.sync_action

    def fail_child(db, identity, **kwargs):
        saved = sync(db, identity, **kwargs)
        if identity != action_id:
            raise RuntimeError("injected child obligation failure")
        return saved

    monkeypatch.setattr(action_obligations, "sync_action", fail_child)
    with pytest.raises(RuntimeError, match="injected child obligation failure"):
        workflow.conversation.correct_outputs(message, action, "missing figure")
    assert store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,)) == action
    assert store.one("SELECT * FROM discussions WHERE id=?", (message_id,)) == message
    assert obligations(store, action_id) == episodes
    assert not store.rows("SELECT * FROM agent_actions WHERE previous_action_id=?", (action_id,))
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events

    monkeypatch.setattr(action_obligations, "sync_action", sync)
    workflow.conversation.correct_outputs(message, action, "missing figure")
    children = store.rows("SELECT * FROM agent_actions WHERE previous_action_id=?", (action_id,))
    assert len(children) == 1 and children[0]["state"] == "pending"
    assert obligations(store, action_id) == episodes
    assert obligations(store, children[0]["id"])[0]["wait"] == "admission"
    assert (
        store.one("SELECT action_id FROM discussions WHERE id=?", (message_id,))["action_id"]
        == children[0]["id"]
    )
    assert (
        len(
            store.rows(
                "SELECT * FROM events WHERE experiment_id=? AND kind='discussion_correction_queued'", (eid,)
            )
        )
        == 1
    )
