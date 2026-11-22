"""Focused SQLite-only owner-request responsibility tests."""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3


def create(store):
    return seed_saved_contract3(store, "Synthetic owner request")


def request(request_id="question", **extra):
    return {
        "id": request_id,
        "action_id": "saved-action",
        "rpc_id": 1,
        "method": "item/tool/requestUserInput",
        "params": {"questions": [{"id": "dataset", "question": "Which dataset?"}]},
        "expired": False,
        **extra,
    }


def episode(store, eid, request_id="question", index=0):
    return store.one("SELECT * FROM obligations WHERE id=?", (f"request:{eid}:{request_id}:{index}",))


def test_saving_request_opens_one_owner_episode(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request())
    saved = episode(store, eid)
    assert saved["state"] == "open"
    assert (saved["owner"], saved["kind"], saved["target"]) == ("owner", "owner_answer", f"{eid}:question")
    assert (saved["wait"], saved["experiment_id"], saved["project_id"]) == (
        "request:question",
        eid,
        "test-project",
    )


def test_replay_and_expired_flag_keep_the_episode(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request())
    opened = episode(store, eid)
    store.update(eid, pending_request=request(expired=True), hold_reason="expired_agent_request")
    assert episode(store, eid) == opened
    assert store.rows("SELECT id FROM obligations WHERE id LIKE 'request:%'") == [{"id": opened["id"]}]


def test_recorded_answer_satisfies_and_clearing_it_keeps_that(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request())
    store.update(eid, pending_request=request(answer="dataset A"))
    assert episode(store, eid)["state"] == "satisfied"
    store.update(eid, pending_request=None, hold_reason=None)
    assert episode(store, eid)["state"] == "satisfied"
    assert not episode(store, eid, index=1)


def test_clear_without_answer_abandons(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request())
    store.update(eid, pending_request=None, hold_reason=None)
    assert episode(store, eid)["state"] == "abandoned"
    assert not episode(store, eid, index=1)


def test_clearing_an_untracked_request_writes_no_history(runtime):
    store = runtime[0]
    eid = create(store)
    with store.tx() as db:
        db.execute("UPDATE experiments SET pending_request=? WHERE id=?", (json.dumps(request("old")), eid))
    store.update(eid, pending_request=None)
    assert store.rows("SELECT * FROM obligations WHERE id LIKE 'request:%'") == []


def test_replacement_abandons_the_old_and_opens_the_new_request(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request("first"))
    store.update(eid, pending_request=request("second"))
    assert episode(store, eid, "first")["state"] == "abandoned"
    assert episode(store, eid, "second")["state"] == "open"


def test_live_request_conflicting_with_terminal_history_is_diagnosed(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request())
    store.update(eid, pending_request=request(answer="dataset A"))
    with pytest.raises(ValueError, match="terminal obligation history"):
        store.update(eid, pending_request=request())
    assert episode(store, eid)["state"] == "satisfied"
    assert store.experiment(eid)["pending_request"]["answer"] == "dataset A"


def test_cancellation_abandons_the_saved_request(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request())
    store.command(uuid4(), "action", {"action": "cancel"}, eid=eid)
    assert episode(store, eid)["state"] == "abandoned"


def test_delete_clears_an_unanswered_request(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request())
    store.update(eid, phase="failed")
    store.command(uuid4(), "delete", {}, eid=eid)
    assert episode(store, eid)["state"] == "abandoned"


def test_request_obligation_and_event_roll_back_together(runtime, monkeypatch):
    store = runtime[0]
    eid = create(store)
    store.update(eid, pending_request=request("first"))
    events = store.one("SELECT COUNT(*) n FROM events WHERE experiment_id=?", (eid,))["n"]
    outbox = store.one("SELECT COUNT(*) n FROM outbox")["n"]

    def fail(*args, **kwargs):
        raise RuntimeError("synthetic transaction failure")

    monkeypatch.setattr(store, "emit", fail)
    with pytest.raises(RuntimeError, match="synthetic"):
        store.update(eid, pending_request=request("second"))
    assert store.experiment(eid)["pending_request"]["id"] == "first"
    assert not episode(store, eid, "second")
    assert episode(store, eid, "first")["state"] == "open"
    assert store.one("SELECT COUNT(*) n FROM events WHERE experiment_id=?", (eid,))["n"] == events
    assert store.one("SELECT COUNT(*) n FROM outbox")["n"] == outbox


@pytest.fixture
def blocked_experiment(runtime):
    store = runtime[0]
    eid = create(store)
    identity = str(uuid4())
    now = store.experiment(eid)["updated_at"]
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,"
            "input,result,attempts,created_at,updated_at) "
            "VALUES(?,?,'prepare',1,'completed','saved-thread','saved-turn','saved input',?,1,?,?)",
            (identity, eid, json.dumps({"outcome": "blocked", "summary": "Missing input"}), now, now),
        )
    store.update(eid, phase="preparing", hold_reason="implementation_blocked", error="Missing input")
    return store, eid, identity
