"""Focused SQLite-only plan-review responsibility tests."""

from uuid import uuid4

import pytest
from helpers import seed_saved_contract3


def create(store):
    return seed_saved_contract3(store, "Synthetic plan review")


def review(revision=1, state="awaiting_review", **extra):
    return {"state": state, "scope": "plan", "revision": revision, "summary": "saved plan", **extra}


def episode(store, eid, revision=1, index=0):
    return store.one("SELECT * FROM obligations WHERE id=?", (f"review:{eid}:{revision}:{index}",))


def test_awaiting_review_opens_one_owner_episode(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    saved = episode(store, eid)
    assert saved["state"] == "open"
    assert (saved["owner"], saved["kind"], saved["target"]) == ("owner", "owner_approval", f"{eid}:1")
    assert (saved["wait"], saved["experiment_id"], saved["project_id"]) == (
        f"review:{eid}:1",
        eid,
        "test-project",
    )


def test_replay_keeps_the_episode(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    opened = episode(store, eid)
    store.update(eid, plan_review=review(summary="edited summary"))
    assert episode(store, eid) == opened
    assert store.rows("SELECT id FROM obligations WHERE id LIKE 'review:%'") == [{"id": opened["id"]}]


def test_drafting_revision_keeps_the_generation_open(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    opened = episode(store, eid)
    store.update(eid, plan_review=review(state="drafting"), hold_reason="agent_recovery_exhausted")
    assert episode(store, eid) == opened
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    assert episode(store, eid) == opened


def test_new_generation_abandons_the_replaced_one(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    store.update(eid, plan_review=review(revision=2), hold_reason="plan_review")
    assert episode(store, eid)["state"] == "abandoned"
    assert episode(store, eid, revision=2)["state"] == "open"


def test_cancellation_abandons_the_saved_review(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    store.command(uuid4(), "action", {"action": "cancel"}, eid=eid)
    assert episode(store, eid)["state"] == "abandoned"


def test_review_obligation_and_event_roll_back_together(runtime, monkeypatch):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    events = store.one("SELECT COUNT(*) n FROM events WHERE experiment_id=?", (eid,))["n"]
    outbox = store.one("SELECT COUNT(*) n FROM outbox")["n"]

    def fail(*args, **kwargs):
        raise RuntimeError("synthetic transaction failure")

    monkeypatch.setattr(store, "emit", fail)
    with pytest.raises(RuntimeError, match="synthetic"):
        store.update(eid, plan_review=review(revision=2), hold_reason="plan_review")
    assert store.experiment(eid)["plan_review"]["revision"] == 1
    assert not episode(store, eid, revision=2)
    assert episode(store, eid)["state"] == "open"
    assert store.one("SELECT COUNT(*) n FROM events WHERE experiment_id=?", (eid,))["n"] == events
    assert store.one("SELECT COUNT(*) n FROM outbox")["n"] == outbox
