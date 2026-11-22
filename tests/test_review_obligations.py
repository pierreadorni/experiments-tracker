"""Focused SQLite-only plan-review responsibility tests."""

from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker.store import Conflict


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


@pytest.mark.usefixtures("contract_three_fixture")
def test_owner_approval_satisfies_the_generation(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    store.command(uuid4(), "plan_review", {"decision": "approve", "plan_revision": 1}, eid=eid)
    assert episode(store, eid)["state"] == "satisfied"
    assert not episode(store, eid, index=1)


@pytest.mark.usefixtures("contract_three_fixture")
def test_revision_request_abandons_and_the_next_revision_opens(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    store.command(
        uuid4(),
        "plan_review",
        {"decision": "revise", "plan_revision": 1, "feedback": "narrow the claim"},
        eid=eid,
    )
    assert episode(store, eid)["state"] == "abandoned"
    assert not episode(store, eid, revision=2)
    store.update(eid, plan_review=review(revision=2), hold_reason="plan_review")
    assert episode(store, eid, revision=2)["state"] == "open"


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


@pytest.mark.usefixtures("contract_three_fixture")
def test_stale_revision_approval_is_still_refused(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(revision=2), hold_reason="plan_review")
    with pytest.raises(Conflict, match="stale"):
        store.command(uuid4(), "plan_review", {"decision": "approve", "plan_revision": 1}, eid=eid)
    assert episode(store, eid, revision=2)["state"] == "open"


@pytest.mark.usefixtures("contract_three_fixture")
def test_live_generation_conflicting_with_terminal_history_is_diagnosed(runtime):
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(), hold_reason="plan_review")
    store.command(uuid4(), "plan_review", {"decision": "approve", "plan_revision": 1}, eid=eid)
    with pytest.raises(ValueError, match="terminal obligation history"):
        store.update(eid, plan_review=review(), hold_reason="plan_review")
    assert episode(store, eid)["state"] == "satisfied"
    assert store.experiment(eid)["plan_review"]["state"] == "approved"


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


@pytest.mark.usefixtures("contract_three_fixture")
def test_autopilot_approval_satisfies_the_generation(runtime, monkeypatch):
    from test_iteration_autopilot import action, activated, tool

    service, run = activated(runtime, monkeypatch)
    store = runtime[0]
    eid = create(store)
    store.update(eid, plan_review=review(schema_version=3), hold_reason="plan_review")
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
            (eid, run["id"], run["project_id"], 1000000),
        )
    tool(
        service,
        action(service, run),
        "review_experiment_plan",
        experiment_id=eid,
        plan_revision=1,
        scope_rationale="Synthetic software acceptance only",
        comparable_protocol="Shared synthetic recipe; no research claim",
        estimated_finish_at=1003600,
    )
    assert store.experiment(eid)["plan_review"]["approved_by"]["kind"] == "autopilot"
    assert episode(store, eid)["state"] == "satisfied"
    assert not episode(store, eid, index=1)
