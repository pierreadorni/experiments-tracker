"""Experiment-lifetime advancement ownership: creation, terminal close, cancellation."""

from uuid import uuid4

import pytest
from helpers import seed_saved_contract3
from legacy_fixture import load_legacy_completed

from experimenttracker import clock, creation_policy
from experimenttracker.cancellation import process_cancellations
from experimenttracker.core import advancement_decisions, obligations
from experimenttracker.models import ActionCommand


def create(store, brief="Synthetic advancement lifetime"):
    return store.command(uuid4(), "create", {"brief": brief, "recipe_id": "agent_experiment_v1"})[
        "experiment_id"
    ]


def saved_experiment(store, brief="Synthetic advancement lifetime"):
    eid = seed_saved_contract3(store, brief)
    with store.tx() as db:
        advancement_decisions.open_lifetime(db, eid)
    return eid


def episodes(store, experiment_id, source="advancement"):
    prefix = f"{source}:{experiment_id}:"
    return store.rows("SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix))


def save(db, eid, exp, reason=None, wait="none"):
    return advancement_decisions.set_decision(
        db, eid, phase=exp["phase"], evidence_revision=exp["evidence_revision"], reason=reason, wait=wait
    )


def cancel(store, eid):
    payload = ActionCommand(request_id=uuid4(), action="cancel").model_dump(mode="json")
    return store.command(payload["request_id"], "action", payload, eid=eid)


def test_creation_opens_exactly_one_lifetime_episode_atomically(runtime, monkeypatch):
    store = runtime[0]
    monkeypatch.setattr(creation_policy, "MODE", "legacy")

    def fail_open(*args, **kwargs):
        raise RuntimeError("synthetic obligation failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(obligations, "open", fail_open)
        with pytest.raises(RuntimeError, match="synthetic obligation failure"):
            create(store)
    assert store.rows("SELECT * FROM experiments") == []
    assert store.rows("SELECT * FROM commands") == []

    eid = create(store)
    saved = episodes(store, eid)
    assert [
        (row["id"], row["experiment_id"], row["project_id"], row["kind"], row["target"], row["owner"])
        for row in saved
    ] == [(f"advancement:{eid}:0", eid, "test-project", "reconcile", eid, "backend")]
    assert [(row["wait"], row["state"], row["detail"]) for row in saved] == [("none", "open", None)]
    create(store)
    assert len(episodes(store, eid)) == 1


def test_command_replay_and_phase_handoffs_keep_the_episode_age(runtime, monkeypatch):
    store = runtime[0]
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    monkeypatch.setattr(clock, "now", lambda: 100.0)
    request_id = uuid4()
    payload = {"brief": "Synthetic lifetime age", "recipe_id": "agent_experiment_v1"}
    eid = store.command(request_id, "create", payload)["experiment_id"]
    opened = episodes(store, eid)

    monkeypatch.setattr(clock, "now", lambda: 500.0)
    store.command(request_id, "create", payload)
    store.update(eid, phase="preparing")
    store.update(eid, error="synthetic executor error")
    assert episodes(store, eid) == opened


def test_complete_satisfies_the_lifetime_and_keeps_its_final_decision(runtime):
    store = runtime[0]
    eid = saved_experiment(store)
    exp = store.experiment(eid)
    with store.tx() as db:
        save(db, eid, exp, "quota_hold", "admission:quota")
    store.update(eid, phase="complete", hold_reason=None)
    assert [(row["id"], row["state"]) for row in episodes(store, eid)] == [
        (f"advancement:{eid}:0", "satisfied")
    ]
    with store.tx() as db:
        assert advancement_decisions.read(db, eid) == {
            "version": 1,
            "phase": exp["phase"],
            "evidence_revision": exp["evidence_revision"],
            "reason": "quota_hold",
            "wait": "admission:quota",
        }


def test_failed_abandons_and_an_approved_retry_opens_the_next_episode(runtime):
    store = runtime[0]
    eid = saved_experiment(store)
    exp = store.experiment(eid)
    with store.tx() as db:
        save(db, eid, exp)
    store.update(eid, phase="failed", hold_reason="compute_failed", error="synthetic failure")
    assert [(row["id"], row["state"]) for row in episodes(store, eid)] == [
        (f"advancement:{eid}:0", "abandoned")
    ]
    store.update(eid, phase="preparing", hold_reason="automatic_repair", error="synthetic failure")
    retried = episodes(store, eid)
    assert [(row["id"], row["state"], row["detail"] is None) for row in retried] == [
        (f"advancement:{eid}:0", "abandoned", False),
        (f"advancement:{eid}:1", "open", True),
    ]
    with store.tx() as db:
        assert advancement_decisions.read(db, eid) is None
        save(db, eid, store.experiment(eid), "agent_busy", "admission:agent_busy")
        assert advancement_decisions.read(db, eid)["reason"] == "agent_busy"


def test_cancellation_closes_advancement_and_opens_its_own_responsibility(runtime):
    store = runtime[0]
    eid = saved_experiment(store)
    cancel(store, eid)
    assert [(row["id"], row["state"]) for row in episodes(store, eid)] == [
        (f"advancement:{eid}:0", "abandoned")
    ]
    assert [
        (row["id"], row["owner"], row["kind"], row["target"], row["wait"], row["state"])
        for row in episodes(store, eid, "cancellation")
    ] == [(f"cancellation:{eid}:0", "backend", "reconcile", eid, "cancellation_pending", "open")]


async def test_cancellation_completion_satisfies_its_responsibility(runtime):
    store, _, _, wf = runtime
    eid = saved_experiment(store)
    cancel(store, eid)
    await process_cancellations(wf)
    assert store.experiment(eid)["hold_reason"] is None
    assert [(row["id"], row["state"]) for row in episodes(store, eid, "cancellation")] == [
        (f"cancellation:{eid}:0", "satisfied")
    ]
    await process_cancellations(wf)
    assert len(episodes(store, eid, "cancellation")) == 1
    await wf.close()


def test_archived_legacy_experiments_get_no_advancement(runtime):
    store, _, _, wf = runtime
    eid = load_legacy_completed(store, wf)
    store.archive_legacy_experiments()
    with store.tx() as db:
        assert advancement_decisions.read(db, eid) is None
        assert advancement_decisions.open_lifetime(db, eid) is None
    assert store.rows("SELECT * FROM obligations WHERE id LIKE 'advancement:%'") == []
