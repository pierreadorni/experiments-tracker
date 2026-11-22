"""Experiment-lifetime advancement ownership: creation, terminal close, cancellation."""

from uuid import uuid4

from helpers import open_saved_lifetime, seed_saved_contract3
from legacy_fixture import load_legacy_completed

from experimenttracker import clock, creation_policy
from experimenttracker.cancellation import process_cancellations
from experimenttracker.core import advancement_decisions
from experimenttracker.models import ActionCommand


def create(store, brief="Synthetic advancement lifetime"):
    return store.command(uuid4(), "create", {"brief": brief, "recipe_id": "agent_experiment_v1"})[
        "experiment_id"
    ]


def saved_experiment(store, brief="Synthetic advancement lifetime"):
    eid = seed_saved_contract3(store, brief)
    with store.tx() as db:
        open_saved_lifetime(db, eid)
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


def test_creation_opens_no_advancement_lifetime(runtime, monkeypatch):
    store = runtime[0]
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    eid = create(store)
    assert episodes(store, eid) == []


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
    load_legacy_completed(store, wf)
    store.archive_legacy_experiments()
    assert store.rows("SELECT * FROM obligations WHERE id LIKE 'advancement:%'") == []
