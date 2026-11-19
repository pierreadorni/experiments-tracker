"""Inert C7 initial action queueing: one bounded first turn per contract 4 experiment."""

import json
from uuid import UUID, uuid4

import pytest
from helpers import seed_saved_contract4

from experimenttracker.core import action_obligations
from experimenttracker.research.prompt import MAX_PROMPT_BYTES, turn_prompt
from experimenttracker.research.session import first_turn
from experimenttracker.store import Store

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def make_experiment(store, *, contract=4, phase="preparing", hold_reason=None, deleted=False):
    eid = seed_saved_contract4(store, BRIEF)
    saved = store.experiment(eid)
    saved["contract"]["execution_contract"] = contract
    values = {
        "contract": json.dumps(saved["contract"]),
        "phase": phase,
        "hold_reason": hold_reason,
        "deleted_at": 1.0 if deleted else None,
    }
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,phase=?,hold_reason=?,deleted_at=? WHERE id=?",
            (values["contract"], values["phase"], values["hold_reason"], values["deleted_at"], eid),
        )
    return eid


def research_actions(store, eid):
    return store.rows(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='research' ORDER BY rowid", (eid,)
    )


def queued_events(store, eid):
    return [
        json.loads(row["payload"])
        for row in store.rows(
            "SELECT payload FROM events WHERE experiment_id=? AND kind='agent_action_queued'", (eid,)
        )
    ]


def test_first_turn_saves_the_exact_bounded_prompt_and_uuid(runtime):
    store, _, _, _ = runtime
    eid = make_experiment(store)

    action = first_turn(store, eid)

    assert action is not None
    assert action["experiment_id"] == eid
    assert action["phase"] == "research"
    assert action["state"] == "pending"
    assert action["attempts"] == 0
    assert action["thread_id"] is None and action["turn_id"] is None
    assert action["evidence_revision"] == store.experiment(eid)["evidence_revision"]
    assert UUID(action["id"])
    assert f"ACTION {action['id']}\n" in action["input"]
    assert action["input"] == turn_prompt(brief=BRIEF, action_id=action["id"])
    assert len(action["input"].encode()) < MAX_PROMPT_BYTES
    assert queued_events(store, eid) == [{"phase": "research", "action_id": action["id"]}]


def test_first_turn_syncs_the_saved_action_obligation_in_the_same_transaction(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    calls, real = [], action_obligations.sync_action

    def spy(db, action_id, *, before=None):
        calls.append(action_id)
        return real(db, action_id, before=before)

    monkeypatch.setattr(action_obligations, "sync_action", spy)
    action = first_turn(store, eid)

    assert calls == [action["id"]]
    assert store.one("SELECT state,owner,kind,wait FROM obligations WHERE target=?", (action["id"],)) == {
        "state": "open",
        "owner": "backend",
        "kind": "agent_turn",
        "wait": "admission",
    }


def test_first_turn_replays_and_reopened_store_never_queues_another(runtime):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    action = first_turn(store, eid)

    assert first_turn(store, eid) is None
    assert [row["id"] for row in research_actions(store, eid)] == [action["id"]]
    assert len(queued_events(store, eid)) == 1

    reopened = Store(store.path)
    try:
        assert first_turn(reopened, eid) is None
        assert [row["id"] for row in research_actions(reopened, eid)] == [action["id"]]
    finally:
        reopened.close()


@pytest.mark.parametrize("state", ["completed", "failed", "cancelled", "superseded", "uncertain"])
def test_a_settled_failed_or_uncertain_first_turn_is_never_replaced(runtime, state):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    action = first_turn(store, eid)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET state=? WHERE id=?", (state, action["id"]))

    assert first_turn(store, eid) is None
    assert [row["id"] for row in research_actions(store, eid)] == [action["id"]]


def test_held_experiment_queues_once_without_dispatching(runtime):
    store, _, _, _ = runtime
    eid = make_experiment(store, hold_reason="quota_hold")

    action = first_turn(store, eid)

    assert action["state"] == "pending"
    assert action["thread_id"] is None and action["turn_id"] is None
    assert store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (eid,)) == []
    assert first_turn(store, eid) is None


@pytest.mark.parametrize(
    ("phase", "deleted", "contract"),
    [
        ("complete", False, 4),
        ("failed", False, 4),
        ("cancelled", False, 4),
        ("preparing", True, 4),
        ("preparing", False, 3),
        ("preparing", False, 5),
    ],
)
def test_first_turn_skips_deleted_terminal_and_non_contract_4_experiments(runtime, phase, deleted, contract):
    store, _, _, _ = runtime
    eid = make_experiment(store, phase=phase, deleted=deleted, contract=contract)

    assert first_turn(store, eid) is None
    assert research_actions(store, eid) == []
    assert queued_events(store, eid) == []


def test_first_turn_skips_a_missing_experiment(runtime):
    store, _, _, _ = runtime

    assert first_turn(store, str(uuid4())) is None


def test_prompt_marker_is_bounded_and_rejects_a_non_uuid():
    action_id = str(uuid4())
    prompt = turn_prompt(brief="é😀" * 20000, checkpoint="c" * 10000, action_id=action_id)
    assert prompt.startswith(f"RESEARCH TURN\nACTION {action_id}\n")
    assert len(prompt.encode()) < MAX_PROMPT_BYTES
    with pytest.raises(ValueError, match="UUID"):
        turn_prompt(brief=BRIEF, action_id="not-a-uuid")
    with pytest.raises(TypeError):
        turn_prompt(brief=BRIEF, action_id=1)
