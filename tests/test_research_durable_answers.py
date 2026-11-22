"""T4.2 durable contract-4 owner questions survive restart and drain.

A saved ``ask_owner`` question names its own ``phase='research'`` action and carries
no app-server ``rpc_id``. Its owner answer is saved for the next research turn (T4.3);
it is never expired on startup or drain and never replayed through an old RPC. Live
RPC requests, contract 3 pending requests and real contract-4 RPCs keep their previous
expire-and-reply behavior.
"""

from uuid import uuid4

import pytest
from helpers import seed_saved_contract3, seed_saved_contract4

from experimenttracker import clock
from experimenttracker.research.session import first_turn

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def make_experiment(store, *, contract, pending):
    assert contract in (3, 4)
    seed = seed_saved_contract3 if contract == 3 else seed_saved_contract4
    eid = seed(store, BRIEF)
    store.update(eid, pending_request=pending)
    return eid


def question(store, *, answer=None, rpc_id=None):
    """Save one contract-4 experiment with a durable question and return its id."""
    eid = seed_saved_contract4(store, BRIEF)
    action = first_turn(store, eid)
    pending = {
        "id": "question",
        "action_id": action["id"],
        "method": "item/tool/requestUserInput",
        "params": {"questions": [{"id": "dataset", "question": "Which dataset?"}]},
        "expired": False,
    }
    if rpc_id is not None:
        pending["rpc_id"] = rpc_id
    if answer is not None:
        pending["answer"] = answer
    store.update(eid, pending_request=pending)
    return eid


def answer(store, eid, text):
    store.command(uuid4(), "answer", {"pending_request_id": "question", "answer": text}, eid=eid)


def saved_pending(store, eid):
    return store.experiment(eid)["pending_request"]


def isolate_start(monkeypatch, workflow):
    async def service(*_args, **_kwargs):
        return None

    for name in ("refresh_cluster", "connect_agent", "start_agent_pool"):
        monkeypatch.setattr(workflow, name, service)
    monkeypatch.setattr(workflow.controller, "start", service)


@pytest.mark.asyncio
async def test_startup_preserves_saved_contract_four_questions(runtime, monkeypatch):
    store, _, _, workflow = runtime
    unanswered = question(store)
    answered = question(store)
    answer(store, answered, "dataset A")
    isolate_start(monkeypatch, workflow)

    await workflow.start()

    assert saved_pending(store, unanswered)["expired"] is False
    saved = saved_pending(store, answered)
    assert (saved["expired"], saved["answer"]) == (False, "dataset A")
    assert store.experiment(answered)["hold_reason"] is None


@pytest.mark.asyncio
async def test_startup_still_expires_old_rpcs_and_contract_three_requests(runtime, monkeypatch):
    store, _, _, workflow = runtime
    answered = question(store)
    answer(store, answered, "dataset A")
    rpc = saved_pending(store, answered)
    rpc["rpc_id"] = 7
    store.update(answered, pending_request=rpc)
    legacy = make_experiment(
        store,
        contract=3,
        pending={
            "id": "legacy",
            "method": "item/tool/requestUserInput",
            "params": {"questions": [{"id": "q"}]},
            "answer": "yes",
        },
    )
    isolate_start(monkeypatch, workflow)

    await workflow.start()

    for eid, hold in ((answered, "expired_agent_request"), (legacy, "legacy_contract_archived")):
        assert saved_pending(store, eid)["expired"] is True
        assert store.experiment(eid)["hold_reason"] == hold


@pytest.mark.asyncio
async def test_drain_preserves_answered_contract_four_question_without_reply(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = question(store)
    answer(store, eid, "dataset A")
    assert saved_pending(store, eid)["answer"] == "dataset A"
    # Store.answer saves the answer into the question; it opens no stage continuation.
    assert store.rows("SELECT * FROM steering WHERE experiment_id=?", (eid,)) == []
    replies = []

    async def reply(pending):
        replies.append(pending["id"])
        return False

    monkeypatch.setattr(workflow.agent_pool, "reply", reply)

    await workflow.step_control_after_observe(clock.now())

    assert replies == []
    saved = saved_pending(store, eid)
    assert (saved["expired"], saved["answer"]) == (False, "dataset A")
    assert store.experiment(eid)["hold_reason"] is None


@pytest.mark.asyncio
async def test_drain_still_expires_a_real_contract_four_rpc(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = question(store, rpc_id=7)
    answer(store, eid, "dataset A")
    replies = []

    async def reply(pending):
        replies.append(pending["id"])
        return False

    monkeypatch.setattr(workflow.agent_pool, "reply", reply)

    await workflow.step_control_after_observe(clock.now())

    assert replies == ["question"]
    saved = saved_pending(store, eid)
    assert (saved["expired"], saved["answer"]) == (True, "dataset A")
    assert store.experiment(eid)["hold_reason"] == "expired_agent_request"
