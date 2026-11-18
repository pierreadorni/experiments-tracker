"""Contract-4 ask_owner through the existing MCP bridge: one durable question (T4.2).

The tool exposes only the question-shaped ``AskOwnerQuestion`` model, reuses the
shared live-research write guard and the exact owning research action, and leaves the
existing ``answer`` command to settle the saved obligation and record the one
owner-answer event wake. No model RPC or contract-3 continuation is opened; turning
that saved answer into a follow-up belongs to T4.3b.
"""

import json
from uuid import uuid4

import pytest
from test_research_durable_answers import isolate_start
from test_research_settlement import setup
from test_research_settlement import turn as turn_end

from experimenttracker import controller
from experimenttracker.research.owner_tool import AskOwnerQuestion
from experimenttracker.research.settlement import complete
from experimenttracker.store import encode

EVENT_KIND = "owner_question_asked"


def question(request_id=None, text="Which dataset should the comparison use?", options=()):
    return AskOwnerQuestion(request_id=request_id or uuid4(), question=text, options=list(options))


def payload(asked):
    return {
        "request_id": str(asked.request_id),
        "question": asked.question,
        "options": list(asked.options),
    }


def saved_pending(store, eid):
    return store.experiment(eid)["pending_request"]


def obligation(store, eid, request_id):
    return store.one("SELECT * FROM obligations WHERE id=?", (f"request:{eid}:{request_id}:0",))


def test_ask_owner_schema_is_advertised_from_the_actual_model():
    served = {item["name"]: item for item in controller.served_tools(research=True)}
    schema = served["ask_owner"]["inputSchema"]
    assert schema == AskOwnerQuestion.model_json_schema()
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["request_id", "question"]
    assert set(schema["properties"]) == {"request_id", "question", "options", "kind", "objective"}


@pytest.mark.parametrize("decision", ["accept", "decline", "cancel"])
async def test_durable_approval_only_grants_exact_owner_accept(runtime, tmp_path, monkeypatch, decision):
    from experimenttracker.research.approval import approved
    from experimenttracker.store import Conflict

    store, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    asked = question()
    request_id = str(asked.request_id)
    objective = "Compare the two synthetic data splits"
    request = {**payload(asked), "kind": "approval", "objective": objective}
    receipt = await workflow.controller.call(eid, "ask_owner", request)
    assert await workflow.controller.call(eid, "ask_owner", request) == receipt
    assert approved(store, eid, request_id, objective) is None
    assert saved_pending(store, eid)["params"]["objective"] == objective
    body = {"pending_request_id": request_id, "answer": decision}
    with pytest.raises(Conflict):
        store.command(uuid4(), "answer", body, eid=eid, origin="agent")
    store.command(uuid4(), "answer", body, eid=eid)
    assert bool(approved(store, eid, request_id, objective)) == (decision == "accept")
    with pytest.raises(Conflict):
        store.command(uuid4(), "answer", body, eid=eid)
    store.update(eid, pending_request=None)
    assert bool(approved(store, eid, request_id, objective)) == (decision == "accept")


async def test_ask_owner_routes_one_question_and_needs_owner_settles(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    asked = question(options=["dataset A", "dataset B"])

    receipt = await workflow.controller.call(eid, "ask_owner", payload(asked))

    saved = saved_pending(store, eid)
    assert receipt == {
        "schema_version": 1,
        "experiment_id": eid,
        "request_id": str(asked.request_id),
        "action_id": action["id"],
        "thread_id": "thread-1",
        "state": "open",
    }
    assert (saved["id"], saved["action_id"], saved["thread_id"], saved["expired"]) == (
        str(asked.request_id),
        action["id"],
        "thread-1",
        False,
    )
    assert "rpc_id" not in saved and "answer" not in saved
    assert obligation(store, eid, asked.request_id)["state"] == "open"

    # The saved question is real evidence, so the needs_owner turn end is accepted.
    settled = await complete(workflow, action, turn_end("needs_owner"))

    assert (settled["state"], json.loads(settled["result"])["status"]) == ("completed", "needs_owner")
    assert store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (eid,)) == []
    assert getattr(workflow.agent, "calls", 0) == 0


async def test_an_exact_replay_adopts_the_saved_question(runtime, tmp_path, monkeypatch):
    store, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    asked = question()

    first = await workflow.controller.call(eid, "ask_owner", payload(asked))
    events = store.rows("SELECT * FROM events WHERE experiment_id=? AND kind=?", (eid, EVENT_KIND))

    assert await workflow.controller.call(eid, "ask_owner", payload(asked)) == first
    assert len(store.rows("SELECT * FROM events WHERE experiment_id=? AND kind=?", (eid, EVENT_KIND))) == len(
        events
    )
    assert len(store.rows("SELECT * FROM inbox WHERE experiment_id=?", (eid,))) == 1


async def test_restart_keeps_the_question_and_the_existing_answer_settles_it(runtime, tmp_path, monkeypatch):
    store, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    asked = question()
    await workflow.controller.call(eid, "ask_owner", payload(asked))
    before = saved_pending(store, eid)

    isolate_start(monkeypatch, workflow)
    await workflow.start()

    assert saved_pending(store, eid) == before
    assert saved_pending(store, eid)["expired"] is False
    assert store.experiment(eid)["hold_reason"] is None

    store.command(
        uuid4(),
        "answer",
        {"pending_request_id": str(asked.request_id), "answer": "dataset B"},
        eid=eid,
    )

    answered = saved_pending(store, eid)
    assert (answered["expired"], answered["answer"]) == (False, "dataset B")
    assert obligation(store, eid, asked.request_id)["state"] == "satisfied"
    # Answering records the durable reply and its one event wake: no model RPC, no C3
    # continuation, and no research action before the window closes and the turn settles.
    assert store.rows("SELECT * FROM steering WHERE experiment_id=?", (eid,)) == []
    assert [
        (row["state"], row["action_id"], json.loads(row["reason"])["source_id"])
        for row in store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (eid,))
    ] == [("pending", None, str(asked.request_id))]
    assert getattr(workflow.agent, "calls", 0) == 0


@pytest.mark.parametrize(
    "phase,state,pause_agent,contract,message",
    [
        ("plan", "running", False, 4, "active research turn"),
        ("research", "pending", False, 4, "active owned session"),
        ("research", "dispatching", False, 4, "not running"),
        ("research", "running", True, 4, "live research experiment"),
        ("research", "running", False, 3, "execution contract 4"),
    ],
)
async def test_ask_owner_keeps_the_common_live_v4_and_running_action_gate(
    runtime, tmp_path, monkeypatch, phase, state, pause_agent, contract, message
):
    store, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET phase=?,state=? WHERE experiment_id=?", (phase, state, eid))
        db.execute("UPDATE experiments SET pause_agent=? WHERE id=?", (int(pause_agent), eid))
        if contract != 4:
            value = json.loads(
                db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()[0]
            )
            value["execution_contract"] = contract
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(value), eid))

    with pytest.raises(ValueError, match=message):
        await workflow.controller.call(eid, "ask_owner", payload(question()))

    assert saved_pending(store, eid) is None
    assert store.rows("SELECT * FROM events WHERE experiment_id=? AND kind=?", (eid, EVENT_KIND)) == []


async def test_ask_owner_refuses_during_maintenance(runtime, tmp_path, monkeypatch):
    store, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    release = "a" * 40
    with store.tx() as db:
        db.execute(
            "INSERT INTO releases(commit_id,summary,state,updated_at) VALUES(?,?,?,?)",
            (release, "maintenance release", "ready", 0),
        )
        db.execute(
            "INSERT INTO maintenance(request_id,release_id,state,created_at,updated_at) VALUES(?,?,?,?,?)",
            (str(uuid4()), release, "waiting", 0, 0),
        )

    with pytest.raises(ValueError, match="maintenance forbids research writes"):
        await workflow.controller.call(eid, "ask_owner", payload(question()))

    assert saved_pending(store, eid) is None
