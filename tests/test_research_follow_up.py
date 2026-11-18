"""C7 owner-answer wake: one follow-up turn only after the window and a settled turn.

T4.3b wires the exact saved contract-4 owner answer as the first event wake. The
answer command records one durable row, the row's 60-second window must close, and the
follow-up binds that row to one new research action while consuming the exact answer
into the bounded prompt. A running, uncertain or failed turn is never replaced, and
job, timer, steering and approval wakes are not wired here.
"""

import json
from uuid import uuid4

import pytest
from scenario import FakeClock

from experimenttracker.research.prompt import MAX_PROMPT_BYTES, turn_prompt
from experimenttracker.research.session import first_turn, follow_up_turn
from experimenttracker.store import Store, encode

BRIEF = "Compare two toy optimizers under one fixed compute budget."
QUESTION = "Which dataset should the comparison use?"
ANSWER = "dataset B"
DEFERRED = "[saved notebook text is not copied into a follow-up turn]"


def make_experiment(store, *, contract=4):
    eid = store.command(uuid4(), "create", {"brief": BRIEF, "recipe_id": "agent_experiment_v1"})[
        "experiment_id"
    ]
    saved = store.experiment(eid)["contract"]
    saved["execution_contract"] = contract
    saved["branch_workspace"] = 1
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(saved), eid))
    return eid


def save_question(store, eid, action, *, request_id="question"):
    """Save one durable question asked by the exact research turn."""
    store.update(
        eid,
        pending_request={
            "id": request_id,
            "action_id": action["id"],
            "method": "item/tool/requestUserInput",
            "params": {"questions": [{"id": "dataset", "question": QUESTION}]},
            "expired": False,
        },
    )


def settled(store, action, *, state="completed"):
    with store.tx() as db:
        db.execute(
            "UPDATE agent_actions SET state=?,result=? WHERE id=?",
            (state, encode({"status": "wait", "summary": "waiting", "next": "owner answer"}), action["id"]),
        )


def asked_turn(store, eid, *, state="completed"):
    """One saved first turn that asked the owner and has settled."""
    action = first_turn(store, eid)
    save_question(store, eid, action)
    settled(store, action, state=state)
    return action


def answer(store, eid, *, text=ANSWER, request_id="question"):
    store.command(uuid4(), "answer", {"pending_request_id": request_id, "answer": text}, eid=eid)


def fake_clock(monkeypatch, wall=1000.0):
    value = FakeClock()
    value.wall, value.monotonic = wall, 0.0
    value.install(monkeypatch)
    return value


def research_actions(store, eid):
    return store.rows(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='research' ORDER BY rowid", (eid,)
    )


def saved_wakeups(store, eid):
    return {
        json.loads(row["reason"])["source_id"]: row
        for row in store.rows(
            "SELECT * FROM agent_wakeups WHERE experiment_id=? ORDER BY created_at,id", (eid,)
        )
    }


def timer_states(store, eid):
    return {
        row["state"]
        for row in store.rows("SELECT state FROM obligations WHERE experiment_id=? AND kind='timer'", (eid,))
    }


def test_the_answer_command_records_one_owner_wake_without_queueing_a_turn(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked_turn(store, eid)
    now = fake_clock(monkeypatch)

    answer(store, eid)

    (wake,) = list(saved_wakeups(store, eid).values())
    # The saved reason is identity only: no question or answer text reaches the row.
    assert (wake["state"], wake["action_id"], wake["due_at"]) == ("pending", None, now.wall + 60)
    assert wake["reason"] == encode({"event": "owner_request_answered", "source_id": "question"})
    assert len(research_actions(store, eid)) == 1


def test_no_follow_up_before_the_window_closes(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked_turn(store, eid)
    now = fake_clock(monkeypatch)
    answer(store, eid)

    now.advance(59)
    assert follow_up_turn(store, eid) is None
    assert len(research_actions(store, eid)) == 1
    assert [row["state"] for row in saved_wakeups(store, eid).values()] == ["pending"]

    now.advance(1)
    assert follow_up_turn(store, eid) is not None


def test_the_due_answer_queues_one_bounded_follow_up_and_binds_its_wake(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked = asked_turn(store, eid)
    now = fake_clock(monkeypatch)
    answer(store, eid)
    now.advance(60)

    action = follow_up_turn(store, eid)

    experiment = store.experiment(eid)
    assert (action["experiment_id"], action["phase"], action["state"]) == (eid, "research", "pending")
    assert (action["attempts"], action["thread_id"], action["turn_id"]) == (0, None, None)
    assert action["evidence_revision"] == experiment["evidence_revision"]
    assert action["id"] != asked["id"]
    # The exact answer is inside the bounded prompt and no checkpoint text is inlined.
    assert ANSWER in action["input"] and QUESTION in action["input"]
    assert DEFERRED in action["input"]
    assert len(action["input"].encode()) < MAX_PROMPT_BYTES
    assert action["input"] == turn_prompt(
        brief=BRIEF,
        checkpoint=DEFERRED,
        events=[f"Owner answer to {QUESTION}: {ANSWER}"],
        action_id=action["id"],
    )
    # The wake is bound to that exact action and its timer obligation settled.
    wake = saved_wakeups(store, eid)["question"]
    assert (wake["state"], wake["action_id"]) == ("queued", action["id"])
    assert timer_states(store, eid) == {"satisfied"}
    # The saved request closed with its owner obligation satisfied.
    assert experiment["pending_request"] is None
    assert store.one("SELECT state FROM obligations WHERE id=?", (f"request:{eid}:question:0",)) == {
        "state": "satisfied"
    }
    # The queue event names only the phase and the action, never the owner text.
    queued = [
        row["payload"]
        for row in store.rows("SELECT kind,payload FROM events WHERE experiment_id=? ORDER BY seq", (eid,))
        if row["kind"] == "agent_action_queued"
    ]
    assert queued[-1] == encode({"phase": "research", "action_id": action["id"]})
    # A second pass has nothing left to queue and never replaces the pending turn.
    assert follow_up_turn(store, eid) is None
    assert [row["id"] for row in research_actions(store, eid)] == [asked["id"], action["id"]]


def test_a_reopened_store_keeps_the_window_and_queues_once(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked = asked_turn(store, eid)
    fake_clock(monkeypatch)
    answer(store, eid)

    reopened = Store(store.path)
    try:
        assert follow_up_turn(reopened, eid, now=1059) is None
        action = follow_up_turn(reopened, eid, now=1060)
        assert [row["id"] for row in research_actions(reopened, eid)] == [asked["id"], action["id"]]
        assert follow_up_turn(reopened, eid, now=1200) is None
    finally:
        reopened.close()


@pytest.mark.parametrize("state", ["pending", "dispatching", "running", "uncertain", "failed"])
def test_a_predecessor_that_has_not_settled_never_gets_a_follow_up(runtime, monkeypatch, state):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked = asked_turn(store, eid)
    now = fake_clock(monkeypatch)
    answer(store, eid)
    settled(store, asked, state=state)
    now.advance(60)

    assert follow_up_turn(store, eid) is None

    assert len(research_actions(store, eid)) == 1
    assert [row["state"] for row in saved_wakeups(store, eid).values()] == ["pending"]
    assert store.experiment(eid)["pending_request"]["answer"] == ANSWER


def enter_maintenance(store):
    commit, request_id = "a" * 40, str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO releases(commit_id,summary,state,receipt,error,updated_at) VALUES(?,?,?,?,?,0)",
            (commit, "Synthetic release", "ready", None, None),
        )
        db.execute(
            "INSERT INTO maintenance(request_id,release_id,state,created_at,updated_at) "
            "VALUES(?,?,'restarting',0,0)",
            (request_id, commit),
        )


def _pause(store, eid):
    store.update(eid, pause_agent=True)


def _cancel(store, eid):
    store.update(eid, phase="cancelled")


def _maintain(store, eid):
    enter_maintenance(store)


@pytest.mark.parametrize("mutate", [_pause, _cancel, _maintain], ids=["paused", "cancelled", "maintenance"])
def test_a_paused_cancelled_or_maintained_experiment_queues_nothing(runtime, monkeypatch, mutate):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked_turn(store, eid)
    now = fake_clock(monkeypatch)
    answer(store, eid)
    mutate(store, eid)
    now.advance(60)

    assert follow_up_turn(store, eid) is None
    assert len(research_actions(store, eid)) == 1
    assert [row["state"] for row in saved_wakeups(store, eid).values()] == ["pending"]
