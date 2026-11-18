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
from test_research_wakeups import saved_job

from experimenttracker.research import wakeups
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


def test_an_earlier_nonterminal_action_cannot_be_bypassed_by_a_completed_latest_action(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    earlier = first_turn(store, eid)
    save_question(store, eid, earlier)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET state='running' WHERE id=?", (earlier["id"],))
    latest = {"evidence_revision": store.experiment(eid)["evidence_revision"]}
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,?,?)",
            (str(uuid4()), eid, "research", latest["evidence_revision"], "completed", "", 2, 2),
        )
        latest = dict(
            db.execute(
                "SELECT * FROM agent_actions WHERE experiment_id=? ORDER BY rowid DESC LIMIT 1", (eid,)
            ).fetchone()
        )
    settled(store, latest)
    fake_clock(monkeypatch)
    answer(store, eid)
    monkeypatch.setattr(
        wakeups,
        "due",
        lambda *_args, **_kwargs: [
            {"event": wakeups.OWNER_REQUEST_ANSWERED, "source_id": "question", "wakeup_id": "wake"}
        ],
    )
    assert follow_up_turn(store, eid) is None
    assert len(research_actions(store, eid)) == 2
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (earlier["id"],))["state"] == "running"


def test_c3_resume_blocked_prevents_a_c4_answer_follow_up_even_when_unheld(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked_turn(store, eid)
    fake_clock(monkeypatch)
    answer(store, eid)
    store.update(eid, hold_reason="_resume_blocked")
    monkeypatch.setattr(
        wakeups,
        "due",
        lambda *_args, **_kwargs: [
            {"event": wakeups.OWNER_REQUEST_ANSWERED, "source_id": "question", "wakeup_id": "wake"}
        ],
    )
    assert follow_up_turn(store, eid) is None
    assert len(research_actions(store, eid)) == 1


@pytest.mark.parametrize("contract", [3, 5])
def test_a_non_contract_four_experiment_queues_no_follow_up(runtime, monkeypatch, contract):
    store, _, _, _ = runtime
    eid = make_experiment(store, contract=contract)
    fake_clock(monkeypatch)

    assert first_turn(store, eid) is None
    assert follow_up_turn(store, eid) is None
    assert research_actions(store, eid) == []


def test_a_job_wake_is_not_consumed_by_the_owner_answer_follow_up(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked_turn(store, eid)
    now = fake_clock(monkeypatch)
    answer(store, eid)
    job = saved_job(store, eid)
    wakeups.record(store, eid, wakeups.JOB_TERMINAL, job)
    now.advance(60)

    action = follow_up_turn(store, eid)

    saved = saved_wakeups(store, eid)
    assert (saved["question"]["state"], saved["question"]["action_id"]) == ("queued", action["id"])
    # Job-terminal wakes stay for their own task: no job wiring in this slice.
    assert (saved[job]["state"], saved[job]["action_id"]) == ("pending", None)
    assert timer_states(store, eid) == {"open", "satisfied"}


def test_a_wake_without_its_saved_answer_queues_nothing(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked_turn(store, eid)
    now = fake_clock(monkeypatch)
    answer(store, eid)
    store.update(eid, pending_request=None)
    now.advance(60)

    assert follow_up_turn(store, eid) is None
    assert len(research_actions(store, eid)) == 1


def test_a_request_asked_by_another_turn_queues_nothing(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked_turn(store, eid)
    other = make_experiment(store)
    first_turn(store, other)
    now = fake_clock(monkeypatch)
    answer(store, eid)
    question = store.experiment(eid)["pending_request"]
    assert question["action_id"] == research_actions(store, eid)[0]["id"]
    store.update(eid, pending_request={**question, "action_id": research_actions(store, other)[0]["id"]})
    now.advance(60)

    assert follow_up_turn(store, eid) is None
    assert len(research_actions(store, eid)) == 1
    assert [row["state"] for row in saved_wakeups(store, eid).values()] == ["pending"]


def test_a_long_owner_answer_is_cut_and_marked(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked_turn(store, eid)
    now = fake_clock(monkeypatch)
    answer(store, eid, text="dataset B " * 900)
    now.advance(60)

    action = follow_up_turn(store, eid)

    assert action is not None
    assert len(action["input"].encode()) < MAX_PROMPT_BYTES
    assert "[truncated]" in action["input"]
    assert action["input"].count(ANSWER) < 900


async def test_the_control_pass_queues_one_follow_up_for_a_due_owner_answer(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = make_experiment(store)
    asked = asked_turn(store, eid)
    now = fake_clock(monkeypatch)
    answer(store, eid)
    prepared = []
    monkeypatch.setattr(
        workflow.workspaces, "ensure_branch", lambda identity: prepared.append(identity) or identity
    )

    now.advance(59)
    await workflow.prepare_research_turns()
    assert len(research_actions(store, eid)) == 1
    now.advance(1)
    await workflow.prepare_research_turns()

    [follow_up] = research_actions(store, eid)[1:]
    assert (follow_up["state"], follow_up["thread_id"]) == ("pending", None)
    assert (follow_up["id"] != asked["id"]) and ANSWER in follow_up["input"]
    assert prepared == [eid]
    # The pass queues the turn but starts no session and binds its one wake.
    assert saved_wakeups(store, eid)["question"]["action_id"] == follow_up["id"]
    assert getattr(workflow.agent, "calls", 0) == 0


def test_a_saved_checkpoint_markdown_stays_out_of_the_follow_up_prompt(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    asked = asked_turn(store, eid)
    secret = "PRIVATE NOTEBOOK: speculative run 7 diverged."
    with store.tx() as db:
        db.execute(
            "INSERT INTO events(id,experiment_id,kind,revision,payload,created_at) VALUES(?,?,?,?,?,?)",
            (
                str(uuid4()),
                eid,
                "research_checkpoint_saved",
                store.experiment(eid)["revision"],
                encode(
                    {
                        "request_id": str(uuid4()),
                        "experiment_id": eid,
                        "action_id": asked["id"],
                        "version": 1,
                        "bytes": len(secret),
                        "markdown": secret,
                    }
                ),
                0,
            ),
        )
    now = fake_clock(monkeypatch)
    answer(store, eid)
    now.advance(60)

    action = follow_up_turn(store, eid)

    assert secret not in action["input"]
    assert DEFERRED in action["input"]
    # The saved checkpoint stays readable in its own event and in the public projection
    # only without its Markdown.
    assert secret in store.one("SELECT payload FROM events WHERE kind='research_checkpoint_saved'")["payload"]
    assert secret not in encode(store.projection(eid))
