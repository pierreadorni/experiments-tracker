"""C7 event wake queue: exact supported events, one window, exact bind (T4.3).

Only an answered, satisfied contract-4 owner request or a saved terminal job records
work; arrivals share one 60-second window, and the consumer binds the due rows to an
already saved pending research action.
"""

from uuid import uuid4

import pytest
from helpers import seed_saved_contract3, seed_saved_contract4
from scenario import FakeClock

from experimenttracker.research import wakeups
from experimenttracker.research.session import first_turn
from experimenttracker.store import Conflict, Store, encode

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def contract_four(store, *, contract=4):
    """One live experiment with a queued research turn; no worktree or dispatch."""
    if contract == 3:
        return seed_saved_contract3(store, BRIEF), None
    assert contract == 4
    eid = seed_saved_contract4(store, BRIEF)
    return eid, first_turn(store, eid)


def answered_request(store, eid, action, *, request_id="question", answer="dataset B", **extra):
    """Save one durable owner question on the exact research turn, then its answer."""
    pending = {
        "id": request_id,
        "action_id": action["id"],
        "method": "item/tool/requestUserInput",
        "params": {"questions": [{"id": "question", "question": "Which dataset?"}]},
        "expired": False,
        **extra,
    }
    store.update(eid, pending_request=pending)
    if answer is not None:
        store.update(eid, pending_request={**pending, "answer": answer})


def saved_job(store, eid, *, state="complete"):
    """Save one job of this experiment in that saved job state and return its intent ID."""
    intent, stage = str(uuid4()), f"stage-{uuid4().hex[:8]}"
    with store.tx() as db:
        db.execute("INSERT INTO stages(experiment_id,id,updated_at) VALUES(?,?,0)", (eid, stage))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,"
            "resources,snapshot_hash,created_at) VALUES(?,?,?,99,?,'synthetic','source','{}','snapshot',0)",
            (intent, eid, stage, state),
        )
    return intent


def fake_clock(monkeypatch, wall=1000.0):
    value = FakeClock()
    value.wall, value.monotonic = wall, 0.0
    value.install(monkeypatch)
    return value


def saved_wakeups(store, eid):
    return store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=? ORDER BY due_at,id", (eid,))


def test_steering_is_consumed_once_in_bounded_next_turn(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    store.db.execute(
        "UPDATE agent_actions SET state='completed',thread_id='saved-thread',turn_id='saved-turn',result=? WHERE id=?",
        ('{"status":"wait","summary":"waiting","next":"continue"}', action["id"]),
    )
    store.update(eid, thread_id="saved-thread")
    fake_clock(monkeypatch)
    first = store.command(uuid4(), "steer", {"text": "Use dataset B", "mode": "later"}, eid=eid)
    assert first["state"] == "applied"
    from experimenttracker.research.session import follow_up_turn

    store.update(eid, thread_id="other-thread")
    assert follow_up_turn(store, eid) is None
    store.update(eid, thread_id="saved-thread", pause_agent=True)
    assert follow_up_turn(store, eid) is None
    store.update(eid, pause_agent=False)
    follow_up = follow_up_turn(store, eid)
    assert follow_up_turn(store, eid) is None
    assert follow_up is not None
    assert "Use dataset B" in follow_up["input"]
    assert store.one("SELECT state FROM steering WHERE experiment_id=?", (eid,))["state"] == "applied"
    assert "Use dataset B" not in str(store.rows("SELECT payload FROM events WHERE experiment_id=?", (eid,)))


def test_one_supported_event_records_one_durable_window(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    now = fake_clock(monkeypatch)
    answered_request(store, eid, action)

    saved = wakeups.record(store, eid, wakeups.OWNER_REQUEST_ANSWERED, "question")

    wakeup = wakeups.wakeup_id(eid, wakeups.OWNER_REQUEST_ANSWERED, "question")
    assert saved == {
        "wakeup_id": wakeup,
        "event": "owner_request_answered",
        "source_id": "question",
        "due_at": 1060.0,
        "state": "pending",
        "action_id": None,
    }
    row = store.one("SELECT * FROM agent_wakeups WHERE id=?", (wakeup,))
    # No action is admitted and the saved payload is identity-only.
    assert (row["experiment_id"], row["state"], row["action_id"]) == (eid, "pending", None)
    assert row["reason"] == encode({"event": "owner_request_answered", "source_id": "question"})
    timer = store.one("SELECT * FROM obligations WHERE id=?", (f"wakeup:{wakeup}:0",))
    assert (timer["state"], timer["kind"], timer["owner"], timer["due_at"]) == (
        "open",
        "timer",
        "backend",
        1060.0,
    )
    now.advance(5)
    assert wakeups.record(store, eid, wakeups.OWNER_REQUEST_ANSWERED, "question") == saved
    assert saved_wakeups(store, eid) == [row]


def test_an_ambient_tick_is_not_a_supported_event(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    fake_clock(monkeypatch)
    for event in ("tick", "backend_compute_check"):
        with pytest.raises(ValueError, match="unsupported research wake event"):
            wakeups.record(store, eid, event, "question")

    assert saved_wakeups(store, eid) == []


def test_one_window_coalesces_arrivals_without_postponing_or_merging(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    now = fake_clock(monkeypatch)
    answered_request(store, eid, action)
    first_job, second_job = saved_job(store, eid, state="failed"), saved_job(store, eid)

    first = wakeups.record(store, eid, wakeups.OWNER_REQUEST_ANSWERED, "question")
    now.advance(30)
    second = wakeups.record(store, eid, wakeups.JOB_TERMINAL, first_job)
    now.advance(29)

    # A later arrival shares the window instead of moving its deadline.
    assert (first["due_at"], second["due_at"]) == (1060.0, 1060.0)
    assert wakeups.due(store, eid) == []
    now.advance(1)
    assert [(item["event"], item["source_id"]) for item in wakeups.due(store, eid)] == [
        (wakeups.OWNER_REQUEST_ANSWERED, "question"),
        (wakeups.JOB_TERMINAL, first_job),
    ]
    # A closed window never absorbs the next arrival, so continuous arrivals are
    # still covered one debounce after their own arrival.
    now.advance(5)
    assert wakeups.record(store, eid, wakeups.JOB_TERMINAL, second_job)["due_at"] == 1125.0


def test_consume_binds_the_exact_saved_action_without_completing_it(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    now = fake_clock(monkeypatch)
    answered_request(store, eid, action)
    job = saved_job(store, eid)
    wakeups.record(store, eid, wakeups.OWNER_REQUEST_ANSWERED, "question")
    wakeups.record(store, eid, wakeups.JOB_TERMINAL, job)
    now.advance(60)
    actions = store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    ids = [item["wakeup_id"] for item in wakeups.due(store, eid)]

    bound = wakeups.consume(store, eid, ids, action["id"])

    assert [(item["state"], item["action_id"]) for item in bound] == [("queued", action["id"])] * 2
    assert {(row["state"], row["action_id"]) for row in saved_wakeups(store, eid)} == {
        ("queued", action["id"])
    }
    timers = store.rows("SELECT state FROM obligations WHERE experiment_id=? AND kind='timer'", (eid,))
    assert {row["state"] for row in timers} == {"satisfied"}
    # Binding admits no work of its own and completes nothing.
    assert store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,)) == actions
    assert wakeups.due(store, eid) == []
    assert wakeups.consume(store, eid, ids, action["id"]) == bound


def test_consume_refuses_an_open_window_another_action_or_a_foreign_row(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    other, other_action = contract_four(store)
    now = fake_clock(monkeypatch)
    answered_request(store, eid, action)
    answered_request(store, other, other_action)
    saved = wakeups.record(store, eid, wakeups.OWNER_REQUEST_ANSWERED, "question")
    timer = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,created_at) VALUES(?,?,?,0,0)",
            (timer, eid, encode({"delay_seconds": 60, "reason": "Backend compute check"})),
        )

    with pytest.raises(Conflict, match="does not belong to this experiment"):
        wakeups.consume(store, eid, [saved["wakeup_id"]], other_action["id"])
    with pytest.raises(Conflict, match="coalescing window"):
        wakeups.consume(store, eid, [saved["wakeup_id"]], action["id"])
    now.advance(60)
    # A contract-3 timer wakeup and an unknown ID are not this experiment's event.
    assert [item["wakeup_id"] for item in wakeups.due(store, eid)] == [saved["wakeup_id"]]
    for wakeup in (timer, str(uuid4())):
        with pytest.raises(Conflict, match="not this experiment's saved event wakeup"):
            wakeups.consume(store, eid, [wakeup], action["id"])
    with pytest.raises(Conflict, match="not this experiment's saved event wakeup"):
        wakeups.consume(store, other, [saved["wakeup_id"]], other_action["id"])
    assert store.one("SELECT state FROM agent_wakeups WHERE id=?", (saved["wakeup_id"],))["state"] == (
        "pending"
    )


def _not_research(store, eid, action):
    answered_request(store, eid, action)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET phase='plan' WHERE id=?", (action["id"],))


def _unsatisfied(store, eid, action):
    pending = {"id": "question", "action_id": action["id"], "expired": False, "answer": "dataset B"}
    with store.tx() as db:
        db.execute("UPDATE experiments SET pending_request=? WHERE id=?", (encode(pending), eid))


@pytest.mark.parametrize(
    "mutate,match",
    [
        (lambda store, eid, action: answered_request(store, eid, action, answer=None), "no answer"),
        (
            lambda store, eid, action: answered_request(store, eid, action, expired=True),
            "expired owner request",
        ),
        (
            lambda store, eid, action: answered_request(store, eid, action, rpc_id="rpc-1"),
            "app-server request",
        ),
        (
            lambda store, eid, action: answered_request(store, eid, action, request_id="other"),
            "another identity",
        ),
        (lambda store, eid, action: store.update(eid, phase="complete"), "deleted or terminal"),
        (_not_research, "no saved research action"),
        (_unsatisfied, "obligation is not satisfied"),
    ],
    ids=["unanswered", "expired", "app-server", "other-identity", "terminal", "not-research", "unsatisfied"],
)
def test_record_refuses_an_unproven_owner_event(runtime, monkeypatch, mutate, match):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    fake_clock(monkeypatch)
    mutate(store, eid, action)

    with pytest.raises(Conflict, match=match):
        wakeups.record(store, eid, wakeups.OWNER_REQUEST_ANSWERED, "question")

    assert saved_wakeups(store, eid) == []


def test_a_healthy_job_and_a_non_research_experiment_owe_no_work(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    legacy, _ = contract_four(store, contract=3)
    now = fake_clock(monkeypatch)
    running = saved_job(store, eid, state="submitted")
    answered_request(store, eid, action, answer=None)

    for intent in (running, str(uuid4())):
        with pytest.raises(Conflict, match="saved terminal job"):
            wakeups.record(store, eid, wakeups.JOB_TERMINAL, intent)
    with pytest.raises(Conflict, match="contract 4 research experiment"):
        wakeups.record(store, legacy, wakeups.OWNER_REQUEST_ANSWERED, "question")
    now.advance(3600)
    assert wakeups.due(store, eid) == []
    assert store.rows("SELECT * FROM agent_wakeups") == []
    assert store.rows("SELECT * FROM obligations WHERE experiment_id=? AND kind='timer'", (eid,)) == []


def test_restart_keeps_the_saved_window_and_binds_it(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid, action = contract_four(store)
    fake_clock(monkeypatch)
    answered_request(store, eid, action)
    saved = wakeups.record(store, eid, wakeups.OWNER_REQUEST_ANSWERED, "question")

    reopened = Store(store.path)
    assert wakeups.due(reopened, eid, now=1059) == []
    assert wakeups.due(reopened, eid, now=1060) == [saved]
    assert wakeups.consume(reopened, eid, [saved["wakeup_id"]], action["id"], now=1060) == [
        {**saved, "state": "queued", "action_id": action["id"]}
    ]
    reopened.close()
