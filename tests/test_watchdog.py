"""Exact C2 liveness watchdog: saved-source defects notify once and never dispatch."""

import time
from uuid import uuid4

import pytest
from test_research_create import create as create_research
from test_research_create import git_project

from experimenttracker.core import (
    action_obligations,
    job_obligations,
    liveness,
    transfer_obligations,
    wakeup_obligations,
)
from experimenttracker.research.session import first_turn
from experimenttracker.store import Store
from experimenttracker.watchdog import engine_watchdog


@pytest.fixture
def clock(monkeypatch):
    """Freeze the store's clock so elapsed minutes are explicit in every test."""

    class Clock:
        now = time.time()

        def __call__(self):
            return self.now

        def advance(self, minutes):
            self.now += minutes * 60

    clock = Clock()
    monkeypatch.setattr("experimenttracker.clock.now", clock)
    return clock


@pytest.fixture
def store(tmp_path):
    store = Store(tmp_path / "state" / "workflow.sqlite")
    yield store
    store.close()


def create(store, hold_reason="agent_stage_blocked", phase="preparing"):
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic watchdog test", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    store.update(eid, phase=phase, hold_reason=hold_reason)
    return eid


def notices(store, eid):
    return store.rows("SELECT kind,text FROM inbox WHERE experiment_id=? ORDER BY created_at", (eid,))


def watch(store, clock, minutes=30, *, started_at=None, serviced=None):
    """One watchdog pass of a process started at `started_at` with those handler returns."""
    evidence = liveness.ServiceEvidence(clock.now if started_at is None else started_at, serviced or {})
    return engine_watchdog(store, clock.now, minutes, evidence)


def action(store, eid, *, state="running", thread_id="thread-1", turn_id="turn-1"):
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,"
            "input,created_at,updated_at) VALUES(?,?,'prepare',1,?,?,?,'synthetic',0,0)",
            (action_id, eid, state, thread_id, turn_id),
        )
        action_obligations.sync_action(db, action_id)
    return action_id


def job(store, eid, *, state="submitted", job_id="101"):
    intent_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at) VALUES(?,?,'prepare',1,?,?,'synthetic','{}','control',0)",
            (intent_id, eid, state, job_id),
        )
        job_obligations.sync_job(db, intent_id)
    return intent_id


def transfer(store, eid, *, state="pending"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO execution_transfers VALUES(?,?,'up','{\"layout\":{},\"settings\":{}}',?,NULL,0)",
            (identity, eid, state),
        )
        transfer_obligations.sync_transfer(db, identity)
    return identity


def wakeup(store, eid, *, due_at, state="pending"):
    wakeup_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_wakeups VALUES(?,?,'backend compute check',?,?,NULL,0)",
            (wakeup_id, eid, due_at, state),
        )
        wakeup_obligations.sync_wakeup(db, wakeup_id)
    return wakeup_id


def work_counts(store, eid):
    return {
        table: store.one(f"SELECT COUNT(*) AS n FROM {table} WHERE experiment_id=?", (eid,))["n"]
        for table in ("agent_actions", "jobs", "execution_transfers", "agent_wakeups")
    }


def test_an_orphaned_experiment_is_reported_once(store, clock):
    eid = create(store)
    clock.advance(29)
    assert watch(store, clock) == 0
    clock.advance(2)
    assert watch(store, clock) == 1
    clock.advance(60)
    assert watch(store, clock) == 0
    rows = notices(store, eid)
    assert [row["kind"] for row in rows] == ["engine_stuck"]
    assert rows[0]["text"].startswith("missing_dependency: advancement:")


@pytest.mark.parametrize("dependency", [None, "paused", "queued"])
def test_orphaned_research_experiment_is_reported_without_creating_work(
    runtime, config, tmp_path, clock, dependency
):
    store, scheduler, agent, _ = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, receipt = create_research(store)
    eid = receipt["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 4
    assert store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)) == []
    if dependency == "paused":
        store.update(eid, pause_agent=True)
    elif dependency == "queued":
        assert first_turn(store, eid) is not None
    before = work_counts(store, eid)

    clock.advance(29)
    assert watch(store, clock) == 0
    clock.advance(2)
    assert watch(store, clock) == (1 if dependency is None else 0)
    clock.advance(31)
    assert watch(store, clock) == 0
    assert [row["kind"] for row in notices(store, eid)] == (["engine_stuck"] if dependency is None else [])
    assert work_counts(store, eid) == before
    assert scheduler.submissions == 0 and agent.calls == 0


def test_the_notice_survives_an_owner_restart(store, clock):
    eid = create(store)
    clock.advance(31)
    assert watch(store, clock) == 1
    restarted = Store(store.path)
    try:
        clock.advance(31)
        assert watch(restarted, clock) == 0
        assert len(notices(restarted, eid)) == 1
    finally:
        restarted.close()


def test_configured_minutes_widen_the_window(store, clock):
    create(store)
    clock.advance(31)
    assert watch(store, clock, 60) == 0
    clock.advance(30)
    assert watch(store, clock, 60) == 1


@pytest.mark.parametrize(
    "hold_reason",
    [
        "plan_review",
        "agent_paused",
        "new_compute_paused",
        "needs_attention",
        "quota_hold",
        "agent_busy",
        "cancellation_pending",
    ],
)
def test_owner_owned_holds_stay_quiet(store, clock, hold_reason):
    create(store, hold_reason)
    clock.advance(120)
    assert watch(store, clock) == 0


@pytest.mark.parametrize("phase", ["complete", "failed", "cancelled"])
def test_terminal_phases_are_not_reported(store, clock, phase):
    eid = create(store, hold_reason=None)
    store.update(eid, phase=phase)
    clock.advance(120)
    assert watch(store, clock) == 0


def test_a_deleted_experiment_is_not_reported(store, clock):
    eid = create(store, hold_reason=None)
    store.update(eid, phase="failed")
    store.command(uuid4(), "delete", {}, eid=eid)
    clock.advance(120)
    assert watch(store, clock) == 0


@pytest.mark.parametrize("state", ["pending", "dispatching", "running"])
def test_a_live_agent_turn_keeps_the_experiment_going(store, clock, state):
    eid = create(store)
    action(store, eid, state=state)
    clock.advance(120)
    assert watch(store, clock) == 0


def test_a_live_job_keeps_the_experiment_going(store, clock):
    eid = create(store)
    job(store, eid)
    clock.advance(29)
    assert watch(store, clock) == 0


def test_a_pending_transfer_keeps_the_experiment_going(store, clock):
    eid = create(store)
    transfer(store, eid)
    clock.advance(29)
    assert watch(store, clock) == 0


def test_settled_sources_leave_the_experiment_stuck(store, clock):
    eid = create(store)
    action(store, eid, state="completed", thread_id=None, turn_id=None)
    job(store, eid, state="complete")
    transfer(store, eid, state="complete")
    clock.advance(31)
    assert watch(store, clock) == 1
    assert len(notices(store, eid)) == 1


def test_an_overdue_timer_is_reported(store, clock):
    eid = create(store)
    wakeup_id = wakeup(store, eid, due_at=clock.now + 60)
    clock.advance(31)
    assert watch(store, clock) == 0
    clock.advance(2)
    assert watch(store, clock) == 1
    assert notices(store, eid)[0]["text"].startswith(f"overdue: wakeup:{wakeup_id}:0")


def test_an_owner_question_needs_the_existing_notice(store, clock):
    answered = create(store)
    store.update(answered, pending_request={"id": "request-1", "question": "Approve?"})
    store.notify(answered, "required_decision", "Codex needs an answer or permission.")
    silent = create(store)
    store.update(silent, pending_request={"id": "request-2", "question": "Approve?"})
    clock.advance(120)
    assert watch(store, clock) == 1
    assert notices(store, silent)[0]["text"].startswith("notice_missing: request:")
    assert [row["kind"] for row in notices(store, answered)] == ["required_decision"]


def test_a_backend_family_needs_a_handler_return_after_the_grace(store, clock):
    eid = create(store)
    action_id = action(store, eid, state="pending")
    started = clock.now
    clock.advance(29)
    assert watch(store, clock, started_at=started) == 0
    clock.advance(2)
    assert watch(store, clock, started_at=started) == 1
    assert notices(store, eid)[0]["text"].startswith(f"unserviced: agent-action:{action_id}:0")
    assert watch(store, clock, started_at=started, serviced={"agent-action": clock.now}) == 0


def test_a_restarted_process_gets_one_interval_of_grace(store, clock):
    eid = create(store)
    job(store, eid, state="submitting", job_id=None)
    clock.advance(120)
    restarted = clock.now
    assert watch(store, clock, started_at=restarted) == 0
    clock.advance(31)
    assert watch(store, clock, started_at=restarted) == 1
    assert notices(store, eid)[0]["text"].startswith("unserviced: job:")


def test_a_contract3_experiment_without_its_lifetime_is_reported(store, clock):
    eid = create(store)
    with store.tx() as db:
        db.execute("DELETE FROM obligations WHERE id=?", (f"advancement:{eid}:0",))
    clock.advance(31)
    assert watch(store, clock) == 1
    assert notices(store, eid)[0]["text"].startswith("missing_lifetime: ")


def test_the_watchdog_only_notifies_and_never_dispatches(store, clock):
    eid = create(store)
    action(store, eid, state="completed", thread_id=None, turn_id=None)
    obligations = store.rows("SELECT * FROM obligations ORDER BY id")
    counts = work_counts(store, eid)
    clock.advance(31)
    assert watch(store, clock) == 1
    assert store.rows("SELECT * FROM obligations ORDER BY id") == obligations
    assert work_counts(store, eid) == counts
    assert store.experiment(eid)["phase"] == "preparing"
    assert [row["kind"] for row in notices(store, eid)] == ["engine_stuck"]


def test_each_new_notice_logs_one_warning(store, clock, caplog):
    create(store)
    clock.advance(31)
    assert watch(store, clock) == 1
    assert watch(store, clock) == 0
    warnings = [record for record in caplog.records if record.levelname == "WARNING"]
    assert len(warnings) == 1 and "missing_dependency: advancement:" in warnings[0].getMessage()


async def test_tick_reports_a_stuck_experiment_and_records_its_service(runtime, clock):
    store, _, _, workflow = runtime
    eid = create(store)
    clock.advance(31)
    await workflow.tick()
    assert [row["kind"] for row in notices(store, eid)] == ["engine_stuck"]
    assert notices(store, eid)[0]["text"].startswith("missing_dependency: ")
    assert {"agent-action", "cancellation", "report-version"} <= set(workflow.serviced_at)
    assert workflow.started_at is not None
    assert workflow.config.watchdog_minutes == 30
