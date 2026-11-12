"""Interim C2 watchdog acceptance over the existing tables; no live resources are used."""

import time
from uuid import uuid4

import pytest

from experimenttracker.store import Store
from experimenttracker.watchdog import engine_watchdog, stuck_experiments


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
    monkeypatch.setattr("experimenttracker.store.time.time", clock)
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


def action(store, eid, state):
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at)"
            " VALUES(?,?,'prepare',1,?,'{}',0,0)",
            (str(uuid4()), eid, state),
        )


def job(store, eid, state):
    store.stage(eid, "prepare", "running")
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,snapshot_hash,"
            "created_at) VALUES(?,?,'prepare',1,?,'synthetic','{}','synthetic',0)",
            (str(uuid4()), eid, state),
        )


def transfer(store, eid, state):
    with store.tx() as db:
        db.execute(
            "INSERT INTO execution_transfers(id,experiment_id,direction,payload,state,updated_at)"
            " VALUES(?,?,'up','{}',?,0)",
            (str(uuid4()), eid, state),
        )


def test_hold_unchanged_for_the_window_reports_once(store, clock):
    eid = create(store)
    clock.advance(29)
    assert engine_watchdog(store, clock.now, 30) == 0
    clock.advance(2)
    assert engine_watchdog(store, clock.now, 30) == 1
    clock.advance(60)
    assert engine_watchdog(store, clock.now, 30) == 0
    rows = notices(store, eid)
    assert [row["kind"] for row in rows] == ["engine_stuck"]
    assert rows[0]["text"].startswith("agent_stage_blocked: ")


def test_notice_survives_an_owner_restart(store, clock):
    eid = create(store)
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 30) == 1
    restarted = Store(store.path)
    try:
        clock.advance(31)
        assert engine_watchdog(restarted, clock.now, 30) == 0
        assert len(notices(restarted, eid)) == 1
    finally:
        restarted.close()


def test_configured_minutes_widen_the_window(store, clock):
    create(store)
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 60) == 0
    clock.advance(30)
    assert engine_watchdog(store, clock.now, 60) == 1


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
def test_owner_owned_holds_are_not_stuck(store, clock, hold_reason):
    create(store, hold_reason)
    clock.advance(120)
    assert stuck_experiments(store, clock.now, 30) == []
    assert engine_watchdog(store, clock.now, 30) == 0


@pytest.mark.parametrize("phase", ["complete", "failed", "cancelled"])
def test_terminal_phases_are_not_stuck(store, clock, phase):
    create(store, phase=phase)
    clock.advance(120)
    assert engine_watchdog(store, clock.now, 30) == 0


def test_deleted_experiments_are_not_stuck(store, clock):
    eid = create(store)
    with store.tx() as db:
        db.execute("UPDATE experiments SET deleted_at=? WHERE id=?", (clock.now, eid))
    clock.advance(120)
    assert engine_watchdog(store, clock.now, 30) == 0


@pytest.mark.parametrize("state", ["pending", "dispatching", "running"])
def test_live_agent_actions_keep_an_experiment_going(store, clock, state):
    eid = create(store)
    action(store, eid, state)
    clock.advance(120)
    assert stuck_experiments(store, clock.now, 30) == []


@pytest.mark.parametrize("state", ["submitting", "submitted"])
def test_live_jobs_keep_an_experiment_going(store, clock, state):
    eid = create(store)
    job(store, eid, state)
    clock.advance(120)
    assert stuck_experiments(store, clock.now, 30) == []


def test_pending_transfer_keeps_an_experiment_going(store, clock):
    eid = create(store)
    transfer(store, eid, "pending")
    clock.advance(120)
    assert stuck_experiments(store, clock.now, 30) == []


def test_settled_work_leaves_an_experiment_stuck(store, clock):
    eid = create(store)
    action(store, eid, "completed")
    job(store, eid, "complete")
    transfer(store, eid, "complete")
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 30) == 1


def test_pending_request_is_owner_work(store, clock):
    eid = create(store)
    store.update(eid, pending_request={"id": "approval"})
    clock.advance(120)
    assert engine_watchdog(store, clock.now, 30) == 0


def test_revision_change_restarts_the_window(store, clock):
    eid = create(store)
    clock.advance(20)
    store.update(eid, analysis={"summary": "Synthetic progress"})
    clock.advance(20)
    assert engine_watchdog(store, clock.now, 30) == 0
    clock.advance(11)
    assert engine_watchdog(store, clock.now, 30) == 1


def test_hold_change_restarts_the_window(store, clock):
    eid = create(store, "automatic_repair")
    clock.advance(20)
    store.update(eid, hold_reason="automatic_repair_pending")
    clock.advance(20)
    assert engine_watchdog(store, clock.now, 30) == 0
    clock.advance(11)
    assert engine_watchdog(store, clock.now, 30) == 1


def test_cleared_hold_is_reported_without_a_hold_reason(store, clock):
    eid = create(store, hold_reason=None)
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 30) == 1
    rows = notices(store, eid)
    assert rows[0]["text"].startswith("no_hold: ")


def test_one_notice_per_hold_reason_when_the_error_changes(store, clock):
    eid = create(store)
    store.update(eid, error="first failure")
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 30) == 1
    first = notices(store, eid)
    assert "agent_stage_blocked: first failure." in first[0]["text"]
    store.update(eid, error="second failure")
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 30) == 0
    assert notices(store, eid) == first


def test_distinct_hold_reasons_report_separately(store, clock):
    eid = create(store, "automatic_repair")
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 30) == 1
    store.update(eid, hold_reason="automatic_repair_pending")
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 30) == 1
    texts = [row["text"] for row in notices(store, eid)]
    assert [text.split(":", 1)[0] for text in texts] == ["automatic_repair", "automatic_repair_pending"]


def test_stuck_report_logs_a_warning_once(store, clock, caplog):
    create(store)
    clock.advance(31)
    assert engine_watchdog(store, clock.now, 30) == 1
    assert engine_watchdog(store, clock.now, 30) == 0
    warnings = [record for record in caplog.records if record.levelname == "WARNING"]
    assert len(warnings) == 1 and "stuck in agent_stage_blocked" in warnings[0].getMessage()


async def test_tick_reports_a_stuck_hold_after_the_advance_loop(runtime, clock):
    store, _, _, workflow = runtime
    eid = create(store)
    clock.advance(31)
    await workflow.tick()
    assert [row["kind"] for row in notices(store, eid)] == ["engine_stuck"]
    assert workflow.config.watchdog_minutes == 30
