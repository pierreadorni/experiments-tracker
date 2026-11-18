import pytest
from test_research_follow_up import fake_clock
from test_research_settlement import setup
from test_research_wakeups import saved_job

from experimenttracker.research import wakeups
from experimenttracker.research.session import follow_up_turn
from experimenttracker.research.settlement import complete
from experimenttracker.store import Store

WAIT = {"status": "wait", "summary": "Waiting for a timed follow-up", "next": "timer:120"}


async def test_exact_timer_survives_reopen_and_binds_one_turn(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    now = fake_clock(monkeypatch)
    assert (await complete(workflow, action, WAIT))["state"] == "completed"
    reopened = Store(store.path)
    try:
        now.advance(119)
        assert not follow_up_turn(reopened, eid)
        now.advance(1)
        follow_up = follow_up_turn(reopened, eid)
        assert follow_up["thread_id"] == action["thread_id"]
        assert not follow_up_turn(reopened, eid)
        wake = reopened.one("SELECT state,action_id FROM agent_wakeups WHERE experiment_id=?", (eid,))
        assert wake == {"state": "queued", "action_id": follow_up["id"]}
    finally:
        reopened.close()


async def test_terminal_job_wakes_before_and_supersedes_long_timer(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    now = fake_clock(monkeypatch)
    assert (await complete(workflow, action, WAIT))["state"] == "completed"
    job = saved_job(store, eid)
    with store.tx() as db:
        db.execute("UPDATE jobs SET job_id='42' WHERE intent_id=?", (job,))
    wakeups.record(store, eid, wakeups.JOB_TERMINAL, job)
    now.advance(60)
    assert follow_up_turn(store, eid)
    identity = wakeups.wakeup_id(eid, wakeups.RESEARCH_TIMER, action["id"])
    assert store.one("SELECT state FROM agent_wakeups WHERE id=?", (identity,)) == {"state": "superseded"}


async def test_timer_failure_rolls_back_settlement(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    record = wakeups.record

    def fail_after_record(*args, **kwargs):
        record(*args, **kwargs)
        raise RuntimeError("synthetic timer write failure")

    monkeypatch.setattr(wakeups, "record", fail_after_record)
    with pytest.raises(RuntimeError, match="timer write failure"):
        await complete(workflow, action, WAIT)
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (action["id"],)) == {"state": "running"}
    assert not store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (eid,))
    monkeypatch.setattr(wakeups, "record", record)
    assert (await complete(workflow, action, WAIT))["state"] == "completed"
