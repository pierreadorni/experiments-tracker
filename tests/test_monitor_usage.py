"""Regression coverage for the first live controller's duplicate monitor chains.

These use saved synthetic telemetry and stubbed inference, never live jobs.
"""

import asyncio
import json
import time
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from test_controller import create

from experimenttracker.store import encode


def running_job(store, wf, eid, *, progress=True, age=0):
    now, identity = time.time(), str(uuid4())
    root = wf.root(eid)
    root.mkdir(parents=True, exist_ok=True)
    store.update(eid, phase="training", thread_id="research-thread", model="gpt-6.1-sol")
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,updated_at) VALUES(?,'train','running',?)",
            (eid, now),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,snapshot_hash,created_at,observed_at,scheduler_state,progress,execution_root) VALUES(?,?,'train','submitted','123','hash','{}','hash',?,?,'RUNNING',?,?)",
            (
                identity,
                eid,
                now - age,
                now,
                encode({"reported_at": now - age, "completed": 3, "total": 10}) if progress else None,
                str(root),
            ),
        )
    return identity


def due(store, wake):
    with store.tx() as db:
        db.execute("UPDATE agent_wakeups SET due_at=0 WHERE id=?", (wake["id"],))


async def test_monitor_tool_and_completion_create_only_one_followup(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    running_job(store, wf, eid)
    action = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,result,attempts,created_at,updated_at) VALUES(?,?,'monitor',1,'completed','prompt',?,1,0,0)",
            (action, eid, encode({"summary": "Progress is normal", "next_check_seconds": 60})),
        )
    wake = wf.controller.schedule(eid, uuid4(), 60, "Initial check")
    with store.tx() as db:
        db.execute("UPDATE agent_wakeups SET state='queued',action_id=? WHERE id=?", (action, wake["id"]))
    first = wf.controller.schedule(eid, uuid4(), 300, "Agent follow-up", action)
    next_request = uuid4()
    second = wf.controller.schedule(eid, next_request, 300, "Updated follow-up", action)
    wf.controller.server = object()
    try:
        await wf.controller.advance()
        pending = store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=? AND state='pending'", (eid,))
        assert [w["id"] for w in pending] == [second["id"]]
        assert (
            wf.controller.schedule(eid, next_request, 300, "Updated follow-up", action)["id"] == second["id"]
        )
        assert (
            store.one("SELECT state FROM agent_wakeups WHERE id=?", (first["id"],))["state"] == "superseded"
        )
    finally:
        wf.controller.server = None
        await wf.close()


async def test_healthy_and_uninstrumented_jobs_make_zero_monitor_calls(runtime):
    store, _, _, wf = runtime
    wf.controller.server = object()
    wf.agent_ready = True
    wf.dispatch = AsyncMock()
    try:
        for progress in (True, False):
            eid = create(store)
            running_job(store, wf, eid, progress=progress)
            for _ in range(4):
                wake = wf.controller.schedule(eid, uuid4(), 60, "Backend check")
                due(store, wake)
                await wf.controller.advance()
            assert not store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
        wf.dispatch.assert_not_called()
    finally:
        wf.controller.server = None
        await wf.close()


async def test_old_wakeup_backlog_coalesces_without_discarding_queued_turn(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    running_job(store, wf, eid)
    with store.tx() as db:
        for _ in range(8):
            db.execute(
                "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,created_at) VALUES(?,?,?,0,0)",
                (str(uuid4()), eid, encode({"reason": "Old follow-up"})),
            )
    wf.controller.server = object()
    try:
        await wf.controller.advance()
        assert len(store.rows("SELECT * FROM agent_wakeups WHERE state='pending'")) == 1
        assert len(store.rows("SELECT * FROM agent_wakeups WHERE state='superseded'")) == 7
        assert not store.rows("SELECT * FROM agent_actions")
    finally:
        wf.controller.server = None
        await wf.close()


async def test_stale_telemetry_requires_fresh_scheduler_and_is_latched_across_restart(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    identity = running_job(store, wf, eid, age=1800)
    jobs = wf.controller.diagnostic_jobs(eid)
    assert [j["intent_id"] for j in jobs] == [identity]
    action = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,attempts,created_at,updated_at) VALUES(?,?,'monitor',1,'completed',?,1,0,0)",
            (action, eid, "Diagnosis\nPAYLOAD:\n" + encode({"jobs": jobs})),
        )
    # A new controller reads saved diagnostics, rather than resetting its latch.
    from experimenttracker.controller import Controller

    assert Controller(wf).diagnostic_jobs(eid) == []
    with store.tx() as db:
        db.execute(
            "UPDATE jobs SET progress=?,observed_at=0 WHERE intent_id=?",
            (
                encode({"reported_at": time.time() - 1800, "completed": 4, "total": 10}),
                identity,
            ),
        )
    assert Controller(wf).diagnostic_jobs(eid) == []
    with store.tx() as db:
        db.execute("UPDATE jobs SET observed_at=? WHERE intent_id=?", (time.time(), identity))
    assert Controller(wf).diagnostic_jobs(eid)
    wf.config.codex.monitor_max_turns_per_job = 1
    assert Controller(wf).diagnostic_jobs(eid) == []
    await wf.close()


async def test_recent_stdout_prevents_false_stall(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    identity = running_job(store, wf, eid, age=1800)
    (wf.root(eid) / f"et-{identity}-123.out").write_text("A new epoch completed\n")
    assert not wf.controller.diagnostic_jobs(eid)
    await wf.close()


async def test_sustained_stall_dispatches_one_small_diagnostic(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    identity = running_job(store, wf, eid, age=1800)
    wf.controller.server = object()
    wf.agent_ready = True
    wf.gate.decide = lambda: SimpleNamespace(admitted=True)
    wf.dispatch = AsyncMock()
    wake = wf.controller.schedule(eid, uuid4(), 60, "Inspect stale progress")
    due(store, wake)
    try:
        await wf.controller.advance()
        await asyncio.sleep(0)
        wf.dispatch.assert_awaited_once()
        action = wf.dispatch.call_args.args[0]
        payload = json.loads(action["input"].split("\nPAYLOAD:\n", 1)[1])
        assert [j["intent_id"] for j in payload["jobs"]] == [identity]
        assert "brief" not in payload and len(action["input"]) < 2000
        await wf.controller.advance()
        wf.dispatch.assert_awaited_once()
    finally:
        wf.controller.server = None
        await wf.close()


async def test_monitor_uses_small_fresh_thread_and_preserves_research_session(runtime):
    store, _, agent, wf = runtime
    eid = create(store)
    running_job(store, wf, eid, age=1800)
    agent.session = AsyncMock(return_value=("diagnostic-thread", "gpt-6.1-sol"))
    agent.begin = AsyncMock(return_value="diagnostic-turn")
    agent.finish = AsyncMock(
        return_value={"summary": "Logs stopped; cause unknown", "next_check_seconds": 60}
    )
    wf.refresh_quota = AsyncMock(return_value=SimpleNamespace(admitted=True))
    action = str(uuid4())
    prompt = "Diagnosis\nPAYLOAD:\n" + encode({"jobs": wf.controller.diagnostic_jobs(eid)})
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) VALUES(?,?,'monitor',1,'pending',?,0,0)",
            (action, eid, prompt),
        )
    await wf.dispatch(store.one("SELECT * FROM agent_actions WHERE id=?", (action,)))
    assert agent.session.call_args.args[0] is None
    assert "writable" not in agent.session.call_args.kwargs
    assert agent.begin.call_args.args[1] == prompt
    assert store.experiment(eid)["thread_id"] == "research-thread"
    assert (
        store.one("SELECT thread_id FROM agent_actions WHERE id=?", (action,))["thread_id"]
        == "diagnostic-thread"
    )
    await wf.close()


async def test_live_token_notifications_are_saved_only_for_exact_turn(runtime):
    store, _, _, wf = runtime
    eid, identity = create(store), str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) VALUES(?,?,'prepare',1,'running','thread','turn','prompt',0,0)",
            (identity, eid),
        )
    wf.active_action = identity
    message = {
        "method": "thread/tokenUsage/updated",
        "params": {
            "threadId": "thread",
            "turnId": "other",
            "tokenUsage": {
                "total": {"inputTokens": 100, "cachedInputTokens": 80, "secret": "excluded"},
                "last": {"outputTokens": 10},
            },
        },
    }
    await wf._activity(message)
    assert not store.rows("SELECT * FROM agent_usage")
    message["params"]["turnId"] = "turn"
    revision = store.experiment(eid)["revision"]
    publications = store.one("SELECT COUNT(*) AS n FROM outbox")["n"]
    await wf._activity(message)
    record = store.one("SELECT * FROM agent_usage")
    usage = json.loads(record["payload"])
    assert (record["action_id"], record["thread_id"], record["turn_id"]) == (identity, "thread", "turn")
    assert usage["thread_total"] == {"inputTokens": 100, "cachedInputTokens": 80}
    assert usage["last_request"] == {"outputTokens": 10}
    assert store.experiment(eid)["revision"] == revision
    assert store.one("SELECT COUNT(*) AS n FROM outbox")["n"] == publications
    message["params"]["tokenUsage"]["total"]["inputTokens"] = 200
    await wf._activity(message)
    assert store.one("SELECT COUNT(*) AS n FROM agent_usage")["n"] == 1
    assert (
        json.loads(store.one("SELECT payload FROM agent_usage")["payload"])["thread_total"]["inputTokens"]
        == 200
    )
    wf.active_action = None
    await wf.close()
