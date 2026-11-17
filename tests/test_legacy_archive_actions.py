import asyncio
import json
from types import SimpleNamespace
from uuid import uuid4

import pytest

from experimenttracker.agent_pool import experiment_owner
from experimenttracker.codex_client import TerminalTurnError
from experimenttracker.legacy_archive_actions import settle_actions


def archived(store, contract=1, phase="training"):
    eid = store.command(uuid4(), "create", {"brief": "saved fixture", "recipe_id": "agent_experiment_v1"})[
        "experiment_id"
    ]
    with store.tx() as db:
        value = json.loads(db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()[0])
        if contract is None:
            value.pop("execution_contract", None)
        else:
            value["execution_contract"] = contract
        db.execute(
            "UPDATE experiments SET contract=?,phase=?,hold_reason='legacy_contract_archived' WHERE id=?",
            (json.dumps(value), phase, eid),
        )
    return eid


def action(store, eid, phase, state="uncertain", thread="thread", turn="turn"):
    identity = str(uuid4())
    now = 1.0
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,?,?,?,?,?,?)",
            (identity, eid, phase, state, thread, turn, "saved prompt", now, now),
        )
    return identity


@pytest.mark.asyncio
async def test_settles_exact_raw_legacy_turn_outcomes_and_replays(runtime, monkeypatch):
    store, _, agent, workflow = runtime
    eid = archived(store, None)
    completed = action(store, eid, "prepare")
    failed = action(store, eid, "interpret")
    unreadable = action(store, eid, "title")
    missing = action(store, eid, "monitor:daily", "running", thread=None, turn=None)
    terminal = archived(store, 2, "failed")
    terminal_action = action(store, terminal, "monitor:terminal")
    current = archived(store, 3)
    current_action = action(store, current, "prepare")
    discussion = action(store, eid, "discuss:1")
    live = action(store, eid, "publish")
    workflow.agent_task = asyncio.create_task(asyncio.Event().wait())
    # The reservation, not a lane field, is what marks this saved turn as live.
    workflow.agent_pool.claim(
        role="stage",
        source_table="agent_actions",
        row=store.one("SELECT * FROM agent_actions WHERE id=?", (live,)),
        owner=experiment_owner(eid),
        workflow=workflow,
    )
    workflow.agent_ready = True
    calls = []

    async def read(thread, turn):
        calls.append((thread, turn))
        if thread == "thread-fail":
            raise TerminalTurnError("turn failed")
        if thread == "thread-read-error":
            raise RuntimeError("read unavailable")
        return {"malformed": [1, 2]}

    for identity, thread in ((failed, "thread-fail"), (unreadable, "thread-read-error")):
        store.db.execute("UPDATE agent_actions SET thread_id=? WHERE id=?", (thread, identity))
    monkeypatch.setattr(agent, "read_turn", read)
    try:
        await settle_actions(workflow)
        assert store.one("SELECT state,result FROM agent_actions WHERE id=?", (completed,)) == {
            "state": "completed",
            "result": '{"malformed":[1,2]}',
        }
        assert store.one("SELECT state,error FROM agent_actions WHERE id=?", (failed,)) == {
            "state": "failed",
            "error": "turn failed",
        }
        assert store.one("SELECT state,error FROM agent_actions WHERE id=?", (unreadable,)) == {
            "state": "uncertain",
            "error": "read unavailable",
        }
        assert store.one("SELECT state FROM agent_actions WHERE id=?", (missing,))["state"] == "running"
        assert (
            store.one("SELECT state FROM agent_actions WHERE id=?", (terminal_action,))["state"]
            == "completed"
        )
        assert store.experiment(terminal)["phase"] == "failed"
        assert store.experiment(terminal)["hold_reason"] == "legacy_contract_archived"
        assert (
            store.one("SELECT state FROM agent_actions WHERE id=?", (current_action,))["state"] == "uncertain"
        )
        assert store.one("SELECT state FROM agent_actions WHERE id=?", (discussion,))["state"] == "uncertain"
        assert store.one("SELECT state FROM agent_actions WHERE id=?", (live,))["state"] == "uncertain"
        event_count = store.one("SELECT COUNT(*) AS n FROM events WHERE experiment_id=?", (eid,))["n"]
        await settle_actions(workflow)
        assert store.one("SELECT COUNT(*) AS n FROM events WHERE experiment_id=?", (eid,))["n"] == event_count
        assert sorted(calls) == sorted(
            [
                ("thread", "turn"),
                ("thread-fail", "turn"),
                ("thread-read-error", "turn"),
                ("thread", "turn"),
                ("thread-read-error", "turn"),
            ]
        )
    finally:
        workflow.agent_task.cancel()
        await asyncio.gather(workflow.agent_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_drains_only_completed_checks_for_active_archived_legacy(runtime):
    store, _, _, workflow = runtime
    eid = archived(store, 2)
    current = archived(store, 3)
    terminal = archived(store, 1, "failed")
    loop = asyncio.get_running_loop()
    done = loop.create_future()
    done.set_result("discard")
    failed = loop.create_future()
    failed.set_exception(RuntimeError("discard failure"))
    waiting = loop.create_future()
    current_check = loop.create_future()
    current_check.set_result("keep")
    terminal_check = loop.create_future()
    terminal_check.set_result("keep")
    intent = str(uuid4())
    stage = store.one("SELECT id FROM stages WHERE experiment_id=? LIMIT 1", (eid,))["id"]
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,snapshot_hash,created_at) "
            "VALUES(?,?,?,'submitted','script','{}','snapshot',1)",
            (intent, eid, stage),
        )
    workflow.evidence_checks[(eid, 1)] = done
    workflow.evidence_checks[(eid, 2)] = failed
    workflow.evidence_checks[(eid, 3)] = waiting
    workflow.evidence_checks[(current, 1)] = current_check
    workflow.evidence_checks[(terminal, 1)] = terminal_check
    workflow.output_checks[intent] = (done, None)
    await settle_actions(workflow)
    assert set(workflow.evidence_checks) == {(eid, 3), (current, 1), (terminal, 1)}
    assert workflow.output_checks == {}
    assert not waiting.done()
    waiting.cancel()


@pytest.mark.asyncio
async def test_readback_uses_a_configured_connection_without_starting_a_turn(runtime):
    store, _, _, workflow = runtime
    eid = archived(store, 1)
    identity = action(store, eid, "title")
    calls = []

    async def read(thread, turn):
        calls.append((thread, turn))
        return {"title": "saved"}

    lane = SimpleNamespace(agent_ready=True, agent=SimpleNamespace(read_turn=read))
    workflow.agent_ready = False
    workflow.slots.append(lane)
    await settle_actions(workflow)
    assert calls == [("thread", "turn")]
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (identity,))["state"] == "completed"

    # Without a configured connection the saved turn stays uncertain.
    second = archived(store, 1)
    pending = action(store, second, "monitor:ready")
    lane.agent_ready = False
    await settle_actions(workflow)
    assert store.one("SELECT state FROM agent_actions WHERE id=?", (pending,))["state"] == "uncertain"
