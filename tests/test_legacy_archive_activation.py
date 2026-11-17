import asyncio
import json
from uuid import uuid4

import pytest

from experimenttracker import legacy_archive
from experimenttracker.agent_pool import experiment_owner
from experimenttracker.core import liveness
from experimenttracker.legacy_archive_actions import settle_actions
from experimenttracker.store import Conflict, encode
from experimenttracker.watchdog import engine_watchdog


def legacy_experiment(store):
    eid = store.command(
        uuid4(), "create", {"brief": "synthetic archive activation", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    exp = store.experiment(eid)
    contract = {**exp["contract"], "execution_contract": 2}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=?,phase='training' WHERE id=?", (encode(contract), eid))
    return eid


def test_sweep_freezes_legacy_and_store_guards_late_mutations(runtime):
    store, _, _, _ = runtime
    eid = legacy_experiment(store)
    store.archive_legacy_experiments()
    assert store.experiment(eid)["hold_reason"] == "legacy_contract_archived"
    store.update(eid, phase="failed", hold_reason=None, error="late worker result")
    exp = store.experiment(eid)
    assert (exp["phase"], exp["hold_reason"], exp["error"]) == (
        "training",
        "legacy_contract_archived",
        "late worker result",
    )
    for kind, payload in (
        ("answer", {"pending_request_id": "pending", "answer": "decline"}),
        ("plan_review", {"decision": "approve", "plan_revision": 1}),
        ("steer", {"text": "continue", "mode": "later"}),
        ("action", {"action": "retry_agent"}),
    ):
        with pytest.raises(Conflict):
            store.command(uuid4(), kind, payload, eid=eid)
    store.command(uuid4(), "action", {"action": "cancel"}, eid=eid)
    assert store.experiment(eid)["phase"] == "cancelled"


def test_frozen_terminal_commands_have_no_effect_but_pause_and_readers_remain(runtime):
    store, _, _, _ = runtime
    eid = legacy_experiment(store)
    store.update(eid, phase="failed", hold_reason="agent_recovery_exhausted")
    before = {
        table: store.one(f"SELECT COUNT(*) AS n FROM {table} WHERE experiment_id=?", (eid,))["n"]
        for table in ("commands", "events", "agent_actions")
    }
    for kind, payload in (
        ("action", {"action": "retry_agent"}),
        ("action", {"action": "summarize_title"}),
        ("steer", {"text": "change the plan", "mode": "later"}),
        ("plan_review", {"decision": "approve", "plan_revision": 1}),
        ("answer", {"pending_request_id": "unbound", "answer": "decline"}),
    ):
        with pytest.raises(Conflict, match="legacy execution is archived"):
            store.command(uuid4(), kind, payload, eid=eid)
    after = {
        table: store.one(f"SELECT COUNT(*) AS n FROM {table} WHERE experiment_id=?", (eid,))["n"]
        for table in before
    }
    assert after == before
    store.command(uuid4(), "action", {"action": "pause_agent"}, eid=eid)
    assert store.experiment(eid)["pause_agent"] is True


def test_watchdog_excludes_archived_owner_hold(runtime):
    store, _, _, _ = runtime
    eid = legacy_experiment(store)
    store.archive_legacy_experiments()
    assert store.experiment(eid)["hold_reason"] == "legacy_contract_archived"
    assert engine_watchdog(store, 10**12, 1, liveness.ServiceEvidence(10**12, {})) == 0


@pytest.mark.asyncio
async def test_tick_settles_saved_turn_before_maintenance_return_without_rpc_reply(runtime, monkeypatch):
    store, _, agent, workflow = runtime
    eid = legacy_experiment(store)
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,'prepare',1,'uncertain','thread','turn','prompt',1,1)",
            (action_id, eid),
        )
    store.update(eid, pending_request={"id": "old", "answer": "decline", "expired": False})
    workflow.started = workflow.agent_ready = True
    workflow.last_scheduler = workflow.last_quota_refresh = workflow.last_agent_start = 10**12
    called = []

    async def noop(*args, **kwargs):
        return None

    async def read(*args):
        return {"unvalidated": True}

    async def reply(*args):
        called.append("reply")

    monkeypatch.setattr(workflow.research.service, "tick", lambda: None)
    monkeypatch.setattr(workflow.research, "reply_inputs", noop)
    monkeypatch.setattr(workflow.research, "interrupt_holds", noop)
    monkeypatch.setattr("experimenttracker.workflow.process_cancellations", noop)
    monkeypatch.setattr("experimenttracker.workflow.early_repair.reconcile_cancellations", noop)
    monkeypatch.setattr(workflow.projects, "tick", lambda: None)
    monkeypatch.setattr(workflow.projects, "settings_tick", lambda: None)
    monkeypatch.setattr(workflow.supervisor.memory, "refresh", lambda: None)
    monkeypatch.setattr(agent, "read_turn", read)
    monkeypatch.setattr(agent, "reply", reply, raising=False)
    monkeypatch.setattr("experimenttracker.workflow.blocked", lambda store: True)
    await workflow.tick()
    assert store.one("SELECT state,result FROM agent_actions WHERE id=?", (action_id,)) == {
        "state": "completed",
        "result": '{"unvalidated":true}',
    }
    assert not called
    assert store.experiment(eid)["pending_request"]["expired"] is False


@pytest.mark.asyncio
async def test_start_sweeps_before_services_and_expires_old_rpc_without_losing_hold(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid = legacy_experiment(store)
    store.update(
        eid,
        pending_request={"id": "old-rpc", "answer": "decline", "expired": False},
    )

    async def refresh():
        assert store.experiment(eid)["hold_reason"] == "legacy_contract_archived"

    monkeypatch.setattr(workflow, "refresh_cluster", refresh)
    monkeypatch.setattr(workflow, "connect_agent", refresh)
    monkeypatch.setattr(workflow.controller, "start", refresh)
    monkeypatch.setattr(workflow, "start_agent_pool", refresh)
    await workflow.start()
    exp = store.experiment(eid)
    assert exp["pending_request"]["expired"] is True
    assert exp["hold_reason"] == "legacy_contract_archived"


@pytest.mark.asyncio
async def test_archived_operator_reconcile_saves_exact_raw_result(runtime, monkeypatch):
    store, _, agent, workflow = runtime
    eid = legacy_experiment(store)
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?, 'prepare',1,'uncertain','thread-exact','turn-exact','saved prompt',1,1)",
            (action_id, eid),
        )
    result = {"legacy": ["raw", 7]}

    async def read(thread_id, turn_id):
        assert (thread_id, turn_id) == ("thread-exact", "turn-exact")
        return result

    monkeypatch.setattr(agent, "read_turn", read)
    store.archive_legacy_experiments()
    before = store.experiment(eid)
    receipt = await workflow.reconcile_action(action_id, turn_id="turn-exact")
    saved = store.one("SELECT state,result FROM agent_actions WHERE id=?", (action_id,))
    assert saved == {"state": "completed", "result": json.dumps(result, separators=(",", ":"))}
    assert receipt["turn_id"] == "turn-exact"
    after = store.experiment(eid)
    assert (after["phase"], after["hold_reason"]) == (before["phase"], before["hold_reason"])
    assert legacy_archive.execution_frozen(after)


@pytest.mark.asyncio
async def test_saved_archived_turn_settles_identity_and_raw_result_without_advancing(runtime, monkeypatch):
    """Replaces the removed ``begin``/``finish`` cross-await flip parametrizations.

    No production path rewrites ``execution_contract``, so the same safety properties
    are proved from a saved legacy row: dispatch never finishes the saved turn,
    settlement stores its raw result verbatim, and neither pass advances a stage,
    dispatches a turn or reads the turn twice.
    """
    store, scheduler, agent, workflow = runtime
    eid = legacy_experiment(store)
    store.archive_legacy_experiments()
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,"
            "input,created_at,updated_at) "
            "VALUES(?,?,'prepare',1,'running','thread-exact','turn-exact','saved prompt',1,1)",
            (action_id, eid),
        )

    def saved_action():
        return store.one("SELECT state,thread_id,turn_id,result FROM agent_actions WHERE id=?", (action_id,))

    def execution_state():
        exp = store.experiment(eid)
        return {
            "experiment": (exp["phase"], exp["hold_reason"], exp["evidence_revision"]),
            "stages": store.rows("SELECT id,state FROM stages WHERE experiment_id=? ORDER BY id", (eid,)),
            "jobs": store.rows(
                "SELECT intent_id,state FROM jobs WHERE experiment_id=? ORDER BY intent_id", (eid,)
            ),
        }

    before = execution_state()
    calls, dispatched = [], []

    async def read(thread, turn):
        assert (thread, turn) == ("thread-exact", "turn-exact")
        calls.append((thread, turn))
        return {"unvalidated": ["raw", 7]}

    async def refused(*args, **kwargs):
        dispatched.append(args)
        raise AssertionError("an archived record must not start or finish a turn")

    monkeypatch.setattr(agent, "read_turn", read)
    monkeypatch.setattr(agent, "begin", refused)
    monkeypatch.setattr(agent, "finish", refused)
    workflow.agent_ready = True
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
    assert await workflow.dispatch(action, workflow.agent_pool.primary) is None
    assert calls == [] and dispatched == [] and scheduler.submissions == 0
    # The saved identity stays exactly as returned and is never finished by dispatch.
    assert saved_action() == {
        "state": "running",
        "thread_id": "thread-exact",
        "turn_id": "turn-exact",
        "result": None,
    }
    await settle_actions(workflow)
    assert saved_action() == {
        "state": "completed",
        "thread_id": "thread-exact",
        "turn_id": "turn-exact",
        "result": '{"unvalidated":["raw",7]}',
    }
    assert calls == [("thread-exact", "turn-exact")]
    assert dispatched == [] and scheduler.submissions == 0
    assert execution_state() == before
    # A second settlement pass reads nothing again: no duplicate dispatch or readback.
    await settle_actions(workflow)
    assert calls == [("thread-exact", "turn-exact")]
    assert dispatched == [] and scheduler.submissions == 0


@pytest.mark.asyncio
async def test_archived_controller_turn_allows_status_and_rejects_effects(runtime):
    store, _, _, workflow = runtime
    eid = legacy_experiment(store)
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
            "VALUES(?,?,'prepare',1,'running','saved prompt',1,1)",
            (action_id, eid),
        )
    store.archive_legacy_experiments()
    assert await workflow.controller.call(eid, "job_status", {"summary_only": True}) == []
    with pytest.raises(ValueError, match="only status reads"):
        await workflow.controller.call(eid, "schedule_wakeup", {})


@pytest.mark.parametrize(
    ("kind", "payload"),
    [
        ("action", {"action": "retry_agent"}),
        ("action", {"action": "summarize_title"}),
        ("steer", {"text": "continue", "mode": "later"}),
        ("plan_review", {"decision": "approve", "plan_revision": 1}),
        ("plan_review", {"decision": "revise", "plan_revision": 1, "feedback": "change"}),
    ],
)
def test_terminal_frozen_commands_reject_legacy_execution_mutations(runtime, kind, payload):
    store, _, _, _ = runtime
    eid = legacy_experiment(store)
    contract = {**store.experiment(eid)["contract"], "execution_contract": 2}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=?,phase='complete' WHERE id=?", (encode(contract), eid))
    with pytest.raises(Conflict, match="legacy execution is archived"):
        store.command(uuid4(), kind, payload, eid=eid)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "binding", ["valid", "wrong_action", "wrong_phase", "wrong_state", "expired", "not_ready"]
)
async def test_frozen_reader_reply_requires_live_owned_discussion(runtime, monkeypatch, binding):
    store, _, agent, workflow = runtime
    eid = legacy_experiment(store)
    action_id = str(uuid4())
    phase = "prepare" if binding == "wrong_phase" else "discuss:1"
    state = "completed" if binding == "wrong_state" else "running"
    contract = {**store.experiment(eid)["contract"], "execution_contract": 2}
    pending = {
        "id": "rpc",
        "action_id": str(uuid4()) if binding == "wrong_action" else action_id,
        "answer": "continue discussion",
        "expired": binding == "expired",
    }
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,?,1,?,'thread','turn','prompt',1,1)",
            (action_id, eid, phase, state),
        )
        db.execute(
            "UPDATE experiments SET contract=?,phase='complete',pending_request=? WHERE id=?",
            (encode(contract), encode(pending), eid),
        )
    workflow.started = True
    workflow.agent_ready = binding != "not_ready"
    workflow.active_action = action_id
    workflow.agent_task = asyncio.create_task(asyncio.Event().wait())
    reservation = workflow.agent_pool.claim(
        role="conversation",
        source_table="agent_actions",
        row=store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,)),
        owner=experiment_owner(eid),
        slot=workflow.agent_pool.primary,
    )
    # A live answer is bound to the process whose request asked for it.
    workflow.agent_pool.remember_answer(
        pending["id"],
        workflow.agent_pool.connection(workflow.agent_pool.primary, workflow.agent.start_count),
        reservation,
    )
    calls = []
    turns = []
    jobs_before = store.rows("SELECT intent_id FROM jobs WHERE experiment_id=?", (eid,))

    async def noop(*args, **kwargs):
        return None

    async def reply(value):
        calls.append(value)

    async def begin(*args, **kwargs):
        turns.append("begin")
        return "unexpected-turn"

    monkeypatch.setattr(agent, "reply", reply, raising=False)
    monkeypatch.setattr(agent, "begin", begin, raising=False)
    monkeypatch.setattr(workflow.research.service, "tick", lambda: None)
    monkeypatch.setattr(workflow.research, "reply_inputs", noop)
    monkeypatch.setattr(workflow.research, "interrupt_holds", noop)
    monkeypatch.setattr("experimenttracker.workflow.process_cancellations", noop)
    monkeypatch.setattr("experimenttracker.workflow.early_repair.reconcile_cancellations", noop)
    monkeypatch.setattr(workflow.projects, "tick", lambda: None)
    monkeypatch.setattr(workflow.projects, "settings_tick", lambda: None)
    monkeypatch.setattr(workflow.supervisor.memory, "refresh", lambda: None)
    monkeypatch.setattr("experimenttracker.workflow.blocked", lambda store: True)
    try:
        await workflow.tick()
    finally:
        workflow.agent_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await workflow.agent_task
    assert len(calls) == (1 if binding == "valid" else 0)
    assert turns == []
    assert store.rows("SELECT intent_id FROM jobs WHERE experiment_id=?", (eid,)) == jobs_before
    assert store.experiment(eid)["phase"] == "complete"
    if binding == "valid":
        assert calls[0]["action_id"] == action_id
        assert store.experiment(eid)["pending_request"] is None
