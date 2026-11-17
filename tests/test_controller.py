"""Autonomous recovery acceptance with normal Python packages and saved runtimes.

These use the real local CPU scheduler and deterministic agents; they do not
claim managed inference, real Slurm scientific success or physical-phone access.
"""

import asyncio
import json
import threading
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest
from helpers import IterationAgent, wait_until
from helpers import approve_plan as approve_current_plan
from helpers import until as until_current

from experimenttracker.api import create_app
from experimenttracker.codex_client import NeedsAttention, TerminalTurnError
from experimenttracker.store import Conflict, Store
from experimenttracker.workflow import Workflow


def create(store):
    return store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]


def failed_prepare(store, eid):
    return store.one(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='prepare' AND state='failed'", (eid,)
    )


class PermissionAgent(IterationAgent):
    """Iteration agent that keeps the plan read-only and prepare workspace writable."""

    async def session(self, thread_id, workspace, **kwargs):
        self.writable = kwargs.get("writable", False)
        return await super().session(thread_id, workspace, **kwargs)

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        phase = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"]
        if phase == "plan":
            assert not self.writable
        elif phase == "prepare":
            assert self.writable and workspace == self.workspace
        return await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)


async def test_slow_shared_backup_keeps_owner_commands_and_event_loop_responsive(runtime, monkeypatch):
    store, _, _, wf = runtime
    eid = create(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    entered, release = threading.Event(), threading.Event()
    from experimenttracker import store as store_module

    copyfile = store_module.shutil.copyfile
    copies = []

    def slow_copy(source, destination):
        copies.append((source, destination))
        entered.set()
        assert release.wait(5)
        return copyfile(source, destination)

    monkeypatch.setattr(store_module.shutil, "copyfile", slow_copy)
    try:
        await wf.tick()
        first = wf.backup_task
        assert await asyncio.to_thread(entered.wait, 3)
        # Shared copy is still blocked, while the writer commits and the owner
        # ticks without starting an overlapping backup or waiting on that copy.
        async with asyncio.timeout(2):
            store.command(uuid4(), "note", {"text": "Owner remains responsive"}, eid=eid)
            wf.last_backup = 0
            await wf.tick()
        assert wf.backup_task is first and not first.done()
        assert len(copies) == 1
        release.set()

        await first
        recovered = Store.read_only(wf.config.backup_dir / "workflow.sqlite")
        try:
            assert recovered.one("PRAGMA integrity_check")["integrity_check"] == "ok"
            assert not recovered.rows("PRAGMA foreign_key_check")
            assert recovered.experiment(eid)["id"] == eid
            assert not recovered.rows("SELECT 1 FROM notes WHERE experiment_id=?", (eid,))
        finally:
            recovered.close()
        await wf.close()
        recovered = Store.read_only(wf.config.backup_dir / "workflow.sqlite")
        try:
            assert (
                recovered.one("SELECT text FROM notes WHERE experiment_id=?", (eid,))["text"]
                == "Owner remains responsive"
            )
        finally:
            recovered.close()
    finally:
        release.set()


async def test_slow_project_catalog_does_not_block_health_or_owner_loop(runtime, monkeypatch):
    store, _, _, wf = runtime
    app = create_app(wf.config, components=(None, store, wf))
    entered, release = threading.Event(), threading.Event()
    catalog = wf.projects.catalog

    def slow_catalog():
        entered.set()
        assert release.wait(5)
        return catalog()

    async with app.router.lifespan_context(app):
        monkeypatch.setattr(wf.projects, "catalog", slow_catalog)
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://127.0.0.1") as client:
            task = asyncio.create_task(
                client.get(
                    "/api/status",
                    headers={"Authorization": "Bearer " + wf.config.api_token_file.read_text().strip()},
                )
            )
            try:
                assert await asyncio.to_thread(entered.wait, 3)
                async with asyncio.timeout(1):
                    health = await client.get("/healthz")
                assert health.status_code == 200 and health.json()["ok"]
                assert not task.done()
            finally:
                release.set()
            response = await task
            assert response.status_code == 200 and response.json()["projects"]


@pytest.mark.parametrize("lost_response", [False, True])
@pytest.mark.parametrize("terminal", [False, True])
async def test_saved_monitor_turn_is_adopted_after_restart_without_duplicate_dispatch(
    runtime, lost_response, terminal
):
    store, scheduler, agent, wf = runtime
    eid = create(store)
    action_id, wake_id = str(uuid4()), str(uuid4())
    result = {"summary": "Saved monitor check", "next_check_seconds": 900}
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,attempts,created_at,updated_at) VALUES(?,?,'monitor',1,'running','saved-thread','saved-monitor','prompt',1,0,0)",
            (action_id, eid),
        )
        db.execute(
            "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,state,action_id,created_at) VALUES(?,?,'{}',0,'queued',?,0)",
            (wake_id, eid, action_id),
        )
    reads = []
    if terminal:
        store.update(eid, phase="complete")

    async def read_turn(thread, turn):
        reads.append((thread, turn))
        if lost_response:
            raise NeedsAttention("saved turn outcome unavailable")
        return result

    agent.read_turn = read_turn
    wf.agent_ready = True
    wf.controller.server = object()
    try:
        await wf.controller.advance()
        action = store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
        assert reads == [("saved-thread", "saved-monitor")]
        assert action["state"] == ("uncertain" if lost_response else "completed")
        assert action["attempts"] == 1 and action["turn_id"] == "saved-monitor"
        if lost_response:
            agent.read_turn = lambda *_: asyncio.sleep(0, result=result)
            await wf.reconcile_action(action_id)
        await wf.controller.advance()
        assert store.one("SELECT state FROM agent_wakeups WHERE id=?", (wake_id,))["state"] == "complete"
        assert len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))) == 1
        assert scheduler.submissions == 0 and agent.calls == 0
    finally:
        wf.controller.server = None
        await wf.close()


async def test_monitor_action_obligation_tracks_creation_and_pending_supersession(runtime, monkeypatch):
    store, _, _, wf = runtime
    eid = create(store)
    store.update(eid, phase="training")
    wake_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,state,created_at) "
            "VALUES(?,?,?,0,'pending',0)",
            (wake_id, eid, json.dumps({"reason": "synthetic telemetry gap"})),
        )
    wf.controller.server = object()
    wf.agent_ready = True
    monkeypatch.setattr(wf.controller, "diagnostic_jobs", lambda _: [{"intent_id": "synthetic-job"}])
    monkeypatch.setattr(wf.gate, "decide", lambda: SimpleNamespace(admitted=True))

    async def no_dispatch(_action):
        return None

    monkeypatch.setattr(wf, "dispatch", no_dispatch)
    try:
        await wf.controller.advance()
        action = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
        assert action["phase"] == "monitor" and action["state"] == "pending"
        obligation_id = f"agent-action:{action['id']}:0"
        assert store.one("SELECT state FROM obligations WHERE id=?", (obligation_id,))["state"] == "open"
        assert store.one("SELECT kind FROM events WHERE kind='agent_wakeup_queued'")
        if wf.agent_task:
            await wf.agent_task
        wf.agent_task = wf.active_action = None
        with store.tx() as db:
            db.execute("UPDATE experiments SET phase='complete' WHERE id=?", (eid,))
        await wf.controller.advance()
        assert (
            store.one("SELECT state FROM agent_actions WHERE id=?", (action["id"],))["state"] == "superseded"
        )
        assert store.one("SELECT state FROM obligations WHERE id=?", (obligation_id,))["state"] == "abandoned"
        assert store.one(
            "SELECT 1 FROM events WHERE kind='agent_action_changed' AND json_extract(payload,'$.action_id')=?",
            (action["id"],),
        )
    finally:
        wf.controller.server = None


async def test_manual_planning_retry_has_same_revision_and_duplicate_receipt(runtime):
    store, scheduler, _, wf = runtime
    eid = create(store)
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,error,attempts,created_at,updated_at) VALUES(?,?,'plan',1,'failed','saved-thread','saved-turn','prompt','agent output validation: invalid plan',1,0,0)",
            (identity, eid),
        )
    wf.fail(eid, "agent_correction_stalled", "invalid plan")
    request = uuid4()
    first = store.command(request, "action", {"action": "retry_agent"}, eid=eid)
    assert store.command(request, "action", {"action": "retry_agent"}, eid=eid) == first
    actions = store.rows("SELECT * FROM agent_actions WHERE experiment_id=? ORDER BY rowid", (eid,))
    assert len(actions) == 2 and actions[0]["turn_id"] == "saved-turn"
    assert actions[1]["previous_action_id"] == identity and actions[1]["evidence_revision"] == 1
    assert scheduler.submissions == 0
    await wf.close()


@pytest.mark.parametrize("uncertain", [False, True])
async def test_explicit_retry_grants_one_turn_at_owner_budget_with_idempotent_receipt(runtime, uncertain):
    store, scheduler, _, old = runtime
    old.config.codex.max_agent_turns = 1
    agent = PermissionAgent()
    wf = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_current_plan(wf, store, eid)
    await until_current(wf, store, eid, lambda exp: exp["phase"] == "failed")
    assert store.experiment(eid)["hold_reason"] == "agent_budget_exhausted"
    assert agent.calls == 1 and scheduler.submissions == 0
    request = uuid4()
    receipt = store.command(request, "action", {"action": "retry_agent"}, eid=eid)
    assert store.command(request, "action", {"action": "retry_agent"}, eid=eid) == receipt
    assert store.experiment(eid)["contract"]["manual_agent_turn_requests"] == 1
    await until_current(wf, store, eid, lambda exp: exp["phase"] == "failed")
    assert agent.calls == 2 and scheduler.submissions == 3
    assert store.experiment(eid)["hold_reason"] == "agent_budget_exhausted"
    if uncertain:
        # Retired legacy repair/report guards (test_auto_repair.py:322, :418) become this
        # analogue: a turn-bearing action with an unestablished outcome cannot be retried.
        prepare = store.one(
            "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='prepare' ORDER BY rowid DESC LIMIT 1",
            (eid,),
        )
        assert (
            prepare["state"] == "completed"
            and prepare["previous_action_id"]
            and prepare["turn_id"]
            and prepare["result"]
        )
        wf._action_state(prepare["id"], "uncertain")
        before = store.experiment(eid)
        action_ids = {
            a["id"] for a in store.rows("SELECT id FROM agent_actions WHERE experiment_id=?", (eid,))
        }
        jobs = store.rows(
            "SELECT intent_id,job_id,state FROM jobs WHERE experiment_id=? ORDER BY rowid", (eid,)
        )
        request = uuid4()
        with pytest.raises(Conflict):
            store.command(request, "action", {"action": "retry_agent"}, eid=eid)
        after = store.experiment(eid)
        assert (after["phase"], after["hold_reason"]) == ("failed", "agent_budget_exhausted")
        assert after["contract"] == before["contract"]
        assert after["contract"]["manual_agent_turn_requests"] == 1
        saved_prepare = store.one("SELECT state FROM agent_actions WHERE id=?", (prepare["id"],))
        assert saved_prepare["state"] == "uncertain"
        assert {
            a["id"] for a in store.rows("SELECT id FROM agent_actions WHERE experiment_id=?", (eid,))
        } == action_ids
        assert not store.rows("SELECT * FROM commands WHERE request_id=?", (str(request),))
        assert agent.calls == 2 and scheduler.submissions == 3
        assert (
            store.rows("SELECT intent_id,job_id,state FROM jobs WHERE experiment_id=? ORDER BY rowid", (eid,))
            == jobs
        )
        await wf.close()
        return
    second_request = uuid4()
    second_receipt = store.command(second_request, "action", {"action": "retry_agent"}, eid=eid)
    assert store.command(second_request, "action", {"action": "retry_agent"}, eid=eid) == second_receipt
    await until_current(wf, store, eid, lambda exp: exp["phase"] == "complete")
    assert agent.calls == 3 and scheduler.submissions == 3
    assert store.experiment(eid)["contract"]["manual_agent_turn_requests"] == 2
    await wf.close()


async def test_known_quota_interruption_resumes_automatically_after_owner_restart(runtime):
    store, scheduler, _, old = runtime
    agent = PermissionAgent()
    wf = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_current_plan(wf, store, eid)
    finish = agent.finish
    interrupted = False
    entered = asyncio.Event()
    release = asyncio.Event()

    async def interrupt_once(thread, turn):
        nonlocal interrupted
        if not interrupted:
            interrupted = True
            store.update(eid, hold_reason="quota_hold")
            entered.set()
            await release.wait()
            raise TerminalTurnError("turn interrupted: None")
        return await finish(thread, turn)

    agent.finish = interrupt_once
    try:
        await wait_until(
            entered.is_set,
            ticks=lambda: wf.tick(force_observe=True),
            describe=lambda: store.experiment(eid),
        )
        task = wf.agent_task
        assert task is not None
        release.set()
        await task
    finally:
        release.set()
    prior = failed_prepare(store, eid)
    assert prior and prior["state"] == "failed" and prior["turn_id"]
    assert prior["attempts"] == 1 and prior["interrupt_reason"] == "quota_hold"
    # The interrupted turn keeps its identity but adopts no result; its exact runtime
    # result stays readable and is never retried as an unknown outcome.
    assert prior["result"] is None
    assert agent.results[(prior["thread_id"], prior["turn_id"])]["schema_version"] == 3
    await wf.close()
    # The saved failure is the whole recovery state: no owner retry was issued.
    assert not store.rows("SELECT 1 FROM agent_actions WHERE previous_action_id=?", (prior["id"],))
    wf = Workflow(old.config, store, scheduler, agent)
    continuation = await wait_until(
        lambda: store.one("SELECT * FROM agent_actions WHERE previous_action_id=?", (prior["id"],)),
        ticks=lambda: wf.tick(force_observe=True),
        describe=lambda: store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,)),
    )
    saved = store.one("SELECT * FROM agent_actions WHERE id=?", (prior["id"],))
    assert saved["state"] == "failed" and saved["turn_id"] == prior["turn_id"]
    assert saved["attempts"] == prior["attempts"]
    assert (
        continuation["phase"] == "prepare" and continuation["evidence_revision"] == prior["evidence_revision"]
    )
    assert continuation["thread_id"] == prior["thread_id"]
    await until_current(wf, store, eid, lambda exp: exp["phase"] == "complete")
    links = store.rows("SELECT * FROM agent_actions WHERE previous_action_id=?", (prior["id"],))
    assert [link["id"] for link in links] == [continuation["id"]]
    resumed = store.one("SELECT * FROM agent_actions WHERE id=?", (prior["id"],))
    assert resumed == prior
    assert store.experiment(eid)["contract"].get("manual_agent_turn_requests", 0) == 0
    assert scheduler.submissions == 3
    await wf.close()


async def test_manifest_tool_wakeups_and_tracked_submission_are_idempotent(runtime):
    store, scheduler, _, old = runtime
    agent = PermissionAgent()
    wf = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_current_plan(wf, store, eid)
    await until_current(wf, store, eid, lambda exp: bool(exp["snapshot"]))
    exp = store.experiment(eid)
    assert exp["snapshot"]["plan"]["schema_version"] == 3
    assert exp["snapshot"]["plan"]["python_executable"]
    assert (await wf.controller.check(exp, "prepare", exp["snapshot"]["plan"]))["valid"]
    with pytest.raises(ValueError, match="reserved"):
        await wf.controller.check(
            exp, "prepare", {**exp["snapshot"]["plan"], "files": ["recipe.py", "local-output-bad.json"]}
        )
    request = uuid4()
    saved = wf.controller.schedule(eid, request, 60, "Read progress")
    assert wf.controller.schedule(eid, request, 60, "Read progress") == saved
    with pytest.raises(Conflict):
        wf.controller.schedule(eid, request, 120, "Read progress")
    job_request = str(uuid4())
    one = await wf.submit_prepared_stage(exp, "train", job_request)
    two = await wf.submit_prepared_stage(exp, "train", job_request)
    assert one == two and scheduler.submissions == 1
    assert store.one("SELECT * FROM jobs WHERE intent_id=?", (job_request,))["execution_manifest"]
    held = await wf.submit_prepared_stage(exp, "evaluate", str(uuid4()))
    assert held == {"state": "held", "reason": "dependencies_pending"}
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=? AND stage_id='evaluate'", (eid,))
    root = wf.root(eid)
    (root / "analysis").mkdir(exist_ok=True)
    (root / "analysis/figure.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    a = wf.register_analysis_file(exp, "figure.svg")
    assert wf.register_analysis_file(exp, "figure.svg") == a
    (root / "analysis/figure.svg").write_text("different")
    with pytest.raises(ValueError, match="cannot change"):
        wf.register_analysis_file(exp, "figure.svg")
    with pytest.raises(ValueError):
        wf.register_analysis_file(exp, "../results.json")
    await wf.close()


async def test_agent_progress_extends_inactivity_window_without_unbounded_event_storage(tmp_path):
    from experimenttracker.codex_client import CodexAgent
    from experimenttracker.config import CodexConfig

    config = CodexConfig().model_copy(update={"turn_timeout_seconds": 0.04, "turn_max_seconds": 0.3})
    agent = CodexAgent(config, tmp_path)
    agent.active = ("thread", "turn")
    task = asyncio.create_task(agent.finish("thread", "turn"))
    for _ in range(5):
        await asyncio.sleep(0.02)
        await agent._notification(
            {
                "method": "item/agentMessage/delta",
                "params": {"threadId": "thread", "turnId": "turn", "delta": "ignored"},
            }
        )
    await agent.server.events.put(
        {
            "method": "item/completed",
            "params": {
                "threadId": "thread",
                "turnId": "turn",
                "item": {"type": "agentMessage", "text": "{}"},
            },
        }
    )
    await agent.server.events.put(
        {
            "method": "turn/completed",
            "params": {"threadId": "thread", "turn": {"id": "turn", "status": "completed"}},
        }
    )
    assert await task == {} and agent.server.events.qsize() == 0


async def test_configured_sessions_let_another_plan_progress_while_one_turn_waits(runtime):
    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2
    release_turns = asyncio.Event()
    phases = {}

    class WaitingAgent(PermissionAgent):
        async def session(self, thread_id, workspace, **kwargs):
            return await super().session(thread_id or str(uuid4()), workspace, **kwargs)

        async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
            turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
            phases[(thread_id, turn)] = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"]
            return turn

        async def finish(self, thread_id, turn_id):
            if phases[(thread_id, turn_id)] == "plan":
                await release_turns.wait()
            return await super().finish(thread_id, turn_id)

    wf = Workflow(old.config, store, scheduler, WaitingAgent())
    first, second = create(store), create(store)
    assert store.experiment(first)["contract"]["execution_contract"] == 3
    assert store.experiment(second)["contract"]["execution_contract"] == 3

    def two_running():
        running = store.rows("SELECT * FROM agent_actions WHERE state='running'")
        return running if len(running) == 2 else None

    def both_awaiting_review():
        return all(
            (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review"
            for eid in [first, second]
        )

    try:
        running = await wait_until(
            two_running,
            ticks=lambda: wf.tick(force_observe=True),
            describe=lambda: store.rows("SELECT * FROM agent_actions WHERE state='running'"),
        )
        assert {a["experiment_id"] for a in running} == {first, second}
        assert {a["phase"] for a in running} == {"plan"}
        assert len({a["thread_id"] for a in running}) == 2
        assert wf.lanes[0].gate is wf.lanes[1].gate
        release_turns.set()
        await wait_until(
            both_awaiting_review,
            ticks=lambda: wf.tick(force_observe=True),
            describe=lambda: [store.experiment(eid)["plan_review"] for eid in [first, second]],
        )
        assert both_awaiting_review()
        assert scheduler.submissions == 0
    finally:
        release_turns.set()
        await wf.close()
