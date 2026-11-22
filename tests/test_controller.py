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
from helpers import IterationAgent, role_step, seed_saved_contract3

from experimenttracker.api import create_app
from experimenttracker.codex_client import NeedsAttention
from experimenttracker.store import Store


def create(store):
    return seed_saved_contract3(store, "Test an entirely new method and show its frontier.")


def create_public(store):
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
    from experimenttracker import creation_policy

    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    eid = create_public(store)
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
@pytest.mark.usefixtures("contract_three_fixture")
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
        await role_step(wf.controller)
        action = store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))
        assert reads == [("saved-thread", "saved-monitor")]
        assert action["state"] == ("uncertain" if lost_response else "completed")
        assert action["attempts"] == 1 and action["turn_id"] == "saved-monitor"
        if lost_response:
            agent.read_turn = lambda *_: asyncio.sleep(0, result=result)
            await wf.reconcile_action(action_id)
        await role_step(wf.controller)
        assert store.one("SELECT state FROM agent_wakeups WHERE id=?", (wake_id,))["state"] == "complete"
        assert len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))) == 1
        assert scheduler.submissions == 0 and agent.calls == 0
    finally:
        wf.controller.server = None
        await wf.close()


@pytest.mark.usefixtures("contract_three_fixture")
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

    async def no_dispatch(_action, _slot):
        return None

    monkeypatch.setattr(wf, "dispatch", no_dispatch)
    try:
        await role_step(wf.controller)
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
        await role_step(wf.controller)
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


@pytest.mark.usefixtures("contract_three_fixture")
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
