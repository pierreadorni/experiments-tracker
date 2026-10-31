"""Autonomous recovery acceptance with normal Python packages and saved runtimes.

These use the real local CPU scheduler and deterministic agents; they do not
claim managed inference, real Slurm scientific success or physical-phone access.
"""

import asyncio
import json
import threading
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
from test_generic import PLAN, SOURCE, ImplementationAgent, approve_plan
from test_workflow import until

from experimenttracker import artifacts
from experimenttracker.api import create_app
from experimenttracker.codex_client import NeedsAttention
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


async def test_slow_shared_backup_keeps_owner_commands_and_event_loop_responsive(runtime, monkeypatch):
    store, _, _, wf = runtime
    eid = create(store)
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


async def test_distinct_manifest_corrections_preserve_every_turn_and_continue_across_phases(runtime):
    store, scheduler, _, old = runtime
    agent = ImplementationAgent()
    begin = agent.begin
    failures = ["filename", "json", "figure", None]

    async def correcting(thread, prompt, schema, **kwargs):
        turn = await begin(thread, prompt, schema, **kwargs)
        phase = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"]
        if phase == "prepare":
            result = json.loads(json.dumps(agent.results[(thread, turn)]))
            kind = failures.pop(0)
            if kind == "filename":
                result["files"].append("local-output-bad.py")
            elif kind == "json":
                result["parameters_json"] = "[]"
            elif kind == "figure":
                result["figures"] = ["missing.svg"]
            agent.results[(thread, turn)] = result
        elif phase == "interpret" and not any(
            a["phase"] == "interpret" and a["state"] == "failed"
            for a in store.rows("SELECT * FROM agent_actions")
        ):
            agent.results[(thread, turn)]["presentation"] = None
        return turn

    agent.begin = correcting
    wf = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    await approve_plan(wf, store, eid)
    await until(wf, store, eid, lambda exp: exp["phase"] == "complete", ticks=200)
    actions = store.rows("SELECT * FROM agent_actions WHERE experiment_id=? ORDER BY rowid", (eid,))
    failed = [a for a in actions if a["state"] == "failed"]
    assert len(failed) == 4 and scheduler.submissions == 2
    assert len({a["turn_id"] for a in actions}) == len(actions)
    assert all(a["result"] and a["thread_id"] and a["turn_id"] for a in failed)
    for prior in failed:
        child = store.one("SELECT * FROM agent_actions WHERE previous_action_id=?", (prior["id"],))
        assert child and child["evidence_revision"] == prior["evidence_revision"]
        assert child["thread_id"] == prior["thread_id"]
    assert not store.rows("PRAGMA foreign_key_check")
    await wf.close()


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


async def test_explicit_retry_grants_one_turn_at_owner_budget_with_idempotent_receipt(runtime):
    store, scheduler, _, old = runtime
    old.config.codex.max_agent_turns = 1
    agent = ImplementationAgent()
    wf = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    await approve_plan(wf, store, eid)
    await until(wf, store, eid, lambda exp: exp["phase"] == "failed")
    assert store.experiment(eid)["hold_reason"] == "agent_budget_exhausted"
    assert agent.calls == 1 and scheduler.submissions == 0
    request = uuid4()
    receipt = store.command(request, "action", {"action": "retry_agent"}, eid=eid)
    assert store.command(request, "action", {"action": "retry_agent"}, eid=eid) == receipt
    assert store.experiment(eid)["contract"]["manual_agent_turn_requests"] == 1
    await until(wf, store, eid, lambda exp: exp["phase"] == "failed")
    assert agent.calls == 2 and scheduler.submissions == 2
    assert store.experiment(eid)["hold_reason"] == "agent_budget_exhausted"
    store.command(uuid4(), "action", {"action": "retry_agent"}, eid=eid)
    await until(wf, store, eid, lambda exp: exp["phase"] == "complete")
    assert agent.calls == 3 and scheduler.submissions == 2
    assert store.experiment(eid)["contract"]["manual_agent_turn_requests"] == 2
    await wf.close()


class CheckpointAgent(ImplementationAgent):
    async def begin(self, thread_id, prompt, schema, *, workspace=None):
        turn = await super().begin(thread_id, prompt, schema, workspace=workspace)
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload["phase"] == "prepare":
            result = json.loads(json.dumps(PLAN))
            result["files"] += ["normal_package/__init__.py", "normal_package/value.py"]
            result["stages"][0]["local_outputs"] = ["checkpoints/model"]
            package = workspace / "normal_package"
            package.mkdir(exist_ok=True)
            (package / "__init__.py").write_text("")
            (package / "value.py").write_text("VALUE = 42\n")
            source = SOURCE.replace("6*7", "__import__('normal_package.value', fromlist=['VALUE']).VALUE")
            source = source.replace(
                "if a.stage == 'z_measure':",
                "if a.stage == 'z_measure':\n    (r/'checkpoints/model').mkdir(parents=True,exist_ok=True)\n    (r/'checkpoints/model/weights.json').write_text('{\\\"weight\\\":42}')",
            )
            if payload.get("repair_context"):
                result["reuse_stages"] = ["z_measure"]
                prior = payload["repair_context"]["previous_outputs"]
                source = source.replace(
                    "else:\n    x=",
                    f"else:\n    assert json.loads((Path({prior!r})/'checkpoints/model/weights.json').read_text())['weight'] == 42\n    x=",
                )
                assert workspace == Path(payload["repair_context"]["previous_workspace"])
            else:
                source = source.replace(
                    "else:\n    x=",
                    "else:\n    raise RuntimeError('evaluation bug after valid training')\n    x=",
                )
            (workspace / "recipe.py").write_text(source)
            self.results[(thread_id, turn)] = result
        return turn


async def test_evaluation_repair_reuses_training_checkpoint_package_and_runtime(runtime):
    from unittest.mock import AsyncMock

    store, scheduler, _, old = runtime
    agent = CheckpointAgent()
    wf = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    await approve_plan(wf, store, eid)
    await until(wf, store, eid, lambda exp: exp["phase"] == "failed")
    original = store.experiment(eid)
    original_work = wf.working_directory(original)
    first = store.one("SELECT * FROM jobs WHERE experiment_id=? AND stage_id='z_measure'", (eid,))
    assert first["state"] == "complete" and scheduler.submissions == 2
    wf.config.mode = "live"
    wf.cluster_snapshot = {"fixture": True}
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    scheduler.preflight = AsyncMock()
    await wf.repair_failed_experiments()
    await wf.close()
    wf = Workflow(old.config, store, scheduler, agent)
    await until(wf, store, eid, lambda exp: exp["phase"] == "complete", ticks=200)
    exp = store.experiment(eid)
    assert scheduler.submissions == 3  # Evaluation alone reran.
    assert wf.working_directory(exp) == original_work
    assert exp["snapshot"]["python_executable"] == original["snapshot"]["python_executable"]
    assert exp["snapshot"]["reused_stages"]["z_measure"]["intent_id"] == first["intent_id"]
    assert (original_work.parent / "checkpoints/model/weights.json").exists()
    assert any(j["intent_id"] == first["intent_id"] for j in store.projection(eid)["jobs"])
    for stage in wf.compute_stages(exp):
        artifacts.validate_stage(wf.root(eid), stage, exp["snapshot"])
    await wf.close()


async def test_manifest_tool_wakeups_and_tracked_submission_are_idempotent(runtime):
    store, scheduler, _, old = runtime
    agent = ImplementationAgent()
    wf = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    await approve_plan(wf, store, eid)
    await until(wf, store, eid, lambda exp: bool(exp["snapshot"]))
    exp = store.experiment(eid)
    assert (await wf.controller.check(exp, "prepare", PLAN))["valid"]
    with pytest.raises(ValueError, match="reserved"):
        await wf.controller.check(exp, "prepare", {**PLAN, "files": ["recipe.py", "local-output-bad.json"]})
    request = uuid4()
    saved = wf.controller.schedule(eid, request, 60, "Read progress")
    assert wf.controller.schedule(eid, request, 60, "Read progress") == saved
    with pytest.raises(Conflict):
        wf.controller.schedule(eid, request, 120, "Read progress")
    job_request = str(uuid4())
    one = await wf.submit_prepared_stage(exp, "z_measure", job_request)
    two = await wf.submit_prepared_stage(exp, "z_measure", job_request)
    assert one == two and scheduler.submissions == 1
    assert store.one("SELECT * FROM jobs WHERE intent_id=?", (job_request,))["execution_manifest"]
    root = wf.root(eid)
    (root / "analysis").mkdir()
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
    from experimenttracker.codex_client import DemoAgent

    store, scheduler, _, old = runtime
    old.config.codex.max_sessions = 2

    class WaitingAgent(ImplementationAgent):
        async def session(self, thread, workspace, *, writable=False):
            identity = thread or str(uuid4())
            self.workspace, self.writable = workspace, writable
            return identity, "deterministic-demo"

        async def finish(self, thread, turn):
            payload = self.results[(thread, turn)]
            if payload.get("title") == "Requested method frontier":
                await asyncio.sleep(0.1)
            return await DemoAgent.finish(self, thread, turn)

    wf = Workflow(old.config, store, scheduler, WaitingAgent())
    first, second = create(store), create(store)
    for _ in range(10):
        await wf.tick(force_observe=True)
        await asyncio.sleep(0.005)
        running = store.rows("SELECT * FROM agent_actions WHERE state='running'")
        if len(running) == 2:
            break
    assert len(running) == 2 and {a["experiment_id"] for a in running} == {first, second}
    assert wf.lanes[0].gate is wf.lanes[1].gate
    for _ in range(50):
        await wf.tick(force_observe=True)
        await asyncio.sleep(0.005)
        if all(
            (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review"
            for eid in [first, second]
        ):
            break
    assert all(store.experiment(eid)["plan_review"]["state"] == "awaiting_review" for eid in [first, second])
    assert scheduler.submissions == 0
    await wf.close()
