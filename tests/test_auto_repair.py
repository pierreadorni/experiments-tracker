"""In-place repair with real local CPU fixtures, never live Slurm acceptance."""

import asyncio
import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_generic import ImplementationAgent, approve_plan

from experimenttracker import artifacts
from experimenttracker.audit import verify_experiment
from experimenttracker.models import CreateExperiment
from experimenttracker.slurm import Observation
from experimenttracker.workflow import Workflow


class RepairAgent(ImplementationAgent):
    async def begin(self, thread_id, prompt, schema, *, workspace=None):
        turn = await super().begin(thread_id, prompt, schema, workspace=workspace)
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload["phase"] == "prepare" and not payload.get("repair_context"):
            with (workspace / "recipe.py").open("a") as stream:
                stream.write("\nraise RuntimeError('buggy implementation')\n")
        if payload.get("repair_context"):
            assert "continuing validation feedback" in payload["repair_instructions"]
            assert payload["repair_context"]["jobs"][0]["job_id"]
        return turn


async def failed_run(runtime):
    store, scheduler, _, old = runtime
    agent = RepairAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    payload = CreateExperiment(
        request_id=uuid4(), brief="Test an entirely new method and show its frontier."
    ).model_dump(mode="json")
    eid = store.command(payload["request_id"], "create", payload)["experiment_id"]
    await approve_plan(workflow, store, eid)
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] == "failed":
            break
    assert store.experiment(eid)["hold_reason"] == "compute_failed"
    # Enable production recovery policy using the local scheduler fixture only.
    workflow.config.mode = "live"
    workflow.config.slurm.max_repairs_per_experiment = 3
    workflow.cluster_snapshot = {"fixture": True}
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    scheduler.preflight = AsyncMock()
    return store, scheduler, agent, workflow, eid


async def test_repair_same_identity_restart_and_only_fixed_evidence(runtime):
    store, scheduler, agent, workflow, eid = await failed_run(runtime)
    store.command(uuid4(), "note", {"text": "Use short local temporary paths for worker sockets."}, eid=eid)
    original_root = workflow.root(eid)
    original_manifest = store.experiment(eid)["snapshot"]
    await workflow.repair_failed_experiments()
    await workflow.repair_failed_experiments()
    exp = store.experiment(eid)
    assert workflow.prompt(exp, "prepare").find("Use short local temporary paths for worker sockets.") >= 0
    assert exp["evidence_revision"] == 2
    assert workflow.root(eid) != original_root
    artifacts.verify_snapshot(original_root, original_manifest)
    await workflow.close()
    workflow = Workflow(workflow.config, store, scheduler, agent)
    for _ in range(150):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    exp = store.experiment(eid)
    assert exp["phase"] == "complete", exp["error"]
    assert len(store.rows("SELECT id FROM experiments")) == 1
    assert scheduler.submissions == 3 and agent.calls == 4
    jobs = store.rows("SELECT attempt,state FROM jobs WHERE experiment_id=? ORDER BY created_at", (eid,))
    assert jobs == [
        {"attempt": 1, "state": "failed"},
        {"attempt": 2, "state": "complete"},
        {"attempt": 2, "state": "complete"},
    ]
    assert (original_root / "snapshot").exists()
    assert (original_root / "work").exists()
    assert (original_root / "raw.json").exists()
    assert all(a["evidence_revision"] == 2 for a in store.projection(eid)["artifacts"])
    assert all(j["attempt"] == 2 for j in store.projection(eid)["jobs"])
    verify_experiment(store, eid, workflow.root(eid))
    await workflow.close()


@pytest.mark.parametrize(
    "block", ["accounting", "identity", "uncertain_turn", "unknown_submission", "budget", "steering", "demo"]
)
async def test_repair_waits_for_saved_outcomes_and_authorization(runtime, block):
    store, scheduler, _, workflow, eid = await failed_run(runtime)
    if block == "accounting":
        scheduler.observe = AsyncMock(return_value=Observation("ACCOUNTING_PENDING"))
    elif block == "identity":
        scheduler.reconcile = AsyncMock(return_value="a-different-job")
    elif block == "uncertain_turn":
        store.db.execute(
            "UPDATE agent_actions SET state='uncertain' WHERE experiment_id=? AND phase='prepare'", (eid,)
        )
    elif block == "unknown_submission":
        store.db.execute("UPDATE jobs SET state='submitting',job_id=NULL WHERE experiment_id=?", (eid,))
    elif block == "budget":
        exp = store.experiment(eid)
        exp["contract"]["max_jobs"] = 1
        store.db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(exp["contract"]), eid))
    elif block == "steering":
        store.command(uuid4(), "steer", {"text": "Use a correction", "mode": "restart"}, eid=eid)
    else:
        workflow.config.mode = "demo"
    await workflow.repair_failed_experiments()
    assert store.experiment(eid)["evidence_revision"] == 1
    assert scheduler.submissions == 1
    await workflow.close()


async def test_three_repairs_then_explicit_idempotent_retry(runtime):
    store, scheduler, agent, workflow, eid = await failed_run(runtime)
    original_begin = agent.begin

    async def still_broken(thread_id, prompt, schema, *, workspace=None):
        turn = await original_begin(thread_id, prompt, schema, workspace=workspace)
        if '"repair_context"' in prompt:
            with (workspace / "recipe.py").open("a") as stream:
                stream.write("\nraise RuntimeError('repair did not work')\n")
        return turn

    agent.begin = still_broken
    for _ in range(300):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] == "failed":
            break
    exp = store.experiment(eid)
    assert exp["phase"] == "failed" and exp["evidence_revision"] == 4
    assert exp["contract"]["repair_count"] == 3
    assert scheduler.submissions == 4
    await workflow.repair_failed_experiments()
    assert store.experiment(eid)["evidence_revision"] == 4
    request_id = uuid4()
    payload = {"action": "retry_agent"}
    store.command(request_id, "action", payload, eid=eid)
    store.command(request_id, "action", payload, eid=eid)
    assert store.experiment(eid)["contract"]["manual_repair_requests"] == 1
    agent.begin = original_begin
    await workflow.close()
    workflow = Workflow(workflow.config, store, scheduler, agent)
    for _ in range(150):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    exp = store.experiment(eid)
    assert exp["phase"] == "complete", exp["error"]
    assert exp["evidence_revision"] == 5 and exp["contract"]["repair_count"] == 4
    assert scheduler.submissions == 6
    assert not workflow.repair_available(exp)
    await workflow.close()


async def test_resource_amendment_is_autonomous_within_authorization(runtime):
    store, scheduler, agent, workflow, eid = await failed_run(runtime)
    original_begin = agent.begin

    async def amended(thread_id, prompt, schema, *, workspace=None):
        turn = await original_begin(thread_id, prompt, schema, workspace=workspace)
        if '"repair_context"' in prompt:
            result = json.loads(json.dumps(agent.results[(thread_id, turn)]))
            result["stages"][0]["resources"]["memory_mb_per_job"] = 2048
            agent.results[(thread_id, turn)] = result
        return turn

    agent.begin = amended
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["hold_reason"] == "plan_review":
            break
    exp = store.experiment(eid)
    assert exp["plan_review"]["state"] == "approved"
    for _ in range(150):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    assert store.experiment(eid)["phase"] == "complete"
    assert scheduler.submissions == 3
    await workflow.close()


async def test_live_failure_waits_for_repair_without_terminal_notification(runtime):
    store, scheduler, _, workflow, eid = await failed_run(runtime)
    store.update(eid, phase="training", hold_reason=None)
    inbox_count = len(store.rows("SELECT * FROM inbox"))
    workflow.fail(eid, "compute_failed", "an implementation bug")
    exp = store.experiment(eid)
    assert exp["phase"] == "preparing" and exp["hold_reason"] == "automatic_repair_pending"
    scheduler.observe = AsyncMock(return_value=Observation("ACCOUNTING_PENDING"))
    await workflow.repair_failed_experiments()
    await workflow.advance(eid)
    assert store.experiment(eid)["evidence_revision"] == 1
    assert len(store.rows("SELECT * FROM inbox")) == inbox_count
    await workflow.close()


@pytest.mark.parametrize("block", [None, "accounting", "identity", "budget"])
async def test_timeout_after_code_repair_has_own_bounded_turn(runtime, block):
    store, scheduler, agent, workflow, eid = await failed_run(runtime)
    original_begin = agent.begin

    async def timeout_repair(thread_id, prompt, schema, *, workspace=None):
        turn = await original_begin(thread_id, prompt, schema, workspace=workspace)
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload.get("repair_context") and payload["repair_context"]["revision"] == 2:
            with (workspace / "recipe.py").open("a") as stream:
                stream.write("\nraise RuntimeError('simulate walltime exhaustion')\n")
        if payload.get("repair_context") and payload["repair_context"]["revision"] == 3:
            assert payload["repair_context"]["previous_workspace"] == payload["workspace"]
            assert {job["attempt"] for job in payload["repair_context"]["jobs"]} == {2}
            assert "checkpoint/resume" in payload["repair_instructions"]
        return turn

    agent.begin = timeout_repair
    repair_failed = workflow.repair_failed_experiments

    async def first_repair_only():
        if store.experiment(eid)["evidence_revision"] == 1:
            await repair_failed()

    # Simulate an older owner that stops after its first implementation repair.
    workflow.repair_failed_experiments = first_repair_only
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        exp = store.experiment(eid)
        if exp["evidence_revision"] == 2 and exp["hold_reason"] == "automatic_repair_pending":
            store.update(eid, phase="failed", hold_reason="compute_failed")
            break
    workflow.repair_failed_experiments = repair_failed
    assert store.experiment(eid)["evidence_revision"] == 2
    failed_root = workflow.root(eid)
    store.db.execute(
        "UPDATE jobs SET scheduler_state='TIMEOUT',exit_code='0:0' WHERE experiment_id=? AND attempt=2",
        (eid,),
    )
    store.update(eid, error="TIMEOUT: 0:0")
    observe = scheduler.observe
    second_job = store.one("SELECT job_id FROM jobs WHERE experiment_id=? AND attempt=2", (eid,))["job_id"]

    async def exact_observe(job_id):
        if job_id == second_job:
            return Observation("TIMEOUT", exit_code="0:0")
        return await observe(job_id)

    scheduler.observe = exact_observe
    if block == "accounting":
        scheduler.observe = AsyncMock(return_value=Observation("ACCOUNTING_PENDING"))
    elif block == "identity":
        scheduler.reconcile = AsyncMock(return_value="unrelated-job")
    elif block == "budget":
        exp = store.experiment(eid)
        exp["contract"]["max_jobs"] = 2
        store.db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(exp["contract"]), eid))
    await workflow.repair_failed_experiments()
    await workflow.repair_failed_experiments()
    if block:
        assert store.experiment(eid)["evidence_revision"] == 2
        assert scheduler.submissions == 2
        await workflow.close()
        return
    exp = store.experiment(eid)
    assert exp["evidence_revision"] == 3
    assert len(exp["contract"]["repair_history"]) == 1
    assert exp["contract"]["auto_repair"]["kind"] == "timeout"
    await workflow.close()
    workflow = Workflow(workflow.config, store, scheduler, agent)
    for _ in range(150):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    exp = store.experiment(eid)
    assert exp["phase"] == "complete", exp["error"]
    assert scheduler.submissions == 4
    assert workflow.working_directory(exp).exists()
    assert (failed_root / "snapshot").exists()
    assert exp["contract"]["repair_count"] == 2
    assert exp["contract"].get("repair_history")
    assert all(job["attempt"] == 3 for job in store.projection(eid)["jobs"])
    verify_experiment(store, eid, workflow.root(eid))
    await workflow.close()


@pytest.mark.parametrize("uncertain", [False, True])
async def test_explicit_retry_of_confirmed_interrupted_repair_turn(runtime, uncertain):
    from experimenttracker.store import Conflict

    store, scheduler, agent, workflow, eid = await failed_run(runtime)
    finish = agent.finish

    async def interrupted(thread_id, turn_id):
        from experimenttracker.codex_client import TerminalTurnError

        raise TerminalTurnError("turn interrupted: None")

    agent.finish = interrupted
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] == "failed":
            break
    exp = store.experiment(eid)
    assert exp["hold_reason"] == "agent_correction_stalled" and not exp["snapshot"]
    old = store.one(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='prepare' AND evidence_revision=2 ORDER BY rowid DESC LIMIT 1",
        (eid,),
    )
    if uncertain:
        store.db.execute("UPDATE agent_actions SET state='uncertain' WHERE id=?", (old["id"],))
        with pytest.raises(Conflict):
            store.command(uuid4(), "action", {"action": "retry_agent"}, eid=eid)
        assert store.experiment(eid)["phase"] == "failed"
        await workflow.close()
        return
    request_id = uuid4()
    store.command(request_id, "action", {"action": "retry_agent"}, eid=eid)
    store.command(request_id, "action", {"action": "retry_agent"}, eid=eid)
    assert store.experiment(eid)["plan_review"]["preparation_revision"] == 2
    agent.finish = finish
    await workflow.close()
    workflow = Workflow(workflow.config, store, scheduler, agent)
    for _ in range(150):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    assert store.experiment(eid)["phase"] == "complete"
    assert store.one("SELECT state,turn_id FROM agent_actions WHERE id=?", (old["id"],)) == {
        "state": "failed",
        "turn_id": old["turn_id"],
    }
    assert scheduler.submissions == 3
    await workflow.close()


@pytest.mark.parametrize("uncertain", [False, True])
async def test_session_retry_preserves_action_and_does_not_block_other_experiments(runtime, uncertain):
    from experimenttracker.codex_client import SessionUnavailable
    from experimenttracker.store import Conflict

    store, scheduler, agent, workflow = runtime
    payload = CreateExperiment(
        request_id=uuid4(), brief="Compare linear and quadratic models on shared synthetic data."
    ).model_dump(mode="json")
    payload["recipe_id"] = "toy_compare_v1"
    eid = store.command(payload["request_id"], "create", payload)["experiment_id"]
    session = agent.session

    async def archived(*args, **kwargs):
        raise SessionUnavailable("session resume rejected: session archived")

    agent.session = archived
    for _ in range(10):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
    exp = store.experiment(eid)
    assert exp["hold_reason"] == "agent_session_unavailable"
    assert exp["phase"] != "failed"
    old = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    assert old["state"] == "failed" and old["turn_id"] is None and old["attempts"] == 0
    assert not workflow.agent_task
    agent.session = session
    other = {**payload, "request_id": str(uuid4())}
    other_id = store.command(other["request_id"], "create", other)["experiment_id"]
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(other_id)["phase"] == "complete":
            break
    assert store.experiment(other_id)["phase"] == "complete", store.experiment(other_id)["error"]
    assert store.experiment(eid)["hold_reason"] == "agent_session_unavailable"
    if uncertain:
        store.db.execute("UPDATE agent_actions SET state='uncertain' WHERE id=?", (old["id"],))
        with pytest.raises(Conflict):
            store.command(uuid4(), "action", {"action": "retry_agent"}, eid=eid)
    else:
        request = uuid4()
        store.command(request, "action", {"action": "retry_agent"}, eid=eid)
        store.command(request, "action", {"action": "retry_agent"}, eid=eid)
        await workflow.close()
        workflow = Workflow(workflow.config, store, scheduler, agent)
        for _ in range(100):
            await workflow.tick(force_observe=True)
            await asyncio.sleep(0.01)
            if store.experiment(eid)["phase"] == "complete":
                break
        assert store.experiment(eid)["phase"] == "complete"
        saved = store.one("SELECT * FROM agent_actions WHERE id=?", (old["id"],))
        assert saved["state"] == "completed" and saved["attempts"] == 1
    await workflow.close()


@pytest.mark.parametrize("uncertain", [False, True])
async def test_retry_failed_report_preserves_compute_and_previous_turn(runtime, uncertain):
    from experimenttracker.store import Conflict

    store, scheduler, _, old = runtime
    agent = ImplementationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]
    begin = agent.begin

    async def invalid_report(thread, prompt, schema, **kwargs):
        turn = await begin(thread, prompt, schema, **kwargs)
        if json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"] == "interpret":
            agent.results[(thread, turn)]["presentation"] = None
        return turn

    agent.begin = invalid_report
    await approve_plan(workflow, store, eid)
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] == "failed":
            break
    exp = store.experiment(eid)
    assert exp["hold_reason"] == "agent_correction_stalled"
    failed = store.one(
        "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='interpret' ORDER BY rowid DESC LIMIT 1",
        (eid,),
    )
    jobs = store.rows("SELECT intent_id,job_id FROM jobs WHERE experiment_id=?", (eid,))
    submissions = scheduler.submissions
    if uncertain:
        store.db.execute("UPDATE agent_actions SET state='uncertain' WHERE id=?", (failed["id"],))
        with pytest.raises(Conflict):
            store.command(uuid4(), "action", {"action": "retry_agent"}, eid=eid)
    else:
        request = uuid4()
        store.command(request, "action", {"action": "retry_agent"}, eid=eid)
        store.command(request, "action", {"action": "retry_agent"}, eid=eid)
        agent.begin = begin
        await workflow.close()
        workflow = Workflow(workflow.config, store, scheduler, agent)
        for _ in range(100):
            await workflow.tick(force_observe=True)
            await asyncio.sleep(0.01)
            if store.experiment(eid)["phase"] in ("complete", "failed"):
                break
        assert store.experiment(eid)["phase"] == "complete"
        assert store.experiment(eid)["snapshot"] == exp["snapshot"]
        assert store.one("SELECT state,turn_id FROM agent_actions WHERE id=?", (failed["id"],)) == {
            "state": "failed",
            "turn_id": failed["turn_id"],
        }
        assert (
            store.one("SELECT state FROM agent_actions WHERE id=?", (str(request),))["state"] == "completed"
        )
    assert scheduler.submissions == submissions
    assert store.rows("SELECT intent_id,job_id FROM jobs WHERE experiment_id=?", (eid,)) == jobs
    await workflow.close()


async def test_repaired_readiness_does_not_wait_for_retired_artifact_uploads(runtime):
    store, scheduler, agent, workflow, eid = await failed_run(runtime)
    for _ in range(150):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] == "complete":
            break
    exp = store.experiment(eid)
    assert exp["phase"] == "complete"
    with store.tx() as db:
        db.execute(
            "UPDATE artifacts SET remote_published=1 WHERE experiment_id=? AND evidence_revision=?",
            (eid, exp["evidence_revision"]),
        )
        db.execute(
            "UPDATE artifacts SET remote_published=0 WHERE experiment_id=? AND evidence_revision<?",
            (eid, exp["evidence_revision"]),
        )
    assert store.one("SELECT id FROM artifacts WHERE experiment_id=? AND remote_published=0", (eid,))
    workflow.config.gateway.url = "https://gateway.example"
    await workflow.publish(exp)
    assert store.experiment(eid)["phase"] == "complete"
    await workflow.close()


def test_repeated_identical_action_state_does_not_flood_outbox(runtime):
    store, _, _, workflow = runtime
    eid = store.command(uuid4(), "create", {"brief": "Compare fixed models.", "recipe_id": "toy_compare_v1"})[
        "experiment_id"
    ]
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) VALUES(?,?,'prepare',1,'pending','',0,0)",
            (action_id, eid),
        )
    workflow._action_state(action_id, "failed", error="session archived")
    count = store.one("SELECT COUNT(*) AS n FROM outbox")["n"]
    for _ in range(20):
        workflow._action_state(action_id, "failed", error="session archived")
    assert store.one("SELECT COUNT(*) AS n FROM outbox")["n"] == count
    workflow._action_state(action_id, "failed", error="different failure")
    assert store.one("SELECT COUNT(*) AS n FROM outbox")["n"] == count + 1
