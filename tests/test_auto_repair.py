"""In-place repair with real local CPU fixtures, never live Slurm acceptance."""

import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from helpers import SOURCE, IterationAgent, wait_for_experiment, wait_until
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
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] == "failed")
    assert store.experiment(eid)["hold_reason"] == "compute_failed"
    # Enable production recovery policy using the local scheduler fixture only.
    workflow.config.mode = "live"
    workflow.config.slurm.max_repairs_per_experiment = 3
    workflow.cluster_snapshot = {"fixture": True}
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    scheduler.preflight = AsyncMock()
    return store, scheduler, agent, workflow, eid


BROKEN_TRAIN = SOURCE.replace(
    "if a.stage=='train':",
    "if a.stage=='train':\n raise RuntimeError('broken initial training')",
)


class ContractAgent(IterationAgent):
    """Keep the replaced fixture's session invariants on contract 3."""

    def __init__(self):
        super().__init__()
        self.writable = False

    async def session(self, thread_id, workspace, *, writable=False, **kwargs):
        self.writable = writable
        return await super().session(thread_id, workspace, writable=writable, **kwargs)

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload["phase"] == "plan":
            assert not self.writable
        if payload["phase"] == "prepare":
            # A writable turn runs in the cwd its dispatch provides: the work tree for a
            # first attempt, the declared repair project for a repair.
            assert self.writable and self.workspace == (kwargs.get("project_workspace") or workspace)
        return await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)


class TrainFailureAgent(ContractAgent):
    """Initial training raises before any declared output; the repair retries every stage."""

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
        if payload["phase"] == "prepare":
            if payload.get("repair_context"):
                result = json.loads(json.dumps(self.results[(thread_id, turn)]))
                result["repair_decisions"] = [
                    {
                        "stage_id": stage["id"],
                        "decision": "retry",
                        "rationale": "No declared output exists, so rerun this task.",
                    }
                    for stage in result["stages"]
                ]
                result["reuse_stages"] = []
                result["reuse_rationale"] = "No attempt produced a declared output, so none is reusable."
                self.results[(thread_id, turn)] = result
            else:
                (workspace / "recipe.py").write_text(BROKEN_TRAIN)
        return turn


async def failed_training(runtime):
    """Contract-3 run whose first training task raises before writing its outputs."""
    store, scheduler, _, old = runtime
    agent = TrainFailureAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Exercise synthetic iteration mechanisms.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_plan(workflow, store, eid)
    exp = await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] == "failed")
    assert exp["hold_reason"] == "compute_failed"
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
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
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


@pytest.mark.parametrize("state,allowed", [("complete", True), ("agent", False)])
async def test_repair_distinguishes_agent_stage_from_scheduler_job(runtime, state, allowed):
    store, scheduler, _, workflow, eid = await failed_run(runtime)
    original = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    store.db.execute(
        "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,"
        "snapshot_hash,created_at,execution_manifest) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            str(uuid4()),
            eid,
            "plan",
            1,
            state,
            original["script_hash"],
            "{}",
            original["snapshot_hash"],
            original["created_at"],
            json.dumps({"executor": "agent"}),
        ),
    )
    await workflow.repair_failed_experiments()
    assert store.experiment(eid)["evidence_revision"] == (2 if allowed else 1)
    assert scheduler.submissions == 1
    await workflow.close()


COMPACT_JOB_KEYS = (
    "intent_id",
    "stage_id",
    "job_id",
    "state",
    "scheduler_state",
    "exit_code",
    "error",
    "started_at",
    "ended_at",
)


@pytest.mark.current_contract
async def test_job_status_can_select_a_compact_owned_job(runtime):
    store, _, _, workflow, eid = await failed_training(runtime)
    job = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert job["state"] == "failed" and job["scheduler_state"] == "FAILED"
    action = store.one("SELECT id FROM agent_actions WHERE experiment_id=? LIMIT 1", (eid,))
    store.db.execute("UPDATE agent_actions SET state='running' WHERE id=?", (action["id"],))
    summary = await workflow.controller.call(eid, "job_status", {"summary_only": True})
    assert summary == [{key: job[key] for key in COMPACT_JOB_KEYS} | {"attempt": job["attempt"]}]
    assert str(workflow.root(eid)) not in json.dumps(summary)
    detail = await workflow.controller.call(eid, "job_status", {"intent_ids": [job["intent_id"]]})
    assert len(detail) == 1 and detail[0]["intent_id"] == job["intent_id"]
    assert set(detail[0]) == set(COMPACT_JOB_KEYS) | {
        "attempt",
        "progress",
        "outputs",
        "command",
        "declared_outputs",
        "logs",
    }
    assert detail[0]["declared_outputs"] == {
        "context": "acacia",
        "host": workflow.config.executor_id,
        "root": job["execution_root"],
        "names": ["model.json", "checkpoints"],
        "note": "Unregistered partial files are diagnostic context; inspect only needed paths on this host.",
    }
    assert detail[0]["command"] == json.loads(job["execution_manifest"])["command"]
    assert detail[0]["outputs"] == [] and detail[0]["logs"] == {}
    assert await workflow.controller.call(eid, "job_status", {"intent_ids": [str(uuid4())]}) == []
    store.db.execute("UPDATE agent_actions SET state='completed' WHERE id=?", (action["id"],))
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
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] == "failed")
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
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
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

    def amendment_approved(exp):
        review = exp["plan_review"] or {}
        return (
            review.get("state") == "approved"
            and review["stages"][0]["resources"]["memory_mb_per_job"] == 2048
        )

    agent.begin = amended
    exp = await wait_for_experiment(workflow, store, eid, amendment_approved)
    assert exp["plan_review"]["state"] == "approved"
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
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
    def awaiting_first_repair():
        exp = store.experiment(eid)
        return exp["evidence_revision"] == 2 and exp["hold_reason"] == "automatic_repair_pending"

    workflow.repair_failed_experiments = first_repair_only
    await wait_until(
        awaiting_first_repair,
        ticks=lambda: workflow.tick(force_observe=True),
        describe=lambda: store.experiment(eid),
    )
    store.update(eid, phase="failed", hold_reason="compute_failed")
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
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
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
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] == "failed")
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
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
    assert store.experiment(eid)["phase"] == "complete"
    assert store.one("SELECT state,turn_id FROM agent_actions WHERE id=?", (old["id"],)) == {
        "state": "failed",
        "turn_id": old["turn_id"],
    }
    assert scheduler.submissions == 3
    await workflow.close()


@pytest.mark.current_contract
@pytest.mark.parametrize("uncertain", [False, True])
async def test_session_retry_preserves_action_and_does_not_block_other_experiments(runtime, uncertain):
    from experimenttracker.codex_client import SessionUnavailable
    from experimenttracker.store import Conflict

    store, scheduler, _, old = runtime
    agent = ContractAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    brief = {"brief": "Exercise synthetic iteration mechanisms.", "recipe_id": "agent_experiment_v1"}
    eid = store.command(uuid4(), "create", brief)["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    session = agent.session

    async def archived(*args, **kwargs):
        raise SessionUnavailable("session resume rejected: session archived")

    agent.session = archived
    exp = await wait_for_experiment(
        workflow,
        store,
        eid,
        lambda e: e["hold_reason"] == "agent_session_unavailable" and not workflow.agent_task,
    )
    assert exp["hold_reason"] == "agent_session_unavailable"
    assert exp["phase"] != "failed"
    saved = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    assert saved["state"] == "failed" and saved["turn_id"] is None and saved["attempts"] == 0
    assert not workflow.agent_task and agent.calls == 0
    agent.session = session
    other = store.command(uuid4(), "create", brief)["experiment_id"]
    assert store.experiment(other)["contract"]["execution_contract"] == 3
    await approve_plan(workflow, store, other)
    other_exp = await wait_for_experiment(workflow, store, other, lambda e: e["phase"] == "complete")
    assert other_exp["phase"] == "complete", other_exp["error"]
    assert store.experiment(eid)["hold_reason"] == "agent_session_unavailable"
    # Contract 3 spends plan, prepare and interpret turns where the toy recipe spent two.
    assert agent.calls == 3 and scheduler.submissions == 3

    def unchanged():
        return (
            store.experiment(eid)["phase"],
            store.experiment(eid)["hold_reason"],
            agent.calls,
            {row["id"] for row in store.rows("SELECT id FROM agent_actions WHERE experiment_id=?", (eid,))},
            store.rows("SELECT intent_id,job_id,state FROM jobs WHERE experiment_id=?", (eid,)),
        )

    if uncertain:
        store.db.execute("UPDATE agent_actions SET state='uncertain' WHERE id=?", (saved["id"],))
        before = unchanged()
        request = uuid4()
        with pytest.raises(Conflict):
            store.command(request, "action", {"action": "retry_agent"}, eid=eid)
        assert unchanged() == before
        assert store.one("SELECT state FROM agent_actions WHERE id=?", (saved["id"],))["state"] == "uncertain"
        assert not store.one("SELECT * FROM commands WHERE request_id=?", (str(request),))
        await workflow.close()
        return
    request = uuid4()
    receipt = store.command(request, "action", {"action": "retry_agent"}, eid=eid)
    assert store.command(request, "action", {"action": "retry_agent"}, eid=eid) == receipt
    assert "manual_agent_turn_requests" not in store.experiment(eid)["contract"]
    assert [
        row["id"] for row in store.rows("SELECT id FROM agent_actions WHERE experiment_id=?", (eid,))
    ] == [saved["id"]]
    assert store.one("SELECT state,turn_id,attempts FROM agent_actions WHERE id=?", (saved["id"],)) == {
        "state": "pending",
        "turn_id": None,
        "attempts": 0,
    }
    await workflow.close()
    workflow = Workflow(workflow.config, store, scheduler, agent)
    await approve_plan(workflow, store, eid)
    resumed = store.one("SELECT * FROM agent_actions WHERE id=?", (saved["id"],))
    assert resumed["phase"] == "plan" and resumed["state"] == "completed" and resumed["turn_id"]
    exp = await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
    assert exp["phase"] == "complete", exp["error"]
    assert agent.calls == 6
    assert [job["attempt"] for job in store.projection(eid)["jobs"]] == [1, 1, 1]
    assert store.one("SELECT state,attempts FROM agent_actions WHERE id=?", (saved["id"],)) == {
        "state": "completed",
        "attempts": 1,
    }
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
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] == "failed")
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
        await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
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


@pytest.mark.current_contract
async def test_repaired_readiness_does_not_wait_for_retired_artifact_uploads(runtime):
    store, scheduler, agent, workflow, eid = await failed_training(runtime)
    original_root = workflow.projects.base_root(eid)
    workflow.config.mode = "live"  # Production repair policy with the local demo scheduler.
    workflow.config.slurm.max_repairs_per_experiment = 3
    workflow.cluster_snapshot = {"fixture": True}
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    scheduler.preflight = AsyncMock()
    await workflow.repair_failed_experiments()
    exp = await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] in ("complete", "failed"))
    assert exp["phase"] == "complete", exp["error"]
    assert exp["evidence_revision"] == 2 and exp["contract"]["repair_count"] == 1
    # One failed first attempt and exactly the three retried graph stages, never a replay of success.
    assert agent.calls == 4 and scheduler.submissions == 4
    jobs = store.rows(
        "SELECT stage_id,attempt,state FROM jobs WHERE experiment_id=? ORDER BY attempt,stage_id", (eid,)
    )
    assert jobs == [
        {"stage_id": "train", "attempt": 1, "state": "failed"},
        {"stage_id": "evaluate", "attempt": 2, "state": "complete"},
        {"stage_id": "plot", "attempt": 2, "state": "complete"},
        {"stage_id": "train", "attempt": 2, "state": "complete"},
    ]
    failed_intent = store.one("SELECT intent_id FROM jobs WHERE experiment_id=? AND attempt=1", (eid,))[
        "intent_id"
    ]
    assert store.rows("SELECT * FROM output_refs WHERE intent_id=?", (failed_intent,)) == []
    producers = store.rows("SELECT stage_id,intent_id FROM stage_producers WHERE experiment_id=?", (eid,))
    assert {producer["stage_id"] for producer in producers} == {"train", "evaluate", "plot"}
    assert len({producer["intent_id"] for producer in producers}) == 3
    for producer in producers:
        assert (
            store.one("SELECT attempt FROM jobs WHERE intent_id=?", (producer["intent_id"],))["attempt"] == 2
        )
        assert producer["intent_id"] != failed_intent
    assert {decision["decision"] for decision in exp["contract"]["early_repair"]["decisions"].values()} == {
        "retry"
    }
    assert exp["snapshot"]["reused_stages"] == {}
    # The superseded revision keeps real, retained rows and bytes under the base root.
    retired = store.rows("SELECT * FROM artifacts WHERE experiment_id=? AND evidence_revision=1", (eid,))
    assert [row["name"] for row in retired] == ["preparation.json"]
    assert retired[0]["path"] == str(original_root / "preparation.json")
    assert workflow.root(eid) != original_root
    assert (original_root / "preparation.json").exists() and (original_root / "snapshot").exists()
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
    assert (original_root / "preparation.json").exists()
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
