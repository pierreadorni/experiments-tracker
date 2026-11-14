"""In-place repair with real local CPU fixtures, never live Slurm acceptance."""

import json
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from helpers import SOURCE, IterationAgent, wait_for_experiment
from test_generic import approve_plan

from experimenttracker.workflow import Workflow

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
    eid = store.command(
        uuid4(), "create", {"brief": "Compare fixed models.", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
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
