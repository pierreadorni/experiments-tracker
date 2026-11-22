"""In-place repair with real local CPU fixtures, never live Slurm acceptance."""

import json
from uuid import uuid4

from helpers import SOURCE, IterationAgent, approve_plan, seed_saved_contract3, wait_for_experiment

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
    eid = seed_saved_contract3(store)
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


def test_repeated_identical_action_state_does_not_flood_outbox(runtime):
    store, _, _, workflow = runtime
    eid = seed_saved_contract3(store, "Compare fixed models.")
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
