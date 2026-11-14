"""Scientific handoffs stay complete without replaying execution audit history."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest
from helpers import PLAN, IterationAgent, approve_plan, until

from experimenttracker import artifacts, iteration
from experimenttracker.models import Implementation, ResearchPlan
from experimenttracker.workflow import Workflow


def create(store):
    return store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]


def test_compact_reference_retains_protocol_and_full_pointers(runtime):
    store, _, _, workflow = runtime
    parent_id = create(store)
    child_id = create(store)
    plan = Implementation.model_validate({**PLAN, "protocol": "Keep the exact aligned crop geometry."})
    workspace = workflow.working_directory(store.experiment(parent_id))
    workspace.mkdir(parents=True)
    (workspace / "recipe.py").write_text("print('synthetic')\n")
    manifest = artifacts.freeze_implementation(workflow.root(parent_id), plan, "", workspace=workspace)
    store.resolve_implementation(parent_id, manifest)
    contract = store.experiment(parent_id)["contract"]
    contract["repair_history"] = [{"old_snapshot": "historical bookkeeping " * 10000}]
    store.update(
        parent_id,
        analysis={
            "summary": "Measured roughness decreased.",
            "limitations": ["Single seed."],
            "presentation": {"huge": "rendered report " * 10000},
        },
    )
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), parent_id))
        db.execute("UPDATE experiments SET reference_experiment_id=? WHERE id=?", (parent_id, child_id))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,snapshot_hash,created_at) "
            "VALUES('train-intent',?,'train','complete','','{}',?,0)",
            (parent_id, manifest["snapshot_hash"]),
        )
        db.execute(
            "INSERT INTO output_refs(id,experiment_id,intent_id,name,context,host,path,kind,created_at) "
            "VALUES('train-model',?,'train-intent','model.json','acacia','local','synthetic/model.json','file',0)",
            (parent_id,),
        )
    review = ResearchPlan.model_validate(
        {
            "schema_version": 3,
            "title": "Synthetic reference follow-up",
            "summary": PLAN["summary"],
            "protocol": plan.protocol,
            "stages": [
                {key: stage[key] for key in ("id", "title", "needs", "resources")}
                | {"summary": "Synthetic task."}
                for stage in PLAN["stages"]
            ],
        }
    )
    store.update(
        child_id, plan_review={**review.model_dump(), "state": "approved", "scope": "plan", "revision": 2}
    )
    parent = store.experiment(parent_id)
    assert parent["contract"]["execution_contract"] == 3
    assert parent["contract"]["repair_history"] == contract["repair_history"]
    assert store.experiment(child_id)["contract"]["execution_contract"] == 3
    assert iteration.manifest_mode(parent["snapshot"])
    prompt = workflow.prompt(store.experiment(child_id), "prepare")
    payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
    pointer = payload["reference_context"]
    assert len(prompt) < 25000
    assert payload["approved_plan"]["revision"] == 2
    assert set(pointer) == {"path", "sha256"}
    content = Path(pointer["path"]).read_bytes()
    assert hashlib.sha256(content).hexdigest() == pointer["sha256"]
    full = json.loads(content)
    assert full["plan"] == plan.model_dump()
    assert full["plan"]["protocol"] == "Keep the exact aligned crop geometry."
    assert full["analysis"] == parent["analysis"]
    assert full["brief"] == parent["brief"]
    assert full["outputs"] == store.rows("SELECT * FROM output_refs WHERE experiment_id=?", (parent_id,))
    assert len(full["outputs"]) == 1
    assert workflow.prompt(store.experiment(child_id), "prepare") == prompt
    Path(pointer["path"]).write_text("corrupted")
    with pytest.raises(ValueError, match="context has changed"):
        workflow.prompt(store.experiment(child_id), "prepare")


async def test_first_implementation_starts_fresh_after_planning(runtime):
    store, scheduler, _, old = runtime

    class HandoffAgent(IterationAgent):
        def __init__(self):
            super().__init__()
            self.sessions = []

        async def session(self, thread_id, workspace, **kwargs):
            self.sessions.append((thread_id, kwargs.get("writable", False)))
            return await super().session(thread_id, workspace, **kwargs)

    agent = HandoffAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_plan(workflow, store, eid)
    await until(workflow, store, eid, lambda exp: exp["phase"] == "training")
    assert next(thread for thread, writable in agent.sessions if writable) is None
    actions = store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    assert all(a["turn_id"] and a["result"] for a in actions)
    assert scheduler.submissions == 1
    await workflow.close()
