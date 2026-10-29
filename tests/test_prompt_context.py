"""Scientific handoffs stay complete without replaying execution audit history."""

import hashlib
import json
from pathlib import Path
from uuid import uuid4

import pytest
from test_generic import ImplementationAgent, approve_plan
from test_workflow import until

from experimenttracker.workflow import Workflow


def create(store):
    return store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]


def test_compact_reference_retains_protocol_and_full_verified_pointers(runtime, monkeypatch):
    store, _, _, workflow = runtime
    parent_id = create(store)
    child_id = create(store)
    contract = store.experiment(parent_id)["contract"]
    contract["repair_history"] = [{"old_snapshot": "historical bookkeeping " * 10000}]
    plan = {
        "protocol": "Keep the exact aligned crop geometry.",
        "stages": [{"id": "train", "needs": [], "resources": {"gpus_per_job": 1}}],
        "internal_history": "obsolete " * 10000,
    }
    store.update(
        parent_id,
        snapshot={"plan": plan},
        analysis={
            "summary": "Measured roughness decreased.",
            "limitations": ["Single seed."],
            "presentation": {"huge": "rendered report " * 10000},
        },
    )
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), parent_id))
        db.execute("UPDATE experiments SET reference_experiment_id=? WHERE id=?", (parent_id, child_id))
    store.update(child_id, plan_review={**plan, "state": "approved", "revision": 2})
    verified = []
    monkeypatch.setattr(
        "experimenttracker.workflow.artifacts.verify_snapshot", lambda *args: verified.append(args)
    )
    prompt = workflow.prompt(store.experiment(child_id), "prepare")
    payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
    ref = payload["reference_context"]
    assert len(prompt) < 25000
    assert ref["plan"]["protocol"] == plan["protocol"]
    assert ref["plan"]["stages"] == plan["stages"]
    assert ref["analysis"]["limitations"] == ["Single seed."]
    assert payload["approved_plan"]["revision"] == 2
    assert verified
    pointer = ref["details"]
    content = Path(pointer["path"]).read_bytes()
    assert hashlib.sha256(content).hexdigest() == pointer["sha256"]
    full = json.loads(content)
    assert full["contract"]["repair_history"] == contract["repair_history"]
    assert full["manifest"]["plan"] == plan
    assert workflow.prompt(store.experiment(child_id), "prepare") == prompt
    Path(pointer["path"]).write_text("corrupted")
    with pytest.raises(ValueError, match="context has changed"):
        workflow.prompt(store.experiment(child_id), "prepare")


async def test_first_implementation_starts_fresh_after_planning(runtime):
    store, scheduler, _, old = runtime

    class HandoffAgent(ImplementationAgent):
        def __init__(self):
            super().__init__()
            self.sessions = []

        async def session(self, thread_id, workspace, *, writable=False):
            self.sessions.append((thread_id, writable))
            return await super().session(thread_id, workspace, writable=writable)

    agent = HandoffAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    await approve_plan(workflow, store, eid)
    await until(workflow, store, eid, lambda exp: exp["phase"] == "training")
    assert next(thread for thread, writable in agent.sessions if writable) is None
    actions = store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    assert all(a["turn_id"] and a["result"] for a in actions)
    assert scheduler.submissions == 1
    await workflow.close()
