"""Saved steering changes planning or safely replaces an in-flight run."""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3, wait_for_experiment

from experimenttracker.slurm import Observation
from experimenttracker.store import Conflict
from experimenttracker.workflow import Workflow


async def wait_for(workflow, store, eid, predicate):
    await wait_for_experiment(workflow, store, eid, predicate)


class WaitingScheduler:
    def __init__(self):
        self.cancelled = []

    async def submit(self, intent_id, script, root, *, resources=None):
        return "12345"

    async def observe(self, job_id):
        return Observation("CANCELLED" if self.cancelled else "RUNNING", "1:0" if self.cancelled else None)

    async def cancel(self, job_id, intent_id):
        self.cancelled.append((job_id, intent_id))
        return True

    async def reconcile(self, intent_id):
        return "12345"


def test_subscription_reserve_survives_workflow_restart(runtime):
    store, scheduler, agent, old = runtime
    identity = str(uuid4())
    payload = {"five_hour_percent": 65, "weekly_percent": 30}
    assert store.command(identity, "reserve", payload) == store.command(identity, "reserve", payload)
    restored = Workflow(old.config, store, scheduler, agent)
    assert restored.status(None)["quota_reserve"] == payload
    assert restored.gate.config.recovery_remaining == (70, 35)
    assert old.config.quota.five_hour_min_remaining == 25


@pytest.fixture
def blocked_store(runtime):
    store, _, _, _ = runtime
    eid = seed_saved_contract3(store, "Synthetic settled implementation")
    identity = str(uuid4())
    now = store.experiment(eid)["updated_at"]
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,"
            "input,result,attempts,created_at,updated_at) "
            "VALUES(?,?,'prepare',1,'completed','saved-thread','saved-turn','saved input',?,1,?,?)",
            (identity, eid, json.dumps({"outcome": "blocked", "summary": "Missing input"}), now, now),
        )
    store.update(eid, phase="preparing", hold_reason="implementation_blocked", error="Missing input")
    return store, eid, identity


def pending_question(store, eid, *, expired=True):
    pending = {
        "id": "saved-question",
        "rpc_id": 42,
        "expired": expired,
        "method": "item/tool/requestUserInput",
        "params": {"questions": [{"id": "dataset", "question": "Which dataset?"}]},
    }
    store.update(eid, pending_request=pending)
    return pending


@pytest.mark.parametrize("invalid", ["stale", "approval", "expired_active"])
def test_answers_keep_identity_permission_and_live_rpc_checks(blocked_store, invalid):
    store, eid, identity = blocked_store
    pending = pending_question(store, eid)
    payload = {"pending_request_id": pending["id"], "answer": "Use dataset A"}
    if invalid == "stale":
        payload["pending_request_id"] = "another-question"
    elif invalid == "approval":
        pending.update(method="item/commandExecution/requestApproval", params={})
        store.update(eid, pending_request=pending)
    else:
        store.update(eid, hold_reason="needs_attention")
        store.db.execute("UPDATE agent_actions SET state='running' WHERE id=?", (identity,))
    with pytest.raises(Conflict):
        store.command(uuid4(), "answer", payload, eid=eid)
    assert store.experiment(eid)["pending_request"] == pending
    assert not store.rows("SELECT * FROM steering WHERE experiment_id=?", (eid,))
