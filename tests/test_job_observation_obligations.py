"""Synthetic regressions for exact job-observation obligation handoffs."""

import json
from uuid import uuid4

from helpers import seed_saved_contract3

from experimenttracker.cancellation import process_cancellations
from experimenttracker.core import job_obligations
from experimenttracker.models import ActionCommand
from experimenttracker.slurm import Observation


def seed_job(store, *, state="submitted", job_id="123"):
    eid = seed_saved_contract3(store, "Synthetic observation mapping")
    intent_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_manifest) VALUES(?,?, 'prepare',1,?,?, 'synthetic','{}',"
            "'control',1,?)",
            (intent_id, eid, state, job_id, json.dumps({"executor": "slurm"})),
        )
        job_obligations.sync_job(db, intent_id)
    return eid, intent_id


def cancel(store, eid):
    payload = ActionCommand(request_id=uuid4(), action="cancel").model_dump(mode="json")
    store.command(payload["request_id"], "action", payload, eid=eid)


class Scheduler:
    def __init__(self, observation):
        self.observation = observation
        self.observed = False
        self.cancelled = []

    async def reconcile(self, _intent_id):
        return "456"

    async def observe(self, _job_id):
        self.observed = True
        return self.observation

    async def cancel(self, job_id, intent_id):
        self.cancelled.append((job_id, intent_id))


async def test_cancel_adoption_refreshes_snapshot_before_same_cycle_terminal_settlement(runtime):
    store, _, _, workflow = runtime
    eid, intent_id = seed_job(store, state="submitting", job_id=None)
    cancel(store, eid)
    scheduler = Scheduler(Observation("CANCELLED", "1:0"))
    workflow.scheduler_for = lambda _exp, _job: scheduler

    await process_cancellations(workflow)

    saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))
    assert (saved["job_id"], saved["state"], saved["scheduler_state"]) == (
        "456",
        "cancelled",
        "CANCELLED",
    )
    assert scheduler.observed and scheduler.cancelled == []
    assert not store.rows("SELECT 1 FROM obligations WHERE state='open' AND target=?", (intent_id,))
    assert store.one("SELECT 1 FROM events WHERE kind='cancelled_job_settled'")
    await workflow.close()
