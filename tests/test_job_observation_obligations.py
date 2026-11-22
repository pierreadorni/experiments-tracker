"""Synthetic regressions for exact job-observation obligation handoffs."""

import json
from contextlib import contextmanager
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker import early_repair
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


@pytest.mark.usefixtures("contract_three_fixture")
async def test_observer_mapper_value_error_rolls_back_without_scientific_failure(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid, intent_id = seed_job(store)
    scheduler = Scheduler(Observation("RUNNING"))
    workflow.scheduler_for = lambda _exp, _job: scheduler

    def conflicting_mapping(*_args, **_kwargs):
        raise ValueError("synthetic obligation history conflict")

    monkeypatch.setattr(job_obligations, "sync_job", conflicting_mapping)
    stages_before = store.rows(
        "SELECT id,state,error,output_manifest FROM stages WHERE experiment_id=? ORDER BY id", (eid,)
    )

    await workflow.observe_jobs()

    saved = store.one("SELECT state,scheduler_state,error FROM jobs WHERE intent_id=?", (intent_id,))
    assert saved == {"state": "submitted", "scheduler_state": None, "error": None}
    assert store.experiment(eid)["phase"] != "failed"
    assert store.experiment(eid)["hold_reason"] == "executor_unavailable"
    assert (
        store.rows(
            "SELECT id,state,error,output_manifest FROM stages WHERE experiment_id=? ORDER BY id", (eid,)
        )
        == stages_before
    )
    assert not store.rows(
        "SELECT 1 FROM events WHERE experiment_id=? AND kind='output_validation_failed'", (eid,)
    )
    assert store.one(
        "SELECT state,owner FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (intent_id,)
    ) == {"state": "open", "owner": "external"}
    await workflow.close()


@pytest.mark.usefixtures("contract_three_fixture")
async def test_parent_cancellation_is_rechecked_inside_observation_transaction(runtime, monkeypatch):
    store, _, _, workflow = runtime
    eid, intent_id = seed_job(store)
    scheduler = Scheduler(Observation("RUNNING"))
    workflow.scheduler_for = lambda _exp, _job: scheduler
    selected_calls = []
    monkeypatch.setattr(early_repair, "selected", lambda *_args: selected_calls.append(True))
    transaction, cancellations = store.tx, []

    @contextmanager
    def cancel_before_observation_write():
        # The observer's own pre-write check has passed; cancellation lands now.
        if scheduler.observed and not cancellations:
            cancellations.append(True)
            cancel(store, eid)
        with transaction() as db:
            yield db

    monkeypatch.setattr(store, "tx", cancel_before_observation_write)

    await workflow.observe_jobs()

    saved = store.one("SELECT state,scheduler_state FROM jobs WHERE intent_id=?", (intent_id,))
    assert saved == {"state": "submitted", "scheduler_state": None}
    assert store.experiment(eid)["phase"] == "cancelled"
    assert not store.rows("SELECT 1 FROM events WHERE experiment_id=? AND kind='job_observed'", (eid,))
    assert cancellations and not selected_calls
    await workflow.close()
