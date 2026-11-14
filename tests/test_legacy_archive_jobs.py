from uuid import uuid4

import pytest

from experimenttracker import legacy_archive
from experimenttracker import remote as remote_module
from experimenttracker.slurm import Observation
from experimenttracker.store import encode


@pytest.mark.parametrize(
    ("contract", "phase", "deleted_at", "frozen", "active"),
    [
        ({}, "training", None, True, True),
        ({"execution_contract": 1}, "training", None, True, True),
        ({"execution_contract": 2}, "training", None, True, True),
        ({"execution_contract": 3}, "training", None, False, False),
        ({"execution_contract": 2}, "failed", None, True, False),
        ({"execution_contract": 2}, "training", 0, True, False),
    ],
)
def test_archive_predicates_match_saved_contract_boundary(contract, phase, deleted_at, frozen, active):
    exp = {"contract": contract, "phase": phase, "deleted_at": deleted_at}
    assert legacy_archive.execution_frozen(exp) is frozen
    assert legacy_archive.archive_active(exp) is active


def legacy_job(store):
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Synthetic archived fixture.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    exp = store.experiment(eid)
    contract = {**exp["contract"], "execution_contract": 2}
    store.update(eid, phase="training", hold_reason="legacy_contract_archived")
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            ("intent-1", eid, "prepare", "submitted", "123", "s", "{}", "h", 1),
        )
    return store.experiment(eid), store.one("SELECT * FROM jobs WHERE intent_id='intent-1'")


class Scheduler:
    def __init__(self, observation=None, error=None):
        self.observation = observation or Observation("RUNNING")
        self.error = error
        self.reconciled = []
        self.observed = []
        self.submitted = 0

    async def reconcile(self, intent):
        self.reconciled.append(intent)
        return "456" if self.observation else None

    async def observe(self, job_id):
        self.observed.append(job_id)
        if self.error:
            raise self.error
        return self.observation

    async def submit(self, *args, **kwargs):
        self.submitted += 1
        raise AssertionError("settlement must not submit")


@pytest.mark.parametrize(
    ("observation", "state"),
    [
        (Observation("COMPLETED", "0:0"), "complete"),
        (Observation("FAILED", "1:0"), "failed"),
        (Observation("ACCOUNTING_PENDING"), "submitted"),
        (Observation("COMPLETED"), "submitted"),
    ],
)
async def test_settlement_records_scheduler_facts_without_advancing(runtime, observation, state):
    store, _, _, workflow = runtime
    exp, job = legacy_job(store)
    scheduler = Scheduler(observation)
    workflow.scheduler_for = lambda saved_exp, saved_job: scheduler
    stages = store.rows(
        "SELECT id,state,output_manifest,error FROM stages WHERE experiment_id=?", (exp["id"],)
    )
    artifacts = store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (exp["id"],))

    assert await legacy_archive.settle_job(workflow, exp, job)

    saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved["state"] == state
    assert saved["scheduler_state"] == observation.state
    assert store.experiment(exp["id"])["phase"] == "training"
    assert store.experiment(exp["id"])["hold_reason"] == "legacy_contract_archived"
    assert (
        store.rows("SELECT id,state,output_manifest,error FROM stages WHERE experiment_id=?", (exp["id"],))
        == stages
    )
    assert store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (exp["id"],)) == artifacts
    assert scheduler.submitted == 0
    event_count = store.one(
        "SELECT COUNT(*) AS n FROM events WHERE experiment_id=? AND kind='job_observed'", (exp["id"],)
    )["n"]
    job = saved
    await legacy_archive.settle_job(workflow, exp, job)
    assert (
        store.one(
            "SELECT COUNT(*) AS n FROM events WHERE experiment_id=? AND kind='job_observed'", (exp["id"],)
        )["n"]
        == event_count
    )


@pytest.mark.parametrize("match", ["456", None])
async def test_missing_saved_job_id_reconciles_only_exact_intent(runtime, match):
    store, _, _, workflow = runtime
    exp, job = legacy_job(store)
    with store.tx() as db:
        db.execute("UPDATE jobs SET job_id=NULL,state='submitting' WHERE intent_id=?", (job["intent_id"],))
    job["job_id"], job["state"] = None, "submitting"
    scheduler = Scheduler(Observation("RUNNING"))

    async def reconcile(intent):
        scheduler.reconciled.append(intent)
        return match

    scheduler.reconcile = reconcile
    workflow.scheduler_for = lambda saved_exp, saved_job: scheduler

    await legacy_archive.settle_job(workflow, exp, job)

    assert scheduler.reconciled == [job["intent_id"]]
    saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved["job_id"] == match
    assert saved["state"] == ("submitted" if match else "submitting")
    assert scheduler.observed == ([match] if match else [])
    assert store.one(
        "SELECT COUNT(*) AS n FROM events WHERE experiment_id=? AND kind='job_reconciled'", (exp["id"],)
    )["n"] == int(bool(match))


async def test_observation_error_only_records_job_error(runtime):
    store, _, _, workflow = runtime
    exp, job = legacy_job(store)
    scheduler = Scheduler(error=OSError("scheduler unavailable"))
    workflow.scheduler_for = lambda saved_exp, saved_job: scheduler

    assert not await legacy_archive.settle_job(workflow, exp, job)

    saved = store.one("SELECT state,error,scheduler_state FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved == {"state": "submitted", "error": "scheduler unavailable", "scheduler_state": None}
    assert store.experiment(exp["id"])["hold_reason"] == "legacy_contract_archived"


@pytest.mark.parametrize("failure_site", ["routing", "remote_due"])
async def test_routing_and_remote_due_errors_only_record_job_error(runtime, monkeypatch, failure_site):
    store, _, _, workflow = runtime
    exp, job = legacy_job(store)
    if failure_site == "routing":
        workflow.scheduler_for = lambda *args: (_ for _ in ()).throw(OSError("route failed"))
        error = "route failed"
    else:

        class Remote:
            def due(self, *args):
                raise OSError("poll clock failed")

        monkeypatch.setattr(remote_module, "RemoteSlurmScheduler", Remote)
        workflow.scheduler_for = lambda *args: Remote()
        error = "poll clock failed"

    assert not await legacy_archive.settle_job(workflow, exp, job)
    saved = store.one("SELECT state,error,scheduler_state FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved == {"state": "submitted", "error": error, "scheduler_state": None}
    assert store.experiment(exp["id"])["hold_reason"] == "legacy_contract_archived"


async def test_saved_route_and_remote_poll_cadence_are_respected(runtime, monkeypatch):
    store, _, _, workflow = runtime
    exp, job = legacy_job(store)
    exp["contract"]["execution"]["context"] = "saved-route"
    remote = type("Remote", (), {"due": lambda self, row, now: False})()
    routes = []
    monkeypatch.setattr(remote_module, "RemoteSlurmScheduler", type(remote))
    workflow.scheduler_for = lambda saved_exp, saved_job: routes.append((saved_exp, saved_job)) or remote

    assert not await legacy_archive.settle_job(workflow, exp, job)
    assert routes == [(exp, job)]
    assert (
        store.one("SELECT remote_polled_at FROM jobs WHERE intent_id=?", (job["intent_id"],))[
            "remote_polled_at"
        ]
        is None
    )
