"""Synthetic C5 submission-resolver decisions: adoption, quarantine and proven absence."""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker import clock, resolver
from experimenttracker.core import job_obligations
from experimenttracker.execution_settings import ExecutionSettings
from experimenttracker.slurm import SubmissionUncertain
from experimenttracker.store import encode


class Scheduler:
    """Scheduler surface for one saved intent; the test scripts every answer."""

    def __init__(self, match=None, error=None, observation=None):
        self.match = match
        self.error = error
        self.observation = observation
        self.reconciled = []
        self.submitted = 0

    async def submit(self, *args, **kwargs):
        self.submitted += 1
        raise AssertionError("the resolver never submits")

    async def reconcile(self, intent_id):
        self.reconciled.append(intent_id)
        if self.error:
            raise self.error
        return self.match

    async def observe(self, job_id):
        assert self.observation is not None, f"synthetic scheduler has no observation for {job_id}"
        return self.observation


def saved_intent(store, *, created_at=None, state="submitting", job_id=None, lag=None):
    eid = seed_saved_contract3(store, "Synthetic resolver decision")
    intent_id = str(uuid4())
    contract = store.experiment(eid)["contract"]
    if lag is not None:
        contract["execution"] = {**contract["execution"], "accounting_lag_seconds": lag}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_manifest) VALUES(?,?, 'prepare',1,?,?, 'synthetic','{}',"
            "'control',?,?)",
            (intent_id, eid, state, job_id, clock.now() if created_at is None else created_at, None),
        )
        job_obligations.sync_job(db, intent_id)
    return eid, store.one("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))


def decisions(store, eid):
    return [
        json.loads(row["payload"])
        for row in store.rows(
            "SELECT payload FROM events WHERE experiment_id=? AND kind='job_reconciled'", (eid,)
        )
    ]


def obligation(store, intent_id):
    return store.one("SELECT * FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (intent_id,))


def test_accounting_lag_defaults_and_overrides_per_execution_context(runtime):
    store = runtime[0]
    assert ExecutionSettings().accounting_lag_seconds == 600
    assert ExecutionSettings(accounting_lag_seconds=3600).accounting_lag_seconds == 3600
    for invalid in (59, 86401, 600.0, "600", True):
        with pytest.raises(ValueError):
            ExecutionSettings(accounting_lag_seconds=invalid)

    eid, _ = saved_intent(store)
    assert resolver.accounting_lag_seconds(store.experiment(eid)) == 600
    contract = store.experiment(eid)["contract"]
    contract["execution"] = {**contract["execution"], "accounting_lag_seconds": 900}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    assert resolver.accounting_lag_seconds(store.experiment(eid)) == 900


async def test_adoption_saves_the_exact_identity_once(runtime):
    store, _, _, workflow = runtime
    eid, job = saved_intent(store)
    workflow.scheduler_for = lambda exp, saved: Scheduler(match="4201")

    adopted = await resolver.resolve(workflow, store.experiment(eid), job)

    assert (adopted["job_id"], adopted["state"]) == ("4201", "submitted")
    assert job["job_id"] is None  # the caller's stale snapshot was never written back
    assert decisions(store, eid) == [
        {
            "intent_id": job["intent_id"],
            "job_id": "4201",
            "evidence": {"query": "squeue+sacct", "observer": "observe_jobs", "matched": 1},
        }
    ]
    saved_obligations = store.rows(
        "SELECT owner,wait,state FROM obligations WHERE target=? ORDER BY id", (job["intent_id"],)
    )
    assert [row["state"] for row in saved_obligations] == ["abandoned", "open"]
    assert (saved_obligations[1]["owner"], saved_obligations[1]["wait"]) == ("external", "job:4201")
    assert store.experiment(eid)["hold_reason"] is None
    assert await resolver.resolve(workflow, store.experiment(eid), adopted) is None
    assert len(decisions(store, eid)) == 1


async def test_several_matches_quarantine_without_adoption(runtime):
    store, _, _, workflow = runtime
    eid, job = saved_intent(store)
    workflow.scheduler_for = lambda exp, saved: Scheduler(error=SubmissionUncertain("multiple matches"))

    assert await resolver.resolve(workflow, store.experiment(eid), job) is None

    saved = store.one("SELECT state,job_id,error FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved == {"state": "submitting", "job_id": None, "error": None}
    experiment = store.experiment(eid)
    assert (experiment["hold_reason"], experiment["error"]) == ("submission_uncertain", "multiple matches")
    [written] = decisions(store, eid)
    assert (written["outcome"], written["evidence"]["matched"]) == ("quarantined", "several")
    assert store.one("SELECT kind FROM inbox WHERE experiment_id=?", (eid,)) == {"kind": "executor_error"}
    assert obligation(store, job["intent_id"])["owner"] == "backend"
    # The same quarantine is not recorded twice.
    await resolver.resolve(workflow, store.experiment(eid), job)
    assert len(decisions(store, eid)) == 1


async def test_unreadable_scheduler_concludes_nothing(runtime):
    store, _, _, workflow = runtime
    eid, job = saved_intent(store, created_at=clock.now() - 86400)
    workflow.scheduler_for = lambda exp, saved: Scheduler(error=TimeoutError("sacct timed out"))

    assert await resolver.resolve(workflow, store.experiment(eid), job) is None

    saved = store.one("SELECT state,job_id,error FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved == {"state": "submitting", "job_id": None, "error": None}
    experiment = store.experiment(eid)
    assert (experiment["hold_reason"], experiment["error"]) == ("submission_uncertain", "sacct timed out")
    assert decisions(store, eid) == []
    assert obligation(store, job["intent_id"])["state"] == "open"


@pytest.mark.parametrize(
    ("waited", "state", "receipt"),
    [(60, "submitting", None), (700, "failed", "not_submitted"), (7 * 86400, "submitting", None)],
)
async def test_absence_is_proven_only_after_the_accounting_lag(runtime, waited, state, receipt):
    store, _, _, workflow = runtime
    eid, job = saved_intent(store, created_at=clock.now() - waited)
    workflow.scheduler_for = lambda exp, saved: Scheduler(match=None)

    assert await resolver.resolve(workflow, store.experiment(eid), job) is None

    saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert (saved["state"], saved["job_id"]) == (state, None)
    experiment = store.experiment(eid)
    if receipt is None:
        # Inside the lag, or beyond the accounting window, absence is unprovable.
        assert saved["error"] is None
        assert (experiment["hold_reason"], experiment["error"]) == (
            "submission_uncertain",
            resolver.WAITING_ERROR,
        )
        assert decisions(store, eid) == []
        return
    assert saved["error"] == "not_submitted: no scheduler record after accounting lag"
    assert job_obligations.not_submitted(saved) and experiment["hold_reason"] is None
    [written] = decisions(store, eid)
    assert (written["outcome"], written["intent_id"]) == (receipt, job["intent_id"])
    assert written["evidence"]["matched"] == 0
    assert written["evidence"]["accounting_lag_seconds"] == 600
    assert written["evidence"]["waited_seconds"] >= waited
    assert obligation(store, job["intent_id"])["state"] == "abandoned"
    # A replay while the row is terminal changes nothing more.
    assert await resolver.resolve(workflow, store.experiment(eid), saved) is None
    assert len(decisions(store, eid)) == 1


async def test_proven_absence_is_not_an_active_predecessor(runtime):
    from experimenttracker import early_repair

    store, _, _, workflow = runtime
    eid, job = saved_intent(store, created_at=clock.now() - 700, lag=600)
    store.db.execute("UPDATE experiments SET evidence_revision=2 WHERE id=?", (eid,))
    workflow.scheduler_for = lambda exp, saved: Scheduler(match=None)
    assert await resolver.resolve(workflow, store.experiment(eid), job) is None

    assert early_repair.active_predecessor(workflow, store.experiment(eid), "prepare") is None
    store.db.execute("UPDATE jobs SET error='known local failure' WHERE intent_id=?", (job["intent_id"],))
    assert not job_obligations.not_submitted(
        store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    )
    assert (
        early_repair.active_predecessor(workflow, store.experiment(eid), "prepare")["intent_id"]
        == job["intent_id"]
    )
