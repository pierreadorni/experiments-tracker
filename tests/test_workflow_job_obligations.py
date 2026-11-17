"""Workflow job source writes stay atomic with existing intent events."""

import json

import pytest
from helpers import prepared, wait_until

from experimenttracker.core import job_obligations
from experimenttracker.slurm import ResourceUnavailable, SubmissionUncertain


def obligations_for(store, intent_id):
    prefix = f"job:{intent_id}:"
    rows = store.rows("SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix))
    return [row for row in rows if row["id"].rsplit(":", 1)[0] == f"job:{intent_id}"]


def submission_event(store, eid, kind, intent_id):
    return next(
        (
            row
            for row in store.rows("SELECT kind,payload FROM events WHERE experiment_id=?", (eid,))
            if row["kind"] == kind and json.loads(row["payload"]).get("intent_id") == intent_id
        ),
        None,
    )


@pytest.mark.asyncio
async def test_intent_is_committed_before_scheduler_submit_and_exact_id_handoff_replays(runtime):
    store, scheduler, _, workflow, eid = await prepared(runtime)
    accepted_id = "scheduler-accepted-42"
    calls = []

    async def submit(intent_id, *args, **kwargs):
        calls.append(intent_id)
        [job] = store.rows("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))
        [backend] = obligations_for(store, intent_id)
        assert job["state"] == "submitting" and job["job_id"] is None
        assert (backend["owner"], backend["kind"], backend["wait"], backend["state"]) == (
            "backend",
            "reconcile",
            f"reconcile:{intent_id}",
            "open",
        )
        assert submission_event(store, eid, "submission_intent", intent_id)
        return accepted_id

    scheduler.submit = submit

    async def tick():
        await workflow.tick(force_observe=True)

    job = await wait_until(
        lambda: next(
            (
                row
                for row in store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
                if row["job_id"] == accepted_id
            ),
            None,
        ),
        ticks=tick,
        describe=lambda: store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)),
    )
    assert calls == [job["intent_id"]]
    rows = obligations_for(store, job["intent_id"])
    assert [(row["owner"], row["kind"], row["wait"], row["state"]) for row in rows] == [
        ("backend", "reconcile", f"reconcile:{job['intent_id']}", "abandoned"),
        ("external", "job", f"job:{accepted_id}", "open"),
    ]
    events = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    assert submission_event(store, eid, "job_submitted", job["intent_id"])
    replay = await workflow.submit_prepared_stage(store.experiment(eid), job["stage_id"], job["intent_id"])
    assert replay == {"intent_id": job["intent_id"], "job_id": accepted_id, "state": "submitted"}
    assert calls == [job["intent_id"]]
    assert obligations_for(store, job["intent_id"]) == rows
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events
    await workflow.close()


@pytest.mark.asyncio
async def test_uncertain_scheduler_response_keeps_the_saved_backend_intent(runtime):
    store, _, _, workflow, eid = await prepared(runtime)
    calls = []

    async def submit(intent_id, *args, **kwargs):
        calls.append(intent_id)
        [job] = store.rows("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))
        [backend] = obligations_for(store, intent_id)
        assert job["state"] == "submitting" and job["job_id"] is None
        assert backend["owner"] == "backend" and backend["state"] == "open"
        assert submission_event(store, eid, "submission_intent", intent_id)
        raise SubmissionUncertain("synthetic accepted outcome is unknown")

    workflow.scheduler.submit = submit

    async def tick():
        await workflow.tick(force_observe=True)

    await wait_until(
        lambda: store.experiment(eid)["hold_reason"] == "submission_uncertain",
        ticks=tick,
        describe=lambda: store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)),
    )
    [job] = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert calls == [job["intent_id"]]
    assert (job["state"], job["job_id"]) == ("submitting", None)
    assert [
        (row["owner"], row["kind"], row["wait"], row["state"])
        for row in obligations_for(store, job["intent_id"])
    ] == [("backend", "reconcile", f"reconcile:{job['intent_id']}", "open")]
    assert submission_event(store, eid, "job_submitted", job["intent_id"]) is None
    await workflow.close()


@pytest.mark.asyncio
async def test_missing_id_reconcile_hands_off_only_after_saved_id(runtime):
    store, scheduler, _, workflow, eid = await prepared(runtime)
    calls = []

    async def submit(intent_id, *args, **kwargs):
        calls.append(intent_id)
        raise SubmissionUncertain("lost response")

    scheduler.submit = submit

    async def tick():
        await workflow.tick(force_observe=True)

    await wait_until(
        lambda: store.experiment(eid)["hold_reason"] == "submission_uncertain",
        ticks=tick,
        describe=lambda: store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)),
    )
    [job] = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    reconciled_id = "reconciled-exact-77"

    async def reconcile(intent_id):
        assert intent_id == job["intent_id"]
        return reconciled_id

    async def fail_observe(job_id):
        assert job_id == reconciled_id
        raise ResourceUnavailable("stop after the identity handoff")

    scheduler.reconcile = reconcile
    scheduler.observe = fail_observe
    await workflow.observe_jobs()

    saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert (saved["state"], saved["job_id"]) == ("submitted", reconciled_id)
    [backend, external] = obligations_for(store, job["intent_id"])
    assert (backend["owner"], backend["state"]) == ("backend", "abandoned")
    assert (external["owner"], external["kind"], external["wait"], external["state"]) == (
        "external",
        "job",
        f"job:{reconciled_id}",
        "open",
    )
    assert calls == [job["intent_id"]]
    await workflow.close()


@pytest.mark.asyncio
async def test_intent_hook_failure_rolls_back_before_any_scheduler_call(runtime, monkeypatch):
    store, scheduler, _, workflow, eid = await prepared(runtime)
    calls = []
    original = job_obligations.sync_job

    def fail_after_mapping(db, intent_id, *, before=None):
        original(db, intent_id, before=before)
        if before is None:
            raise RuntimeError("injected intent obligation failure")

    monkeypatch.setattr(job_obligations, "sync_job", fail_after_mapping)

    async def submit(intent_id, *args, **kwargs):
        calls.append(intent_id)
        return "must-not-be-submitted"

    scheduler.submit = submit

    async def tick():
        await workflow.tick(force_observe=True)

    await wait_until(
        lambda: store.experiment(eid)["hold_reason"] == "executor_error",
        ticks=tick,
        describe=lambda: store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)),
    )
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)) == []
    assert not store.rows("SELECT * FROM events WHERE experiment_id=? AND kind='submission_intent'", (eid,))
    assert not store.rows("SELECT * FROM obligations WHERE id LIKE 'job:%'")
    assert calls == []
    await workflow.close()


@pytest.mark.asyncio
async def test_missing_id_mapper_conflict_rolls_back_without_scientific_failure(runtime, monkeypatch):
    store, _, _, workflow, eid = await prepared(runtime)

    async def submit(intent_id, *args, **kwargs):
        raise SubmissionUncertain("lost response")

    workflow.scheduler.submit = submit

    async def tick():
        await workflow.tick(force_observe=True)

    await wait_until(
        lambda: store.experiment(eid)["hold_reason"] == "submission_uncertain",
        ticks=tick,
        describe=lambda: store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)),
    )
    [job] = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))

    async def reconcile(intent_id):
        assert intent_id == job["intent_id"]
        return "mapper-conflict-id"

    workflow.scheduler.reconcile = reconcile
    original = job_obligations.sync_job

    def fail_after_mapping(db, intent_id, *, before=None):
        original(db, intent_id, before=before)
        if before is not None:
            raise ValueError("synthetic obligation history conflict")

    monkeypatch.setattr(job_obligations, "sync_job", fail_after_mapping)
    await workflow.observe_jobs()

    saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert (saved["state"], saved["job_id"]) == ("submitting", None)
    assert [
        (row["owner"], row["kind"], row["wait"], row["state"])
        for row in obligations_for(store, job["intent_id"])
    ] == [("backend", "reconcile", f"reconcile:{job['intent_id']}", "open")]
    assert not store.rows(
        "SELECT 1 FROM events WHERE experiment_id=? AND kind='output_validation_failed'", (eid,)
    )
    assert store.experiment(eid)["hold_reason"] == "executor_unavailable"
    await workflow.close()


@pytest.mark.asyncio
async def test_submit_acceptance_does_not_overwrite_newer_saved_job_state(runtime):
    store, _, _, workflow, eid = await prepared(runtime)
    accepted_id = "scheduler-accepted-after-cancel"
    calls = []

    async def submit(intent_id, *args, **kwargs):
        calls.append(intent_id)
        with store.tx() as db:
            db.execute("UPDATE jobs SET state='cancelled' WHERE intent_id=?", (intent_id,))
        return accepted_id

    workflow.scheduler.submit = submit

    async def tick():
        await workflow.tick(force_observe=True)

    await wait_until(
        lambda: store.one("SELECT 1 FROM jobs WHERE experiment_id=? AND state='cancelled'", (eid,)),
        ticks=tick,
        describe=lambda: store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)),
    )

    [job] = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert calls == [job["intent_id"]]
    assert (job["state"], job["job_id"]) == ("cancelled", None)
    assert [
        (row["owner"], row["kind"], row["wait"], row["state"])
        for row in obligations_for(store, job["intent_id"])
    ] == [("backend", "reconcile", f"reconcile:{job['intent_id']}", "open")]
    assert submission_event(store, eid, "job_submitted", job["intent_id"]) is None
    await workflow.close()
