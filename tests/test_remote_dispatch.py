import asyncio
from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_research_admission import payload
from test_research_job_intent import PREPARED, setup_intent

from experimenttracker.research import remote_dispatch
from experimenttracker.research.job_dispatch import record_pre_send_failure, record_submission
from experimenttracker.research.job_intent import save
from experimenttracker.slurm import SubmissionUncertain
from experimenttracker.store import Conflict


class DispatchStore:
    def __init__(self):
        self.jobs = {}

    def one(self, query, params):
        assert query == "SELECT 1 FROM jobs WHERE intent_id=?"
        return {"intent_id": params[0]} if params[0] in self.jobs else None

    def experiment(self, _):
        return {"id": "experiment"}


class DispatchWorkflow:
    def __init__(self, scheduler):
        self.store = DispatchStore()
        self.submissions_in_flight = set()
        self.scheduler = scheduler
        self.selected_job = None

    def scheduler_for(self, _experiment, job=None):
        self.selected_job = job
        return self.scheduler


def request_for(intent):
    return {**payload(intent, "synthetic"), "request_id": intent}


def harness(monkeypatch, *, send):
    intent = str(uuid4())
    events = []
    job = {"intent_id": intent, "state": "submitting"}
    receipt = {"intent_id": intent}
    scheduler = SimpleNamespace()

    async def prepare(*_):
        events.append("prepare")
        assert workflow.submissions_in_flight == {intent}
        return SimpleNamespace(prepared="prepared", execution={"context": "jean-zay"})

    def save(*_args, **_kwargs):
        events.append("save")
        workflow.store.jobs[intent] = job
        return job, True

    async def stage(_job):
        events.append("stage")
        return receipt

    async def submit(_job, _receipt):
        events.append("send")
        assert workflow.submissions_in_flight == {intent}
        return await send()

    def record(store, eid, saved_job, identity):
        events.append(("record", identity))
        saved_job.update(state="submitted", job_id=identity)
        return saved_job

    def fail(store, eid, saved_job, error):
        events.append(("not-sent", str(error)))
        saved_job.update(state="failed", job_id=None, error="not_submitted: pre-send failure")
        return saved_job

    monkeypatch.setattr(remote_dispatch, "prepare_remote", prepare)
    monkeypatch.setattr(remote_dispatch, "save", save)
    monkeypatch.setattr(remote_dispatch, "record_submission", record)
    monkeypatch.setattr(remote_dispatch, "record_pre_send_failure", fail)
    scheduler.stage_research_source = stage
    scheduler.submit_research_job = submit
    workflow = DispatchWorkflow(scheduler)
    return workflow, intent, events, job


def test_dispatch_uses_saved_job_route_persists_exact_identity_and_rejects_replay(monkeypatch):
    async def send():
        return "remote-id"

    workflow, intent, events, job = harness(monkeypatch, send=send)

    async def run():
        result = await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))
        assert workflow.selected_job is job
        assert result["job"]["job_id"] == "remote-id"
        assert intent not in workflow.submissions_in_flight
        with pytest.raises(Conflict, match="not permission to send or replay"):
            await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))

    asyncio.run(run())
    assert events == ["prepare", "save", "stage", "send", ("record", "remote-id")]


@pytest.mark.parametrize("failure_point", ["stage", "paused_preflight"])
def test_proven_pre_send_failure_is_durable_and_not_replayed(monkeypatch, failure_point):
    async def send():
        raise RuntimeError("agent dispatch paused")

    workflow, intent, events, job = harness(monkeypatch, send=send)
    if failure_point == "stage":

        async def fail_stage(_job):
            events.append("stage")
            raise ValueError("source staging failed")

        workflow.scheduler.stage_research_source = fail_stage

    async def run():
        with pytest.raises((ValueError, RuntimeError)):
            await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))
        assert job["state"] == "failed" and job["error"].startswith("not_submitted:")
        with pytest.raises(Conflict, match="not permission to send or replay"):
            await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))
        assert intent not in workflow.submissions_in_flight

    asyncio.run(run())
    assert events.count("send") == (0 if failure_point == "stage" else 1)


def test_saved_exact_identity_and_obligation_survive_restart(runtime):
    store, _scheduler, _agent, workflow = runtime
    eid, action, request = setup_intent(store)
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    job, created = save(workflow, eid, action, request, PREPARED)
    assert created

    saved = record_submission(store, eid, job, "12345")
    assert saved["state"] == "submitted" and saved["job_id"] == "12345"
    obligation = store.one(
        "SELECT owner,kind,wait FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (job["intent_id"],)
    )
    assert obligation == {"owner": "external", "kind": "job", "wait": "job:12345"}

    restarted = DispatchWorkflow(SimpleNamespace())
    restarted.store = store
    with pytest.raises(Conflict, match="not permission to send or replay"):
        asyncio.run(remote_dispatch.dispatch(restarted, eid, action, request))


def test_pre_send_failure_is_durable_with_no_external_obligation(runtime):
    store, _scheduler, _agent, workflow = runtime
    eid, action, request = setup_intent(store)
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    job, created = save(workflow, eid, action, request, PREPARED)
    assert created

    failed = record_pre_send_failure(store, eid, job, RuntimeError("agent dispatch paused"))
    assert failed["state"] == "failed" and failed["job_id"] is None
    assert failed["error"].startswith("not_submitted: pre-send failure:")
    assert (
        store.one(
            "SELECT state FROM obligations WHERE target=? ORDER BY id DESC LIMIT 1", (job["intent_id"],)
        )["state"]
        == "abandoned"
    )


def test_uncertain_send_restart_adopts_exact_identity_without_replay(monkeypatch):
    async def send():
        raise SubmissionUncertain("send outcome unknown")

    workflow, intent, events, job = harness(monkeypatch, send=send)

    async def run():
        with pytest.raises(SubmissionUncertain):
            await remote_dispatch.dispatch(workflow, "experiment", {}, request_for(intent))
        assert job["state"] == "submitting" and job.get("job_id") is None
        assert intent not in workflow.submissions_in_flight

        # Simulate process restart and exact reconciliation adopting one identity.
        restarted = DispatchWorkflow(workflow.scheduler)
        restarted.store.jobs[intent] = job
        job.update(state="submitted", job_id="adopted-id")
        with pytest.raises(Conflict, match="not permission to send or replay"):
            await remote_dispatch.dispatch(restarted, "experiment", {}, request_for(intent))
        assert job["job_id"] == "adopted-id"

    asyncio.run(run())
    assert events.count("send") == 1
