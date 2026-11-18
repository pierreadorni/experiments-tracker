from types import SimpleNamespace
from uuid import uuid4

import pytest
from test_research_job_source import setup_request

from experimenttracker.research.job_dispatch import dispatch
from experimenttracker.slurm import SubmissionUncertain
from experimenttracker.store import Conflict


class FakeScheduler:
    def __init__(self, workflow, lose_response):
        self.workflow = workflow
        self.lose_response = lose_response
        self.submissions = 0

    async def preflight(self, intent, script, root, resources):
        assert intent in self.workflow.submissions_in_flight
        assert not self.workflow.store.one("SELECT 1 FROM jobs WHERE intent_id=?", (intent,))
        assert script.is_file()

    async def submit(self, intent, script, root, *, resources):
        assert intent in self.workflow.submissions_in_flight
        assert (
            self.workflow.store.one("SELECT state FROM jobs WHERE intent_id=?", (intent,))["state"]
            == "submitting"
        )
        self.submissions += 1
        if self.lose_response:
            raise SubmissionUncertain("lost synthetic reply")
        return "42"

    async def reconcile(self, intent):
        return "42"

    async def observe(self, job_id):
        assert job_id == "42"
        return SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9)


def accept(store, eid, request):
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )


@pytest.mark.parametrize("lose_response", [False, True])
async def test_sends_once_and_adopts_exact_job_after_lost_response(
    runtime, tmp_path, monkeypatch, lose_response
):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    accept(store, eid, request)
    scheduler = FakeScheduler(workflow, lose_response)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)
    if lose_response:
        with pytest.raises(SubmissionUncertain, match="lost synthetic reply"):
            await dispatch(workflow, eid, action, request)
        assert (await dispatch(workflow, eid, action, request))["state"] == "submitting"
    else:
        assert (await dispatch(workflow, eid, action, request))["job_id"] == "42"
    with pytest.raises(Conflict, match="another submission"):
        await dispatch(workflow, eid, action, {**request, "name": "different"})
    await workflow.observe_jobs()
    assert (await dispatch(workflow, eid, action, request))["state"] == "complete"
    assert scheduler.submissions == 1
    assert len(store.rows("SELECT id FROM agent_wakeups WHERE experiment_id=?", (eid,))) == 2
    assert not workflow.submissions_in_flight


async def test_pause_during_preflight_refuses_persistence_and_submission(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    accept(store, eid, request)
    scheduler = FakeScheduler(workflow, False)

    async def pause(*args):
        store.update(eid, pause_compute=True)

    scheduler.preflight = pause
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)
    with pytest.raises(ValueError, match="paused"):
        await dispatch(workflow, eid, action, request)
    assert not store.rows("SELECT * FROM jobs")
    assert scheduler.submissions == 0
    assert not workflow.submissions_in_flight
