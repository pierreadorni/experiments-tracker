"""Contract-4 submit_job MCP bridge and bounded receipt."""

from uuid import uuid4

import pytest
from test_research_job_source import setup_request

from experimenttracker import controller
from experimenttracker.research.admission import SubmitJobRequest
from experimenttracker.slurm import SubmissionUncertain


def payload(request_id=None, approval_id=None):
    return {
        "request_id": str(request_id or uuid4()),
        "approval_request_id": str(approval_id or uuid4()),
        "objective": "Compare synthetic inputs",
        "name": "fit",
        "command": ["python", "comparison.py"],
        "resources": {"cpus_per_job": 2},
    }


def accept(store, eid, request):
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )


def test_submit_job_schema_is_advertised_exactly():
    served = {item["name"]: item for item in controller.served_tools(research=True)}
    assert served["submit_job"]["inputSchema"] == SubmitJobRequest.model_json_schema()
    assert "submit_job" not in {item["name"] for item in controller.served_tools()}


async def test_submit_job_refuses_before_approval(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)

    def impossible(*args):
        pytest.fail("Git must not be changed before approval")

    monkeypatch.setattr(workflow.workspaces, "commit_branch", impossible)
    # establish the exact pool claim expected of the live MCP caller
    pool = workflow.agent_pool
    slot = pool.primary
    pool.claim(
        role="research", source_table="agent_actions", row=action, owner=("experiment", eid), slot=slot
    )
    with pytest.raises(ValueError, match="exact owner approval"):
        await workflow.controller.call(eid, "submit_job", request)
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))


class FakeScheduler:
    def __init__(self, workflow, lose):
        self.workflow, self.lose, self.submissions = workflow, lose, 0

    async def preflight(self, *args):
        pass

    async def submit(self, *args, **kwargs):
        self.submissions += 1
        if self.lose:
            raise SubmissionUncertain("lost reply")
        return "42"


@pytest.mark.parametrize("lose", [False, True])
async def test_submit_job_receipt_and_lost_reply_replay_never_resubmit(runtime, tmp_path, monkeypatch, lose):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    accept(store, eid, request)
    pool = workflow.agent_pool
    pool.claim(
        role="research",
        source_table="agent_actions",
        row=action,
        owner=("experiment", eid),
        slot=pool.primary,
    )
    scheduler = FakeScheduler(workflow, lose)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)
    request_id = request["request_id"]
    if lose:
        with pytest.raises(SubmissionUncertain):
            await workflow.controller.call(eid, "submit_job", request)
    else:
        receipt = await workflow.controller.call(eid, "submit_job", request)
        assert set(receipt) == {"state", "intent_id", "job_id", "commit"}
        assert receipt["intent_id"] == request_id and receipt["job_id"] == "42"
        assert "command" not in str(receipt) and "execution_root" not in receipt
    replay = await workflow.controller.call(eid, "submit_job", request)
    assert replay["state"] == ("submitting" if lose else "submitted")
    assert scheduler.submissions == 1
