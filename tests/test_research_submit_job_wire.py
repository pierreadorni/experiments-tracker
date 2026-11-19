"""Contract-4 submit_job MCP bridge and bounded receipt."""

from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_research_job_source import setup_request

from experimenttracker import controller
from experimenttracker.research.admission import SubmitJobRequest, preflight
from experimenttracker.slurm import SubmissionUncertain
from experimenttracker.store import Conflict, encode


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


def test_submit_job_optional_result_file_preserves_historical_request_identity():
    request = payload()
    model = SubmitJobRequest.model_validate(request)
    expected = model.model_dump(mode="json", exclude={"result_file", "tensorboard"})
    assert expected["resources"] != request["resources"]  # Historical receipts include defaults.
    assert model.saved_request() == expected
    assert SubmitJobRequest.model_validate({**request, "result_file": None}).saved_request() == expected
    assert SubmitJobRequest.model_validate({**request, "tensorboard": None}).saved_request() == expected
    assert SubmitJobRequest.model_validate(
        {**request, "result_file": "metrics/result.json"}
    ).saved_request() == {**expected, "result_file": "metrics/result.json"}
    tensorboard = {"log_dir": "tensorboard/fit", "scalar_tag": "loss/train", "image_tag": "prediction"}
    assert SubmitJobRequest.model_validate({**request, "tensorboard": tensorboard}).saved_request() == {
        **expected,
        "tensorboard": tensorboard,
    }
    for path in ("", "../outside.json", "/tmp/result.json", "C:\\\\result.json", "a\x00b", "x" * 1025):
        with pytest.raises(ValidationError):
            SubmitJobRequest.model_validate({**request, "result_file": path})
    with pytest.raises(ValidationError):
        SubmitJobRequest.model_validate({**request, "tensorboard": {"log_dir": "../outside"}})


async def test_jean_zay_submit_rejects_unsupported_tensorboard_selection(runtime, tmp_path):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    exp = store.experiment(eid)
    contract = dict(
        exp["contract"],
        execution={**exp["contract"].get("execution", {}), "context": "jean-zay"},
    )
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    request["result_file"] = "metrics/result.json"
    request["tensorboard"] = {
        "log_dir": "tensorboard/fit",
        "scalar_tag": "loss/train",
        "image_tag": None,
    }

    with pytest.raises(ValueError, match="TensorBoard previews are not supported for Jean Zay"):
        preflight(workflow, eid, action, request, remote_source=True)

    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)) == []


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
@pytest.mark.parametrize("result_file", [None, "metrics/result.json"])
async def test_submit_job_receipt_and_lost_reply_replay_never_resubmit(
    runtime, tmp_path, monkeypatch, lose, result_file
):
    store, _, _, workflow = runtime
    _, eid, action, request = setup_request(workflow, tmp_path)
    if result_file is not None:
        request["result_file"] = result_file
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
    replay = await workflow.controller.call(eid, "submit_job", {**request, "result_file": result_file})
    assert replay["state"] == ("submitting" if lose else "submitted")
    with pytest.raises(Conflict, match="another submission"):
        await workflow.controller.call(eid, "submit_job", {**request, "result_file": "different.json"})
    assert scheduler.submissions == 1
