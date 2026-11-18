from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_research_approval import request_fixture

from experimenttracker.research.admission import SubmitJobRequest, preflight


def payload(approval_id=None, objective="Compare the baseline"):
    return {
        "request_id": str(uuid4()),
        "approval_request_id": approval_id or str(uuid4()),
        "objective": objective,
        "name": "fit",
        "command": ["python", "comparison.py", "a b"],
        "resources": {"cpus_per_job": 2},
    }


@pytest.mark.parametrize("command", ["python run.py", [""], ["python", "bad\0arg"]])
def test_submit_schema_refuses_invalid_argv(command):
    with pytest.raises(ValidationError):
        SubmitJobRequest.model_validate({**payload(), "command": command})


def test_admission_requires_real_owner_approval_and_preserves_argv(runtime):
    store, scheduler, agent, workflow = runtime
    eid, approval_id, objective = request_fixture(store)
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET thread_id='thread',contract=json_set(contract,'$.branch_workspace',1) WHERE id=?",
            (eid,),
        )
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id='thread',turn_id='turn' WHERE experiment_id=?",
            (eid,),
        )
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    request = payload(approval_id, objective)
    with pytest.raises(ValueError, match="exact owner approval"):
        preflight(workflow, eid, action, request)
    store.command(uuid4(), "answer", {"pending_request_id": approval_id, "answer": "accept"}, eid=eid)
    request = preflight(workflow, eid, action, request)
    assert request.request_id != request.approval_request_id
    assert request.command[-1] == "a b"
    workflow.config.slurm.limits.cpus_per_job = 1
    with pytest.raises(ValueError, match="executor policy"):
        preflight(workflow, eid, action, request)
    store.update(eid, pause_compute=True)
    with pytest.raises(ValueError, match="paused"):
        preflight(workflow, eid, action, request)
    assert scheduler.submissions == agent.calls == 0
    assert not store.rows("SELECT intent_id FROM jobs")
