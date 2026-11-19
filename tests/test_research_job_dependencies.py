from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_research_job_source import setup_request
from test_research_submit_job_wire import FakeScheduler, accept

from experimenttracker.research.admission import SubmitJobRequest
from experimenttracker.store import Conflict


def test_depends_on_is_bounded_unique_and_empty_preserves_historical_identity():
    request = {
        "request_id": str(uuid4()),
        "approval_request_id": str(uuid4()),
        "objective": "Compare synthetic inputs",
        "name": "fit",
        "command": ["python", "fit.py"],
        "resources": {"cpus_per_job": 2},
    }
    base = SubmitJobRequest.model_validate(request).saved_request()
    assert SubmitJobRequest.model_validate({**request, "depends_on": []}).saved_request() == base
    dependency = str(uuid4())
    assert SubmitJobRequest.model_validate({**request, "depends_on": [dependency]}).saved_request() == {
        **base,
        "depends_on": [dependency],
    }
    with pytest.raises(ValidationError, match="unique job intent"):
        SubmitJobRequest.model_validate({**request, "depends_on": [dependency, dependency]})
    with pytest.raises(ValidationError):
        SubmitJobRequest.model_validate({**request, "depends_on": [str(uuid4()) for _ in range(33)]})
    assert SubmitJobRequest.model_validate({**request, "inputs": []}).saved_request() == base
    assert SubmitJobRequest.model_validate({**request, "inputs": [dependency]}).saved_request()["inputs"] == [
        dependency
    ]
    with pytest.raises(ValidationError, match="unique register_result"):
        SubmitJobRequest.model_validate({**request, "inputs": [dependency, dependency]})
    with pytest.raises(ValidationError):
        SubmitJobRequest.model_validate({**request, "inputs": [str(uuid4()) for _ in range(17)]})


async def test_unknown_dependency_refuses_before_source_or_scheduler_effect(runtime, tmp_path, monkeypatch):
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
    request["depends_on"] = [str(uuid4())]

    def no_source(*args, **kwargs):
        pytest.fail("dependency admission must precede Git/source effects")

    monkeypatch.setattr(workflow.workspaces, "commit_branch", no_source)
    scheduler = FakeScheduler(workflow, False)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)
    with pytest.raises(ValueError, match="wait for the terminal wake, then request"):
        await workflow.controller.call(eid, "submit_job", request)
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)) == []
    assert scheduler.submissions == 0


@pytest.mark.parametrize("dependency_state", ["submitting", "failed", "foreign"])
async def test_noncomplete_or_foreign_dependency_refuses_without_saved_intent(
    runtime, tmp_path, monkeypatch, dependency_state
):
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
    dependency = str(uuid4())
    dependency_eid = eid
    if dependency_state == "foreign":
        from test_research_first_turn import create

        dependency_eid = create(store)
    stage = "research-" + dependency
    with store.tx() as db:
        db.execute("INSERT INTO stages(experiment_id,id,updated_at) VALUES(?,?,1)", (dependency_eid, stage))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,snapshot_hash,created_at) "
            "VALUES(?,?,?,?,?,?,?,1)",
            (
                dependency,
                dependency_eid,
                stage,
                dependency_state if dependency_state != "foreign" else "complete",
                "b" * 64,
                "{}",
                "a" * 40,
            ),
        )
    request["depends_on"] = [dependency]
    monkeypatch.setattr(
        workflow.workspaces,
        "commit_branch",
        lambda *args, **kwargs: pytest.fail("source effect before admission"),
    )
    scheduler = FakeScheduler(workflow, False)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)
    with pytest.raises(ValueError, match="wait for the terminal wake, then request"):
        await workflow.controller.call(eid, "submit_job", request)
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=? AND intent_id!=?", (eid, dependency)) == []
    assert scheduler.submissions == 0


async def test_completed_dependency_admits_once_through_controller(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, action, parent_request = setup_request(workflow, tmp_path)
    accept(store, eid, parent_request)
    pool = workflow.agent_pool
    pool.claim(
        role="research",
        source_table="agent_actions",
        row=action,
        owner=("experiment", eid),
        slot=pool.primary,
    )
    scheduler = FakeScheduler(workflow, False)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)

    await workflow.controller.call(eid, "submit_job", parent_request)
    parent_id = parent_request["request_id"]
    with store.tx() as db:
        db.execute("UPDATE jobs SET state='complete' WHERE intent_id=?", (parent_id,))

    child_request = {**parent_request, "request_id": str(uuid4()), "depends_on": [parent_id]}
    receipt = await workflow.controller.call(eid, "submit_job", child_request)
    replay = await workflow.controller.call(eid, "submit_job", child_request)
    assert receipt == replay
    assert receipt["intent_id"] == child_request["request_id"]
    assert scheduler.submissions == 2
    with pytest.raises(Conflict, match="another submission"):
        await workflow.controller.call(eid, "submit_job", {**child_request, "depends_on": []})
    assert scheduler.submissions == 2
