from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_research_approval import request_fixture

from experimenttracker.autopilot_models import AutopilotPolicy
from experimenttracker.research.admission import SubmitJobRequest, preflight
from experimenttracker.research.job_intent import save


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


def test_autopilot_member_policy_adds_compute_authority(runtime, monkeypatch):
    store, scheduler, _, workflow = runtime
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
    store.command(uuid4(), "answer", {"pending_request_id": approval_id, "answer": "accept"}, eid=eid)
    service = workflow.research.service
    now = 1_000_000
    monkeypatch.setattr("experimenttracker.clock.now", lambda: now)
    policy = AutopilotPolicy(
        subject="Research",
        scope="Human-approved CPU",
        publication_goal="Review",
        venue="Test",
        start_at=now,
        handoff_at=now + 1_000_000,
        submission_at=now + 2_000_000,
        weekly_gpu_hours=10,
        anchor_at=now,
    )
    service.command(
        "test-project",
        {
            "request_id": str(uuid4()),
            "kind": "configure",
            "expected_revision": 0,
            "policy": policy.model_dump(),
        },
    )
    run = service.run("test-project")
    service.command(
        "test-project", {"request_id": str(uuid4()), "kind": "activate", "expected_revision": run["revision"]}
    )
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
            (eid, run["id"], "test-project", now),
        )
    assert preflight(workflow, eid, action, request)
    with pytest.raises(ValueError, match="exact owner approval"):
        preflight(workflow, eid, action, {**request, "objective": "Changed objective"})
    now += 950_000
    with pytest.raises(ValueError, match="autopilot_writing_scope"):
        preflight(workflow, eid, action, request)
    now -= 950_000
    with store.tx() as db:
        db.execute(
            "INSERT INTO experiment_branches VALUES(?,?,?,?,?,?,'active',NULL,0,0)",
            (eid, "/synthetic", "exp/synthetic", "/synthetic/tree", "a" * 40, "a" * 40),
        )
        db.execute("UPDATE autopilot_runs SET status='paused' WHERE id=?", (run["id"],))
    with pytest.raises(ValueError, match="autopilot_paused"):
        save(workflow, eid, action, request, {"commit": "a" * 40, "script_hash": "b" * 64})
    assert not store.rows("SELECT * FROM jobs")
    with store.tx() as db:
        db.execute("UPDATE autopilot_runs SET status='active' WHERE id=?", (run["id"],))
        db.execute("UPDATE autopilot_members SET deferred=1 WHERE experiment_id=?", (eid,))
    with pytest.raises(ValueError, match="autopilot_deferred"):
        preflight(workflow, eid, action, request)
    with store.tx() as db:
        db.execute("UPDATE autopilot_members SET deferred=0 WHERE experiment_id=?", (eid,))
        db.execute("UPDATE autopilot_runs SET status='paused' WHERE id=?", (run["id"],))
    with pytest.raises(ValueError, match="autopilot_paused"):
        preflight(workflow, eid, action, request)
    with store.tx() as db:
        db.execute("UPDATE autopilot_runs SET status='active' WHERE id=?", (run["id"],))
        db.execute(
            "UPDATE autopilot_policies SET policy=json_set(policy,'$.start_at',?) WHERE run_id=?",
            (now + 1, run["id"]),
        )
    with pytest.raises(ValueError, match="autopilot_not_started"):
        preflight(workflow, eid, action, request)
    assert not store.rows("SELECT * FROM jobs")
    assert scheduler.submissions == 0


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
