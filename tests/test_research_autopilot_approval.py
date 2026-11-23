import json
from uuid import uuid4

import pytest
from helpers import lead_ready, seed_saved_contract4
from test_iteration_autopilot import activated

from experimenttracker.autopilot import Autopilot
from experimenttracker.autopilot_budget import WEEK
from experimenttracker.autopilot_models import AutopilotPolicy
from experimenttracker.autopilot_policy import lead_id
from experimenttracker.research import wakeups
from experimenttracker.research.autopilot_approval import OrchestratorApproval, approved
from experimenttracker.research.owner_tool import AskOwnerQuestion, ask_owner
from experimenttracker.research.session import first_turn, follow_up_turn
from experimenttracker.store import encode


def test_orchestrator_approval_parser_requires_exact_shape():
    approval = {
        "request_id": str(uuid4()),
        "experiment_id": str(uuid4()),
        "run_id": str(uuid4()),
        "policy_revision": 2,
        "control_revision": 4,
        "objective": "Run the CPU comparison",
    }
    parsed = OrchestratorApproval.parse(json.dumps({"approval": approval}))
    assert parsed.model_dump(mode="json") == approval
    assert OrchestratorApproval.parse({"approval": {**approval, "objective": " "}}) is None
    assert OrchestratorApproval.parse({"summary": "approved"}) is None
    assert OrchestratorApproval.parse("not JSON") is None


def test_c4_approval_request_wakes_the_lead_with_a_project_update(runtime, monkeypatch):
    store = runtime[0]
    service, run = activated(runtime, monkeypatch)
    eid = seed_saved_contract4(store, "Synthetic C4 scheduling fixture")
    action_id, request_id = str(uuid4()), uuid4()
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id='research-thread' WHERE id=?", (eid,))
        db.execute(
            "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
            (eid, run["id"], "test-project", 1_000_000),
        )
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?, 'research',0,'running','research-thread','research-turn','{}',1000000,1000000)",
            (action_id, eid),
        )
    objective = "Run the exact synthetic CPU comparison"
    ask_owner(
        store,
        store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,)),
        AskOwnerQuestion(
            request_id=request_id,
            question="May the synthetic CPU comparison proceed?",
            kind="approval",
            objective=objective,
        ),
    )
    # The retired project-turn engine gets no input; the lead gets one project update.
    assert service.run(run_id=run["id"])["input_cursor"] == run["input_cursor"]
    (wake,) = store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (lead_id(run["id"]),))
    assert json.loads(wake["reason"]) == {
        "event": wakeups.PROJECT_UPDATE,
        "source_id": f"approval:{eid}:{request_id}",
    }
    status = service.project_status(service.run(run_id=run["id"]))
    assert status["pending_objectives"] == [
        {"experiment_id": eid, "approval_request_id": str(request_id), "objective": objective}
    ]


def test_autopilot_approval_proof_is_exact_and_separate_from_owner_answer(runtime, monkeypatch):
    store = runtime[0]
    now = 1_000_000
    monkeypatch.setattr("experimenttracker.clock.now", lambda: now)
    lead_ready(store)
    service = Autopilot(store, runtime[3].config)
    policy = AutopilotPolicy(
        subject="Synthetic acceptance",
        scope="CPU comparison",
        publication_goal="Review",
        venue="Synthetic venue",
        start_at=now,
        handoff_at=now + 10 * WEEK,
        submission_at=now + 11 * WEEK,
        weekly_gpu_hours=0,
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
    service.command("test-project", {"request_id": str(uuid4()), "kind": "activate", "expected_revision": 1})
    run = service.run("test-project")

    eid = seed_saved_contract4(store, "Synthetic C4 proof")
    experiment = store.experiment(eid)
    contract = experiment["contract"]
    assert contract["execution_contract"] == 4
    request_id = str(uuid4())
    objective = "Run the proposed CPU comparison"
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,pending_request=? WHERE id=?",
            (
                json.dumps(contract),
                json.dumps(
                    {
                        "id": request_id,
                        "kind": "approval",
                        "params": {"objective": objective},
                    }
                ),
                eid,
            ),
        )
        db.execute(
            "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
            (eid, run["id"], "test-project", now),
        )
        store.emit(
            db,
            eid,
            "owner_approval_requested",
            {
                "experiment_id": eid,
                "project_id": "test-project",
                "brief": experiment["brief"],
                "request_id": request_id,
                "objective": objective,
            },
        )

    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_actions(id,run_id,cursor,control_revision,state,created_at,updated_at) "
            "VALUES(?,?,0,?,'running',?,?)",
            (action_id, run["id"], run["control_revision"], now, now),
        )
    # The receipt and event a retired project turn saved; such proofs must still verify.
    approval = {
        "request_id": request_id,
        "experiment_id": eid,
        "run_id": run["id"],
        "policy_revision": run["policy_revision"],
        "control_revision": run["control_revision"],
        "objective": objective,
    }
    result = {"approval": approval, "request_id": str(uuid4()), "revision": run["revision"]}
    payload = {
        "name": "approve_experiment_objective",
        "arguments": {
            "request_id": result["request_id"],
            "expected_revision": run["revision"],
            "experiment_id": eid,
            "approval_request_id": request_id,
            "objective": objective,
        },
    }
    with store.tx() as db:
        store.emit(
            db,
            eid,
            "autopilot_objective_approved",
            {**approval, "author": "agent:" + action_id, "action_id": action_id},
        )
        db.execute(
            "INSERT INTO autopilot_requests VALUES(?,?,?,?,?,'applied',?,NULL,?)",
            (
                result["request_id"],
                "test-project",
                run["id"],
                encode(payload),
                "agent:" + action_id,
                encode(result),
                now,
            ),
        )
    assert approved(store, eid, request_id, objective, now=now) is None
    # A drained installation keeps only terminal project turns; a completed one's proof verifies.
    with store.tx() as db:
        db.execute("UPDATE autopilot_actions SET state='completed' WHERE id=?", (action_id,))

    proof = OrchestratorApproval.model_validate(result["approval"])
    assert approved(store, eid, request_id, objective, now=now) == proof
    from experimenttracker.research.approval import approved as owner_approved

    assert owner_approved(store, eid, request_id, objective) is None
    assert approved(store, eid, request_id, "changed objective", now=now) is None
    assert approved(store, str(uuid4()), request_id, objective, now=now) is None
    assert approved(store, eid, str(uuid4()), objective, now=now) is None

    predecessor = first_turn(store, eid)
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id='research-thread' WHERE id=?", (eid,))
        db.execute(
            "UPDATE agent_actions SET state='completed',thread_id='research-thread',turn_id='research-turn',result=? WHERE id=?",
            (encode({"status": "wait", "summary": "awaiting approval", "next": "owner"}), predecessor["id"]),
        )
        saved = store.experiment(eid)["pending_request"]
        saved.update(
            action_id=predecessor["id"],
            answer="Approved by the project orchestrator.",
            answer_source={"kind": "orchestrator", "run_id": run["id"], "action_id": action_id},
        )
        db.execute("UPDATE experiments SET pending_request=? WHERE id=?", (encode(saved), eid))
        from experimenttracker.core import request_obligations

        request_obligations.sync_request(db, eid, before={"id": request_id})

    recorded = wakeups.record(store, eid, wakeups.ORCHESTRATOR_APPROVAL, request_id, now=now)
    assert wakeups.record(store, eid, wakeups.ORCHESTRATOR_APPROVAL, request_id, now=now) == recorded
    # The orchestrator can change policy after saving approval but before this wake
    # is consumed. That must become an informational follow-up, not stale authority.
    with store.tx() as db:
        db.execute("UPDATE autopilot_runs SET control_revision=control_revision+1 WHERE id=?", (run["id"],))
    assert approved(store, eid, request_id, objective, now=now) is None
    follow_up = follow_up_turn(store, eid, now=now + 60)
    assert follow_up is not None
    assert '"approved_by": "project orchestrator"' not in follow_up["input"]
    assert "request a new approval" in follow_up["input"]
    assert '"answer": "Approved by the project orchestrator."' not in follow_up["input"]
    # Restore the fixture revision for the existing admission checks below.
    with store.tx() as db:
        db.execute("UPDATE autopilot_runs SET control_revision=control_revision-1 WHERE id=?", (run["id"],))
    assert store.experiment(eid)["pending_request"] is None
    assert "request a new approval" in follow_up["input"]
    assert "\nOWNER APPROVAL DECISION\n{" not in follow_up["input"]
    assert approved(store, eid, request_id, objective, now=now) is None
    assert (
        store.one("SELECT state FROM agent_wakeups WHERE id=?", (recorded["wakeup_id"],))["state"] == "queued"
    )
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=json_set(contract,'$.branch_workspace',1) WHERE id=?", (eid,)
        )
        db.execute(
            "UPDATE agent_actions SET state='running',turn_id='follow-up-turn' WHERE id=?", (follow_up["id"],)
        )
    from experimenttracker.research.admission import preflight

    submit = {
        "request_id": str(uuid4()),
        "approval_request_id": request_id,
        "objective": objective,
        "name": "fit",
        "command": ["python", "fit.py"],
        "resources": {"cpus_per_job": 1},
    }
    research_action = store.one("SELECT * FROM agent_actions WHERE id=?", (follow_up["id"],))
    assert preflight(runtime[3], eid, research_action, submit)
    with store.tx() as db:
        db.execute("UPDATE autopilot_members SET deferred=1 WHERE experiment_id=?", (eid,))
    with pytest.raises(ValueError, match="autopilot_deferred"):
        preflight(runtime[3], eid, research_action, submit)
    with store.tx() as db:
        db.execute("UPDATE autopilot_members SET deferred=0 WHERE experiment_id=?", (eid,))
        db.execute(
            "UPDATE agent_wakeups SET action_id=? WHERE id=?", (predecessor["id"], recorded["wakeup_id"])
        )
    with pytest.raises(ValueError, match="exact owner approval or attributed"):
        preflight(runtime[3], eid, research_action, submit)
    with store.tx() as db:
        db.execute(
            "UPDATE agent_wakeups SET action_id=? WHERE id=?", (follow_up["id"], recorded["wakeup_id"])
        )
    commit = "a" * 40
    with store.tx() as db:
        db.execute(
            "INSERT INTO experiment_branches VALUES(?,?,?,?,?,?,'active',NULL,0,0)",
            (eid, "/synthetic", "exp/synthetic", "/synthetic/tree", commit, commit),
        )
    from experimenttracker.research.job_intent import save

    job, created = save(
        runtime[3],
        eid,
        research_action,
        submit,
        {"commit": commit, "script_hash": "b" * 64},
    )
    assert created and job["state"] == "submitting"
    with pytest.raises(ValueError, match="exact owner approval or attributed"):
        preflight(runtime[3], eid, research_action, {**submit, "objective": "altered"})
    with store.tx() as db:
        db.execute("UPDATE autopilot_runs SET control_revision=control_revision+1 WHERE id=?", (run["id"],))
    assert approved(store, eid, request_id, objective, now=now) is None
    with pytest.raises(ValueError, match="exact owner approval or attributed"):
        preflight(runtime[3], eid, research_action, submit)
    assert (
        store.one(
            "SELECT 1 FROM commands WHERE experiment_id=? AND kind='answer' AND json_extract(payload,'$.pending_request_id')=?",
            (eid, request_id),
        )
        is None
    )
