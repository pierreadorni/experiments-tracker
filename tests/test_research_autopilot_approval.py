import json
from uuid import uuid4

import pytest

from experimenttracker.autopilot import Autopilot
from experimenttracker.autopilot_budget import WEEK
from experimenttracker.autopilot_models import AutopilotPolicy
from experimenttracker.research import wakeups
from experimenttracker.research.autopilot_approval import OrchestratorApproval, approved
from experimenttracker.research.session import first_turn, follow_up_turn
from experimenttracker.store import Conflict, encode


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


def test_autopilot_approval_proof_is_exact_and_separate_from_owner_answer(runtime, monkeypatch):
    store = runtime[0]
    now = 1_000_000
    monkeypatch.setattr("experimenttracker.clock.now", lambda: now)
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

    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic C4 proof", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    experiment = store.experiment(eid)
    contract = experiment["contract"]
    contract["execution_contract"] = 4
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
    discovery = service.invoke(
        action_id,
        "get_experiment",
        {"experiment_id": eid},
    )
    assert discovery["experiment"]["pending_request"]["id"] == request_id
    assert discovery["experiment"]["pending_request"]["kind"] == "approval"
    assert discovery["experiment"]["pending_request"]["params"]["objective"] == objective

    arguments = {
        "request_id": str(uuid4()),
        "expected_revision": run["revision"],
        "experiment_id": eid,
        "approval_request_id": request_id,
        "objective": objective,
    }
    with pytest.raises(Conflict, match="revision is stale"):
        service.invoke(
            action_id,
            "approve_experiment_objective",
            {**arguments, "expected_revision": run["revision"] - 1},
        )
    with pytest.raises(Conflict, match="not currently pending"):
        service.invoke(action_id, "approve_experiment_objective", {**arguments, "objective": "altered"})
    with pytest.raises(Conflict, match="membership"):
        service.invoke(
            action_id,
            "approve_experiment_objective",
            {**arguments, "experiment_id": str(uuid4())},
        )
    result = service.invoke(action_id, "approve_experiment_objective", arguments)
    assert result["approval"]["objective"] == objective
    assert approved(store, eid, request_id, objective, now=now) is None
    with store.tx() as db:
        db.execute(
            "UPDATE autopilot_actions SET result=? WHERE id=?",
            (
                encode(
                    {
                        "summary": "Objective approved",
                        "central_argument": "The bounded comparison is worthwhile.",
                        "uncertainty": "Results remain unknown.",
                        "outcome": "continue",
                    }
                ),
                action_id,
            ),
        )
    from experimenttracker.autopilot_controller import ResearchController

    wake_id = wakeups.wakeup_id(eid, wakeups.ORCHESTRATOR_APPROVAL, request_id)
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_wakeups(id,experiment_id,reason,due_at,created_at) VALUES(?,?,?,?,?)",
            (wake_id, eid, encode({"event": "wrong", "source_id": request_id}), now + 60, now),
        )
    controller = ResearchController(runtime[3])
    row = store.one("SELECT * FROM autopilot_actions WHERE id=?", (action_id,))
    with pytest.raises(Conflict, match="reused for a different event"):
        controller.complete(row)
    assert store.one("SELECT state FROM autopilot_actions WHERE id=?", (action_id,))["state"] == "running"
    assert store.experiment(eid)["pending_request"].get("answer") is None
    with store.tx() as db:
        db.execute("DELETE FROM agent_wakeups WHERE id=?", (wake_id,))
    controller.complete(store.one("SELECT * FROM autopilot_actions WHERE id=?", (action_id,)))

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
    follow_up = follow_up_turn(store, eid, now=now + 60)
    assert follow_up is not None
    assert "PROJECT ORCHESTRATOR APPROVAL" in follow_up["input"]
    assert "\nOWNER APPROVAL DECISION\n{" not in follow_up["input"]
    assert approved(store, eid, request_id, objective, now=now) is None
    assert (
        store.one("SELECT state FROM agent_wakeups WHERE id=?", (recorded["wakeup_id"],))["state"] == "queued"
    )
    with store.tx() as db:
        db.execute("UPDATE autopilot_runs SET control_revision=control_revision+1 WHERE id=?", (run["id"],))
    assert approved(store, eid, request_id, objective, now=now) is None
    assert (
        store.one(
            "SELECT 1 FROM commands WHERE experiment_id=? AND kind='answer' AND json_extract(payload,'$.pending_request_id')=?",
            (eid, request_id),
        )
        is None
    )
