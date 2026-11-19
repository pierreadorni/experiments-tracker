import json
from uuid import uuid4

import pytest

from experimenttracker.autopilot import Autopilot
from experimenttracker.autopilot_budget import WEEK
from experimenttracker.autopilot_models import AutopilotPolicy
from experimenttracker.research.autopilot_approval import OrchestratorApproval, approved
from experimenttracker.store import Conflict


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
        db.execute("UPDATE autopilot_actions SET state='completed' WHERE id=?", (action_id,))

    proof = OrchestratorApproval.model_validate(result["approval"])
    assert approved(store, eid, request_id, objective, now=now) == proof
    assert approved(store, eid, request_id, "changed objective", now=now) is None
    assert approved(store, str(uuid4()), request_id, objective, now=now) is None
    assert approved(store, eid, str(uuid4()), objective, now=now) is None
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
