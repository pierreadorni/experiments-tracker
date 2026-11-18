import json
from uuid import uuid4

from experimenttracker.core import request_obligations
from experimenttracker.research.approval import approved
from experimenttracker.research.session import first_turn


def request_fixture(store):
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Synthetic approval evidence fixture", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    experiment = store.experiment(eid)
    contract = experiment["contract"]
    contract["execution_contract"] = 4
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    action = first_turn(store, eid)
    request_id = str(uuid4())
    objective = "Run the proposed comparison"
    pending = {
        "id": request_id,
        "action_id": action["id"],
        "method": "item/commandExecution/requestApproval",
        "params": {"questions": [{"id": "approval", "question": objective}]},
        "expired": False,
    }
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,pending_request=? WHERE id=?",
            (json.dumps(contract), json.dumps(pending), eid),
        )
        request_obligations.sync_request(db, eid, before=None)
        store.emit(
            db,
            eid,
            "owner_approval_requested",
            {
                "experiment_id": eid,
                "project_id": contract["project"]["id"],
                "brief": experiment["brief"],
                "request_id": request_id,
                "objective": objective,
            },
        )
    return eid, request_id, objective


def test_explicit_approval_requires_exact_applied_answer_and_satisfied_request(runtime):
    store = runtime[0]
    eid, request_id, objective = request_fixture(store)
    assert approved(store, eid, request_id, objective) is None
    store.command(
        uuid4(), "answer", {"pending_request_id": request_id, "answer": "accept"}, eid=eid, origin="local"
    )
    assert approved(store, eid, request_id, objective) == {
        "experiment_id": eid,
        "project_id": "test-project",
        "brief": "Synthetic approval evidence fixture",
        "request_id": request_id,
        "objective": objective,
    }


def test_wrong_objective_and_non_owner_answer_do_not_approve(runtime):
    store = runtime[0]
    eid, request_id, objective = request_fixture(store)
    with store.tx() as db:
        store.emit(db, eid, "owner_question_asked", {"request_id": request_id})
    store.command(
        uuid4(), "answer", {"pending_request_id": request_id, "answer": "accept"}, eid=eid, origin="agent"
    )
    assert approved(store, eid, request_id, "different objective") is None
    assert approved(store, eid, request_id, objective) is None


def test_conflicting_owner_decisions_fail_closed(runtime):
    store = runtime[0]
    eid, request_id, objective = request_fixture(store)
    for decision in ("accept", "decline"):
        store.command(uuid4(), "answer", {"pending_request_id": request_id, "answer": decision}, eid=eid)
    assert approved(store, eid, request_id, objective) is None
