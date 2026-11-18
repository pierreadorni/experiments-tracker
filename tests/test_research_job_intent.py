from uuid import uuid4

import pytest
from test_research_admission import payload
from test_research_approval import request_fixture

from experimenttracker.research.job_intent import save
from experimenttracker.store import Conflict

COMMIT = "a" * 40
PREPARED = {"commit": COMMIT, "script_hash": "b" * 64}


def setup_intent(store):
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
        db.execute(
            "INSERT INTO experiment_branches VALUES(?,?,?,?,?,?,'active',NULL,0,0)",
            (eid, "/synthetic", "exp/synthetic", "/synthetic/tree", COMMIT, COMMIT),
        )
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
    return eid, action, payload(approval_id, objective)


def test_persists_uncertain_intent_and_replays_without_permission_to_send(runtime):
    store, scheduler, _, workflow = runtime
    eid, action, request = setup_intent(store)
    with pytest.raises(ValueError, match="owner approval"):
        save(workflow, eid, action, request, PREPARED)
    store.command(
        uuid4(), "answer", {"pending_request_id": request["approval_request_id"], "answer": "accept"}, eid=eid
    )
    with pytest.raises(Conflict, match="saved worktree commit"):
        save(workflow, eid, action, request, {**PREPARED, "commit": "c" * 40})
    assert not store.rows("SELECT * FROM jobs")
    job, created = save(workflow, eid, action, request, PREPARED)
    assert created and job["state"] == "submitting" and job["job_id"] is None
    assert job["intent_id"] == request["request_id"]
    assert job["execution_root"].endswith("/attempts/" + job["intent_id"])
    assert save(workflow, eid, action, request, PREPARED) == (job, False)
    with pytest.raises(Conflict, match="another submission"):
        save(workflow, eid, action, {**request, "name": "different"}, PREPARED)
    assert store.one("SELECT kind,state FROM obligations WHERE target=?", (job["intent_id"],)) == {
        "kind": "reconcile",
        "state": "open",
    }
    assert scheduler.submissions == 0
