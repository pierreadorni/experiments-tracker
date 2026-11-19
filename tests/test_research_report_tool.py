"""Contract-4 update_report persistence: one inert, idempotent working version."""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract4
from pydantic import ValidationError

from experimenttracker.codex_client import NeedsAttention
from experimenttracker.research.report_tool import UpdateReport, update_report
from experimenttracker.store import Conflict, encode

BRIEF = "Compare two toy optimizers under one fixed compute budget."


def document(standfirst="A bounded synthetic comparison."):
    return {
        "title": "A readable result",
        "standfirst": standfirst,
        "blocks": [
            {
                "kind": "paragraph",
                "text": "Question and interpretation, before the saved evidence.",
            }
        ],
    }


def contract_four(store):
    return seed_saved_contract4(store, BRIEF)


def add_action(store, eid, *, phase="research", state="running", thread_id="thread-1"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,input,"
            "created_at,updated_at) VALUES(?,?,?,1,?,?,'prompt',0,0)",
            (identity, eid, phase, state, thread_id),
        )
        db.execute("UPDATE experiments SET thread_id=? WHERE id=?", (thread_id, eid))
    return store.one("SELECT * FROM agent_actions WHERE id=?", (identity,))


def report_request(payload=None, request_id=None):
    return UpdateReport(request_id=request_id or uuid4(), document=payload or document())


def test_update_report_input_requires_a_uuid_and_a_valid_report_document():
    request_id = uuid4()
    valid = UpdateReport.model_validate({"request_id": str(request_id), "document": document()})
    assert valid.request_id == request_id
    assert valid.document.title == "A readable result"

    for payload in (
        {},
        {"request_id": "not-a-uuid", "document": document()},
        {"request_id": str(request_id)},
        {"request_id": str(request_id), "document": document(), "extra": 1},
        {"request_id": str(request_id), "document": {**document(), "blocks": []}},
        {
            "request_id": str(request_id),
            "document": {**document(), "blocks": [{"kind": "paragraph", "text": "  "}]},
        },
    ):
        with pytest.raises(ValidationError):
            UpdateReport.model_validate(payload)


def test_update_report_persists_one_queued_version_idempotently(runtime):
    store, scheduler, agent, _ = runtime
    eid = contract_four(store)
    action = add_action(store, eid)
    request_id = uuid4()

    receipt = update_report(store, store.experiment(eid), action, report_request(request_id=request_id))
    assert receipt == {
        "request_id": str(request_id),
        "report_version": 1,
        "state": "queued",
    }
    saved = store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,))
    assert [(row["version"], row["state"], row["request_id"]) for row in saved] == [
        (1, "queued", str(request_id))
    ]
    assert saved[0]["artifact_ids"] == "[]" and saved[0]["report_artifact_id"] is None
    events = store.rows(
        "SELECT payload FROM events WHERE experiment_id=? AND kind='report_requested'", (eid,)
    )
    assert [json.loads(row["payload"]) for row in events] == [{"report_version": 1}]

    replay = update_report(store, store.experiment(eid), action, report_request(request_id=request_id))
    assert replay == receipt
    assert len(store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,))) == 1
    assert (
        len(store.rows("SELECT * FROM events WHERE experiment_id=? AND kind='report_requested'", (eid,))) == 1
    )

    with pytest.raises(Conflict, match="another document"):
        update_report(
            store,
            store.experiment(eid),
            action,
            report_request(payload=document("A different comparison."), request_id=request_id),
        )
    assert len(store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,))) == 1

    store.report_state(eid, 1, "ready")
    replay = update_report(store, store.experiment(eid), action, report_request(request_id=request_id))
    assert replay["report_version"] == 1 and replay["state"] == "ready"

    second = update_report(store, store.experiment(eid), action, report_request())
    assert second["report_version"] == 2
    assert [
        row["version"] for row in store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,))
    ] == [1, 2]
    assert store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)) == []
    assert store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)) == []
    assert scheduler.submissions == 0 and agent.calls == 0


@pytest.mark.parametrize(
    "change,message",
    [
        ("contract", "execution contract 4"),
        ("phase", "active research turn"),
        ("pending", "active research turn"),
        ("paused", "live research experiment"),
        ("complete", "live research experiment"),
        ("deleted", "live research experiment"),
    ],
)
def test_update_report_refuses_a_non_live_research_identity(runtime, change, message):
    store, _, _, _ = runtime
    eid = contract_four(store)
    action = add_action(store, eid, state="pending" if change == "pending" else "running")
    if change == "contract":
        contract = store.experiment(eid)["contract"]
        contract["execution_contract"] = 3
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    elif change == "phase":
        with store.tx() as db:
            db.execute("UPDATE agent_actions SET phase='interpret' WHERE id=?", (action["id"],))
    elif change == "paused":
        with store.tx() as db:
            db.execute("UPDATE experiments SET pause_agent=1 WHERE id=?", (eid,))
    elif change == "complete":
        with store.tx() as db:
            db.execute("UPDATE experiments SET phase='complete' WHERE id=?", (eid,))
    elif change == "deleted":
        with store.tx() as db:
            db.execute("UPDATE experiments SET deleted_at=1 WHERE id=?", (eid,))

    action = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
    with pytest.raises(ValueError, match=message):
        update_report(store, store.experiment(eid), action, report_request())
    assert store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,)) == []


def test_update_report_refuses_another_experiments_active_turn_and_a_missing_row(runtime):
    store, _, _, _ = runtime
    eid, other = contract_four(store), contract_four(store)
    action = add_action(store, other)
    with pytest.raises(ValueError, match="active research turn"):
        update_report(store, store.experiment(eid), action, report_request())
    with pytest.raises(NeedsAttention, match="saved action disappeared"):
        update_report(store, store.experiment(eid), {"id": str(uuid4())}, report_request())
    assert store.rows("SELECT * FROM report_versions") == []


def test_update_report_refuses_a_changed_caller_action_before_writing(runtime):
    store, _, _, _ = runtime
    eid = contract_four(store)
    action = add_action(store, eid)
    stale = dict(action)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET state='dispatching' WHERE id=?", (action["id"],))

    with pytest.raises(NeedsAttention, match="changed during reconciliation"):
        update_report(store, store.experiment(eid), stale, report_request())
    assert store.rows("SELECT * FROM report_versions") == []


@pytest.mark.parametrize(
    "action_thread,experiment_thread",
    [(None, "thread-1"), ("thread-1", None), ("thread-1", "thread-2")],
)
def test_update_report_refuses_missing_or_mismatched_research_thread(
    runtime, action_thread, experiment_thread
):
    store, _, _, _ = runtime
    eid = contract_four(store)
    action = add_action(store, eid)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET thread_id=? WHERE id=?", (action_thread, action["id"]))
        db.execute("UPDATE experiments SET thread_id=? WHERE id=?", (experiment_thread, eid))
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))

    with pytest.raises(ValueError, match="exact saved research thread"):
        update_report(store, store.experiment(eid), action, report_request())
    assert store.rows("SELECT * FROM report_versions") == []


def test_update_report_scenario_stays_local_and_queued(runtime):
    """Deterministic demo: two versions remain queued local rows, never publications."""
    store, scheduler, agent, workflow = runtime
    eid = contract_four(store)
    action = add_action(store, eid)
    first, second = uuid4(), uuid4()

    update_report(store, store.experiment(eid), action, report_request(payload=document(), request_id=first))
    update_report(
        store,
        store.experiment(eid),
        action,
        report_request(payload=document("A second local revision."), request_id=second),
    )
    saved = store.rows("SELECT * FROM report_versions WHERE experiment_id=? ORDER BY version", (eid,))
    assert [(row["version"], row["state"]) for row in saved] == [(1, "queued"), (2, "queued")]
    assert {row["request_id"] for row in saved} == {str(first), str(second)}
    assert store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)) == []
    assert scheduler.submissions == 0 and agent.calls == 0
    assert workflow.config.gateway.url is None
