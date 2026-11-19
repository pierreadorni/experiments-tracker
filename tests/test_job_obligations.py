"""Saved Slurm intent obligations are pure mappings with no dispatch authority."""

import json
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3

from experimenttracker.core import job_obligations, obligations
from experimenttracker.store import encode


def seed_job(
    store,
    *,
    state="submitting",
    job_id=None,
    scheduler_state=None,
    exit_code=None,
    executor="slurm",
):
    eid = seed_saved_contract3(store, "Synthetic job obligation mapping")
    intent_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,scheduler_state,exit_code,execution_manifest) "
            "VALUES(?,?, 'prepare',1,?,?, 'synthetic','{}','control',1,?,?,?)",
            (
                intent_id,
                eid,
                state,
                job_id,
                scheduler_state,
                exit_code,
                json.dumps({"executor": executor}) if executor else None,
            ),
        )
    return eid, intent_id


def job_row(db, intent_id):
    return dict(db.execute("SELECT * FROM jobs WHERE intent_id=?", (intent_id,)).fetchone())


def obligations_for(store, intent_id):
    prefix = f"job:{intent_id}:"
    rows = store.rows("SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix))
    return [row for row in rows if row["id"].rsplit(":", 1)[0] == f"job:{intent_id}"]


def test_unknown_intent_is_backend_reconciliation_and_failed_unknown_stays_open(runtime):
    store = runtime[0]
    eid, intent_id = seed_job(store)
    with store.tx() as db:
        first = job_obligations.sync_job(db, intent_id)
    assert (first["id"], first["kind"], first["owner"], first["wait"], first["target"]) == (
        f"job:{intent_id}:0",
        "reconcile",
        "backend",
        f"reconcile:{intent_id}",
        intent_id,
    )
    assert first["project_id"] == store.experiment(eid)["contract"]["project"]["id"]

    with store.tx() as db:
        before = job_row(db, intent_id)
        db.execute(
            "UPDATE jobs SET state='failed',scheduler_state='RUNNING',error='output check failed' "
            "WHERE intent_id=?",
            (intent_id,),
        )
        replay = job_obligations.sync_job(db, intent_id, before=before)
    assert replay == first and replay["state"] == "open"
    assert obligations_for(store, intent_id) == [first]


def test_saved_exact_job_hands_off_only_with_same_transaction_identity_proof(runtime):
    store = runtime[0]
    _, intent_id = seed_job(store)
    with store.tx() as db:
        backend = job_obligations.sync_job(db, intent_id)

    with pytest.raises(ValueError, match="proven source transition"):
        with store.tx() as db:
            db.execute("UPDATE jobs SET job_id='123',state='submitted' WHERE intent_id=?", (intent_id,))
            job_obligations.sync_job(db, intent_id)
    assert job_row(store.db, intent_id)["job_id"] is None
    assert obligations_for(store, intent_id) == [backend]

    with store.tx() as db:
        before = job_row(db, intent_id)
        db.execute("UPDATE jobs SET job_id='123',state='submitted' WHERE intent_id=?", (intent_id,))
        external = job_obligations.sync_job(db, intent_id, before=before)
    rows = obligations_for(store, intent_id)
    assert [(row["owner"], row["kind"], row["wait"], row["state"]) for row in rows] == [
        ("backend", "reconcile", f"reconcile:{intent_id}", "abandoned"),
        ("external", "job", "job:123", "open"),
    ]
    assert external == rows[1]

    with pytest.raises(ValueError, match="cannot change or be cleared"):
        with store.tx() as db:
            before = job_row(db, intent_id)
            db.execute("UPDATE jobs SET job_id='456' WHERE intent_id=?", (intent_id,))
            job_obligations.sync_job(db, intent_id, before=before)
    assert job_row(store.db, intent_id)["job_id"] == "123"
    assert obligations_for(store, intent_id) == rows


@pytest.mark.parametrize(
    ("scheduler_state", "exit_code", "source_state", "terminal"),
    [
        ("COMPLETED", "0:0", "failed", "satisfied"),
        ("COMPLETED", "1:0", "failed", "abandoned"),
        ("FAILED", None, "cancelled", "abandoned"),
    ],
)
def test_exact_scheduler_terminal_outcome_closes_only_an_existing_open_episode(
    runtime, scheduler_state, exit_code, source_state, terminal
):
    store = runtime[0]
    _, intent_id = seed_job(store, state="submitted", job_id="123", scheduler_state="RUNNING", exit_code=None)
    with store.tx() as db:
        active = job_obligations.sync_job(db, intent_id)
    assert active["owner"] == "external" and active["wait"] == "job:123"

    with store.tx() as db:
        before = job_row(db, intent_id)
        db.execute(
            "UPDATE jobs SET state=?,scheduler_state=?,exit_code=? WHERE intent_id=?",
            (source_state, scheduler_state, exit_code, intent_id),
        )
        result = job_obligations.sync_job(db, intent_id, before=before)
    assert result["state"] == terminal
    assert obligations_for(store, intent_id) == [result]


def test_terminal_scheduler_fact_without_history_does_not_create_phantom_rows(runtime):
    store = runtime[0]
    _, intent_id = seed_job(
        store, state="complete", job_id="123", scheduler_state="COMPLETED", exit_code="0:0"
    )
    with store.tx() as db:
        assert job_obligations.sync_job(db, intent_id) is None
    assert obligations_for(store, intent_id) == []


def test_complete_missing_accounting_and_unknown_id_cancellation_remain_unresolved(runtime):
    store = runtime[0]
    _, intent_id = seed_job(
        store, state="complete", job_id="123", scheduler_state="COMPLETED", exit_code=None
    )
    with store.tx() as db:
        external = job_obligations.sync_job(db, intent_id)
    assert external["owner"] == "external" and external["state"] == "open"

    _, unknown = seed_job(store, state="cancelled", scheduler_state="CANCELLED", exit_code="1:0")
    with store.tx() as db:
        backend = job_obligations.sync_job(db, unknown)
    assert backend["owner"] == "backend" and backend["state"] == "open"


def test_agent_pseudo_job_never_opens_external_work_and_closes_stale_row(runtime):
    store = runtime[0]
    eid, intent_id = seed_job(store, state="agent", executor="agent")
    with store.tx() as db:
        assert job_obligations.sync_job(db, intent_id) is None
    assert obligations_for(store, intent_id) == []

    project_id = store.experiment(eid)["contract"]["project"]["id"]
    with store.tx() as db:
        db.execute("UPDATE jobs SET state='complete' WHERE intent_id=?", (intent_id,))
        stale = obligations.open(
            db,
            f"job:{intent_id}:0",
            experiment_id=eid,
            project_id=project_id,
            kind="job",
            target=intent_id,
            owner="external",
            wait="job:orphan",
            detail="stage=prepare;attempt=1",
        )
        closed = job_obligations.sync_job(db, intent_id)
    assert closed["id"] == stale["id"] and closed["state"] == "abandoned"
    assert obligations_for(store, intent_id) == [closed]


def test_source_event_and_job_obligation_roll_back_together(runtime):
    store = runtime[0]
    eid, intent_id = seed_job(store)
    events_before = store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,))
    with pytest.raises(RuntimeError, match="injected rollback"):
        with store.tx() as db:
            before = job_row(db, intent_id)
            db.execute("UPDATE jobs SET job_id='123',state='submitted' WHERE intent_id=?", (intent_id,))
            store.emit(db, eid, "job_submitted", {"intent_id": intent_id, "job_id": "123"})
            job_obligations.sync_job(db, intent_id, before=before)
            raise RuntimeError("injected rollback")
    assert job_row(store.db, intent_id)["job_id"] is None
    assert obligations_for(store, intent_id) == []
    assert store.rows("SELECT * FROM events WHERE experiment_id=?", (eid,)) == events_before


def test_sync_requires_transaction_and_saved_scope_cannot_rebind(runtime):
    store = runtime[0]
    eid, intent_id = seed_job(store, job_id="123", state="submitted")
    with pytest.raises(RuntimeError, match="active transaction"):
        job_obligations.sync_job(store.db, intent_id)
    with store.tx() as db:
        first = job_obligations.sync_job(db, intent_id)
    contract = store.experiment(eid)["contract"]
    contract["project"]["id"] = "other-project"
    with pytest.raises(ValueError, match="scope, stage, or attempt"):
        with store.tx() as db:
            db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
            job_obligations.sync_job(db, intent_id)
    assert store.experiment(eid)["contract"]["project"]["id"] == first["project_id"]
    assert obligations_for(store, intent_id) == [first]


def test_saved_wait_id_and_every_episode_keep_the_exact_job_source_identity(runtime):
    store = runtime[0]
    _, intent_id = seed_job(store, job_id="123", state="submitted")
    with store.tx() as db:
        original = job_obligations.sync_job(db, intent_id)

    with store.tx() as db:
        db.execute("UPDATE obligations SET wait='job:456' WHERE id=?", (original["id"],))
    with pytest.raises(ValueError, match="external scheduler identity"):
        with store.tx() as db:
            job_obligations.sync_job(db, intent_id)

    with store.tx() as db:
        db.execute("UPDATE obligations SET wait=? WHERE id=?", (original["wait"], original["id"]))
        db.execute("UPDATE obligations SET detail='stage=other;attempt=1' WHERE id=?", (original["id"],))
    with pytest.raises(ValueError, match="stage, or attempt identity"):
        with store.tx() as db:
            job_obligations.sync_job(db, intent_id)

    with store.tx() as db:
        db.execute("UPDATE obligations SET detail=? WHERE id=?", (original["detail"], original["id"]))
        replay = job_obligations.sync_job(db, intent_id)
    assert replay == original


def test_backfill_rejects_changed_id_hidden_by_later_backend_episode(runtime):
    store = runtime[0]
    eid, intent_id = seed_job(store, job_id="123", state="submitted")
    with store.tx() as db:
        external = job_obligations.sync_job(db, intent_id)
    with store.tx() as db:
        backend = obligations.open(
            db,
            f"job:{intent_id}:1",
            experiment_id=eid,
            project_id=external["project_id"],
            kind="reconcile",
            target=intent_id,
            owner="backend",
            wait=f"reconcile:{intent_id}",
            detail=external["detail"],
        )
        db.execute("UPDATE jobs SET job_id='456' WHERE intent_id=?", (intent_id,))

    with pytest.raises(ValueError, match="external scheduler identity"):
        with store.tx() as db:
            job_obligations.sync_job(db, intent_id)
    assert obligations_for(store, intent_id) == [external, backend]
