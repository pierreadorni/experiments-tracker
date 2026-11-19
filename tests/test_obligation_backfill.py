"""Bounded backfill of obligations for sources saved before the source hooks.

Each test saves its sources through their real writers and mappers, records the
resulting obligations, then deletes them to reproduce a database created before
the hooks. The backfill must restore exactly those responsibilities without
changing a source row, emitting an event or dispatching work.
"""

import logging
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3
from test_legacy_archive_activation import legacy_experiment

from experimenttracker.core import (
    action_obligations,
    advancement_decisions,
    job_obligations,
    obligation_backfill,
    transfer_obligations,
    wakeup_obligations,
)

IDENTITY = (
    "id",
    "experiment_id",
    "project_id",
    "kind",
    "target",
    "owner",
    "wait",
    "detail",
    "due_at",
    "state",
)


def create(store, brief="Synthetic obligation backfill"):
    eid = seed_saved_contract3(store, brief)
    with store.tx() as db:
        advancement_decisions.open_lifetime(db, eid)
    return eid


def insert_raw_action(store, eid, *, phase, state="pending"):
    """Save an action row the way a pre-hook database holds it: without obligations."""
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) "
            "VALUES(?,?,?,1,?,'synthetic',1,1)",
            (action_id, eid, phase, state),
        )
    return action_id


def insert_action(store, eid, *, phase="prepare", state="pending"):
    """Save an action row and replay its writer's hook in that transaction."""
    action_id = insert_raw_action(store, eid, phase=phase, state=state)
    with store.tx() as db:
        action_obligations.sync_action(db, action_id)
    return action_id


def insert_job(store, eid, *, state="submitting", job_id=None):
    intent_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at) VALUES(?,?,'prepare',1,?,?,'synthetic','{}','control',1)",
            (intent_id, eid, state, job_id),
        )
        job_obligations.sync_job(db, intent_id)
    return intent_id


def insert_transfer(store, eid, *, state="pending"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO execution_transfers VALUES(?,?,'up','{\"layout\":{},\"settings\":{}}',?,NULL,1)",
            (identity, eid, state),
        )
        transfer_obligations.sync_transfer(db, identity)
    return identity


def insert_wakeup(store, eid, *, state="pending"):
    wakeup_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_wakeups VALUES(?,?,'backend compute check',123.0,?,NULL,1)",
            (wakeup_id, eid, state),
        )
        wakeup_obligations.sync_wakeup(db, wakeup_id)
    return wakeup_id


def seed_live(store):
    """Save one live source of every kind plus one saved cancellation procedure."""
    eid = create(store)
    insert_action(store, eid)
    insert_job(store, eid)
    insert_transfer(store, eid)
    insert_wakeup(store, eid)
    store.update(eid, pending_request={"id": "request-1", "question": "Approve?", "expired": False})
    store.update(eid, plan_review={"revision": 1, "state": "awaiting_review"})
    store.working_report(eid, {"title": "Synthetic", "blocks": []}, "report-1")
    cancelled = create(store, "Synthetic backfill cancellation")
    store.command(uuid4(), "action", {"action": "cancel"}, eid=cancelled)
    return eid, cancelled


def saved(store):
    return store.rows("SELECT * FROM obligations ORDER BY id")


def action_rows(store):
    return store.rows("SELECT * FROM obligations WHERE id LIKE 'agent-action:%' ORDER BY id")


def forget(store):
    with store.tx() as db:
        db.execute("DELETE FROM obligations")


def identity(rows):
    return [{field: row[field] for field in IDENTITY} for row in rows]


def test_backfill_opens_the_episode_each_hook_saved_for_live_sources(runtime):
    store = runtime[0]
    eid, cancelled = seed_live(store)
    hooked = saved(store)
    # A terminal source keeps no history: the abandoned lifetime of the cancelled
    # experiment is a hook outcome, not a backfill candidate.
    wanted = [row for row in hooked if row["id"] != f"advancement:{cancelled}:0"]
    assert {row["id"] for row in wanted} >= {
        f"advancement:{eid}:0",
        f"request:{eid}:request-1:0",
        f"review:{eid}:1:0",
        f"report-version:{eid}:1:0",
        f"cancellation:{cancelled}:0",
    }

    forget(store)
    counts = obligation_backfill.backfill(store)
    assert identity(saved(store)) == identity(wanted)
    assert counts == {
        "agent_action": {"synced": 1, "conflicts": 0},
        "job": {"synced": 1, "conflicts": 0},
        "transfer": {"synced": 1, "conflicts": 0},
        "request": {"synced": 1, "conflicts": 0},
        "review": {"synced": 1, "conflicts": 0},
        "report_version": {"synced": 1, "conflicts": 0},
        "wakeup": {"synced": 1, "conflicts": 0},
        "advancement": {"synced": 1, "conflicts": 0},
        "cancellation": {"synced": 1, "conflicts": 0},
    }


def test_a_second_backfill_run_changes_no_row(runtime):
    store = runtime[0]
    seed_live(store)
    forget(store)
    obligation_backfill.backfill(store)
    first = saved(store)
    obligation_backfill.backfill(store)
    assert saved(store) == first


def test_terminal_sources_and_archived_legacy_get_no_history(runtime):
    store = runtime[0]
    eid = create(store)
    insert_action(store, eid, state="completed")
    insert_job(store, eid, state="complete", job_id="101")
    insert_transfer(store, eid, state="complete")
    insert_wakeup(store, eid, state="superseded")
    version = store.working_report(eid, {"title": "Synthetic", "blocks": []}, "report-1")
    with store.tx() as db:
        db.execute(
            "UPDATE report_versions SET state='failed' WHERE experiment_id=? AND version=?", (eid, version)
        )
    store.update(eid, phase="complete", hold_reason=None)
    legacy = legacy_experiment(store)
    store.archive_legacy_experiments()

    forget(store)
    counts = obligation_backfill.backfill(store)
    assert saved(store) == []
    assert {name: count["synced"] for name, count in counts.items()} == dict.fromkeys(counts, 0)
    assert store.rows("SELECT * FROM obligations WHERE id LIKE ?", (f"advancement:{legacy}:%",)) == []


def test_conflicting_terminal_history_is_counted_logged_and_left_as_saved(runtime, caplog):
    store = runtime[0]
    eid = create(store)
    conflicted = insert_raw_action(store, eid, phase="prepare")
    conflict_id = f"agent-action:{conflicted}:0"
    project_id = store.experiment(eid)["contract"]["project"]["id"]
    with store.tx() as db:
        db.execute(
            "INSERT INTO obligations(id,experiment_id,project_id,kind,target,owner,wait,state,detail,"
            "created_at,updated_at,due_at) VALUES(?,?,?,'agent_turn',?,'backend','admission','satisfied',?,1,1,NULL)",
            (conflict_id, eid, project_id, conflicted, "phase=prepare;evidence_revision=1"),
        )
    healthy = insert_raw_action(store, eid, phase="interpret")

    with caplog.at_level(logging.WARNING, logger="experimenttracker.core.obligation_backfill"):
        counts = obligation_backfill.backfill(store)
    assert counts["agent_action"] == {"synced": 1, "conflicts": 1}
    assert [row["id"] for row in action_rows(store)] == sorted([conflict_id, f"agent-action:{healthy}:0"])
    assert store.one("SELECT state FROM obligations WHERE id=?", (conflict_id,)) == {"state": "satisfied"}
    messages = [record.getMessage() for record in caplog.records]
    assert any("agent_action" in message and conflicted in message for message in messages)


def test_fixed_size_batches_cover_every_candidate(runtime):
    store = runtime[0]
    for index in range(3):
        insert_action(store, create(store, f"Synthetic batch {index}"), phase=f"phase-{index}")
    forget(store)
    counts = obligation_backfill.backfill(store, batch_size=1)
    assert counts["agent_action"] == {"synced": 3, "conflicts": 0}
    assert [row["state"] for row in action_rows(store)] == ["open"] * 3


@pytest.mark.asyncio
async def test_workflow_start_backfills_before_its_services(runtime, monkeypatch):
    store, _, _, workflow = runtime
    legacy = legacy_experiment(store)
    action_id = insert_raw_action(store, create(store), phase="prepare")
    order = []
    real = obligation_backfill.backfill

    def recording(target, batch_size=200):
        order.append(("backfill", store.experiment(legacy)["hold_reason"]))
        return real(target, batch_size)

    async def service():
        order.append(("service", None))

    monkeypatch.setattr("experimenttracker.core.obligation_backfill.backfill", recording)
    monkeypatch.setattr(workflow, "refresh_cluster", service)
    monkeypatch.setattr(workflow, "connect_agent", service)
    monkeypatch.setattr(workflow.controller, "start", service)
    monkeypatch.setattr(workflow, "start_agent_pool", service)
    await workflow.start()
    assert order == [("backfill", "legacy_contract_archived")] + [("service", None)] * 4
    assert [row["id"] for row in action_rows(store)] == [f"agent-action:{action_id}:0"]
