"""Exact C2 liveness over saved sources: read-only, no dispatch.

Each case saves its source through the real mapper, then drifts the saved row
with raw SQL to reproduce a defect as the watchdog sees it.
"""

from uuid import uuid4

import pytest
from helpers import seed_saved_contract3
from test_advancement_lifetime import cancel

from experimenttracker import clock
from experimenttracker.core import (
    action_obligations,
    advancement_decisions,
    job_obligations,
    liveness,
    obligation_backfill,
    transfer_obligations,
    wakeup_obligations,
)

INTERVAL = 60.0


@pytest.fixture
def store(runtime):
    return runtime[0]


def create(store, brief="Synthetic liveness"):
    eid = seed_saved_contract3(store, brief)
    with store.tx() as db:
        now = clock.now()
        db.execute("UPDATE experiments SET created_at=?,updated_at=? WHERE id=?", (now, now, eid))
        advancement_decisions.open_lifetime(db, eid)
    return eid


def save_action(store, eid, *, state="pending", thread_id=None, turn_id=None):
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,"
            "input,created_at,updated_at) VALUES(?,?,'prepare',1,?,?,?,'synthetic',1,1)",
            (action_id, eid, state, thread_id, turn_id),
        )
        action_obligations.sync_action(db, action_id)
    return action_id


def save_job(store, eid, *, state="submitted", job_id="101"):
    intent_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at) VALUES(?,?,'prepare',1,?,?,'synthetic','{}','control',1)",
            (intent_id, eid, state, job_id),
        )
        job_obligations.sync_job(db, intent_id)
    return intent_id


def save_transfer(store, eid, *, state="pending"):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO execution_transfers VALUES(?,?,'up','{\"layout\":{},\"settings\":{}}',?,NULL,1)",
            (identity, eid, state),
        )
        transfer_obligations.sync_transfer(db, identity)
    return identity


def save_wakeup(store, eid, *, due_at=600.0, state="pending"):
    wakeup_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO agent_wakeups VALUES(?,?,'backend compute check',?,?,NULL,1)",
            (wakeup_id, eid, due_at, state),
        )
        wakeup_obligations.sync_wakeup(db, wakeup_id)
    return wakeup_id


def run(store, sql, args=()):
    """Drift a saved row past its mapper, as an unsettled or stale source would."""
    with store.tx() as db:
        db.execute(sql, args)


def inspect(store, now, *, serviced=None, started_at=None, owner_holds=()):
    evidence = liveness.ServiceEvidence(now if started_at is None else started_at, serviced or {})
    with store.tx() as db:
        return liveness.inspect(db, now, interval=INTERVAL, service=evidence, owner_holds=owner_holds)


def cause(report, obligation_id):
    matches = [verdict.cause for verdict in report.verdicts if verdict.obligation_id == obligation_id]
    assert len(matches) == 1, obligation_id
    return matches[0]


def test_an_agent_turn_is_live_only_with_its_exact_saved_turn(store):
    eid = create(store)
    action_id = save_action(store, eid, state="running", thread_id="thread-1", turn_id="turn-1")
    obligation_id = f"agent-action:{action_id}:0"
    assert cause(inspect(store, 100.0), obligation_id) is None

    run(store, "UPDATE agent_actions SET turn_id='turn-2' WHERE id=?", (action_id,))
    assert cause(inspect(store, 100.0), obligation_id) == "identity_mismatch"

    run(store, "UPDATE agent_actions SET state='completed' WHERE id=?", (action_id,))
    assert cause(inspect(store, 100.0), obligation_id) == "source_not_live"


def test_owner_questions_and_reviews_need_the_existing_notice(store):
    asked = create(store, "Synthetic owner question")
    store.update(asked, pending_request={"id": "request-1", "question": "Approve?"})
    store.notify(asked, "required_decision", "Codex needs an answer or permission.")
    reviewed = create(store, "Synthetic owner review")
    store.update(reviewed, plan_review={"revision": 1, "state": "awaiting_review"})
    store.notify(reviewed, "required_decision", "Review the experiment plan.")
    silent = create(store, "Synthetic notice missing")
    store.update(silent, pending_request={"id": "request-2", "question": "Approve?"})

    report = inspect(store, 100.0)
    assert cause(report, f"request:{asked}:request-1:0") is None
    assert cause(report, f"review:{reviewed}:1:0") is None
    assert cause(report, f"request:{silent}:request-2:0") == "notice_missing"


def test_external_work_is_live_only_while_its_saved_identity_is_not_terminal(store):
    eid = create(store)
    intent_id = save_job(store, eid)
    obligation_id = f"job:{intent_id}:0"
    assert cause(inspect(store, 100.0), obligation_id) is None

    run(store, "UPDATE jobs SET job_id='999' WHERE intent_id=?", (intent_id,))
    assert cause(inspect(store, 100.0), obligation_id) == "identity_mismatch"

    run(store, "UPDATE jobs SET job_id='101',state='complete' WHERE intent_id=?", (intent_id,))
    assert cause(inspect(store, 100.0), obligation_id) == "source_not_live"

    identity = save_transfer(store, eid)
    assert cause(inspect(store, 100.0), f"transfer:{identity}:0") is None
    run(store, "UPDATE execution_transfers SET state='complete' WHERE id=?", (identity,))
    assert cause(inspect(store, 100.0), f"transfer:{identity}:0") == "source_not_live"


def test_a_timer_needs_its_saved_due_time_and_one_interval_of_slack(store):
    eid = create(store)
    wakeup_id = save_wakeup(store, eid, due_at=600.0)
    obligation_id = f"wakeup:{wakeup_id}:0"
    assert cause(inspect(store, 100.0), obligation_id) is None
    assert cause(inspect(store, 660.0), obligation_id) is None
    assert cause(inspect(store, 661.0), obligation_id) == "overdue"

    run(store, "UPDATE agent_wakeups SET due_at=601.0 WHERE id=?", (wakeup_id,))
    assert cause(inspect(store, 100.0), obligation_id) == "identity_mismatch"

    run(store, "UPDATE agent_wakeups SET due_at=600.0,state='queued' WHERE id=?", (wakeup_id,))
    assert cause(inspect(store, 100.0), obligation_id) == "source_not_live"


def test_backend_families_need_service_evidence_or_restart_grace(store):
    eid = create(store)
    action_id = save_action(store, eid, state="pending")
    obligation_id = f"agent-action:{action_id}:0"
    assert cause(inspect(store, 100.0), obligation_id) is None
    assert cause(inspect(store, 160.0, started_at=100.0), obligation_id) is None
    assert cause(inspect(store, 161.0, started_at=100.0), obligation_id) == "unserviced"
    assert cause(inspect(store, 161.0, serviced={"agent-action": 150.0}), obligation_id) is None
    assert cause(inspect(store, 211.0, serviced={"agent-action": 150.0}), obligation_id) == "unserviced"


def test_advancement_is_live_with_an_open_dependency_or_a_fresh_revision(store):
    eid = create(store)
    obligation_id = f"advancement:{eid}:0"
    run(store, "UPDATE experiments SET updated_at=100.0 WHERE id=?", (eid,))
    assert cause(inspect(store, 100.0), obligation_id) is None
    run(store, "UPDATE experiments SET updated_at=0.0 WHERE id=?", (eid,))
    assert cause(inspect(store, 100.0), obligation_id) == "missing_dependency"

    identity = save_transfer(store, eid)
    assert cause(inspect(store, 100.0), obligation_id) is None
    assert cause(inspect(store, 100.0), f"transfer:{identity}:0") is None

    run(store, "UPDATE execution_transfers SET state='complete' WHERE id=?", (identity,))
    with store.tx() as db:
        transfer_obligations.sync_transfer(db, identity)
    assert cause(inspect(store, 100.0), obligation_id) == "missing_dependency"
    run(store, "UPDATE experiments SET updated_at=100.0 WHERE id=?", (eid,))
    assert cause(inspect(store, 100.0), obligation_id) is None


def test_an_owner_hold_alone_keeps_advancement_live(store):
    eid = create(store)
    run(store, "UPDATE experiments SET updated_at=0.0,hold_reason='needs_attention' WHERE id=?", (eid,))
    obligation_id = f"advancement:{eid}:0"
    assert cause(inspect(store, 100.0), obligation_id) == "missing_dependency"
    assert cause(inspect(store, 100.0, owner_holds=("needs_attention",)), obligation_id) is None


def test_a_contract3_experiment_without_its_lifetime_is_a_defect_until_backfilled(store):
    eid = create(store)
    run(store, "DELETE FROM obligations WHERE id=?", (f"advancement:{eid}:0",))

    report = inspect(store, 100.0)
    assert [verdict for verdict in report.defects if verdict.experiment_id == eid] == [
        liveness.LivenessVerdict(eid, None, "advancement", "missing_lifetime")
    ]

    obligation_backfill.backfill(store)
    assert [verdict for verdict in inspect(store, 100.0).defects if verdict.experiment_id == eid] == []


def test_cancellation_work_is_inspected_on_a_terminal_experiment(store):
    eid = create(store)
    cancel(store, eid)
    obligation_id = f"cancellation:{eid}:0"
    assert cause(inspect(store, 100.0), obligation_id) is None
    assert cause(inspect(store, 161.0, started_at=100.0), obligation_id) == "unserviced"
    assert cause(inspect(store, 100.0, serviced={"cancellation": 100.0}), obligation_id) is None


def test_inspection_reports_every_open_obligation_and_writes_nothing(store):
    eid = create(store)
    save_action(store, eid, state="running", thread_id="thread-1", turn_id="turn-1")
    save_job(store, eid)
    save_transfer(store, eid)
    save_wakeup(store, eid)
    store.update(eid, pending_request={"id": "request-1", "question": "Approve?"})
    before_changes = store.db.total_changes
    before_rows = store.rows("SELECT * FROM obligations ORDER BY id")

    report = inspect(store, 100.0)

    assert sorted(verdict.obligation_id for verdict in report.verdicts) == [
        row["id"] for row in before_rows if row["state"] == "open"
    ]
    assert store.db.total_changes == before_changes
    assert store.rows("SELECT * FROM obligations ORDER BY id") == before_rows


def test_an_unsupported_saved_responsibility_is_reported_not_raised(store):
    eid = create(store)
    action_id = save_action(store, eid, state="running", thread_id="thread", turn_id="turn")
    obligation_id = f"agent-action:{action_id}:0"
    run(store, "UPDATE obligations SET owner='owner' WHERE id=?", (obligation_id,))
    assert cause(inspect(store, 100.0), obligation_id) == "unsupported_responsibility"
