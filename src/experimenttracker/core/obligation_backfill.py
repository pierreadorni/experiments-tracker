"""Backfill responsibility obligations for sources saved before the source hooks.

The normal start sequence runs this once before any tick: it replays the same
mapping helpers the source writers use over each saved live source, so a
database created before the hooks carries exactly the responsibilities those
hooks would have created. It never changes a source row, contacts a scheduler or
agent, emits an event, dispatches work or manufactures an episode: a conflicting
obligation history is counted, logged and left as saved.
"""

import logging

from . import (
    action_obligations,
    advancement_decisions,
    job_obligations,
    obligation_sources,
    report_obligations,
    request_obligations,
    review_obligations,
    transfer_obligations,
    wakeup_obligations,
)

log = logging.getLogger(__name__)

_SAVEPOINT = "obligation_backfill"
_ACTION_LIVE = "'pending','dispatching','running','uncertain'"
_JOB_TERMINAL = "'complete','failed','cancelled'"
_REPORT_LIVE = "'queued','building','publishing'"
_EXPERIMENT_TERMINAL = "('complete','failed','cancelled')"
_CANCELLATION_HOLDS = "('cancellation_pending','cancellation_unconfirmed')"


def backfill(store, batch_size=200):
    """Replay every saved live source through its mapper; return per-source counts.

    Each source is read in fixed-size rowid keyset batches and each batch is its
    own write transaction, so a restart resumes from a saved obligation history
    instead of a process cursor. Every candidate is re-read by its mapper inside
    that transaction with ``before=None``: the saved row is the only evidence, so
    a replay (a second run included) preserves the episode, age and timestamps.
    """
    if isinstance(batch_size, bool) or not isinstance(batch_size, int) or batch_size < 1:
        raise ValueError("batch_size must be a positive integer")
    counts = {group[0]: _sync_group(store, group, batch_size) for group in _GROUPS}
    log.info("obligation backfill %s", counts)
    return counts


def _sync_group(store, group, batch_size):
    name, sql, columns, sync = group
    counts = {"synced": 0, "conflicts": 0}
    cursor = 0
    while True:
        with store.tx() as db:
            rows = db.execute(sql + " ORDER BY rowid LIMIT ?", (cursor, batch_size)).fetchall()
            if not rows:
                return counts
            cursor = rows[-1]["cursor"]
            for row in rows:
                db.execute(f"SAVEPOINT {_SAVEPOINT}")
                try:
                    sync(db, *(row[column] for column in columns))
                except ValueError as exc:
                    db.execute(f"ROLLBACK TO {_SAVEPOINT}")
                    counts["conflicts"] += 1
                    log.warning("obligation backfill conflict for %s %s: %s", name, row[columns[0]], exc)
                else:
                    counts["synced"] += 1
                finally:
                    db.execute(f"RELEASE {_SAVEPOINT}")


def _sync_request(db, experiment_id):
    return request_obligations.sync_request(db, experiment_id, before=None)


def _sync_review(db, experiment_id):
    return review_obligations.sync_review(db, experiment_id, before=None)


def _open_advancement(db, experiment_id):
    if obligation_sources.latest_episode(db, advancement_decisions.SOURCE_TYPE, experiment_id) is not None:
        return None
    return advancement_decisions.open_lifetime(db, experiment_id)


def _open_cancellation(db, experiment_id, unconfirmed):
    if obligation_sources.latest_episode(db, advancement_decisions.CANCELLATION, experiment_id) is not None:
        return None
    return advancement_decisions.open_cancellation(db, experiment_id, unconfirmed=bool(unconfirmed))


# Pre-selection only: the mapper reads the saved row again and decides. Frozen
# legacy has no advancement lifetime, so only contract-3 experiments are asked.
_GROUPS = (
    (
        "agent_action",
        f"SELECT rowid AS cursor,id FROM agent_actions WHERE rowid>? AND state IN ({_ACTION_LIVE})",
        ("id",),
        action_obligations.sync_action,
    ),
    (
        "job",
        f"SELECT rowid AS cursor,intent_id FROM jobs WHERE rowid>? AND state NOT IN ({_JOB_TERMINAL})",
        ("intent_id",),
        job_obligations.sync_job,
    ),
    (
        "transfer",
        "SELECT rowid AS cursor,id FROM execution_transfers WHERE rowid>? AND state='pending'",
        ("id",),
        transfer_obligations.sync_transfer,
    ),
    (
        "request",
        "SELECT rowid AS cursor,id FROM experiments WHERE rowid>? AND pending_request IS NOT NULL",
        ("id",),
        _sync_request,
    ),
    (
        "review",
        "SELECT rowid AS cursor,id FROM experiments WHERE rowid>? AND plan_review IS NOT NULL",
        ("id",),
        _sync_review,
    ),
    (
        "report_version",
        "SELECT rowid AS cursor,experiment_id,version FROM report_versions "
        f"WHERE rowid>? AND state IN ({_REPORT_LIVE})",
        ("experiment_id", "version"),
        report_obligations.sync_report,
    ),
    (
        "wakeup",
        "SELECT rowid AS cursor,id FROM agent_wakeups WHERE rowid>? AND state='pending'",
        ("id",),
        wakeup_obligations.sync_wakeup,
    ),
    (
        "advancement",
        "SELECT rowid AS cursor,id FROM experiments WHERE rowid>? AND deleted_at IS NULL "
        f"AND phase NOT IN {_EXPERIMENT_TERMINAL} "
        "AND json_extract(contract,'$.execution_contract') IS 3",
        ("id",),
        _open_advancement,
    ),
    (
        "cancellation",
        "SELECT rowid AS cursor,id,hold_reason='cancellation_unconfirmed' AS unconfirmed "
        f"FROM experiments WHERE rowid>? AND phase='cancelled' AND hold_reason IN {_CANCELLATION_HOLDS}",
        ("id", "unconfirmed"),
        _open_cancellation,
    ),
)
