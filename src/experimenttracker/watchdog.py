"""Interim C2 liveness watchdog over existing tables; it notifies and never dispatches."""

import logging
from dataclasses import dataclass

log = logging.getLogger(__name__)

TERMINAL_PHASES = ("complete", "failed", "cancelled")
# Holds whose next step belongs to the owner, so silence is expected until they answer.
OWNER_HOLDS = (
    "plan_review",
    "agent_paused",
    "new_compute_paused",
    "needs_attention",
    "quota_hold",
    "agent_busy",
    "cancellation_pending",
)
LIVE_ACTIONS = ("pending", "dispatching", "running")
LIVE_JOBS = ("submitting", "submitted")
NOTICE_KIND = "engine_stuck"
HINT = "Inspect the saved work and resolve this hold; reconcile uncertain outcomes before retrying."


def _quoted(values):
    return ",".join(f"'{value}'" for value in values)


# One cheap query per tick: an experiment is stuck when nothing live and nothing
# owner-owned can still move it. The time bound also covers the revision, because
# experiments.updated_at is written with revision+1 in one statement (see
# stuck_experiments).
STUCK_SQL = f"""
    SELECT e.id,e.hold_reason,e.error,e.updated_at FROM experiments e
    WHERE e.deleted_at IS NULL
      AND e.phase NOT IN ({_quoted(TERMINAL_PHASES)})
      AND COALESCE(e.hold_reason,'') NOT IN ({_quoted(OWNER_HOLDS)})
      AND e.pending_request IS NULL
      AND e.updated_at <= ?
      AND NOT EXISTS (
        SELECT 1 FROM agent_actions a WHERE a.experiment_id=e.id AND a.state IN ({_quoted(LIVE_ACTIONS)}))
      AND NOT EXISTS (
        SELECT 1 FROM jobs j WHERE j.experiment_id=e.id AND j.state IN ({_quoted(LIVE_JOBS)}))
      AND NOT EXISTS (
        SELECT 1 FROM execution_transfers t WHERE t.experiment_id=e.id AND t.state='pending')
    ORDER BY e.updated_at,e.id
"""

NOTICED_SQL = "SELECT text FROM inbox WHERE experiment_id=? AND kind=?"


@dataclass(frozen=True)
class StuckExperiment:
    """An experiment that nothing has changed and nothing can still move."""

    id: str
    hold_reason: str | None
    error: str | None
    unchanged_seconds: float

    @property
    def hold(self):
        return self.hold_reason or "no_hold"


def stuck_experiments(store, now, minutes):
    """Experiments whose hold reason and revision have been unchanged for `minutes`.

    `Store.emit` writes `experiments.updated_at` in the same statement that bumps
    `revision`, and `Store.update` returns before emitting when no field changes, so
    `updated_at` is the commit time of the current revision. Comparing `updated_at`
    with `now` therefore observes both conditions of the rule at once, and it is read
    from the database, so the observation is unchanged by an owner restart and needs no
    new schema, in-memory timer or per-experiment bookkeeping.
    """
    rows = store.rows(STUCK_SQL, (now - minutes * 60,))
    return [
        StuckExperiment(row["id"], row["hold_reason"], row["error"], now - row["updated_at"]) for row in rows
    ]


def engine_watchdog(store, now, minutes):
    """Report stuck experiments once per (experiment, hold reason); never dispatch work.

    `inbox` has UNIQUE(experiment_id,kind,text), and the notice text starts with the
    exact hold reason, so repeated ticks and restarts cannot add a second notice. Text
    is deterministic for a given hold and error, and the check of already reported hold
    reasons keeps one notice per (experiment, hold reason) even when the recorded error
    changed under the same hold.
    """
    notices = 0
    for experiment in stuck_experiments(store, now, minutes):
        if experiment.hold in _noticed_holds(store, experiment.id):
            continue
        log.warning(
            "engine watchdog: experiment %s has been stuck in %s for %.0f minutes",
            experiment.id,
            experiment.hold,
            experiment.unchanged_seconds / 60,
        )
        store.notify(experiment.id, NOTICE_KIND, _notice_text(experiment))
        notices += 1
    return notices


def _noticed_holds(store, eid):
    """Hold reasons with an existing notice; the text always starts with the exact hold."""
    return {row["text"].split(":", 1)[0] for row in store.rows(NOTICED_SQL, (eid, NOTICE_KIND))}


def _notice_text(experiment):
    return f"{experiment.hold}: {experiment.error or 'no error recorded'}. {HINT}"
