"""C5 resolution of saved Slurm intents that never saved a scheduler identity.

Job observation calls :func:`resolve` at the scheduler cadence for every active
intent in ``jobs.state='submitting' AND job_id IS NULL``, before admission may
consider another submission for the experiment. Observers that only record
scheduler facts (cancellation, archived settlement) call :func:`adopt` directly.
One owner decides adoption, quarantine and proven absence: the resolver never
submits or retries, and an unreadable scheduler concludes nothing.
"""

from . import clock
from .core import job_obligations
from .execution_settings import ExecutionSettings
from .slurm import SubmissionUncertain

QUERY = "squeue+sacct"
ACCOUNTING_WINDOW_SECONDS = 6 * 86400
QUARANTINE_NOTICE = "Several scheduler jobs match one submission intent; operator reconciliation is required."
WAITING_ERROR = "No matching job found; operator reconciliation required"


def accounting_lag_seconds(exp):
    """The saved accounting lag for this experiment's execution context."""
    settings = ExecutionSettings.model_validate(exp["contract"].get("execution") or {})
    return settings.accounting_lag_seconds


def adopt(store, eid, job, job_id, *, observer):
    """Save one exact scheduler identity for the saved intent in one transaction.

    ``observer`` names the caller in the receipt evidence. Returns the saved job
    row, or None when the intent changed before the identity could be saved.
    """
    intent_id = job["intent_id"]
    with store.tx() as db:
        before = job_obligations.locked_job(db, job, "state")
        if before is None:
            return None
        cursor = db.execute(
            "UPDATE jobs SET job_id=?,state='submitted' WHERE intent_id=? AND job_id IS NULL AND state=?",
            (job_id, intent_id, job["state"]),
        )
        if not cursor.rowcount:
            return None
        job_obligations.sync_backend(db, intent_id, before=before)
        store.emit(
            db,
            eid,
            "job_reconciled",
            {
                "intent_id": intent_id,
                "job_id": job_id,
                "evidence": {"query": QUERY, "observer": observer, "matched": 1},
            },
        )
    return store.one("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))


async def resolve(workflow, exp, job):
    """Decide one active intent without a scheduler identity.

    Returns the saved job row when exactly one match was adopted, otherwise None.
    """
    store, eid = workflow.store, exp["id"]
    try:
        job_id = await workflow.scheduler_for(exp, job).reconcile(job["intent_id"])
    except SubmissionUncertain as exc:
        return _quarantine(store, eid, job, str(exc))
    except (RuntimeError, TimeoutError, OSError) as exc:
        # Unreadable scheduler: keep waiting. Remote reads are cached for the
        # configured poll interval, so repeats stay bounded without new state.
        store.update(eid, hold_reason="submission_uncertain", error=str(exc))
        return None
    if job_id:
        saved = adopt(store, eid, job, job_id, observer="observe_jobs")
        if saved:
            store.update(eid, hold_reason=None, error=None)
        return saved
    lag = accounting_lag_seconds(exp)
    waited = clock.now() - job["created_at"]
    # The accounting query covers seven days; older intents could have aged out,
    # so absence is provable only inside that window.
    if waited <= lag or waited > ACCOUNTING_WINDOW_SECONDS:
        store.update(eid, hold_reason="submission_uncertain", error=WAITING_ERROR)
        return None
    return _not_submitted(store, eid, job, lag)


def _quarantine(store, eid, job, error):
    """Several exact matches: keep the intent and leave the owner to reconcile it."""
    saved = store.experiment(eid)
    recorded = (saved["hold_reason"], saved["error"]) == ("submission_uncertain", error)
    store.update(eid, hold_reason="submission_uncertain", error=error)
    if recorded:
        return None
    with store.tx() as db:
        store.emit(
            db,
            eid,
            "job_reconciled",
            {
                "intent_id": job["intent_id"],
                "outcome": "quarantined",
                "evidence": {"query": QUERY, "matched": "several", "error": error},
            },
        )
    store.notify(eid, "executor_error", QUARANTINE_NOTICE)
    return None


def _not_submitted(store, eid, job, lag):
    """No record after the accounting lag: write the receipt and end the intent."""
    intent_id = job["intent_id"]
    with store.tx() as db:
        before = job_obligations.locked_job(db, job, "state")
        if before is None:
            return None
        cursor = db.execute(
            "UPDATE jobs SET state='failed',error=? WHERE intent_id=? AND job_id IS NULL AND state='submitting'",
            (job_obligations.NOT_SUBMITTED_ERROR, intent_id),
        )
        if not cursor.rowcount:
            return None
        job_obligations.sync_backend(db, intent_id, before=before)
        store.emit(
            db,
            eid,
            "job_reconciled",
            {
                "intent_id": intent_id,
                "outcome": "not_submitted",
                "evidence": {
                    "query": QUERY,
                    "matched": 0,
                    "waited_seconds": round(clock.now() - job["created_at"]),
                    "accounting_lag_seconds": lag,
                },
            },
        )
    store.update(eid, hold_reason=None, error=None)
    return None
