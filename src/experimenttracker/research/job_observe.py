"""Read-only scheduler observation for saved contract 4 research jobs.

This helper records only scheduler-terminal outcomes. It neither submits jobs nor
reads outputs or advances the experiment graph.
"""

from .. import clock
from ..core import job_obligations
from . import wakeups

_TERMINAL_BAD = frozenset(
    {
        "FAILED",
        "CANCELLED",
        "TIMEOUT",
        "OUT_OF_MEMORY",
        "NODE_FAIL",
        "PREEMPTED",
        "BOOT_FAIL",
        "DEADLINE",
        "REVOKED",
    }
)


async def observe(workflow, experiment, job):
    """Observe one exact saved contract 4 job; return its terminal saved row or None.

    Scheduler read failures and nonterminal/accounting-pending responses have no
    side effects. A terminal job and its exact wake event are committed together.
    """
    if (
        (experiment["contract"] or {}).get("execution_contract") != 4
        or experiment["id"] != job["experiment_id"]
        or job["state"] != "submitted"
        or not job["job_id"]
    ):
        return None
    store = workflow.store
    with store.tx() as db:
        if job_obligations.locked_job(db, job, "state") is None:
            return None
    observed = await workflow.scheduler_for(experiment, job).observe(job["job_id"])
    if observed.state == "COMPLETED":
        if observed.exit_code is None:
            return None
        state = "complete" if observed.exit_code == "0:0" else "failed"
        error = None if state == "complete" else f"job exit {observed.exit_code}"
    elif observed.state in _TERMINAL_BAD:
        state, error = "failed", f"{observed.state}: {observed.exit_code}"
    else:
        return None

    with store.tx() as db:
        saved = store.experiment(experiment["id"])
        if saved["deleted_at"] or saved["phase"] in ("complete", "failed", "cancelled"):
            return None
        before = job_obligations.locked_job(db, job, "state")
        if before is None:
            return None
        db.execute(
            "UPDATE jobs SET state=?,scheduler_state=?,exit_code=?,observed_at=?,ended_at=COALESCE(?,ended_at),error=? "
            "WHERE intent_id=?",
            (
                state,
                observed.state,
                observed.exit_code,
                clock.now(),
                observed.ended_at,
                error,
                job["intent_id"],
            ),
        )
        job_obligations.sync_backend(db, job["intent_id"], before=before)
        wakeups.record(store, job["experiment_id"], wakeups.JOB_TERMINAL, job["intent_id"], db=db)
        store.emit(
            db,
            job["experiment_id"],
            "job_observed",
            {"intent_id": job["intent_id"], "state": state, "scheduler_state": observed.state},
        )
    return store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
