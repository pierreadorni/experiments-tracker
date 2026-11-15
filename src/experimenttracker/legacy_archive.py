"""Inert helpers for observing saved work on frozen execution contracts."""

from . import clock

TERMINAL_BAD = {
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


def execution_frozen(exp):
    return exp["contract"].get("execution_contract") != 3


def archive_active(exp):
    return (
        execution_frozen(exp)
        and exp.get("deleted_at") is None
        and exp["phase"]
        not in {
            "complete",
            "failed",
            "cancelled",
        }
    )


async def settle_job(workflow, exp, job):
    """Record scheduler facts for one saved legacy job without advancing its stage."""
    if (
        not execution_frozen(exp)
        or exp.get("deleted_at") is not None
        or exp["phase"] == "cancelled"
        or job["state"] not in ("submitting", "submitted")
    ):
        return False

    intent_id = job["intent_id"]
    try:
        from .remote import RemoteSlurmScheduler

        scheduler = workflow.scheduler_for(exp, job)
        now = clock.now()
        if isinstance(scheduler, RemoteSlurmScheduler):
            if not scheduler.due(job, now):
                return False
            with workflow.store.tx() as db:
                db.execute("UPDATE jobs SET remote_polled_at=? WHERE intent_id=?", (now, intent_id))

        if not job.get("job_id"):
            job_id = await scheduler.reconcile(intent_id)
            if not job_id:
                return False
            with workflow.store.tx() as db:
                cursor = db.execute(
                    "UPDATE jobs SET job_id=?,state='submitted' "
                    "WHERE intent_id=? AND job_id IS NULL AND state=?",
                    (job_id, intent_id, job["state"]),
                )
                if cursor.rowcount:
                    workflow.store.emit(db, exp["id"], "job_reconciled", {"job_id": job_id})
            saved = workflow.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent_id,))
            if not saved or saved["job_id"] != job_id or saved["state"] != "submitted":
                return False
            job = saved

        observed = await scheduler.observe(job["job_id"])
        workflow.research.service.budget.observe(
            job,
            observed,
            exp["contract"].get("execution", {}).get("context", "acacia"),
        )
        observed_at = observed.observed_at or clock.now()
        state = "submitted"
        error = None
        if observed.state == "COMPLETED" and observed.exit_code is not None:
            if observed.exit_code == "0:0":
                state = "complete"
            else:
                state, error = "failed", f"job exit {observed.exit_code}"
        elif observed.state in TERMINAL_BAD:
            state, error = "failed", f"{observed.state}: {observed.exit_code}"

        changed = any(
            job.get(field) != value
            for field, value in (
                ("state", state),
                ("scheduler_state", observed.state),
                ("exit_code", observed.exit_code),
                ("started_at", observed.started_at or job.get("started_at")),
                ("ended_at", observed.ended_at or job.get("ended_at")),
            )
        )
        with workflow.store.tx() as db:
            cursor = db.execute(
                "UPDATE jobs SET state=?,scheduler_state=?,exit_code=?,observed_at=?,error=?,"
                "started_at=COALESCE(?,started_at),ended_at=COALESCE(?,ended_at) "
                "WHERE intent_id=? AND job_id=? AND state=?",
                (
                    state,
                    observed.state,
                    observed.exit_code,
                    observed_at,
                    error,
                    observed.started_at,
                    observed.ended_at,
                    intent_id,
                    job["job_id"],
                    job["state"],
                ),
            )
            if cursor.rowcount and changed:
                workflow.store.emit(
                    db,
                    exp["id"],
                    "job_observed",
                    {"job_id": job["job_id"], "state": observed.state},
                )
        return bool(cursor.rowcount)
    except Exception as exc:
        with workflow.store.tx() as db:
            db.execute(
                "UPDATE jobs SET error=? WHERE intent_id=? AND job_id IS ? AND state=?",
                (str(exc), intent_id, job.get("job_id"), job["state"]),
            )
        return False
