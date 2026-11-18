"""Read-only scheduler observation for saved contract 4 research jobs.

This helper records only scheduler-terminal outcomes. It neither submits jobs nor
reads outputs or advances the experiment graph.
"""

import json
from pathlib import Path

from .. import clock
from ..core import job_obligations
from ..execution_settings import ExecutionSettings
from ..remote import TransferPending
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


def _settle_terminal(workflow, job, *, state, scheduler_state, exit_code, ended_at, error):
    """Persist scheduler-terminal evidence and its exact wake as one transaction."""
    store = workflow.store
    with store.tx() as db:
        before = job_obligations.locked_job(
            db, job, "state", "scheduler_state", "exit_code", "ended_at", "execution_manifest"
        )
        parent = db.execute(
            "SELECT phase,deleted_at FROM experiments WHERE id=?", (job["experiment_id"],)
        ).fetchone()
        if (
            before is None
            or before["state"] != "submitted"
            or parent is None
            or parent["deleted_at"] is not None
            or parent["phase"] in ("complete", "failed", "cancelled")
        ):
            return None
        db.execute(
            "UPDATE jobs SET state=?,scheduler_state=?,exit_code=?,observed_at=?,"
            "ended_at=COALESCE(?,ended_at),error=? WHERE intent_id=?",
            (state, scheduler_state, exit_code, clock.now(), ended_at, error, job["intent_id"]),
        )
        job_obligations.sync_backend(db, job["intent_id"], before=before)
        wakeups.record(store, job["experiment_id"], wakeups.JOB_TERMINAL, job["intent_id"], db=db)
        store.emit(
            db,
            job["experiment_id"],
            "job_observed",
            {"intent_id": job["intent_id"], "state": state, "scheduler_state": scheduler_state},
        )
    return store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))


async def observe(workflow, experiment, job):
    """Observe a local contract 4 job; C3 remains outside this helper."""
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

    return _settle_terminal(
        workflow,
        job,
        state=state,
        scheduler_state=observed.state,
        exit_code=observed.exit_code,
        ended_at=observed.ended_at,
        error=error,
    )


def validate_remote_route(experiment, job):
    route = json.loads(job.get("execution_manifest") or "{}").get("execution") or {}
    expected = experiment["contract"].get("execution", {}).get("context")
    settings = ExecutionSettings.model_validate(route.get("settings"))
    layout = route.get("layout") or {}
    if (
        expected != "jean-zay"
        or route.get("context") != expected
        or settings.context != expected
        or layout.get("host") != settings.ssh_host
        or not Path(layout.get("remote_run") or "").is_absolute()
    ):
        raise ValueError("saved remote execution route is missing, invalid, or incompatible")


async def observe_remote(workflow, experiment, job):
    """Settle one exact saved remote C4 job after accounting and result delivery."""
    if (
        experiment["contract"].get("execution_contract") != 4
        or experiment["id"] != job["experiment_id"]
        or job["state"] != "submitted"
        or not job["job_id"]
    ):
        return None
    store = workflow.store
    scheduler = workflow.scheduler_for(experiment, job)

    async def deliver(saved):
        try:
            await scheduler.fetch_result(saved)
        except TransferPending:
            return None
        return _settle_terminal(
            workflow,
            saved,
            state="complete",
            scheduler_state="COMPLETED",
            exit_code="0:0",
            ended_at=saved["ended_at"],
            error=None,
        )

    # A restart resumes delivery only from durable successful accounting. It must
    # not ask the scheduler again after that evidence has been saved.
    if job["scheduler_state"] == "COMPLETED" and job["exit_code"] == "0:0" and job["ended_at"] is not None:
        return await deliver(job)

    observed = await scheduler.observe(job["job_id"])
    if observed.state == "COMPLETED" and observed.exit_code == "0:0":
        # Accounting may expose the exit status before its terminal timestamp.
        if observed.ended_at is None:
            return None
        with store.tx() as db:
            before = job_obligations.locked_job(db, job, "state", "scheduler_state", "exit_code")
            parent = db.execute(
                "SELECT phase,deleted_at FROM experiments WHERE id=?", (job["experiment_id"],)
            ).fetchone()
            if (
                before is None
                or before["state"] != "submitted"
                or parent is None
                or parent["deleted_at"] is not None
                or parent["phase"] in ("complete", "failed", "cancelled")
            ):
                return None
            db.execute(
                "UPDATE jobs SET scheduler_state='COMPLETED',exit_code='0:0',observed_at=?,"
                "started_at=COALESCE(?,started_at),ended_at=?,error=NULL WHERE intent_id=?",
                (clock.now(), getattr(observed, "started_at", None), observed.ended_at, job["intent_id"]),
            )
            job_obligations.sync_backend(db, job["intent_id"], before=before)
        saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
        return await deliver(saved)

    if observed.state == "COMPLETED" and observed.exit_code is None:
        return None
    if observed.state == "COMPLETED" or observed.state in _TERMINAL_BAD:
        error = (
            f"job exit {observed.exit_code}"
            if observed.state == "COMPLETED"
            else f"{observed.state}: {observed.exit_code}"
        )
        return _settle_terminal(
            workflow,
            job,
            state="failed",
            scheduler_state=observed.state,
            exit_code=observed.exit_code,
            ended_at=observed.ended_at,
            error=error,
        )
    return None
