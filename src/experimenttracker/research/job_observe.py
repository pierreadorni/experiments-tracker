"""Read-only scheduler observation for saved contract 4 research jobs.

This helper records exact scheduler outcomes and optional local previews. It
neither submits jobs nor advances the experiment graph.
"""

import asyncio
import json
from pathlib import Path

from .. import clock
from ..core import job_obligations
from ..remote import TransferPending
from ..store import encode
from ..tfevents import read_previews
from ..tfevents import selection as tensorboard_selection
from . import wakeups
from .remote_route import validate_remote_route

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


def _record_active(workflow, job, observed):
    """Refresh the saved observation and publish changes without waking the agent."""
    store = workflow.store
    with store.tx() as db:
        saved = job_obligations.locked_job(db, job, "state", "observed_at", "scheduler_state")
        if saved is None or saved["scheduler_state"] == "COMPLETED":
            return
        started_at = getattr(observed, "started_at", None) or saved["started_at"]
        updated = db.execute(
            "UPDATE jobs SET scheduler_state=?,started_at=?,observed_at=? WHERE intent_id=? "
            "AND EXISTS (SELECT 1 FROM experiments WHERE id=jobs.experiment_id "
            "AND deleted_at IS NULL AND phase NOT IN ('complete','failed','cancelled'))",
            (observed.state, started_at, clock.now(), job["intent_id"]),
        )
        if updated.rowcount and (observed.state, started_at) != (
            saved["scheduler_state"],
            saved["started_at"],
        ):
            store.emit(
                db,
                job["experiment_id"],
                "job_observed",
                {"intent_id": job["intent_id"], "state": saved["state"], "scheduler_state": observed.state},
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
    await _observe_tensorboard(workflow, job)
    if observed.state == "COMPLETED":
        if observed.exit_code is None:
            return None
        state = "complete" if observed.exit_code == "0:0" else "failed"
        error = None if state == "complete" else f"job exit {observed.exit_code}"
    elif observed.state in _TERMINAL_BAD:
        state, error = "failed", f"{observed.state}: {observed.exit_code}"
    else:
        _record_active(workflow, job, observed)
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


def _merge_tensorboard_preview(job, preview):
    previous = json.loads(job["tensorboard"]) if job.get("tensorboard") else None
    if previous:
        if not preview["image"]:
            preview["image"] = previous["image"]
        if preview["scalar"] and not preview["scalar"]["points"]:
            preview["scalar"] = previous["scalar"]
        preview["updated_at"] = max(preview["updated_at"], previous["updated_at"])
        if preview["image"]:
            preview["image_pending"] = False
    return previous, preview


def _publish_tensorboard(workflow, job, preview):
    previous, preview = _merge_tensorboard_preview(job, preview)
    if preview == previous:
        return
    with workflow.store.tx() as db:
        saved = job_obligations.locked_job(db, job, "state", "execution_manifest", "tensorboard")
        if saved is None:
            return
        db.execute("UPDATE jobs SET tensorboard=? WHERE intent_id=?", (encode(preview), job["intent_id"]))
        workflow.store.emit(db, job["experiment_id"], "job_progress", {"job_id": job["job_id"]})


async def _observe_tensorboard(workflow, job):
    """Persist an optional bounded local C4 preview without creating a wake."""
    manifest = json.loads(job.get("execution_manifest") or "{}")
    if not tensorboard_selection(manifest, job["stage_id"]) or not job.get("execution_root"):
        return
    if manifest.get("execution", {}).get("context", "acacia") != "acacia":
        return
    preview = await asyncio.to_thread(read_previews, Path(job["execution_root"]), job, manifest)
    if preview is not None:
        _publish_tensorboard(workflow, job, preview)


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
    manifest = json.loads(job.get("execution_manifest") or "{}")
    if experiment["contract"].get("execution", {}).get("context") == "jean-zay":
        validate_remote_route(
            experiment, job["intent_id"], manifest.get("execution"), job.get("execution_root")
        )
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
    if manifest.get("execution", {}).get("context") == "jean-zay" and observed.state in {
        "PENDING",
        "CONFIGURING",
        "RUNNING",
        "SUSPENDED",
        "COMPLETING",
    }:
        try:
            preview = await scheduler.fetch_event_preview(job)
        except Exception:
            pass
        else:
            if preview is not None:
                _publish_tensorboard(workflow, job, preview)
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
    _record_active(workflow, job, observed)
    return None
