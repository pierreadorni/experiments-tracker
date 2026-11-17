"""Map saved Slurm intents to stable responsibility obligations.

Call ``sync_job`` inside the transaction that changes a job. It tracks saved
identity only; it never submits, retries, or decides that a missing job is absent.
"""

import json

from .. import legacy_archive
from . import obligation_sources

_SOURCE = "job"
_IDENTITY_FIELDS = ("intent_id", "experiment_id", "stage_id", "attempt")
_ALLOWED_RESPONSIBILITIES = {("backend", "reconcile"), ("external", "job")}


def locked_job(db, job, *fields):
    """Re-read ``job`` in the caller's write transaction; None if its identity or ``fields`` changed."""
    row = db.execute("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],)).fetchone()
    if row is None or any(row[field] != job[field] for field in (*_IDENTITY_FIELDS, "job_id", *fields)):
        return None
    return dict(row)


def sync_backend(db, intent_id, *, before):
    """Run ``sync_job`` so ledger conflicts stay off scientific ``ValueError`` paths."""
    try:
        sync_job(db, intent_id, before=before)
    except ValueError as exc:
        raise RuntimeError(f"job obligation mapping failed: {exc}") from exc


def sync_job(db, intent_id, *, before=None):
    """Synchronize a saved Slurm intent in its caller's transaction.

    ``before`` is the row captured inside that transaction before its update. A
    missing-to-saved scheduler ID proves the backend-to-external handoff. A
    nonempty saved scheduler ID is immutable. Terminal callbacks close only an
    already-open episode and only when exact scheduler accounting is saved.
    """
    if not db.in_transaction:
        raise RuntimeError("job obligation sync requires the caller's active transaction")
    if not isinstance(intent_id, str) or not intent_id:
        raise ValueError("intent_id must be a non-empty string")
    saved = db.execute("SELECT * FROM jobs WHERE intent_id=?", (intent_id,)).fetchone()
    if saved is None:
        raise ValueError(f"saved job intent does not exist: {intent_id}")
    job = dict(saved)
    if job["intent_id"] != intent_id:
        raise ValueError("saved job intent identity changed")
    previous = None if before is None else dict(before)
    if previous is not None:
        if any(previous.get(field) != job[field] for field in _IDENTITY_FIELDS):
            raise ValueError("saved job stage or attempt identity cannot change")
        old_id = previous.get("job_id")
        if old_id is not None and old_id != job["job_id"]:
            raise ValueError("saved scheduler job ID cannot change or be cleared")

    experiment = db.execute("SELECT contract FROM experiments WHERE id=?", (job["experiment_id"],)).fetchone()
    if experiment is None:
        raise ValueError(f"saved job experiment does not exist: {job['experiment_id']}")
    contract = json.loads(experiment["contract"])
    project_id = (contract.get("project") or {}).get("id")
    detail = f"stage={job['stage_id']};attempt={job['attempt']}"
    latest = obligation_sources.latest_episode(db, _SOURCE, intent_id)
    pseudo_job = _is_agent_pseudo_job(job)
    _check_saved_identity(db, latest, job, project_id, detail, pseudo=pseudo_job)

    if pseudo_job:
        return _terminalize(db, latest, intent_id, "abandon")

    job_id = job["job_id"]
    if job_id is not None and (not isinstance(job_id, str) or not job_id):
        raise ValueError("saved scheduler job ID must be a non-empty string or None")
    scheduler_outcome = _scheduler_outcome(job) if job_id is not None else None
    source_transition = bool(previous is not None and previous.get("job_id") is None and job_id is not None)

    if scheduler_outcome is not None and latest is None:
        return None
    if latest is not None and latest["state"] != "open":
        if scheduler_outcome is not None:
            return latest
        raise ValueError("active or unresolved saved job conflicts with terminal obligation history")

    if job_id is None:
        owner, kind, wait = "backend", "reconcile", f"reconcile:{intent_id}"
    else:
        owner, kind, wait = "external", "job", f"job:{job_id}"
    obligation = obligation_sources.open_episode(
        db,
        _SOURCE,
        intent_id,
        source_transition=source_transition,
        experiment_id=job["experiment_id"],
        project_id=project_id,
        kind=kind,
        target=intent_id,
        owner=owner,
        wait=wait,
        detail=detail,
    )
    if obligation["state"] != "open":
        raise ValueError("unresolved saved job cannot reopen terminal obligation history")
    if scheduler_outcome is None:
        return obligation
    return _terminalize(db, obligation, intent_id, scheduler_outcome)


def _scheduler_outcome(job):
    state = job["scheduler_state"]
    exit_code = job["exit_code"]
    if state == "COMPLETED" and exit_code is not None:
        return "satisfy" if exit_code == "0:0" else "abandon"
    if state in legacy_archive.TERMINAL_BAD:
        return "abandon"
    return None


def _is_agent_pseudo_job(job):
    if job["state"] == "agent":
        return True
    raw = job.get("execution_manifest")
    if not raw:
        return False
    manifest = json.loads(raw)
    if not isinstance(manifest, dict):
        raise ValueError("saved job execution manifest must be an object")
    return manifest.get("executor") == "agent"


def _check_saved_identity(db, latest, job, project_id, detail, *, pseudo=False):
    if latest is None:
        return
    source_key = f"{_SOURCE}:{job['intent_id']}"
    wanted = {
        "experiment_id": job["experiment_id"],
        "project_id": project_id,
        "target": job["intent_id"],
        "detail": detail,
    }
    prefix = source_key + ":"
    history = [
        dict(row)
        for row in db.execute(
            "SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id", (len(prefix), prefix)
        )
        if row["id"].rsplit(":", 1)[0] == source_key
    ]
    for row in history:
        if any(row[field] != value for field, value in wanted.items()):
            raise ValueError("saved job obligation scope, stage, or attempt identity conflicts")
        if (row["owner"], row["kind"]) not in _ALLOWED_RESPONSIBILITIES:
            raise ValueError("saved job obligation owner/kind pair conflicts with job responsibility")
    for row in history:
        if row["owner"] == "backend" and row["wait"] != f"reconcile:{job['intent_id']}":
            raise ValueError("saved backend job reconciliation identity conflicts")
        if row["owner"] == "external" and not pseudo:
            saved_id = job["job_id"]
            if saved_id is None or row["wait"] != f"job:{saved_id}":
                raise ValueError("saved external scheduler identity conflicts with the current job ID")


def _terminalize(db, latest, intent_id, operation):
    if latest is None or latest["state"] != "open":
        return latest
    episode = int(latest["id"].rsplit(":", 1)[1])
    terminalize = (
        obligation_sources.satisfy_episode if operation == "satisfy" else obligation_sources.abandon_episode
    )
    return terminalize(db, _SOURCE, intent_id, episode)
