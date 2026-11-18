"""Send one exact local research job intent; leave uncertain sends to observation."""

import asyncio
import hashlib
import json

from .. import artifacts
from ..core import action_obligations, job_obligations
from ..slurm import SubmissionUncertain
from ..store import Conflict
from .admission import SubmitJobRequest, preflight
from .job_intent import save
from .job_source import prepare
from .tools import require_live_research


def _replay(store, eid, action, request):
    with store.tx() as db:
        saved = action_obligations.expected_saved(db, action)
        experiment = store.experiment(eid)
        require_live_research(experiment, saved)
        if (
            saved["state"] != "running"
            or not saved["turn_id"]
            or not saved["thread_id"]
            or (saved["thread_id"] != experiment["thread_id"])
        ):
            raise ValueError("submission requires the exact running research turn")
        row = db.execute("SELECT * FROM jobs WHERE intent_id=?", (str(request.request_id),)).fetchone()
        if row is None:
            return None
        manifest = json.loads(row["execution_manifest"] or "{}")
        if (
            row["experiment_id"] != eid
            or manifest.get("research_action_id") != saved["id"]
            or manifest.get("research_request") != request.saved_request()
        ):
            raise Conflict("job intent identity already belongs to another submission")
        return dict(row)


async def dispatch(workflow, eid, action, request):
    """Return a saved receipt; only a fresh intent grants one scheduler call."""
    request = SubmitJobRequest.model_validate(request)
    intent = str(request.request_id)
    store = workflow.store
    prior = _replay(store, eid, action, request)
    if prior is not None:
        return prior
    if intent in workflow.submissions_in_flight:
        raise Conflict("this job request is already in flight")
    workflow.submissions_in_flight.add(intent)
    try:
        source = await asyncio.to_thread(prepare, workflow, eid, action, request)
        preflight(workflow, eid, action, request)
        scheduler = workflow.scheduler_for(store.experiment(eid))
        await scheduler.preflight(intent, source.script, source.root, request.resources)
        script = artifacts.allowed_path(source.script, source.root)
        if hashlib.sha256(script.read_bytes()).hexdigest() != source.prepared.script_hash:
            raise Conflict("prepared research script changed during preflight")
        # save rechecks admission in the transaction that creates the intent.
        job, created = save(workflow, eid, action, request, source.prepared, execution=source.execution)
        if not created:
            return job
        job_id = await scheduler.submit(intent, script, source.root, resources=request.resources)
        if not isinstance(job_id, str) or not job_id.strip():
            raise SubmissionUncertain("scheduler returned no exact job identity")
        with store.tx() as db:
            before = job_obligations.locked_job(db, job, "state", "execution_manifest", "script_hash")
            if before is None:
                raise SubmissionUncertain("saved intent changed before scheduler identity was recorded")
            db.execute("UPDATE jobs SET state='submitted',job_id=? WHERE intent_id=?", (job_id, intent))
            job_obligations.sync_backend(db, intent, before=before)
            store.emit(db, eid, "job_submitted", {"intent_id": intent, "job_id": job_id})
            return dict(db.execute("SELECT * FROM jobs WHERE intent_id=?", (intent,)).fetchone())
    finally:
        workflow.submissions_in_flight.discard(intent)
