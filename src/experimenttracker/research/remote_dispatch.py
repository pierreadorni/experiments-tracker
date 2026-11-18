"""Fresh-intent orchestration for the saved Jean Zay research route."""

from ..slurm import SubmissionUncertain
from ..store import Conflict
from .admission import SubmitJobRequest
from .job_dispatch import record_pre_send_failure, record_submission
from .job_intent import save
from .job_source import prepare_remote


async def dispatch(workflow, experiment_id, action, request):
    """Stage and send one newly saved remote intent; never replay it."""
    request = SubmitJobRequest.model_validate(request)
    intent = str(request.request_id)
    store = workflow.store
    if intent in workflow.submissions_in_flight:
        raise Conflict("this job request is already in flight")
    workflow.submissions_in_flight.add(intent)
    try:
        if store.one("SELECT 1 FROM jobs WHERE intent_id=?", (intent,)):
            raise Conflict("saved remote job intent is not permission to send or replay")
        prepared = await prepare_remote(workflow, experiment_id, action, request)
        job, created = save(
            workflow,
            experiment_id,
            action,
            request,
            prepared.prepared,
            execution=prepared.execution,
        )
        if not created:
            raise Conflict("saved remote job intent is not permission to send or replay")
        scheduler = workflow.scheduler_for(store.experiment(experiment_id), job)
        try:
            receipt = await scheduler.stage_research_source(job)
            await scheduler.preflight_research_job(job, receipt)
        except Exception as exc:
            # Staging and the explicit read-only preflight precede the send boundary.
            record_pre_send_failure(store, experiment_id, job, exc)
            raise
        try:
            # Once the send operation starts, every exception is uncertain: transport
            # errors cannot establish whether Slurm accepted the exact intent.
            identity = await scheduler.submit_research_job(job, receipt)
        except Exception as exc:
            if isinstance(exc, SubmissionUncertain):
                raise
            raise SubmissionUncertain(f"remote submission outcome unknown: {exc}") from exc
        saved_job = record_submission(store, experiment_id, job, identity)
        return {"job": saved_job, "receipt": receipt, "identity": identity}
    finally:
        workflow.submissions_in_flight.discard(intent)
