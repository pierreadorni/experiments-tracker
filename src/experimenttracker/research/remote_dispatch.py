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
        try:
            scheduler = workflow.scheduler_for(store.experiment(experiment_id), job)
            receipt = await scheduler.stage_research_source(job)
            # submit_research_job performs the single gated preflight immediately
            # before its scheduler call. SubmissionUncertain means send outcome unknown.
            identity = await scheduler.submit_research_job(job, receipt)
        except SubmissionUncertain:
            # The send may have succeeded; only exact reconciliation can settle it.
            raise
        except Exception as exc:
            record_pre_send_failure(store, experiment_id, job, exc)
            raise
        saved_job = record_submission(store, experiment_id, job, identity)
        return {"job": saved_job, "receipt": receipt, "identity": identity}
    finally:
        workflow.submissions_in_flight.discard(intent)
