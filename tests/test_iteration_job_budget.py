"""Preparation job budget for contract-3 mixed plans counts scheduled work only."""

from unittest.mock import AsyncMock

import pytest
from helpers import until
from test_iteration_agent_stages import setup_mixed
from test_iteration_early_repair import resource_hold


def saved_jobs(store, eid):
    return store.rows(
        "SELECT stage_id, intent_id, job_id, state, "
        "json_extract(execution_manifest, '$.executor') AS executor FROM jobs "
        "WHERE experiment_id=? ORDER BY created_at",
        (eid,),
    )


def saved_refs(store, eid):
    return store.rows(
        "SELECT r.id, r.intent_id, r.name, r.path FROM output_refs r "
        "JOIN jobs j ON j.intent_id = r.intent_id WHERE j.experiment_id=?",
        (eid,),
    )


def identities(jobs, refs):
    return (
        {(job["intent_id"], job["job_id"], job["state"]) for job in jobs},
        {(ref["id"], ref["intent_id"], ref["name"], ref["path"]) for ref in refs},
    )


async def test_initial_mixed_agent_plan_fits_a_saved_three_job_budget(runtime):
    _, _, _, old = runtime
    old.config.slurm.max_jobs_per_experiment = 3
    store, scheduler, agent, wf, eid = await setup_mixed(runtime)
    exp = store.experiment(eid)
    review = exp["plan_review"]
    # The owner's saved budget counts the three scheduled stages, not the check.
    assert exp["contract"]["execution_contract"] == 3 and exp["contract"].get("max_jobs") is None
    assert review["state"] == "approved" and review["schema_version"] == 3
    assert [stage["executor"] for stage in review["stages"]] == ["agent", "slurm", "slurm", "slurm"]
    # Fail fast on a rejected preparation instead of waiting out the whole window.
    await until(wf, store, eid, lambda e: e["phase"] in ("complete", "failed"))
    exp = store.experiment(eid)
    assert exp["phase"] == "complete", exp["error"]
    assert scheduler.submissions == 3
    executors = [job["executor"] for job in saved_jobs(store, eid)]
    assert len(executors) == 4 and executors.count("agent") == 1
    assert agent.check_calls == 1
    assert agent.calls == 4  # plan, prepare, model check, interpret; no correction turns
    await wf.close()


@pytest.mark.parametrize("cap", [4, 3])
async def test_resource_repair_counts_only_the_one_new_slurm_job(runtime, cap):
    store, scheduler, agent, wf, eid = await resource_hold(runtime)
    jobs = saved_jobs(store, eid)
    before = identities(jobs, saved_refs(store, eid))
    originals = {job["intent_id"] for job in jobs}
    assert len(jobs) == 4 and before[1]
    # Queueing revision 3 needs room for its one new job; the ceiling applies from then on.
    wf.config.slurm.max_jobs_per_experiment = 4
    wf.last_preflight.clear()
    scheduler.preflight = AsyncMock()
    await wf.repair_failed_experiments()
    wf.config.slurm.max_jobs_per_experiment = cap
    await until(wf, store, eid, lambda e: e["phase"] in ("complete", "failed"))
    exp = store.experiment(eid)
    after = saved_jobs(store, eid)
    saved = identities(after, saved_refs(store, eid))
    # Reused workers and the completed agent turn keep their exact identities.
    assert before[0] <= saved[0] and before[1] <= saved[1]
    new = [job for job in after if job["intent_id"] not in originals]
    if cap == 3:
        assert exp["phase"] == "failed" and exp["hold_reason"] == "agent_correction_stalled"
        assert "remaining stages exceed the total saved job budget" in exp["error"]
        assert new == [] and scheduler.submissions == 3 and scheduler.preflight.await_count == 0
        # A denied preparation must leave every saved job and output reference untouched.
        assert saved == before
    else:
        assert exp["phase"] == "complete", exp["error"]
        assert [(job["stage_id"], job["state"]) for job in new] == [("plot", "complete")]
        assert len(after) == 5 and [job["executor"] for job in after].count("agent") == 1
        assert (scheduler.submissions, scheduler.preflight.await_count) == (4, 1)
        assert agent.check_calls == 1
        assert set(exp["contract"]["reused_job_intents"]) == {
            job["intent_id"] for job in jobs if job["stage_id"] != "plot"
        }
        assert exp["evidence_revision"] == 3 and exp["contract"]["repair_count"] == 1
    await wf.close()
