"""Synthetic selective repair state/race tests; no live Slurm acceptance."""

import asyncio
import json
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from helpers import IterationAgent, approve_plan, seed_saved_contract3
from test_iteration import prepared
from test_iteration_agent_stages import MixedAgent, setup_mixed
from test_workflow import until

from experimenttracker import early_repair
from experimenttracker.config import ResourceLimits
from experimenttracker.models import Implementation
from experimenttracker.slurm import Observation, ResourceUnavailable
from experimenttracker.workflow import Workflow

pytestmark = pytest.mark.usefixtures("contract_three_engine")


async def failed(runtime):
    store, scheduler, agent, wf, eid = await prepared(runtime, True)
    await until(wf, store, eid, lambda e: e["phase"] == "failed")
    wf.config.mode = "live"
    wf.cluster_snapshot = {"fixture": True}
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    scheduler.preflight = AsyncMock()
    return store, scheduler, agent, wf, eid


def candidate(exp, *, train="keep"):
    return Implementation.model_validate(
        {
            **exp["snapshot"]["plan"],
            "reuse_stages": ["evaluate"],
            "reuse_rationale": "Retain successful metrics.",
            "repair_decisions": [
                {"stage_id": "train", "decision": train, "rationale": "Unaffected running branch."},
                {
                    "stage_id": "evaluate",
                    "decision": "keep",
                    "rationale": "Successful direct metric producer.",
                },
                {"stage_id": "plot", "decision": "retry", "rationale": "Fix failed figure only."},
            ],
        }
    )


async def test_early_candidate_preserves_running_attempt_and_selective_admission(runtime):
    store, scheduler, _, wf, eid = await failed(runtime)
    old = store.experiment(eid)
    train = store.one("SELECT * FROM jobs WHERE stage_id='train'")
    original_source = Path(train["execution_root"]) / "snapshot/recipe.py"
    original_bytes = original_source.read_bytes()
    old_work = wf.working_directory(old)
    store.db.execute(
        "UPDATE jobs SET state='submitted',scheduler_state='RUNNING' WHERE intent_id=?", (train["intent_id"],)
    )
    store.stage(eid, "train", "running")
    # Early diagnosis must not wait on this unaffected scheduler identity.
    scheduler.reconcile = AsyncMock(side_effect=AssertionError("diagnosis waited for sibling"))
    await wf.repair_failed_experiments()
    exp = store.experiment(eid)
    assert early_repair.preparing(exp)
    wf.config.slurm.max_repairs_per_experiment = 1
    wf.fail(eid, "compute_failed", "A second branch failed during diagnosis")
    assert store.experiment(eid)["phase"] == "preparing"
    assert store.experiment(eid)["hold_reason"] == "automatic_repair"
    assert exp["snapshot"] == old["snapshot"]
    prompt = json.loads(wf.prompt(exp, "prepare").split("\nPAYLOAD:\n", 1)[1])
    assert isinstance(prompt["output_refs"], dict) and "path" in prompt["output_refs"]
    assert all("outputs" not in job and "logs" not in job for job in prompt["jobs"])
    refs = json.loads(await asyncio.to_thread(Path(prompt["output_refs"]["path"]).read_text))
    assert refs and refs[0]["intent_id"]
    assert wf.controller.status(eid, intent_ids={train["intent_id"]})[0]["outputs"]
    assert store.one("SELECT state FROM stages WHERE id='train'")["state"] == "running"
    assert wf.working_directory(exp) != old_work
    (wf.working_directory(exp) / "recipe.py").write_text("# candidate correction\n")
    assert original_source.read_bytes() == original_bytes
    assert (old_work / "recipe.py").read_bytes() != b"# candidate correction\n"
    await wf.repair_failed_experiments()
    assert store.experiment(eid)["evidence_revision"] == exp["evidence_revision"]
    plan = candidate(exp)
    early_repair.validate(wf, exp, plan)
    manifest = {**deepcopy(exp["snapshot"]), "plan": plan.model_dump()}
    early_repair.apply(wf, exp, plan, manifest)
    assert store.one("SELECT state FROM stages WHERE id='evaluate'")["state"] == "complete"
    assert early_repair.submission_hold(wf, store.experiment(eid), "train")["reason"] == "predecessor_pending"
    assert early_repair.selected(wf, store.experiment(eid), train)
    assert store.one("SELECT state FROM stages WHERE id='plot'")["state"] == "pending"
    await wf.close()


async def test_retry_old_completion_cannot_replace_selected_producer(runtime):
    store, _, _, wf, eid = await failed(runtime)
    train = store.one("SELECT * FROM jobs WHERE stage_id='train'")
    store.db.execute(
        "UPDATE jobs SET state='submitted',scheduler_state='RUNNING',exit_code=NULL WHERE intent_id=?",
        (train["intent_id"],),
    )
    await wf.repair_failed_experiments()
    exp = store.experiment(eid)
    plan = candidate(exp, train="retry")
    early_repair.apply(wf, exp, plan, {**exp["snapshot"], "plan": plan.model_dump()})
    assert not early_repair.selected(wf, store.experiment(eid), train)
    assert early_repair.active_predecessor(wf, store.experiment(eid), "train")
    store.db.execute(
        "UPDATE jobs SET state='failed',scheduler_state='RUNNING' WHERE intent_id=?", (train["intent_id"],)
    )
    assert early_repair.active_predecessor(wf, store.experiment(eid), "train")
    store.db.execute(
        "UPDATE jobs SET state='complete',scheduler_state='COMPLETED',exit_code='0:0' WHERE intent_id=?",
        (train["intent_id"],),
    )
    assert not early_repair.active_predecessor(wf, store.experiment(eid), "train")
    assert store.one("SELECT state FROM stages WHERE id='train'")["state"] == "pending"
    await wf.close()


async def test_cancel_intent_replay_racing_success_and_unknown_accounting(runtime):
    store, scheduler, _, wf, eid = await failed(runtime)
    job = store.one("SELECT * FROM jobs WHERE stage_id='train'")
    await wf.repair_failed_experiments()
    request = str(uuid4())
    scheduler.reconcile = AsyncMock(return_value=job["job_id"])
    scheduler.observe = AsyncMock(return_value=Observation("RUNNING", None))
    scheduler.cancel = AsyncMock(side_effect=RuntimeError("lost cancellation response"))
    first = await early_repair.cancel(
        wf, store.experiment(eid), job["intent_id"], request, "Affected source bug"
    )
    assert first["state"] == "uncertain"
    again = await early_repair.cancel(
        wf, store.experiment(eid), job["intent_id"], request, "Affected source bug"
    )
    assert again == first and scheduler.cancel.await_count == 1
    with pytest.raises(ValueError, match="another decision"):
        await early_repair.cancel(wf, store.experiment(eid), job["intent_id"], request, "Different reason")
    # Restart drops in-memory throttling, but absent accounting is not terminal.
    wf.last_cancellation.clear()
    scheduler.observe.return_value = Observation("ACCOUNTING_PENDING", None)
    await early_repair.reconcile_cancellations(wf)
    assert store.experiment(eid)["contract"]["job_cancellations"][request]["state"] == "uncertain"
    wf.last_cancellation.clear()
    scheduler.observe.return_value = Observation("COMPLETED", "0:0")
    await early_repair.reconcile_cancellations(wf)
    assert store.experiment(eid)["contract"]["job_cancellations"][request]["state"] == "terminal"
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "complete"
    assert scheduler.cancel.await_count == 1
    await wf.close()


async def test_uncertain_submission_and_agent_turn_cannot_be_retried(runtime):
    store, _, _, wf, eid = await failed(runtime)
    train = store.one("SELECT * FROM jobs WHERE stage_id='train'")
    await wf.repair_failed_experiments()
    exp = store.experiment(eid)
    for state in ("submitting", "agent"):
        store.db.execute("UPDATE jobs SET state=? WHERE intent_id=?", (state, train["intent_id"]))
        with pytest.raises(ValueError, match="reconcile exact"):
            early_repair.validate(wf, exp, candidate(exp, train="retry"))
    await wf.close()


async def test_repair_dispatch_uses_only_candidate_workspaces_and_preserves_pause(runtime):
    store, _, agent, wf, eid = await failed(runtime)
    original = wf.working_directory(store.experiment(eid))
    await wf.repair_failed_experiments()
    exp = store.experiment(eid)
    project = Path(exp["contract"]["repair_project"])
    work = wf.working_directory(exp)
    calls = []
    session = agent.session
    begin = agent.begin

    async def checked_session(thread_id, workspace, **kwargs):
        assert workspace == project
        calls.append(workspace)
        return await session(thread_id, workspace, **kwargs)

    async def checked_begin(thread_id, prompt, schema, **kwargs):
        assert kwargs["workspace"] == work
        assert kwargs["project_workspace"] == project
        assert original not in kwargs.get("additional_writable_roots", [])
        return await begin(thread_id, prompt, schema, **kwargs)

    agent.session, agent.begin = checked_session, checked_begin
    store.update(eid, pause_agent=True)
    await wf.agent_stage(store.experiment(eid), "prepare")
    assert not calls
    store.update(eid, pause_agent=False)
    agent.fail_plot = False
    wf.config.mode = "demo"
    # Only assert the preparation boundary, before interpretation uses analysis.
    await until(wf, store, eid, lambda e: e["contract"]["early_repair"]["state"] == "applied")
    assert calls
    await wf.close()


async def test_public_repair_projection_keeps_decisions_without_local_diagnostic_paths(runtime):
    store, _, _, wf, eid = await failed(runtime)
    await wf.repair_failed_experiments()
    from experimenttracker.store import public_projection

    exp = store.experiment(eid)
    raw = store.projection(eid)
    assert exp["contract"]["early_repair"]["original_work"]
    projected = public_projection(raw)
    assert projected["contract"]["early_repair"]["state"] == "preparing"
    assert "original_work" not in projected["contract"]["early_repair"]
    assert "repair_project" not in projected["contract"]
    queued = next(e for e in projected["events"] if e["kind"] == "selective_repair_queued")
    assert "original_work" not in queued["payload"]
    assert store.experiment(eid)["contract"]["repair_project"]
    await wf.close()


async def test_completed_repair_action_replays_after_apply_before_readiness(runtime, monkeypatch):
    store, _, agent, wf, eid = await failed(runtime)
    await wf.repair_failed_experiments()
    agent.fail_plot = False
    wf.config.mode = "demo"
    original_apply = early_repair.apply

    class SyntheticCrash(BaseException):
        pass

    def interrupted_apply(*args):
        original_apply(*args)
        raise SyntheticCrash("synthetic crash after repair apply")

    monkeypatch.setattr(early_repair, "apply", interrupted_apply)
    with pytest.raises(SyntheticCrash, match="synthetic crash"):
        await until(wf, store, eid, lambda e: False)
    action = store.one("SELECT * FROM agent_actions WHERE phase='prepare' ORDER BY rowid DESC LIMIT 1")
    assert action["state"] == "completed"
    before = store.experiment(eid)
    assert before["contract"]["early_repair"]["state"] == "applied"
    assert store.one("SELECT state FROM stages WHERE id='prepare'")["state"] != "complete"
    monkeypatch.setattr(early_repair, "apply", original_apply)
    calls = agent.calls
    await wf.agent_stage(before, "prepare")
    assert agent.calls == calls
    assert store.experiment(eid)["snapshot"] == before["snapshot"]
    assert store.one("SELECT state FROM stages WHERE id='prepare'")["state"] == "complete"
    assert store.one("SELECT state FROM stages WHERE id='evaluate'")["state"] == "complete"
    assert store.one("SELECT state FROM stages WHERE id='plot'")["state"] == "pending"
    await wf.close()


class EarlyRepairAgent(MixedAgent):
    """Repair reply that keeps the completed interactive check and measured producers."""

    async def begin(self, thread_id, prompt, schema, **kwargs):
        turn = await super().begin(thread_id, prompt, schema, **kwargs)
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload["phase"] == "prepare" and payload.get("repair_context"):
            value = self.results[(thread_id, turn)]
            value["repair_decisions"] = [
                {
                    "stage_id": "model_check",
                    "decision": "keep",
                    "rationale": "The interactive check completed with its corrected source.",
                },
                *value["repair_decisions"],
            ]
            value["reuse_stages"] = ["model_check", "train", "evaluate"]
        return turn


def saved_repair_state(store, scheduler, agent, eid):
    exp = store.experiment(eid)
    return {
        "phase": exp["phase"],
        "hold": exp["hold_reason"],
        "revision": exp["evidence_revision"],
        "repair_count": exp["contract"].get("repair_count"),
        "auto_repair": (exp["contract"].get("auto_repair") or {}).get("revision"),
        "jobs": sorted(
            (job["intent_id"], job["job_id"], job["state"], job["stage_id"])
            for job in store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
        ),
        "actions": sorted(
            (action["id"], action["state"])
            for action in store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))
        ),
        "events": [
            event["kind"]
            for event in store.rows("SELECT kind FROM events WHERE experiment_id=? ORDER BY seq", (eid,))
        ],
        "submissions": scheduler.submissions,
        "calls": agent.calls,
    }


async def resource_hold(runtime):
    """Mixed graph whose plot failed, repaired once, then held by a refused plot request."""
    store, scheduler, agent, wf, eid = await setup_mixed(runtime, EarlyRepairAgent())
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    agent.fail_plot = True
    await until(wf, store, eid, lambda e: e["hold_reason"] == "compute_failed")
    assert store.experiment(eid)["phase"] == "failed"
    assert "early_repair" not in store.experiment(eid)["contract"]
    # The repaired source is valid; only the scheduler refusal must hold the new plot intent.
    agent.fail_plot = False
    wf.config.mode = "live"
    wf.cluster_snapshot = {"fixture": True}
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    scheduler.preflight = AsyncMock(side_effect=ResourceUnavailable("requested nodes are drained"))
    await until(wf, store, eid, lambda e: e["hold_reason"] == "resources_unavailable")
    exp = store.experiment(eid)
    jobs = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    agent_jobs = [
        job for job in jobs if json.loads(job["execution_manifest"] or "{}").get("executor") == "agent"
    ]
    scheduler_jobs = [job for job in jobs if job not in agent_jobs]
    assert exp["phase"] == "preparing" and exp["evidence_revision"] == 2
    assert exp["contract"]["repair_count"] == 1
    assert exp["contract"]["early_repair"]["state"] == "applied"
    assert len(agent_jobs) == 1 and agent_jobs[0]["job_id"] is None and agent_jobs[0]["state"] == "complete"
    assert len(scheduler_jobs) == 3 and all(job["job_id"] for job in scheduler_jobs)
    assert [job["stage_id"] for job in scheduler_jobs if job["state"] == "failed"] == ["plot"]
    assert scheduler.submissions == 3
    # The repair reached the scheduler boundary and created no job row there.
    assert (eid, 2, "plot") in wf.last_preflight
    return store, scheduler, agent, wf, eid


RESOURCE_GATES = [
    "settled",
    "demo",
    "steering_later",
    "uncertain_prepare",
    "submitting_without_identity",
    "wrong_reconcile_identity",
    "accounting_pending",
    "failed_without_exit_code",
    "job_budget_three",
    "agent_row_unsettled",
    "preflight_backoff",
    "pending_request",
]


@pytest.mark.parametrize("case", RESOURCE_GATES)
async def test_resource_hold_repair_requires_one_exact_settled_gate(runtime, case):
    store, scheduler, agent, wf, eid = await resource_hold(runtime)
    jobs = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    agent_job = next(
        job for job in jobs if json.loads(job["execution_manifest"] or "{}").get("executor") == "agent"
    )
    plot = next(job for job in jobs if job["stage_id"] == "plot")
    wf.config.slurm.max_jobs_per_experiment = 3 if case == "job_budget_three" else 4
    if case != "preflight_backoff":
        wf.last_preflight.clear()
    if case == "demo":
        wf.config.mode = "demo"
    elif case == "steering_later":
        store.command(uuid4(), "steer", {"text": "Widen the synthetic figure.", "mode": "later"}, eid=eid)
    elif case == "uncertain_prepare":
        prepare = store.one(
            "SELECT * FROM agent_actions WHERE experiment_id=? AND phase='prepare' ORDER BY rowid DESC LIMIT 1",
            (eid,),
        )
        # Save the real turn identity and result; only the state becomes uncertain.
        assert prepare["state"] == "completed" and prepare["turn_id"] and prepare["result"]
        store.db.execute("UPDATE agent_actions SET state='uncertain' WHERE id=?", (prepare["id"],))
    elif case == "submitting_without_identity":
        store.db.execute(
            "UPDATE jobs SET state='submitting',job_id=NULL WHERE intent_id=?", (plot["intent_id"],)
        )
    elif case == "wrong_reconcile_identity":
        scheduler.reconcile = AsyncMock(return_value=f"demo-{uuid4()}")
    elif case == "accounting_pending":
        scheduler.observe = AsyncMock(return_value=Observation("ACCOUNTING_PENDING", None))
    elif case == "failed_without_exit_code":
        scheduler.observe = AsyncMock(return_value=Observation("FAILED", None))
    elif case == "agent_row_unsettled":
        store.db.execute("UPDATE jobs SET state='agent' WHERE intent_id=?", (agent_job["intent_id"],))
    elif case == "pending_request":
        store.update(
            eid,
            pending_request={"id": str(uuid4()), "text": "Synthetic question?", "expired": False},
        )
    before = saved_repair_state(store, scheduler, agent, eid)
    assert before["phase"] in ("failed", "preparing", "training", "evaluating")
    preflight = scheduler.preflight
    if case != "settled":
        await wf.repair_failed_experiments()
        assert saved_repair_state(store, scheduler, agent, eid) == before
        assert preflight.await_count == 1
        await wf.close()
        return
    reconciles = AsyncMock(wraps=scheduler.reconcile)
    observations = AsyncMock(wraps=scheduler.observe)
    scheduler.reconcile, scheduler.observe = reconciles, observations
    await wf.repair_failed_experiments()
    slurm = store.rows(
        "SELECT * FROM jobs WHERE experiment_id=? AND json_extract(execution_manifest, '$.executor') IS NOT 'agent'",
        (eid,),
    )
    assert len(slurm) == 3
    assert {call.args[0] for call in reconciles.await_args_list} == {job["intent_id"] for job in slurm}
    assert {call.args[0] for call in observations.await_args_list} == {job["job_id"] for job in slurm}
    assert agent_job["intent_id"] not in {call.args[0] for call in reconciles.await_args_list}
    exp = store.experiment(eid)
    assert exp["phase"] == "preparing" and exp["hold_reason"] == "automatic_repair"
    assert exp["evidence_revision"] == 3
    assert exp["contract"]["repair_count"] == 1
    assert exp["contract"]["auto_repair"]["kind"] == "resources"
    assert exp["contract"]["auto_repair"]["revision"] == 3
    after = saved_repair_state(store, scheduler, agent, eid)
    assert len(after["jobs"]) == 4
    assert len(after["events"]) == len(before["events"]) + 2
    assert after["events"][: len(before["events"])] == before["events"]
    assert after["events"][-2:] == ["automatic_repair_started", "automatic_repair_queued"]
    assert after["jobs"] == before["jobs"] and after["actions"] == before["actions"]
    assert after["submissions"] == before["submissions"] and after["calls"] == before["calls"]
    assert preflight.await_count == 1
    # The queued revision is already persisted; repeating the direct call is a no-op.
    await wf.repair_failed_experiments()
    assert saved_repair_state(store, scheduler, agent, eid) == after
    await wf.close()


async def test_repair_cap_keeps_one_manual_retry_and_the_original_producers(runtime):
    store, scheduler, agent, wf, eid = await failed(runtime)
    wf.config.slurm.max_repairs_per_experiment = 3
    train = store.one("SELECT * FROM jobs WHERE stage_id='train'")
    evaluate = store.one("SELECT * FROM jobs WHERE stage_id='evaluate'")
    producers = {
        ref["name"]: ref
        for ref in store.rows(
            "SELECT * FROM output_refs WHERE intent_id IN (?,?)", (train["intent_id"], evaluate["intent_id"])
        )
    }
    assert {"model.json", "metrics.json"} <= set(producers)
    await until(
        wf,
        store,
        eid,
        lambda e: e["contract"].get("repair_count") == 3 and e["phase"] == "failed" and not wf.agent_task,
    )
    exp = store.experiment(eid)
    assert exp["hold_reason"] == "compute_failed"
    assert not store.rows("SELECT * FROM agent_actions WHERE state IN ('running','dispatching','uncertain')")
    assert (scheduler.submissions, agent.calls) == (6, 5)
    plot_attempts = [
        job["attempt"] for job in store.rows("SELECT * FROM jobs WHERE stage_id='plot' ORDER BY attempt")
    ]
    assert plot_attempts == [1, 2, 3, 4]
    capped = saved_repair_state(store, scheduler, agent, eid)
    # The saved repair count survives a restart under the same configured owner ceiling.
    await wf.close()
    restarted = Workflow(wf.config, store, scheduler, agent)
    for _ in range(3):
        await restarted.tick(force_observe=True)
    assert saved_repair_state(store, scheduler, agent, eid) == capped
    # One explicit owner request adds exactly one manual repair; the duplicate is the same command.
    request = uuid4()
    first = store.command(request, "action", {"action": "retry_agent"}, eid=eid)
    assert store.command(request, "action", {"action": "retry_agent"}, eid=eid) == first
    exp = store.experiment(eid)
    assert exp["contract"]["manual_repair_requests"] == 1
    assert exp["phase"] == "preparing" and exp["hold_reason"] == "automatic_repair_pending"
    agent.fail_plot = False
    await until(restarted, store, eid, lambda e: e["phase"] == "complete")
    exp = store.experiment(eid)
    assert exp["contract"]["repair_count"] == 4
    assert exp["contract"]["manual_repair_requests"] == 1
    assert not restarted.repair_available(exp)
    assert (scheduler.submissions, agent.calls) == (7, 7)
    assert (
        len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=? AND phase='prepare'", (eid,))) == 5
    )
    assert len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=? AND phase='plan'", (eid,))) == 1
    assert (
        len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=? AND phase='interpret'", (eid,)))
        == 1
    )
    jobs = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert len(jobs) == 7
    assert sorted(job["attempt"] for job in jobs if job["stage_id"] == "plot") == [1, 2, 3, 4, 5]
    for saved, stage in ((train, "train"), (evaluate, "evaluate")):
        row = store.one("SELECT * FROM jobs WHERE experiment_id=? AND stage_id=?", (eid, stage))
        assert (row["intent_id"], row["attempt"], row["state"]) == (saved["intent_id"], 1, "complete")
    retained = {
        ref["name"]: ref
        for ref in store.rows(
            "SELECT * FROM output_refs WHERE intent_id IN (?,?)", (train["intent_id"], evaluate["intent_id"])
        )
    }
    assert retained == producers
    for ref in retained.values():
        assert await asyncio.to_thread(Path(ref["path"]).exists)
    assert exp["plan_review"]["state"] == "approved" and exp["plan_review"]["revision"] == 1
    assert len(store.rows("SELECT * FROM commands WHERE experiment_id=? AND kind='plan_review'", (eid,))) == 1
    await restarted.close()


class MemoryAmendmentAgent(IterationAgent):
    """Initial preparation keeps 1024 MiB; the resource repair reruns every stage with 2048 MiB."""

    async def begin(self, thread_id, prompt, schema, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        repairing = payload["phase"] == "prepare" and bool(payload.get("repair_context"))
        turn = await super().begin(thread_id, prompt, schema, **kwargs)
        if repairing:
            value = self.results[(thread_id, turn)]
            # The helper replies with the shared PLAN stages; amend a private copy only.
            value["stages"] = deepcopy(value["stages"])
            for stage in value["stages"]:
                stage["resources"] = {**stage["resources"], "memory_mb_per_job": 2048}
            value["reuse_stages"] = []
            value["reuse_rationale"] = "No stage ran under the refused admission; rerun the same work."
            value["repair_decisions"] = [
                {
                    "stage_id": stage["id"],
                    "decision": "retry",
                    "rationale": "Nothing ran under the refused admission; retry the same work.",
                }
                for stage in value["stages"]
            ]
        return turn


async def test_resource_amendment_raises_memory_without_a_second_approval(runtime):
    store, scheduler, _, old = runtime
    # Replacing the limits (and the Slurm config) keeps the shared defaults untouched.
    config = old.config.model_copy(
        update={
            "mode": "live",
            "slurm": old.config.slurm.model_copy(update={"limits": ResourceLimits(memory_mb_per_job=4096)}),
        }
    )
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    requests = []

    async def preflight(intent, script, root, resources):
        requests.append(resources.memory_mb_per_job)
        if resources.memory_mb_per_job != 2048:
            raise ResourceUnavailable("1024 MiB per job is not currently available")

    scheduler.preflight = preflight
    agent = MemoryAmendmentAgent()
    wf = Workflow(config, store, scheduler, agent)
    wf.cluster_snapshot = {"fixture": True}
    eid = seed_saved_contract3(store)
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_plan(wf, store, eid)
    await until(wf, store, eid, lambda e: e["hold_reason"] == "resources_unavailable")
    exp = store.experiment(eid)
    assert exp["phase"] == "preparing" and exp["evidence_revision"] == 1
    assert exp["error"] == "1024 MiB per job is not currently available"
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert requests == [1024]
    saved_plan = exp["snapshot"]["plan"]
    wf.last_preflight.clear()
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    exp = store.experiment(eid)
    # The refused admission ran no stage, so every stage reruns under the amended request.
    assert requests == [1024, 2048, 2048, 2048]
    assert exp["contract"]["repair_count"] == 0
    assert exp["contract"]["auto_repair"]["kind"] == "resources"
    assert exp["contract"]["auto_repair"]["revision"] == 2
    jobs = store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    assert sorted(job["stage_id"] for job in jobs) == ["evaluate", "plot", "train"]
    assert {job["attempt"] for job in jobs} == {2}
    assert all(json.loads(job["resources"])["memory_mb_per_job"] == 2048 for job in jobs)
    assert scheduler.submissions == 3
    assert agent.calls == 4
    assert (
        len(store.rows("SELECT * FROM events WHERE experiment_id=? AND kind='submission_intent'", (eid,)))
        == 3
    )
    started = store.rows(
        "SELECT * FROM events WHERE experiment_id=? AND kind='automatic_repair_started'", (eid,)
    )
    assert len(started) == 1
    previous = Path(exp["contract"]["auto_repair"]["previous_root"])
    assert (previous / "snapshot/recipe.py").exists()
    amended = exp["snapshot"]["plan"]
    for key in ("protocol", "parameters_json", "figures", "files", "summary", "synthetic_demo"):
        assert amended[key] == saved_plan[key]
    assert [
        (stage["id"], stage["needs"], stage["outputs"], stage["sample_count"], stage["expected_metrics"])
        for stage in amended["stages"]
    ] == [
        (stage["id"], stage["needs"], stage["outputs"], stage["sample_count"], stage["expected_metrics"])
        for stage in saved_plan["stages"]
    ]
    assert all(stage["resources"]["memory_mb_per_job"] == 2048 for stage in amended["stages"])
    assert all(decision["decision"] == "retry" for decision in amended["repair_decisions"])
    assert len(amended["repair_decisions"]) == 3
    assert amended["reuse_stages"] == [] and amended["reuse_rationale"]
    assert exp["plan_review"]["state"] == "approved" and exp["plan_review"]["revision"] == 1
    assert len(store.rows("SELECT * FROM commands WHERE experiment_id=? AND kind='plan_review'", (eid,))) == 1
    assert not store.rows("SELECT * FROM events WHERE experiment_id=? AND kind='validation_started'", (eid,))
    await wf.close()
