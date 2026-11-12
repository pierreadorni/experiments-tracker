"""Synthetic selective repair state/race tests; no live Slurm acceptance."""

import asyncio
import json
from copy import deepcopy
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_iteration import prepared
from test_workflow import until

from experimenttracker import early_repair
from experimenttracker.models import Implementation
from experimenttracker.slurm import Observation


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
