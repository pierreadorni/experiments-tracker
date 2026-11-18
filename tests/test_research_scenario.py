"""T4.6 synthetic contract-4 end-to-end acceptance through saved backend seams."""

import json
from pathlib import Path
from uuid import uuid4

import pytest
from test_research_create import create, git_project
from test_research_dispatch import ScriptedAgent, run_pass, scripted
from test_research_follow_up import fake_clock
from test_research_job_dispatch import FakeScheduler, accept
from test_research_job_observe import FakeScheduler as ObserveScheduler
from test_research_job_observe import saved_job
from test_research_write_tools import add_research_turn, document

from experimenttracker.research.session import first_turn, follow_up_turn
from experimenttracker.research.settlement import complete
from experimenttracker.slurm import SubmissionUncertain
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow


@pytest.mark.asyncio
async def test_contract4_synthetic_owner_job_result_report_done(runtime, config, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, receipt = create(store)
    eid = receipt["experiment_id"]
    workflow.workspaces.ensure_branch(eid)
    action = add_research_turn(store, eid)
    approval_id, request_id = str(uuid4()), str(uuid4())
    with store.tx() as db:
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id='thread',turn_id='turn' WHERE id=?",
            (action["id"],),
        )
        db.execute("UPDATE experiments SET thread_id='thread' WHERE id=?", (eid,))
    action = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
    request = {
        "request_id": request_id,
        "approval_request_id": approval_id,
        "objective": "Compare synthetic inputs",
        "name": "fit",
        "command": ["python", "comparison.py"],
        "resources": {"cpus_per_job": 2},
    }
    pool = workflow.agent_pool

    def claim(action):
        pool.claim(
            role="stage",
            source_table="agent_actions",
            row=action,
            owner=("experiment", eid),
            slot=pool.primary,
        )

    def activate(action, turn_id):
        with store.tx() as db:
            db.execute(
                "UPDATE agent_actions SET state='running', turn_id=? WHERE id=?", (turn_id, action["id"])
            )
        saved = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
        claim(saved)
        return saved

    now = fake_clock(monkeypatch)
    claim(action)
    await workflow.controller.call(
        eid,
        "ask_owner",
        {
            "request_id": approval_id,
            "question": "Approve this comparison?",
            "kind": "approval",
            "objective": request["objective"],
        },
    )
    assert (
        await complete(
            workflow, action, {"status": "needs_owner", "summary": "Approval needed", "next": "Compare"}
        )
    )["state"] == "completed"
    pool.settle_row("agent_actions", action["id"])
    accept(store, eid, request)
    assert follow_up_turn(store, eid) is None
    now.advance(60)
    action = activate(follow_up_turn(store, eid), "execution-turn")
    scheduler = FakeScheduler(workflow, True)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)
    with pytest.raises(SubmissionUncertain):
        await workflow.controller.call(eid, "submit_job", request)
    assert (await workflow.controller.call(eid, "submit_job", request))["state"] == "submitting"
    assert scheduler.submissions == 1
    assert (
        await complete(workflow, action, {"status": "wait", "summary": "Job pending", "next": "Observe job"})
    )["state"] == "completed"
    pool.settle_row("agent_actions", action["id"])
    await workflow.observe_jobs()
    assert scheduler.submissions == 1
    job = store.one("SELECT intent_id,job_id,state,execution_root FROM jobs WHERE experiment_id=?", (eid,))
    assert (job["job_id"], job["state"]) == ("42", "complete")
    assert follow_up_turn(store, eid) is None
    now.advance(60)
    action = activate(follow_up_turn(store, eid), "result-turn")
    root = Path(job["execution_root"])
    (root / "results.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "protocol": "synthetic",
                "metrics": [{"name": "mse", "variant": "toy", "value": 0.25, "unit": "fraction"}],
            }
        )
    )
    result = await workflow.controller.call(
        eid,
        "register_result",
        {"request_id": str(uuid4()), "intent_id": job["intent_id"], "path": "results.json"},
    )
    assert result["intent_id"] == job["intent_id"]
    report_document = document()
    report_document["blocks"][1]["rows"][0][1]["metric"]["variant"] = "toy"
    report = await workflow.controller.call(
        eid, "update_report", {"request_id": str(uuid4()), "document": report_document}
    )
    assert report["state"] == "queued"
    await workflow.advance_reports()
    saved_report = store.one("SELECT state,error FROM report_versions WHERE experiment_id=?", (eid,))
    assert saved_report["state"] == "ready", saved_report
    settled = await complete(
        workflow,
        action,
        {
            "status": "done",
            "summary": "Synthetic report complete",
            "next": "None",
            "integrate": {"value": False, "reason": "Synthetic scenario"},
        },
    )
    assert settled["state"] == "completed", settled
    assert store.experiment(eid)["phase"] == "complete"
    assert follow_up_turn(store, eid) is None


@pytest.mark.asyncio
async def test_contract4_pause_survives_workflow_restart_and_resume(runtime, config, tmp_path):
    store, scheduler, agent, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, receipt = create(store)
    eid = receipt["experiment_id"]
    action = first_turn(store, eid)

    store.command(str(uuid4()), "action", {"action": "pause_agent"}, eid=eid)
    await workflow.prepare_research_turns()
    await workflow.research_dispatch.advance()
    assert store.experiment(eid)["pause_agent"] is True
    assert agent.calls == 0

    await workflow.close()
    store.close()
    reopened = Store(config.state_dir / "workflow.sqlite")
    restarted = Workflow(config, reopened, scheduler, agent)
    try:
        assert reopened.experiment(eid)["pause_agent"] is True
        reopened.command(str(uuid4()), "action", {"action": "resume_agent"}, eid=eid)
        await restarted.prepare_research_turns()
        assert reopened.experiment(eid)["pause_agent"] is False
        assert (
            reopened.one("SELECT state FROM agent_actions WHERE id=?", (action["id"],))["state"] == "pending"
        )
        scripted_agent = scripted(
            restarted,
            ScriptedAgent(result={"status": "wait", "summary": "paused then resumed", "next": "timer:60"}),
        )
        await run_pass(restarted, restarted.research_dispatch)
        saved = reopened.one("SELECT state,thread_id,turn_id FROM agent_actions WHERE id=?", (action["id"],))
        assert saved == {"state": "completed", "thread_id": "thread-1", "turn_id": "turn-1"}
        assert [call[0] for call in scripted_agent.calls] == ["begin", "finish"]
    finally:
        await restarted.close()
        reopened.close()


@pytest.mark.asyncio
async def test_contract4_healthy_job_scenario_observes_without_inference(runtime):
    store, scheduler, agent, workflow = runtime
    exp, job = saved_job(store)
    observer = ObserveScheduler(
        type("Running", (), {"state": "RUNNING", "exit_code": None, "ended_at": None})()
    )
    workflow.scheduler_for = lambda *_: observer

    await workflow.observe_jobs()

    assert observer.calls == [job["job_id"]]
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "submitted"
    assert store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],)) == []
    assert agent.calls == 0 and scheduler.submissions == 0


@pytest.mark.asyncio
async def test_contract4_experiments_edit_same_source_file_in_isolated_branches(runtime, config, tmp_path):
    from experimenttracker.workspaces import git

    store, _, _, workflow = runtime
    project, _ = git_project(config, store, tmp_path, research_loop=True)
    source = project.checkout
    (source / "shared.py").write_text("value = 0\n")
    git(source, "add", "shared.py")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "baseline")
    experiment_ids = [create(store)[2]["experiment_id"] for _ in range(2)]
    for eid in experiment_ids:
        workflow.workspaces.ensure_branch(eid)
    branches = [workflow.workspaces.branch(eid) for eid in experiment_ids]

    for branch, value in zip(branches, (1, 2), strict=True):
        worktree = Path(branch["worktree"])
        (worktree / "shared.py").write_text(f"value = {value}\n")
        git(worktree, "add", "shared.py")
        git(
            worktree,
            "-c",
            "user.name=Test",
            "-c",
            "user.email=test@localhost",
            "commit",
            "-m",
            f"experiment {value}",
        )

    assert (source / "shared.py").read_text() == "value = 0\n"
    assert [(Path(branch["worktree"]) / "shared.py").read_text() for branch in branches] == [
        "value = 1\n",
        "value = 2\n",
    ]
    assert branches[0]["branch"] != branches[1]["branch"]
