"""T4.6 synthetic contract-4 end-to-end acceptance through saved backend seams."""

import json
from pathlib import Path
from uuid import uuid4

import pytest
from test_research_create import create, git_project
from test_research_follow_up import fake_clock
from test_research_job_dispatch import FakeScheduler, accept
from test_research_write_tools import add_research_turn, document

from experimenttracker.research.session import follow_up_turn
from experimenttracker.research.settlement import complete
from experimenttracker.slurm import SubmissionUncertain


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
