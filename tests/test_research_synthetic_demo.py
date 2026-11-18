"""A bounded C4 synthetic source-gate using the real research dispatcher."""

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import pytest
from test_research_create import create, git_project
from test_research_dispatch import ScriptedAgent, research_setup, run_pass, scripted
from test_research_follow_up import fake_clock
from test_research_job_dispatch import FakeScheduler
from test_research_write_tools import document

from experimenttracker.research.dispatch import ResearchDispatch
from experimenttracker.research.session import follow_up_turn
from experimenttracker.research.settlement import complete


@pytest.mark.asyncio
async def test_contract4_synthetic_dispatch_saves_owner_approval(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    _, eid, _, action = research_setup(runtime, tmp_path, monkeypatch)
    objective = "Compare two synthetic inputs with one fixed split."
    approval_id = str(uuid4())

    class ApprovalAgent(ScriptedAgent):
        async def finish(self, thread_id, turn_id):
            self.calls.append(("finish", thread_id, turn_id))
            receipt = await workflow.controller.call(
                eid,
                "ask_owner",
                {
                    "request_id": approval_id,
                    "question": "Approve the synthetic comparison?",
                    "kind": "approval",
                    "objective": objective,
                },
            )
            assert receipt["request_id"] == approval_id
            return {
                "status": "needs_owner",
                "summary": "Waiting for explicit approval",
                "next": "Resume after approval",
            }

    now = fake_clock(monkeypatch)
    agent = scripted(workflow, ApprovalAgent())
    dispatcher = ResearchDispatch(
        workflow,
        lambda saved, result: complete(workflow, saved, result),
    )
    await run_pass(workflow, dispatcher)

    saved = store.one("SELECT state,thread_id,turn_id FROM agent_actions WHERE id=?", (action["id"],))
    assert saved == {"state": "completed", "thread_id": "thread-1", "turn_id": "turn-1"}
    pending = store.experiment(eid)["pending_request"]
    assert pending["id"] == approval_id and pending["params"]["objective"] == objective
    assert agent.calls[0][0] == "begin" and agent.calls[-1][0] == "finish"

    answer_id = str(uuid4())
    store.command(
        answer_id,
        "answer",
        {
            "request_id": answer_id,
            "schema_version": 1,
            "pending_request_id": approval_id,
            "answer": "accept",
        },
        eid=eid,
    )
    assert follow_up_turn(store, eid) is None
    now.advance(60)
    continuation = follow_up_turn(store, eid)
    decision = json.loads(continuation["input"].split("\nOWNER APPROVAL DECISION\n", 1)[1].splitlines()[0])
    assert decision == {
        "approval_request_id": approval_id,
        "objective": objective,
        "answer": "accept",
    }


@pytest.mark.asyncio
async def test_contract4_synthetic_gate_runs_dispatcher_to_done(runtime, config, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    _, _, receipt = create(store)
    eid = receipt["experiment_id"]
    workflow.workspaces.ensure_branch(eid)
    approval_id, job_request_id = str(uuid4()), str(uuid4())
    objective = "Compare two synthetic inputs with one fixed split."
    now = fake_clock(monkeypatch)

    class ScenarioAgent(ScriptedAgent):
        async def finish(self, thread_id, turn_id):
            self.calls.append(("finish", thread_id, turn_id))
            turn = len([call for call in self.calls if call[0] == "finish"])
            if turn == 1:
                receipt = await workflow.controller.call(
                    eid,
                    "ask_owner",
                    {
                        "request_id": approval_id,
                        "question": "Approve the synthetic comparison?",
                        "kind": "approval",
                        "objective": objective,
                    },
                )
                assert receipt["request_id"] == approval_id
                return {"status": "needs_owner", "summary": "Approval needed", "next": "Wait"}
            if turn == 2:
                job = await workflow.controller.call(
                    eid,
                    "submit_job",
                    {
                        "request_id": job_request_id,
                        "approval_request_id": approval_id,
                        "objective": objective,
                        "name": "synthetic-cpu",
                        "command": ["python", "comparison.py"],
                        "resources": {"cpus_per_job": 1},
                    },
                )
                assert job["state"] == "submitted"
                return {"status": "wait", "summary": "CPU job submitted", "next": "Observe"}
            if turn == 3:
                job = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
                await asyncio.to_thread(
                    Path(job["execution_root"], "results.json").write_text,
                    json.dumps(
                        {
                            "schema_version": 1,
                            "protocol": "synthetic",
                            "metrics": [{"name": "mse", "variant": "toy", "value": 0.25, "unit": "fraction"}],
                        }
                    ),
                )
                result = await workflow.controller.call(
                    eid,
                    "register_result",
                    {
                        "request_id": str(uuid4()),
                        "intent_id": job["intent_id"],
                        "path": "results.json",
                    },
                )
                assert result["intent_id"] == job["intent_id"]
                report_document = document()
                report_document["blocks"][1]["rows"][0][1]["metric"]["variant"] = "toy"
                report = await workflow.controller.call(
                    eid, "update_report", {"request_id": str(uuid4()), "document": report_document}
                )
                assert report["state"] == "queued"
                return {"status": "wait", "summary": "Metrics registered", "next": "timer:60"}
            return {
                "status": "done",
                "summary": "Synthetic report complete",
                "next": "None",
                "integrate": {"value": False, "reason": "Synthetic scenario"},
            }

    agent = scripted(workflow, ScenarioAgent())
    scheduler = FakeScheduler(workflow, False)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)

    async def dispatch_pass():
        await workflow.prepare_research_turns()
        await run_pass(workflow, workflow.research_dispatch)

    await dispatch_pass()
    assert store.experiment(eid)["pending_request"]["id"] == approval_id
    answer_id = str(uuid4())
    store.command(
        answer_id,
        "answer",
        {
            "request_id": answer_id,
            "schema_version": 1,
            "pending_request_id": approval_id,
            "answer": "accept",
        },
        eid=eid,
    )
    assert follow_up_turn(store, eid) is None
    now.advance(60)
    await dispatch_pass()
    assert scheduler.submissions == 1
    await workflow.observe_jobs()
    job = store.one("SELECT intent_id,state FROM jobs WHERE experiment_id=?", (eid,))
    assert (job["state"], scheduler.submissions) == ("complete", 1)
    assert follow_up_turn(store, eid) is None
    now.advance(60)
    assert follow_up_turn(store, eid) is not None
    await run_pass(workflow, workflow.research_dispatch)
    assert store.one("SELECT state FROM report_versions WHERE experiment_id=?", (eid,))["state"] == "queued"
    await workflow.advance_reports()
    assert store.one("SELECT state FROM report_versions WHERE experiment_id=?", (eid,))["state"] == "ready"
    now.advance(60)
    await dispatch_pass()

    assert len([call for call in agent.calls if call[0] == "finish"]) == 4
    assert store.experiment(eid)["phase"] == "complete"
    assert store.one("SELECT state FROM report_versions WHERE experiment_id=?", (eid,))["state"] == "ready"
    assert (
        len(
            store.rows(
                "SELECT * FROM events WHERE experiment_id=? AND kind='research_result_registered'", (eid,)
            )
        )
        == 1
    )
    assert scheduler.submissions == 1
