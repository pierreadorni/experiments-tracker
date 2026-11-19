"""A bounded C4 synthetic source-gate using the real research dispatcher."""

import asyncio
import json
from pathlib import Path
from uuid import uuid4

import pytest
from test_iteration_autopilot import policy
from test_research_create import create, git_project
from test_research_dispatch import ScriptedAgent, research_setup, run_pass, scripted
from test_research_follow_up import fake_clock
from test_research_job_dispatch import FakeScheduler
from test_research_write_tools import document

from experimenttracker.autopilot import Autopilot
from experimenttracker.research.demo_fixture import result_bytes
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
    config.codex.max_sessions = 2
    _, _, receipt = create(store)
    eid = receipt["experiment_id"]
    workflow.workspaces.ensure_branch(eid)
    approval_id, job_request_id = str(uuid4()), str(uuid4())
    objective = "Compare two synthetic inputs with one fixed split."
    now = fake_clock(monkeypatch)
    project = Autopilot(store, config)
    project.command(
        "anyup-rs",
        {
            "request_id": str(uuid4()),
            "kind": "configure",
            "expected_revision": 0,
            "policy": policy(now.wall).model_dump(),
        },
    )
    project.command("anyup-rs", {"request_id": str(uuid4()), "kind": "activate", "expected_revision": 1})
    run = project.run("anyup-rs")
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
            (eid, run["id"], "anyup-rs", now.wall),
        )

    class ScenarioAgent(ScriptedAgent):
        async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
            self.turn_kind = "project" if "central_argument" in schema["properties"] else "research"
            if self.turn_kind == "research":
                self.research_turns = getattr(self, "research_turns", 0) + 1
                return await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
            self.calls.append(("begin_project", thread_id, prompt, Path(workspace)))
            return "project-turn"

        async def finish(self, thread_id, turn_id):
            self.calls.append(("finish", thread_id, turn_id))
            if self.turn_kind == "project":
                project_action = store.one(
                    "SELECT * FROM autopilot_actions WHERE run_id=? AND state='running' ORDER BY created_at DESC LIMIT 1",
                    (run["id"],),
                )
                approval = await workflow.research.call(
                    project_action["id"],
                    "approve_experiment_objective",
                    {
                        "request_id": str(uuid4()),
                        "expected_revision": project.run(run_id=run["id"])["revision"],
                        "experiment_id": eid,
                        "approval_request_id": approval_id,
                        "objective": objective,
                    },
                )
                assert approval["approval"]["objective"] == objective
                self.approval_action_id = project_action["id"]
                return {
                    "summary": "Approved exact synthetic objective",
                    "central_argument": "Bounded synthetic comparison",
                    "uncertainty": "Synthetic evidence only",
                    "outcome": "continue",
                }
            turn = self.research_turns
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
                store.update(eid, pause_compute=True)
                with pytest.raises(ValueError, match="paused"):
                    await workflow.controller.call(
                        eid,
                        "submit_job",
                        {
                            "request_id": str(uuid4()),
                            "approval_request_id": approval_id,
                            "objective": objective,
                            "name": "synthetic-cpu-held",
                            "command": ["python", "comparison.py"],
                            "resources": {"cpus_per_job": 1},
                        },
                    )
                store.update(eid, pause_compute=False)
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
                    Path(job["execution_root"], "results.json").write_bytes,
                    result_bytes(),
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
                report_document["blocks"][1]["rows"][0][1]["metric"]["variant"] = "linear"
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
    workflow.agent_ready = True
    workflow.gate.update(await agent.quota())
    agent.resolved_model = "scripted"
    agent.models = [{"id": "scripted", "reasoning_efforts": ["low"], "default_effort": "low"}]
    scheduler = FakeScheduler(workflow, False)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)

    async def dispatch_pass():
        await workflow.prepare_research_turns()
        await run_pass(workflow, workflow.research_dispatch)

    async def project_pass():
        await workflow.research.advance()
        assert workflow.agent_pool.turn_capacity() > 0, workflow.agent_pool.turn_debt()
        assert workflow.agent_pool.primary.agent_ready
        assert workflow.agent_pool.proposals, store.rows(
            "SELECT status,input_cursor,consumed_cursor FROM autopilot_runs WHERE id=?", (run["id"],)
        )
        assert workflow.agent_pool.owner_free(
            workflow.agent_pool.proposals[0].owner, workflow.agent_pool.proposals[0].key
        )
        assert workflow.agent_pool.free_connection(workflow.agent_pool.proposals[0]) is not None
        workflow.agent_pool.admit()
        task = workflow.research.task
        if task is not None:
            await task
        received = store.one("SELECT * FROM autopilot_actions WHERE run_id=?", (run["id"],))
        assert received["state"] == "received", received["error"]
        workflow.research.complete(received)

    await dispatch_pass()
    assert store.experiment(eid)["pending_request"]["id"] == approval_id
    await project_pass()
    pending = store.experiment(eid)["pending_request"]
    assert pending.get("answer") == "Approved by the project orchestrator.", (
        store.rows("SELECT state,error,input,result FROM autopilot_actions WHERE run_id=?", (run["id"],)),
        agent.calls,
    )
    assert pending["answer_source"] == {
        "kind": "orchestrator",
        "run_id": run["id"],
        "action_id": agent.approval_action_id,
    }
    assert store.one(
        "SELECT 1 FROM events WHERE experiment_id=? AND kind='autopilot_objective_approved' "
        "AND json_extract(payload,'$.action_id')=? AND json_extract(payload,'$.request_id')=?",
        (eid, agent.approval_action_id, approval_id),
    )
    assert (
        store.one(
            "SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase='research' AND state='running'",
            (eid,),
        )
        is None
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

    assert len([call for call in agent.calls if call[0] == "finish"]) == 5
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
