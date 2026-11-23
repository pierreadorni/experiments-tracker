"""A bounded C4 synthetic source-gate using the real research dispatcher."""

import json
from pathlib import Path
from uuid import uuid4

import pytest
from test_iteration_autopilot import policy
from test_research_create import git_project
from test_research_dispatch import ScriptedAgent, research_setup, run_pass, scripted
from test_research_follow_up import fake_clock
from test_research_job_dispatch import FakeScheduler

from experimenttracker.autopilot import Autopilot
from experimenttracker.autopilot_policy import lead_id
from experimenttracker.controller import served_tools
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
async def test_lead_creates_member_approves_its_objective_and_member_submits_once(
    runtime, config, tmp_path, monkeypatch
):
    """T5.1c: lead creates a member, the member asks approval, the lead approves, one submit."""
    store, _, _, workflow = runtime
    git_project(config, store, tmp_path, research_loop=True)
    config.codex.max_sessions = 2
    objective = "Compare two synthetic inputs with one fixed split."
    approval_id, job_request_id = str(uuid4()), str(uuid4())
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
    lead = lead_id(run["id"])
    assert {"submit_job", "cancel_job", "register_result"}.isdisjoint(
        tool["name"] for tool in served_tools(True, lead=True)
    )
    state = {"member": None, "member_turns": 0, "receipts": []}

    class ScenarioAgent(ScriptedAgent):
        async def session(self, thread_id, workspace, *, writable=False, **kwargs):
            self.sessions.append((thread_id, Path(workspace), writable))
            return thread_id or f"thread-{len(self.sessions)}", "scripted"

        async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
            self.calls.append(("begin", thread_id, prompt, Path(workspace)))
            kind = "lead" if "\nPROJECT STATUS\n" in prompt else "member"
            return f"{kind}-{len(self.calls)}"

        async def finish(self, thread_id, turn_id):
            self.calls.append(("finish", thread_id, turn_id))
            wait = {"status": "wait", "summary": "Waiting", "next": "timer:60"}
            if turn_id.startswith("lead"):
                with pytest.raises(ValueError, match="only its project tools"):
                    await workflow.controller.call(lead, "submit_job", {"request_id": str(uuid4())})
                if state["member"] is None:
                    created = await workflow.controller.call(
                        lead,
                        "create_experiment",
                        {
                            "request_id": str(uuid4()),
                            "brief": "Synthetic software acceptance only; compare two inputs.",
                            "purpose": "exploration",
                            "estimated_finish_at": now.wall + 3600,
                        },
                    )
                    state["member"] = created["experiment_id"]
                    return wait
                status = await workflow.controller.call(lead, "project_status", {"request_id": str(uuid4())})
                for pending in status["pending_objectives"]:
                    arguments = {
                        "request_id": str(uuid4()),
                        "experiment_id": pending["experiment_id"],
                        "approval_request_id": pending["approval_request_id"],
                        "objective": pending["objective"],
                    }
                    receipt = await workflow.controller.call(lead, "approve_experiment_objective", arguments)
                    # A lost reply replays the saved receipt instead of approving twice.
                    replay = await workflow.controller.call(lead, "approve_experiment_objective", arguments)
                    assert replay == receipt
                    state["receipts"].append(receipt)
                return wait
            member = state["member"]
            state["member_turns"] += 1
            if state["member_turns"] == 1:
                await workflow.controller.call(
                    member,
                    "ask_owner",
                    {
                        "request_id": approval_id,
                        "question": "Approve the synthetic comparison?",
                        "kind": "approval",
                        "objective": objective,
                    },
                )
                return {"status": "needs_owner", "summary": "Approval needed", "next": "Wait"}
            request = {
                "request_id": job_request_id,
                "approval_request_id": approval_id,
                "objective": objective,
                "name": "synthetic-cpu",
                "command": ["python", "comparison.py"],
                "resources": {"cpus_per_job": 1},
            }
            job = await workflow.controller.call(member, "submit_job", request)
            assert await workflow.controller.call(member, "submit_job", request) == job
            return wait

    agent = scripted(workflow, ScenarioAgent())
    workflow.agent_ready = True
    workflow.gate.update(await agent.quota())
    scheduler = FakeScheduler(workflow, False)
    monkeypatch.setattr(workflow, "scheduler_for", lambda *_: scheduler)

    for _ in range(12):
        await workflow.prepare_research_turns()
        await run_pass(workflow, workflow.research_dispatch)
        if scheduler.submissions:
            break
        now.advance(60)
    assert scheduler.submissions == 1 and state["member_turns"] == 2
    member = state["member"]
    assert store.one("SELECT run_id,purpose FROM autopilot_members WHERE experiment_id=?", (member,)) == {
        "run_id": run["id"],
        "purpose": "exploration",
    }
    assert [receipt["approval"]["request_id"] for receipt in state["receipts"]] == [approval_id]
    answered = store.experiment(member)["pending_request"]
    assert answered is None or answered["answer_source"]["kind"] == "orchestrator"
    assert store.one("SELECT state FROM jobs WHERE experiment_id=?", (member,))["state"] == "submitted"
    assert not store.rows("SELECT 1 FROM jobs WHERE experiment_id=?", (lead,))
    # The old project-turn engine took no turn: the lead's tools are C4 research tools.
    assert not store.rows("SELECT 1 FROM autopilot_actions")
