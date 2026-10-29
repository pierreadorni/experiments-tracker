"""Recover saved terminal output without spending turns or duplicating compute."""

import copy
import json
import sys
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_dag import PLAN as DAG_PLAN
from test_generic import PLAN, ImplementationAgent
from test_workflow import until

from experimenttracker.models import Implementation, ResearchPlan
from experimenttracker.workflow import Workflow


def research_plan():
    return {
        "schema_version": 2,
        "title": "A compute preparation stage",
        "summary": "Prepare features then evaluate.",
        "protocol": DAG_PLAN["protocol"],
        "stages": [
            {
                "id": "prepare",
                "needs": [],
                "title": "Features",
                "summary": "Prepare features.",
                "resources": DAG_PLAN["stages"][0]["resources"],
            },
            {
                "id": "compute_prepare",
                "needs": ["prepare"],
                "title": "Results",
                "summary": "Check outputs.",
                "resources": DAG_PLAN["stages"][0]["resources"],
            },
        ],
    }


def test_plan_namespaces_executor_names_and_edges_without_mutating_raw_result():
    raw = research_plan()
    original = copy.deepcopy(raw)
    plan = ResearchPlan.model_validate(raw)
    assert raw == original
    assert [s.id for s in plan.stages] == ["compute_prepare_1", "compute_prepare"]
    assert plan.stages[-1].needs == ["compute_prepare_1"]
    duplicate = copy.deepcopy(raw)
    duplicate["stages"][-1]["id"] = "prepare"
    with pytest.raises(ValidationError, match="unique"):
        ResearchPlan.model_validate(duplicate)


async def saved_plan_failure(runtime):
    store, scheduler, _, old = runtime
    agent = ImplementationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "plan_review")
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=? AND phase='plan'", (eid,))
    value = research_plan()
    agent.results[(action["thread_id"], action["turn_id"])] = value
    workflow._action_state(
        action["id"], "failed", result=value, error="agent output validation: reserved stage name"
    )
    store.update(
        eid,
        phase="failed",
        hold_reason="agent_recovery_exhausted",
        plan_review={"state": "drafting", "revision": 1},
    )
    return store, scheduler, agent, workflow, eid, action


async def test_revalidate_exact_plan_is_idempotent_and_survives_restart(runtime):
    store, scheduler, agent, workflow, eid, action = await saved_plan_failure(runtime)
    calls = agent.calls
    await workflow.reconcile_agent_output(eid)
    await workflow.reconcile_agent_output(eid)
    await workflow.close()
    workflow = Workflow(workflow.config, store, scheduler, agent)
    await until(workflow, store, eid, lambda e: e["hold_reason"] == "plan_review")
    assert agent.calls == calls and scheduler.submissions == 0
    saved = store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))
    assert saved["attempts"] == action["attempts"] and saved["turn_id"] == action["turn_id"]
    assert json.loads(saved["result"]) == research_plan()
    assert store.experiment(eid)["plan_review"]["stages"][0]["id"] == "compute_prepare_1"
    assert len(store.rows("SELECT * FROM events WHERE kind='agent_output_reconciled'")) == 1
    await workflow.close()


@pytest.mark.parametrize("block", ["uncertain", "mismatch", "nonterminal", "steering"])
async def test_output_recovery_refuses_unestablished_outcomes(runtime, block):
    store, _, agent, workflow, eid, action = await saved_plan_failure(runtime)
    if block == "uncertain":
        workflow._action_state(action["id"], "uncertain")
    elif block == "mismatch":
        agent.results[(action["thread_id"], action["turn_id"])] = {
            **research_plan(),
            "title": "Different turn result",
        }
    elif block == "nonterminal":
        agent.read_turn = AsyncMock(side_effect=ValueError("turn is still running"))
    else:
        store.command(uuid4(), "steer", {"text": "Change the experiment", "mode": "restart"}, eid=eid)
    with pytest.raises(ValueError):
        await workflow.reconcile_agent_output(eid)
    assert store.experiment(eid)["phase"] == "failed"
    await workflow.close()


def test_repair_can_reuse_only_exact_saved_runtime_from_its_own_experiment(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = store.command(
        uuid4(), "create", {"brief": "Test runtime reuse.", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    base = workflow.projects.base_root(eid)
    python = base / "repairs/2/work/runtime-env/bin/python"
    python.parent.mkdir(parents=True)
    python.symlink_to(sys.executable)
    exp = store.experiment(eid)
    exp["evidence_revision"] = 3
    exp["contract"]["auto_repair"] = {"snapshot": {"python_executable": str(python)}}
    plan = Implementation.model_validate({**PLAN, "python_executable": str(python)})
    workflow.project_context = lambda exp: {"checkout": str(tmp_path / "isolated-checkout")}
    assert workflow.prepared_python(exp, plan) == python
    other = tmp_path / "other-experiment/bin/python"
    other.parent.mkdir(parents=True)
    other.symlink_to(sys.executable)
    exp["contract"]["auto_repair"]["snapshot"]["python_executable"] = str(other)
    with pytest.raises(ValueError, match="must belong"):
        workflow.prepared_python(
            exp, Implementation.model_validate({**PLAN, "python_executable": str(other)})
        )


def test_success_cleanup_keeps_the_runtime_used_by_current_results(runtime):
    store, _, _, workflow = runtime
    eid = store.command(
        uuid4(), "create", {"brief": "Test runtime retention.", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    base = workflow.projects.base_root(eid)
    python = base / "work/runtime-env/bin/python"
    python.parent.mkdir(parents=True)
    python.symlink_to(sys.executable)
    (base / "snapshot").mkdir()
    (base / "snapshot/old-source.py").write_text("# Failed source\n")
    exp = store.experiment(eid)
    exp["contract"]["auto_repair"] = {
        "revision": 2,
        "previous_revision": 1,
        "snapshot": {"expected_outputs": {}},
        "jobs": [],
    }
    store.db.execute(
        "UPDATE experiments SET phase='complete',evidence_revision=2,contract=?,snapshot=? WHERE id=?",
        (json.dumps(exp["contract"]), json.dumps({"python_executable": str(python)}), eid),
    )
    workflow.discard_failed_implementation()
    workflow.discard_failed_implementation()
    assert python.is_file()
    assert (base / "snapshot").exists()
    assert not store.experiment(eid)["contract"]["auto_repair"].get("discarded")


def test_clarification_cannot_masquerade_as_executable_plan():
    raw = research_plan()
    with pytest.raises(ValidationError, match="no compute stages"):
        ResearchPlan.model_validate({**raw, "questions": ["Which dataset?"]})
    for questions in ([], ["  "]):
        with pytest.raises(ValidationError):
            ResearchPlan.model_validate({**raw, "stages": [], "questions": questions})
    assert ResearchPlan.model_validate({**raw, "stages": [], "questions": ["Which dataset?"]}).questions
