"""The final interpretation opts in to integration when the run completes (T3.4).

The saved analysis carries ``integrate`` and its reason. Only a named branch is
eligible, and only the existing Store ``integrate`` command queues it, inside the
transaction that marks the run complete. Missing or false decisions, the Don't
merge flag and a request that is already saved queue nothing; the main checkout
and Git stay untouched. Replaying the boundary reuses the same deterministic
request ID.
"""

import json
from uuid import uuid4

import pytest
from helpers import IterationAgent, approve_plan, until
from pydantic import ValidationError
from test_branch_workspaces import create, git_project

from experimenttracker.engine import integration_queue
from experimenttracker.models import Analysis
from experimenttracker.workflow import Workflow
from experimenttracker.workspaces import git


def saved_analysis(**extra):
    return {
        "title": "Synthetic result",
        "summary": "Synthetic 42.",
        "observations": [{"text": "Synthetic 42.", "evidence_ids": ["evidence"]}],
        "limitations": ["Synthetic infrastructure test."],
        "next_ideas": [],
        "scientific_outcome": "inconclusive",
        **extra,
    }


class OptInAgent(IterationAgent):
    """Return the usual synthetic analysis with the requested integration decision."""

    def __init__(self, decision):
        super().__init__()
        self.decision = decision

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
        if payload["phase"] == "interpret":
            self.results[(thread_id, turn)] |= self.decision
        return turn


def integration_rows(store, eid):
    rows = store.rows("SELECT * FROM obligations WHERE kind='integration' AND experiment_id=?", (eid,))
    return [(row, json.loads(row["detail"])) for row in rows]


def integrate_commands(store):
    return store.rows("SELECT * FROM commands WHERE kind='integrate'")


async def completed(runtime, agent, tmp_path=None):
    """Drive one synthetic experiment to completion; the caller closes the workflow."""
    store, scheduler, _, old = runtime
    source = git_project(old, store, tmp_path) if tmp_path else None
    workflow = Workflow(old.config, store, scheduler, agent)
    payload = {
        "brief": "Test an entirely new method and show its frontier.",
        "recipe_id": "agent_experiment_v1",
    }
    if source:
        payload["project_id"] = "anyup-rs"
    eid = store.command(uuid4(), "create", payload)["experiment_id"]
    await approve_plan(workflow, store, eid)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    return workflow, store, eid, source


async def test_completed_named_branch_queues_the_explicit_opt_in_once(runtime, tmp_path):
    workflow, store, eid, source = await completed(
        runtime,
        OptInAgent({"integrate": True, "integrate_reason": "Keep the synthetic improvement."}),
        tmp_path,
    )
    try:
        branch = store.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (eid,))
        entries = integration_rows(store, eid)
        assert len(entries) == 1
        row, detail = entries[0]
        assert (detail["stage"], detail["request"]["kind"]) == ("queued", "merge")
        assert detail["request"]["reason"] == "Keep the synthetic improvement."
        assert detail["request"]["branch"] == branch["branch"]
        command = integrate_commands(store)[0]
        assert (command["experiment_id"], command["origin"]) == (eid, "agent")
        assert json.loads(command["payload"]) == {"reason": "Keep the synthetic improvement."}
        # The queued request keeps the command's caller identity, and this boundary
        # adds no second command and moves no Git ref.
        assert row["id"].startswith("integration:" + command["request_id"] + ":")
        assert store.experiment(eid)["analysis"]["integrate"] is True
        assert branch["state"] == "active"
        assert git(source, "rev-parse", "HEAD").decode().strip() == branch["base_commit"]
        assert git(source, "status", "--porcelain", "-z") == b""

        # While its request is open, replaying the completion boundary adds nothing.
        assert store.request_completion_integration(eid) is None
        assert len(integration_rows(store, eid)) == 1
        assert len(integrate_commands(store)) == 1

        # Once the request is terminal, the deterministic ID replays its command
        # instead of queueing the same completed interpretation twice.
        request_id = row["id"][len("integration:") :].rsplit(":", 1)[0]
        with store.tx() as db:
            integration_queue.finish(db, request_id, "abandoned")
        replay = store.request_completion_integration(eid)
        assert replay["state"] == "applied"
        assert [saved["state"] for saved, _ in integration_rows(store, eid)] == ["abandoned"]
        assert len(integrate_commands(store)) == 1
    finally:
        await workflow.close()


async def test_default_false_completion_queues_nothing(runtime, tmp_path):
    workflow, store, eid, _ = await completed(runtime, OptInAgent({}), tmp_path)
    try:
        assert integration_rows(store, eid) == []
        assert integrate_commands(store) == []
        assert store.experiment(eid)["analysis"]["integrate"] is False
    finally:
        await workflow.close()


async def test_dont_merge_flag_blocks_the_completion_request(runtime, tmp_path):
    store, scheduler, _, old = runtime
    git_project(old, store, tmp_path)
    workflow = Workflow(old.config, store, scheduler, OptInAgent({}))
    eid = create(store)
    store.command(uuid4(), "dont_merge", {"value": True}, eid=eid)
    try:
        await approve_plan(workflow, store, eid)
        await until(workflow, store, eid, lambda e: e["phase"] == "complete")
        assert integration_rows(store, eid) == []
        assert integrate_commands(store) == []
    finally:
        await workflow.close()


async def test_an_owner_request_is_not_duplicated_at_completion(runtime, tmp_path):
    store, scheduler, _, old = runtime
    git_project(old, store, tmp_path)
    agent = OptInAgent({"integrate": True, "integrate_reason": "Keep the synthetic improvement."})
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = create(store)
    assert await workflow.prepare_workspace(store.experiment(eid))
    store.command(uuid4(), "integrate", {"reason": "Owner requested this merge."}, eid=eid)
    try:
        await approve_plan(workflow, store, eid)
        await until(workflow, store, eid, lambda e: e["phase"] == "complete")
        entries = integration_rows(store, eid)
        assert [detail["request"].get("reason") for _, detail in entries] == ["Owner requested this merge."]
        assert [row["origin"] for row in integrate_commands(store)] == ["local"]
    finally:
        await workflow.close()


async def test_the_named_branch_interpretation_prompt_explains_the_opt_in(runtime, tmp_path):
    workflow, store, eid, _ = await completed(runtime, OptInAgent({}), tmp_path)
    try:
        prompt = workflow.prompt(store.experiment(eid), "interpret")
        assert "integrate:true" in prompt
        assert "only requests integration once this run completes" in prompt
        assert "Don't merge flag still settle whether it lands" in prompt
        assert "The default integrate:false requests nothing" in prompt
    finally:
        await workflow.close()


async def test_shared_checkout_completion_queues_nothing_and_offers_no_opt_in(runtime):
    workflow, store, eid, _ = await completed(
        runtime, OptInAgent({"integrate": True, "integrate_reason": "Keep the synthetic improvement."})
    )
    try:
        assert store.one("SELECT 1 FROM experiment_branches WHERE experiment_id=?", (eid,)) is None
        assert integration_rows(store, eid) == []
        assert integrate_commands(store) == []
        assert "integrate:true" not in workflow.prompt(store.experiment(eid), "interpret")
    finally:
        await workflow.close()


def test_old_saved_analyses_stay_valid_and_an_opt_in_needs_a_reason():
    saved = Analysis.model_validate(saved_analysis())
    assert (saved.integrate, saved.integrate_reason) == (False, None)
    assert Analysis.model_validate(saved_analysis(integrate=True, integrate_reason="Keep it.")).integrate
    with pytest.raises(ValidationError, match="needs a reason"):
        Analysis.model_validate(saved_analysis(integrate=True))
    with pytest.raises(ValidationError, match="needs a reason"):
        Analysis.model_validate(saved_analysis(integrate=True, integrate_reason="  "))
