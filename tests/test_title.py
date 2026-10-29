import asyncio
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_generic import ImplementationAgent
from test_steering import wait_for

from experimenttracker.store import Conflict
from experimenttracker.title import TitleSummary, luna_title_model, title_prompt
from experimenttracker.workflow import Workflow


def test_luna_selection_requires_advertised_low_effort():
    catalog = [
        {"id": "gpt-6.1-sol", "name": "GPT-6.1 Sol", "reasoning_efforts": ["low", "medium"]},
        {"id": "gpt-6-luna", "name": "GPT-6 Luna", "reasoning_efforts": ["low", "medium"]},
    ]
    assert luna_title_model(catalog) == ("gpt-6-luna", "low")
    assert luna_title_model(catalog[:1]) is None
    assert luna_title_model([{**catalog[1], "reasoning_efforts": ["medium"]}]) is None


def test_title_output_is_bounded_and_prompt_includes_brief():
    assert TitleSummary.model_validate({"title": "DINOv3 decoder baseline on SpaceNet 7"}).title
    with pytest.raises(ValidationError):
        TitleSummary.model_validate({"title": "A" * 101})
    prompt = title_prompt("Frozen DINOv3 at scale 1", "anyup-rs", "Segmentation baseline")
    assert "Frozen DINOv3 at scale 1" in prompt
    assert "avoid adding claims about results" in prompt


async def test_saved_luna_turn_is_reconciled_after_plan_approval(runtime):
    store, scheduler, _, old = runtime

    class TitleAgent(ImplementationAgent):
        models = [{"id": "gpt-6-luna", "name": "GPT-6 Luna", "reasoning_efforts": ["low"]}]

        async def session(self, thread_id, workspace, *, writable=False, model=None):
            self.chosen = model
            return await super().session(thread_id, workspace, writable=writable)

        async def begin(self, thread, prompt, schema, *, workspace=None, effort=None, **kwargs):
            if "Plan title:" in prompt:
                assert self.chosen == "gpt-6-luna" and effort == "low"
                self.calls += 1
                turn = f"title-{self.calls}"
                self.results[(thread, turn)] = {"title": "Concise managed title"}
                self.title_turn = turn
                return turn
            return await super().begin(thread, prompt, schema, workspace=workspace)

        async def finish(self, thread, turn):
            if turn == getattr(self, "title_turn", None):
                await asyncio.Event().wait()
            return await super().finish(thread, turn)

    agent = TitleAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await wait_for(
        workflow,
        store,
        eid,
        lambda exp: bool(
            store.one(
                "SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase='title' AND state='running'",
                (eid,),
            )
        ),
    )
    assert scheduler.submissions == 0
    store.command(uuid4(), "plan_review", {"decision": "approve", "plan_revision": 1}, eid=eid)
    await workflow.close()
    calls = agent.calls
    restored = Workflow(old.config, store, scheduler, agent)
    await wait_for(restored, store, eid, lambda exp: exp["title"] == "Concise managed title")
    assert store.one("SELECT state FROM agent_actions WHERE phase='title'")["state"] == "completed"
    assert agent.calls <= calls + 1  # preparation may start; the saved title is not dispatched again
    await restored.close()


async def test_manual_summary_updates_a_failed_legacy_run_without_restarting_it(runtime):
    store, scheduler, _, old = runtime

    class SummaryAgent(ImplementationAgent):
        models = [{"id": "gpt-6-luna", "name": "GPT-6 Luna", "reasoning_efforts": ["low"]}]

        async def session(self, thread_id, workspace, *, writable=False, model=None):
            assert not writable and model == "gpt-6-luna"
            return await super().session(thread_id, workspace, writable=writable)

        async def begin(self, thread, prompt, schema, *, effort=None, **kwargs):
            assert effort == "low" and "Plan title:" in prompt
            self.calls += 1
            turn = f"summary-{self.calls}"
            self.results[(thread, turn)] = {"title": f"DINOv3 baseline {self.calls}"}
            return turn

    agent = SummaryAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(), "create", {"brief": "Frozen DINOv3 baseline", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    store.update(eid, phase="failed", error="Existing pipeline failure")
    identity = str(uuid4())
    payload = {"action": "summarize_title"}
    assert store.command(identity, "action", payload, eid=eid) == store.command(
        identity, "action", payload, eid=eid
    )
    with pytest.raises(Conflict, match="title summary is already pending"):
        store.command(uuid4(), "action", payload, eid=eid)
    await wait_for(workflow, store, eid, lambda e: e["title"] == "DINOv3 baseline 1")
    store.command(uuid4(), "action", payload, eid=eid)
    await wait_for(workflow, store, eid, lambda e: e["title"] == "DINOv3 baseline 2")
    for _ in range(5):
        await workflow.tick()
    exp = store.experiment(eid)
    assert exp["phase"] == "failed" and exp["error"] == "Existing pipeline failure"
    assert exp["title"] == "DINOv3 baseline 2"
    assert exp["plan_review"] is None and exp["snapshot"] is None
    assert scheduler.submissions == 0 and agent.calls == 2
    assert not (workflow.root(eid) / "work/recipe.py").exists()
    await workflow.close()
