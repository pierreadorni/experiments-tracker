from uuid import uuid4

import pytest
from helpers import seed_saved_contract3
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


@pytest.mark.usefixtures("contract_three_engine")
async def test_current3_manual_summary_updates_failed_run_without_restarting_it(runtime):
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
    eid = seed_saved_contract3(store, "Frozen DINOv3 baseline")
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
