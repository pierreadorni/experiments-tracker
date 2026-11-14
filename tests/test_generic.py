"""The planner supplies methods and graph; the executor never selects a model family."""

import json
from uuid import uuid4

import pytest
from helpers import IterationAgent, approve_plan, wait_for_experiment
from pydantic import ValidationError

from experimenttracker import artifacts
from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import JobResources
from experimenttracker.models import (
    MAX_METRICS,
    MAX_VALIDATION_CHECKS,
    ExperimentResult,
    ExperimentValidation,
    Implementation,
)
from experimenttracker.workflow import Workflow

PLAN = {
    "schema_version": 1,
    "summary": "Implement the requested method.",
    "protocol": "Measure a requested computation; independently verify its result.",
    "synthetic_demo": True,
    "parameters_json": '{"seed": 42}',
    "files": ["recipe.py"],
    "stages": [
        {
            "id": "z_measure",
            "title": "Requested measurement",
            "outputs": ["raw.json"],
            "resources": JobResources().model_dump(),
        },
        {
            "id": "a_verify",
            "title": "Independent checks",
            "outputs": ["results.json", "validation.json", "frontier.svg"],
            "resources": JobResources().model_dump(),
        },
    ],
    "figures": ["frontier.svg"],
}
SOURCE = """import argparse, json
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('stage'); p.add_argument('--output'); a=p.parse_args()
r=Path(a.output); r.mkdir(exist_ok=True, parents=True)
if a.stage == 'z_measure':
    (r/'raw.json').write_text(json.dumps({'value': 6*7}))
else:
    x=json.loads((r/'raw.json').read_text())['value']
    assert x == sum([7]*6)
    (r/'results.json').write_text(json.dumps({'schema_version':1,'protocol':PROTOCOL,'metrics':[{'name':'measurement','variant':'requested method','value':x,'unit':''}]}))
    (r/'validation.json').write_text(json.dumps({'schema_version':1,'passed':True,'checks':[{'name':'independent recomputation','passed':True,'detail':'sum agrees with product'}]}))
    (r/'frontier.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
""".replace("PROTOCOL", repr(PLAN["protocol"]))


class ImplementationAgent(DemoAgent):
    async def session(self, thread_id, workspace, *, writable=False):
        self.workspace = workspace
        self.writable = writable
        return await super().session(thread_id, workspace)

    async def begin(self, thread_id, prompt, schema, *, workspace=None):
        self.calls += 1
        turn = f"test-{self.calls}"
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload["phase"] == "plan":
            assert not self.writable
            result = {
                "schema_version": PLAN["schema_version"],
                "title": "Requested method frontier",
                "summary": "Measure the requested method and verify its output.",
                "protocol": PLAN["protocol"],
                "stages": [
                    {
                        "id": stage["id"],
                        "title": stage["title"],
                        "summary": "Run and record the requested computation.",
                        "resources": stage["resources"],
                        **({"needs": stage["needs"]} if "needs" in stage else {}),
                    }
                    for stage in PLAN["stages"]
                ],
            }
        elif payload["phase"] == "prepare":
            assert self.writable and workspace == self.workspace
            assert payload["brief"] == "Test an entirely new method and show its frontier."
            assert "there is no whitelist" in prompt
            assert (
                payload["output_schemas"]["results.json"]["properties"]["metrics"]["maxItems"] == MAX_METRICS
            )
            assert (
                payload["output_schemas"]["validation.json"]["properties"]["checks"]["maxItems"]
                == MAX_VALIDATION_CHECKS
            )
            (workspace / "recipe.py").write_text(SOURCE)
            result = PLAN
        else:
            # Live reporting now has a writable analysis workspace.
            result = {
                "schema_version": 1,
                "title": "Requested experiment",
                "summary": "Observed measurement 42.",
                "observations": [{"text": "Verified 42.", "evidence_ids": [payload["evidence_ids"][0]]}],
                "limitations": ["A test fixture, not real research."],
                "next_ideas": [],
                "scientific_outcome": "supported",
                "presentation": {
                    "title": "Requested experiment",
                    "standfirst": "Observed measurement 42.",
                    "blocks": [
                        {
                            "kind": "paragraph",
                            "text": "Measured outcome.",
                            "evidence_ids": [payload["evidence_ids"][0]],
                        },
                        {
                            "kind": "table",
                            "caption": "The checked measurement.",
                            "columns": ["Measurement"],
                            "rows": [[{"metric": {"name": "measurement", "variant": "requested method"}}]],
                        },
                    ],
                },
            }
        self.results[(thread_id, turn)] = result
        return turn


@pytest.mark.parametrize(
    "change",
    [
        {"files": ["../recipe.py"]},
        {"figures": ["undeclared.svg"]},
        {"stages": [{"id": "publish", "title": "Bad", "outputs": ["results.json", "validation.json"]}]},
        {"stages": [{"id": "fit", "title": "Missing validation", "outputs": ["results.json"]}]},
    ],
)
def test_invalid_plans_do_not_cross_compute_boundary(change):
    with pytest.raises(ValidationError):
        Implementation.model_validate({**PLAN, **change})


def test_json_exponent_overflow_rejected(tmp_path):
    path = tmp_path / "result.json"
    path.write_text('{"value":1e999}')
    with pytest.raises(ValueError, match="non-finite"):
        artifacts.read_json(path, tmp_path)


@pytest.mark.parametrize(
    ("model", "field", "limit", "item"),
    [
        (
            ExperimentResult,
            "metrics",
            MAX_METRICS,
            {"name": "IoU", "variant": "run", "value": 0.4, "unit": "fraction"},
        ),
        (
            ExperimentValidation,
            "checks",
            MAX_VALIDATION_CHECKS,
            {"name": "recomputed", "passed": True, "detail": "Matches retained predictions."},
        ),
    ],
)
def test_detailed_evidence_retains_bounded_lists(model, field, limit, item):
    payload = {"protocol": "Requested protocol"} if field == "metrics" else {"passed": True}
    assert len(getattr(model.model_validate({**payload, field: [item] * limit}), field)) == limit
    for items in ([], [item] * (limit + 1)):
        with pytest.raises(ValidationError):
            model.model_validate({**payload, field: items})


class BrokenSourceAgent(IterationAgent):
    """Keep the helper's valid preparation, then save a source that cannot compile."""

    async def session(self, thread_id, workspace, *, writable=False, **kwargs):
        self.writable = writable
        return await super().session(thread_id, workspace, **kwargs)

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
        if payload["phase"] == "plan":
            assert not self.writable
        elif payload["phase"] == "prepare":
            assert self.writable and workspace == self.workspace
            (workspace / "recipe.py").write_text("def broken(:\n")
        return turn


async def test_invalid_prepared_source_fails_bounded_repair_before_compute(runtime):
    store, scheduler, _, old = runtime
    agent = BrokenSourceAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Exercise synthetic iteration mechanisms.",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_plan(workflow, store, eid)
    await wait_for_experiment(workflow, store, eid, lambda e: e["phase"] == "failed")
    exp = store.experiment(eid)
    assert "does not compile" in exp["error"]
    assert exp["snapshot"] is None
    assert scheduler.submissions == 0 and agent.calls == 4
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    await workflow.close()
