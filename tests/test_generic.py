"""The planner supplies methods and graph; the executor never selects a model family."""

import asyncio
import json
from uuid import uuid4

import pytest
from pydantic import ValidationError

from experimenttracker import artifacts
from experimenttracker.audit import verify_experiment
from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import JobResources
from experimenttracker.models import (
    MAX_METRICS,
    MAX_VALIDATION_CHECKS,
    CreateExperiment,
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


async def approve_plan(workflow, store, eid):
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.001)
        review = store.experiment(eid)["plan_review"]
        if review and review["state"] == "awaiting_review":
            store.command(
                uuid4(), "plan_review", {"decision": "approve", "plan_revision": review["revision"]}, eid=eid
            )
            return
    pytest.fail("plan review was not reached")


async def test_generic_dynamic_graph_restart_and_evidence(runtime):
    store, scheduler, _, old = runtime
    agent = ImplementationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    payload = CreateExperiment(
        request_id=uuid4(), brief="Test an entirely new method and show its frontier."
    ).model_dump(mode="json")
    eid = store.command(payload["request_id"], "create", payload)["experiment_id"]
    assert store.command(payload["request_id"], "create", payload)["experiment_id"] == eid
    await approve_plan(workflow, store, eid)
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["snapshot"]:
            break
    assert store.experiment(eid)["snapshot"]
    await workflow.close()
    workflow = Workflow(old.config, store, scheduler, agent)
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    assert store.experiment(eid)["phase"] == "complete", store.experiment(eid)
    assert scheduler.submissions == 2 and agent.calls == 3
    assert [r["id"] for r in store.projection(eid)["stages"]] == [
        "plan",
        "prepare",
        "z_measure",
        "a_verify",
        "validate",
        "interpret",
        "publish",
    ]
    assert verify_experiment(store, eid, workflow.root(eid))["artifacts_verified"] == 13
    for _ in range(3):
        await workflow.tick(force_observe=True)
    assert scheduler.submissions == 2 and agent.calls == 3
    root = workflow.root(eid)
    result = json.loads((root / "results.json").read_text())
    result["metrics"][0]["value"] = float("inf")
    (root / "results.json").write_text(json.dumps(result))
    with pytest.raises(ValueError, match="non-finite"):
        artifacts.validate_stage(root, "a_verify", store.experiment(eid)["snapshot"])
    await workflow.close()


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


async def test_detailed_outputs_reconcile_saved_job_without_retraining(runtime, monkeypatch):
    store, scheduler, _, old = runtime
    source = SOURCE.replace(
        "'metrics':[{'name':'measurement','variant':'requested method','value':x,'unit':''}]",
        "'metrics':[{'name':'measurement','variant':'requested method' if i == 0 else f'run {i}','value':x,'unit':''} for i in range(1238)]",
    ).replace(
        "'checks':[{'name':'independent recomputation','passed':True,'detail':'sum agrees with product'}]",
        "'checks':[{'name':f'check {i}','passed':True,'detail':'sum agrees with product'} for i in range(112)]",
    )
    monkeypatch.setitem(globals(), "SOURCE", source)
    validate = artifacts.validate_stage

    def old_validator(root, stage, manifest, **kwargs):
        names = validate(root, stage, manifest, **kwargs)
        if stage == "a_verify":
            raise ValueError("legacy metrics limit exceeded")
        return names

    monkeypatch.setattr(artifacts, "validate_stage", old_validator)
    agent = ImplementationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    for _ in range(200):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] == "failed":
            break
    assert store.experiment(eid)["hold_reason"] == "validation_failed"
    assert scheduler.submissions == 2 and agent.calls == 2
    root = workflow.root(eid)
    original = (root / "results.json").read_bytes()
    monkeypatch.setattr(artifacts, "validate_stage", validate)
    result = await workflow.reconcile_outputs(eid)
    assert result == await workflow.reconcile_outputs(eid)
    assert scheduler.submissions == 2 and agent.calls == 2
    await workflow.close()
    workflow = Workflow(old.config, store, scheduler, agent)
    store.command(uuid4(), "action", {"action": "resume_new_compute"}, eid=eid)
    for _ in range(200):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] in ("complete", "failed"):
            break
    assert store.experiment(eid)["phase"] == "complete", store.experiment(eid)["error"]
    assert scheduler.submissions == 2 and agent.calls == 3
    assert (root / "results.json").read_bytes() == original
    assert len(json.loads(original)["metrics"]) == 1238
    assert verify_experiment(store, eid, root)["artifacts_verified"] == 13
    await workflow.close()


async def test_invalid_prepared_source_fails_bounded_repair_before_compute(runtime, monkeypatch):
    store, scheduler, _, old = runtime
    monkeypatch.setitem(globals(), "SOURCE", "def broken(:\n")
    agent = ImplementationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    for _ in range(100):
        await workflow.tick(force_observe=True)
        await asyncio.sleep(0.01)
        if store.experiment(eid)["phase"] == "failed":
            break
    assert store.experiment(eid)["phase"] == "failed"
    assert "does not compile" in store.experiment(eid)["error"]
    assert scheduler.submissions == 0 and agent.calls == 4
    assert store.experiment(eid)["snapshot"] is None
    await workflow.close()
