"""Independent frozen stages submit together; joins wait for checked outputs."""

import asyncio
import json
from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_generic import ImplementationAgent, approve_plan
from test_workflow import until

from experimenttracker.config import JobResources
from experimenttracker.models import Implementation, ResearchPlan
from experimenttracker.slurm import Observation
from experimenttracker.workflow import Workflow

STAGES = [
    {"id": "features", "title": "Features", "needs": [], "outputs": ["features.json"]},
    {"id": "train_a", "title": "Train A", "needs": ["features"], "outputs": ["a.json"]},
    {"id": "train_b", "title": "Train B", "needs": ["features"], "outputs": ["b.json"]},
    {
        "id": "evaluate",
        "title": "Evaluate",
        "needs": ["train_a", "train_b"],
        "outputs": ["results.json", "validation.json"],
    },
]
PLAN = {
    "schema_version": 2,
    "summary": "Run independent training branches and join their evidence.",
    "protocol": "Measure two variants from checked shared features.",
    "synthetic_demo": True,
    "parameters_json": '{"seed": 42}',
    "files": ["recipe.py"],
    "stages": [{**stage, "resources": JobResources().model_dump()} for stage in STAGES],
    "figures": [],
}
SOURCE = """import argparse, json
from pathlib import Path
p=argparse.ArgumentParser(); p.add_argument('stage'); p.add_argument('--output'); a=p.parse_args()
r=Path(a.output); r.mkdir(exist_ok=True, parents=True)
if a.stage == 'features':
    (r/'features.json').write_text(json.dumps({'value': 21}))
elif a.stage in ('train_a', 'train_b'):
    value=json.loads((r/'features.json').read_text())['value']
    (r/('a.json' if a.stage == 'train_a' else 'b.json')).write_text(json.dumps({'value': value}))
else:
    value=json.loads((r/'a.json').read_text())['value']+json.loads((r/'b.json').read_text())['value']
    (r/'results.json').write_text(json.dumps({'schema_version':1,'protocol':PROTOCOL,'metrics':[{'name':'measurement','variant':'requested method','value':value,'unit':''}]}))
    (r/'validation.json').write_text(json.dumps({'schema_version':1,'passed':True,'checks':[{'name':'both branches','passed':value==42,'detail':'equal checked inputs'}]}))
""".replace("PROTOCOL", repr(PLAN["protocol"]))


@pytest.mark.parametrize(
    "needs,error",
    [
        ({"train_a": ["missing"]}, "invalid dependencies"),
        ({"features": ["train_a"]}, "cycle"),
        ({"evaluate": ["train_a"]}, "depend on every other stage"),
    ],
)
def test_invalid_graphs_stop_before_review_or_compute(needs, error):
    stages = [{**stage, "needs": needs.get(stage["id"], stage["needs"])} for stage in STAGES]
    with pytest.raises(ValidationError, match=error):
        Implementation.model_validate({**PLAN, "stages": stages})
    planned = [
        {key: value for key, value in stage.items() if key not in ("outputs",)}
        | {"summary": "Run stage.", "resources": JobResources().model_dump()}
        for stage in stages
    ]
    with pytest.raises(ValidationError, match=error):
        ResearchPlan.model_validate(
            {
                "schema_version": 2,
                "title": "Parallel experiment",
                "summary": "Two branches.",
                "protocol": PLAN["protocol"],
                "stages": planned,
            }
        )


async def test_dag_branches_submit_together_and_join_after_restart(runtime, monkeypatch):
    import test_generic

    monkeypatch.setattr(test_generic, "PLAN", PLAN)
    monkeypatch.setattr(test_generic, "SOURCE", SOURCE)
    store, scheduler, _, old = runtime
    old.config.slurm.max_concurrent_jobs = None
    agent = ImplementationAgent()
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    assert store.experiment(eid)["plan_review"]["schema_version"] == 2
    original_observe = scheduler.observe

    async def running(job_id):
        return Observation("RUNNING", None)

    scheduler.observe = running
    await until(workflow, store, eid, lambda e: scheduler.submissions == 1)
    for _ in range(3):
        await workflow.tick(force_observe=True)
    assert [row["stage_id"] for row in store.rows("SELECT stage_id FROM jobs")] == ["features"]
    scheduler.observe = original_observe
    await until(workflow, store, eid, lambda e: scheduler.submissions == 3)
    assert [row["stage_id"] for row in store.rows("SELECT stage_id FROM jobs ORDER BY created_at")] == [
        "features",
        "train_a",
        "train_b",
    ]
    scheduler.observe = running
    await workflow.close()
    resumed = Workflow(old.config, store, scheduler, agent)
    for _ in range(3):
        await resumed.tick(force_observe=True)
        await asyncio.sleep(0.001)
    assert scheduler.submissions == 3
    scheduler.observe = original_observe
    await until(resumed, store, eid, lambda e: e["phase"] == "complete")
    assert scheduler.submissions == 4
    assert len(store.rows("SELECT * FROM inbox WHERE experiment_id=? AND kind='result_ready'", (eid,))) == 1
    await resumed.close()


async def test_restart_reconciles_each_active_branch_before_replacement(runtime, monkeypatch):
    import test_generic

    monkeypatch.setattr(test_generic, "PLAN", PLAN)
    monkeypatch.setattr(test_generic, "SOURCE", SOURCE)
    store, scheduler, _, old = runtime
    old.config.slurm.max_concurrent_jobs = None
    workflow = Workflow(old.config, store, scheduler, ImplementationAgent())
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    original_observe = scheduler.observe
    cancelled = set()

    async def observe(job_id):
        if job_id in cancelled:
            return Observation("CANCELLED", "1:0")
        row = store.one("SELECT stage_id FROM jobs WHERE job_id=?", (job_id,))
        if row and row["stage_id"] in ("train_a", "train_b"):
            return Observation("RUNNING", None)
        return await original_observe(job_id)

    async def cancel(job_id, intent_id):
        assert job_id == f"demo-{intent_id}"
        cancelled.add(job_id)
        return True

    scheduler.observe = observe
    scheduler.cancel = cancel
    await until(workflow, store, eid, lambda e: scheduler.submissions == 3)
    branch_jobs = store.rows(
        "SELECT job_id FROM jobs WHERE experiment_id=? AND stage_id IN ('train_a','train_b')", (eid,)
    )
    store.command(uuid4(), "steer", {"text": "Correct the protocol.", "mode": "restart"}, eid=eid)
    for _ in range(30):
        workflow.last_cancellation.clear()  # Advance the normal 30-second cancellation retry interval.
        await workflow.tick(force_observe=True)
        if store.experiment(eid)["phase"] == "cancelled":
            break
    assert store.experiment(eid)["phase"] == "cancelled"
    assert cancelled == {job["job_id"] for job in branch_jobs}
    assert len(store.rows("SELECT id FROM experiments WHERE replaces_experiment_id=?", (eid,))) == 1
    await workflow.close()


async def test_changed_dependencies_are_autonomous_within_the_approved_objective(runtime, monkeypatch):
    import test_generic

    monkeypatch.setattr(test_generic, "PLAN", PLAN)
    monkeypatch.setattr(test_generic, "SOURCE", SOURCE)

    class ChangedGraphAgent(ImplementationAgent):
        async def begin(self, thread_id, prompt, schema, *, workspace=None):
            turn = await super().begin(thread_id, prompt, schema, workspace=workspace)
            if json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"] == "prepare":
                result = json.loads(json.dumps(self.results[(thread_id, turn)]))
                result["stages"][2]["needs"] = ["train_a"]
                self.results[(thread_id, turn)] = result
            return turn

    store, scheduler, _, old = runtime
    workflow = Workflow(old.config, store, scheduler, ChangedGraphAgent())
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Test an entirely new method and show its frontier.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    await approve_plan(workflow, store, eid)
    await until(workflow, store, eid, lambda e: bool(e["snapshot"]))
    review = store.experiment(eid)["plan_review"]
    assert review["state"] == "approved"
    assert review["stages"][2]["needs"] == ["train_a"]
    assert scheduler.submissions == 0
    await workflow.close()
