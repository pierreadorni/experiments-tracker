"""Independent frozen stages submit together; joins wait for checked outputs."""

import json
import sys

import pytest
from helpers import IterationAgent
from pydantic import ValidationError

from experimenttracker.config import JobResources
from experimenttracker.models import Implementation, ResearchPlan

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


CURRENT_STAGES = [
    {"id": "features", "title": "Features", "needs": [], "outputs": ["features.json"]},
    {"id": "train_a", "title": "Train A", "needs": ["features"], "outputs": ["a.json"]},
    {"id": "train_b", "title": "Train B", "needs": ["features"], "outputs": ["b.json"]},
    {
        "id": "evaluate",
        "title": "Evaluate",
        "needs": ["train_a", "train_b"],
        "outputs": ["results.json"],
        "result_file": "results.json",
        "expected_metrics": ["score"],
    },
]
CURRENT_PLAN = {
    "schema_version": 3,
    "summary": "Run independent training branches and join their evidence.",
    "protocol": "Measure two variants from checked shared features.",
    "synthetic_demo": True,
    "parameters_json": '{"seed": 42}',
    "files": ["recipe.py"],
    "stages": [{**stage, "resources": JobResources().model_dump()} for stage in CURRENT_STAGES],
    "figures": [],
}
CURRENT_SOURCE = """import argparse,json,os
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('stage');p.add_argument('--output');a=p.parse_args()
r=Path(a.output)
inputs={x['name']:Path(x['path']) for x in json.loads(Path(os.environ['EXPERIMENT_INPUTS']).read_text())}
if a.stage=='features':
    (r/'features.json').write_text(json.dumps({'value': 21}))
elif a.stage in ('train_a','train_b'):
    value=json.loads(inputs['features.json'].read_text())['value']
    (r/('a.json' if a.stage=='train_a' else 'b.json')).write_text(json.dumps({'value': value}))
else:
    value=json.loads(inputs['a.json'].read_text())['value']+json.loads(inputs['b.json'].read_text())['value']
    (r/'results.json').write_text(json.dumps({'schema_version':1,'protocol':PROTOCOL,'metrics':[{'name':'score','variant':'synthetic','value':value,'unit':''}]}))
""".replace("PROTOCOL", repr(CURRENT_PLAN["protocol"]))


class DagAgent(IterationAgent):
    """IterationAgent whose plan and prepare turns carry this module's graph."""

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload["phase"] == "interpret":
            return await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
        self.calls += 1
        turn = f"dag-{self.calls}"
        if payload["phase"] == "plan":
            result = {
                "schema_version": 3,
                "title": "Parallel synthetic branches",
                "summary": CURRENT_PLAN["summary"],
                "protocol": CURRENT_PLAN["protocol"],
                "stages": [
                    {key: stage[key] for key in ("id", "title", "needs", "resources")}
                    | {"summary": f"Run {stage['id']}."}
                    for stage in CURRENT_PLAN["stages"]
                ],
            }
        else:
            assert payload["phase"] == "prepare"
            runtime = workspace / "runtime/bin/python"
            runtime.parent.mkdir(parents=True, exist_ok=True)
            if not runtime.exists():
                runtime.symlink_to(sys.executable)
            (workspace / "recipe.py").write_text(CURRENT_SOURCE)
            result = {**CURRENT_PLAN, "python_executable": str(runtime)}
        self.results[(thread_id, turn)] = result
        return turn


class ChangedGraphAgent(DagAgent):
    """DagAgent whose prepare turn makes the second branch train on the first."""

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
        if json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])["phase"] == "prepare":
            result = json.loads(json.dumps(self.results[(thread_id, turn)]))
            result["stages"][2]["needs"] = ["train_a"]
            self.results[(thread_id, turn)] = result
        return turn
