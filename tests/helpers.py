"""Deterministic waiting for tests that drive the owner step by step.

A test advances the system explicitly (a workflow tick, a controller advance)
and a wait re-checks its condition after such a tick. Nothing counts ticks: a
wait ends when its predicate holds or when ``timeout`` seconds of wall time
pass, so a loaded machine makes a passing wait take longer instead of making it
give up too early. The timeout covers the whole wait, including a tick that
blocks, so a broken wait fails the suite with a diagnostic instead of hanging.

The predicate is checked before the first tick: a condition that already holds
advances nothing, so waiting never dispatches work on its own.

The short pause between ticks is what lets subprocess- and timer-driven work
(the demo scheduler, owner threads) finish while the test waits.

The module also holds the contract-3 execution seams that the older test modules
define and re-export: the plan-3 ``PLAN``/``SOURCE`` fixture, the
``IterationAgent`` that writes it, ``prepared()`` and ``approve_plan()``.
Imports go one way only: nothing here imports a ``test_*`` module, because
``tests/conftest.py`` keys the contract downgrade on the test file's name.
"""

import asyncio
import inspect
import json
import sys
from uuid import uuid4

import pytest

from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import JobResources
from experimenttracker.store import encode
from experimenttracker.workflow import Workflow

POLL_SECONDS = 0.01


async def wait_until(predicate, *, ticks, timeout=30.0, describe=None):
    """Return the first truthy ``predicate()``, ticking until ``timeout`` expires.

    ``ticks`` advances the system by one step and may be sync or async. On
    timeout the test fails with ``describe``. A ``TimeoutError`` raised by the
    tick itself is not an unmet condition and propagates unchanged.
    """
    limit = asyncio.timeout(timeout)
    try:
        async with limit:
            while True:
                value = predicate()
                if value:
                    return value
                result = ticks()
                if inspect.isawaitable(result):
                    await result
                await asyncio.sleep(POLL_SECONDS)
    except TimeoutError:
        if not limit.expired():
            raise
        detail = f": {describe()}" if describe else ""
        pytest.fail(f"condition not reached within {timeout}s{detail}")


async def wait_for_experiment(workflow, store, eid, predicate, *, timeout=30.0):
    """Tick ``workflow`` until ``predicate(experiment)`` holds and return the experiment.

    Owner waits take one tick before the first predicate check: they drive an
    owner that starts settled, for example at a failed phase that only repairs
    or continues on the next tick.
    """
    ticked = False

    async def tick():
        nonlocal ticked
        await workflow.tick(force_observe=True)
        ticked = True

    await wait_until(
        lambda: ticked and predicate(store.experiment(eid)),
        ticks=tick,
        timeout=timeout,
        describe=lambda: store.experiment(eid),
    )
    return store.experiment(eid)


async def until(workflow, store, eid, predicate):
    await wait_for_experiment(workflow, store, eid, predicate)


async def role_step(controller):
    """Advance one role as its engine step does, then run the pool's selection.

    A role prepares proposals and the pool claims and starts the eligible ones; the
    engine calls both halves, so a test that drives one role directly does too.
    """
    await controller.advance()
    controller.workflow.agent_pool.admit()


async def prepare_branch(workflow, eid):
    """Create or verify the saved checkout as a contract 4 dispatch does before a turn."""
    if workflow.store.experiment(eid)["contract"].get("branch_workspace"):
        await asyncio.to_thread(workflow.workspaces.ensure_branch, eid)
        await asyncio.to_thread(workflow.workspaces.shared_runtime, eid)
    elif workflow.workspaces.row(eid):
        await asyncio.to_thread(workflow.workspaces.ensure, eid)
    return True


async def approve_plan(workflow, store, eid):
    await wait_until(
        lambda: (store.experiment(eid)["plan_review"] or {}).get("state") == "awaiting_review",
        ticks=lambda: workflow.tick(force_observe=True),
        describe=lambda: store.experiment(eid),
    )
    review = store.experiment(eid)["plan_review"]
    store.command(
        uuid4(), "plan_review", {"decision": "approve", "plan_revision": review["revision"]}, eid=eid
    )


PLAN = {
    "schema_version": 3,
    "summary": "Synthetic iteration mechanism acceptance.",
    "protocol": "Synthetic mechanism check, not scientific research.",
    "synthetic_demo": True,
    "parameters_json": "{}",
    "files": ["recipe.py"],
    "figures": ["plot.svg"],
    "stages": [
        {
            "id": "train",
            "title": "Synthetic training",
            "needs": [],
            "outputs": ["model.json"],
            "local_outputs": ["checkpoints"],
        },
        {
            "id": "evaluate",
            "title": "Synthetic evaluation",
            "needs": ["train"],
            "outputs": ["metrics.json"],
            "result_file": "metrics.json",
            "expected_metrics": ["score"],
            "sample_count": 12,
        },
        {"id": "plot", "title": "Synthetic plot", "needs": ["evaluate"], "outputs": ["plot.svg"]},
    ],
}
PLAN["stages"] = [{**s, "resources": JobResources().model_dump()} for s in PLAN["stages"]]
SOURCE = """import argparse,json,os
from pathlib import Path
p=argparse.ArgumentParser();p.add_argument('stage');p.add_argument('--output');a=p.parse_args()
r=Path(a.output)
refs=json.loads(Path(os.environ['EXPERIMENT_INPUTS']).read_text())
if a.stage=='train':
 (r/'model.json').write_text('{"weight":42}')
 (r/'checkpoints').mkdir()
 with (r/'checkpoints/large.bin').open('wb') as f:f.truncate(1024*1024*1024)
elif a.stage=='evaluate':
 model=next(x for x in refs if x['name']=='model.json')
 value=json.loads(Path(model['path']).read_text())['weight']
 (r/'metrics.json').write_text(json.dumps({'schema_version':1,'protocol':PROTOCOL,'sample_count':12,'metrics':[{'name':'score','variant':'synthetic','value':value,'unit':''}]}))
else:
 FAIL
 (r/'plot.svg').write_text('<svg xmlns="http://www.w3.org/2000/svg" width="100" height="60"><text x="4" y="30">Synthetic 42</text></svg>')
""".replace("PROTOCOL", repr(PLAN["protocol"]))


class IterationAgent(DemoAgent):
    def __init__(self, fail_plot=False):
        super().__init__()
        self.fail_plot = fail_plot

    async def session(self, thread_id, workspace, **kwargs):
        self.workspace = workspace
        return await super().session(thread_id, workspace)

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        self.calls += 1
        turn = f"iteration-{self.calls}"
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        phase = payload["phase"]
        if phase == "plan":
            result = {
                "schema_version": 3,
                "title": "Synthetic iteration",
                "summary": PLAN["summary"],
                "protocol": PLAN["protocol"],
                "stages": [
                    {k: s[k] for k in ("id", "title", "needs", "resources")} | {"summary": "Synthetic task."}
                    for s in PLAN["stages"]
                ],
            }
        elif phase == "prepare":
            runtime = workspace / "runtime/bin/python"
            runtime.parent.mkdir(parents=True, exist_ok=True)
            if not runtime.exists():
                runtime.symlink_to(sys.executable)
            (workspace / "recipe.py").write_text(
                SOURCE.replace(
                    " FAIL", " raise RuntimeError('synthetic plot failure')" if self.fail_plot else " pass"
                )
            )
            result = {**PLAN, "python_executable": str(runtime)}
            if payload.get("repair_context"):
                result |= {
                    "repair_decisions": [
                        {
                            "stage_id": s["id"],
                            "decision": "retry" if s["id"] == "plot" else "keep",
                            "rationale": "Repair failed plot; retain measured producers.",
                        }
                        for s in PLAN["stages"]
                    ],
                    "reuse_stages": ["train", "evaluate"],
                    "reuse_rationale": "Reuse measured synthetic outputs; repair only plotting.",
                }
        else:
            result = {
                "title": "Synthetic result",
                "summary": "Synthetic 42.",
                "observations": [{"text": "Synthetic 42.", "evidence_ids": [payload["evidence_ids"][0]]}],
                "limitations": ["Synthetic infrastructure test."],
                "next_ideas": [],
                "scientific_outcome": "inconclusive",
                "presentation": {
                    "title": "Synthetic result",
                    "standfirst": "Infrastructure only.",
                    "blocks": [
                        {
                            "kind": "table",
                            "caption": "Synthetic measurement",
                            "columns": ["Score"],
                            "rows": [[{"metric": {"name": "score", "variant": "synthetic"}}]],
                        }
                    ],
                },
            }
        self.results[(thread_id, turn)] = result
        return turn


def save_snapshot(store, eid, manifest):
    """Write a historical C3 snapshot and its stage graph as saved state, without the engine."""
    from experimenttracker import clock

    with store.tx() as db:
        contract = json.loads(db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()[0])
        contract.update(
            parameters=manifest["parameters"],
            synthetic_demo=manifest["plan"].get("synthetic_demo", False),
            plan=manifest["plan"],
        )
        graph = [
            *(["plan"] if contract.get("planning_version") else []),
            "prepare",
            *[stage["id"] for stage in manifest["plan"]["stages"]],
            "interpret",
            "publish",
        ]
        for ordinal, stage in enumerate(graph):
            db.execute(
                "INSERT INTO stages(experiment_id,id,ordinal,updated_at) VALUES(?,?,?,?) "
                "ON CONFLICT(experiment_id,id) DO UPDATE SET ordinal=excluded.ordinal",
                (eid, stage, ordinal, clock.now()),
            )
        db.execute(
            "UPDATE experiments SET contract=?,snapshot=? WHERE id=?",
            (encode(contract), encode(manifest), eid),
        )


def lead_ready(store):
    """Give the default test project the writable Git checkout an Autopilot lead needs."""
    from experimenttracker.config import ProjectConfig
    from experimenttracker.workspaces import initialize_project

    if store.projects["test-project"].checkout is not None:
        return store.projects["test-project"].checkout
    folder = store.path.parent / "test-project-checkout"
    if not folder.exists():
        folder.mkdir()
        initialize_project(folder)
    store.projects["test-project"] = ProjectConfig(id="test-project", name="Test project", checkout=folder)
    return folder


def seed_saved_contract3(
    store,
    brief="Exercise synthetic iteration mechanisms.",
    *,
    project_id="test-project",
    branch_workspace=False,
    workspace_mode="auto",
):
    """Insert the minimal saved C3 shape directly, without invoking current create policy."""
    return _seed_saved_experiment(store, brief, 3, project_id, branch_workspace, workspace_mode)


def seed_saved_contract4(
    store, brief, *, project_id="test-project", branch_workspace=False, workspace_mode="auto"
):
    """Seed a C4 row for unit tests that exercise saved state, not workspace creation."""
    return _seed_saved_experiment(store, brief, 4, project_id, branch_workspace, workspace_mode)


def _seed_saved_experiment(
    store, brief, execution_contract, project_id, branch_workspace, workspace_mode="auto"
):
    from experimenttracker.execution_settings import ExecutionSettings

    eid = str(uuid4())
    project = store.projects[project_id]
    contract = {
        "schema_version": 1,
        "recipe_id": "agent_experiment_v1",
        "resource_policy_version": 2,
        "recovery_turns": 1,
        "brief_driven": True,
        "reporting_version": 1,
        **({"planning_version": 1} if execution_contract == 3 else {}),
        "execution_contract": execution_contract,
        "workspace_mode": workspace_mode,
        "execution": ExecutionSettings().model_dump(),
        "project": {
            "id": project.id,
            "name": project.name,
            **({"writable": True} if branch_workspace else {}),
        },
        **({"branch_workspace": True} if branch_workspace else {}),
    }
    with store.tx() as db:
        db.execute(
            "INSERT INTO experiments(id,brief,recipe_id,contract,created_at,updated_at) VALUES(?,?,?,?,0,0)",
            (eid, brief, "agent_experiment_v1", encode(contract)),
        )
        if project.writable and project.checkout is not None:
            folder = project.checkout.resolve(strict=True)
            info = folder.stat()
            db.execute(
                "INSERT INTO experiment_workspaces VALUES(?,?,?,?,?)",
                (
                    eid,
                    str(folder),
                    info.st_dev,
                    info.st_ino,
                    str(folder / ".experimenttracker/experiments" / eid),
                ),
            )
        if execution_contract == 3:
            db.executemany(
                "INSERT INTO stages(experiment_id,id,updated_at,ordinal) VALUES(?,?,0,?)",
                [
                    (eid, stage, ordinal)
                    for ordinal, stage in enumerate(("plan", "prepare", "interpret", "publish"))
                ],
            )
    assert store.experiment(eid)["contract"] == contract
    return eid


async def prepared(runtime, fail_plot=False):
    store, scheduler, _, old = runtime
    agent = IterationAgent(fail_plot)
    wf = Workflow(old.config, store, scheduler, agent)
    eid = seed_saved_contract3(store)
    await approve_plan(wf, store, eid)
    return store, scheduler, agent, wf, eid


def open_saved_lifetime(db, eid):
    """Save the advancement lifetime obligation a historical C3 creation opened."""
    from experimenttracker.core import obligation_sources

    contract = json.loads(db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()[0])
    return obligation_sources.open_episode(
        db,
        "advancement",
        eid,
        source_transition=False,
        experiment_id=eid,
        project_id=(contract.get("project") or {}).get("id"),
        kind="reconcile",
        target=eid,
        owner="backend",
        wait="none",
        detail=None,
    )
