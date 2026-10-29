"""Iteration contract acceptance with real synthetic processes; no live research."""

import asyncio
import json
import sys
from pathlib import Path
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from test_generic import approve_plan
from test_workflow import until

from experimenttracker import artifacts, iteration, remote_worker, reporting
from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import JobResources
from experimenttracker.models import Implementation
from experimenttracker.slurm import SubmissionUncertain
from experimenttracker.workflow import Workflow

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

    async def begin(self, thread_id, prompt, schema, *, workspace=None):
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


async def prepared(runtime, fail_plot=False):
    store, scheduler, _, old = runtime
    agent = IterationAgent(fail_plot)
    wf = Workflow(old.config, store, scheduler, agent)
    eid = store.command(
        uuid4(),
        "create",
        {"brief": "Exercise synthetic iteration mechanisms.", "recipe_id": "agent_experiment_v1"},
    )["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    await approve_plan(wf, store, eid)
    return store, scheduler, agent, wf, eid


async def test_metrics_survive_plot_failure_repair_restart_and_direct_bindings(runtime, monkeypatch):
    store, scheduler, agent, wf, eid = await prepared(runtime, True)

    def forbidden(*args, **kwargs):
        raise AssertionError("scientific provenance traversal")

    monkeypatch.setattr(artifacts, "local_output_identity", forbidden)
    monkeypatch.setattr(artifacts, "local_output_manifest", forbidden)
    monkeypatch.setattr(wf, "dependencies", forbidden)
    await until(wf, store, eid, lambda e: bool(e.get("partial_results")))
    document = {
        "title": "Working synthetic report",
        "standfirst": "Evaluation is available; plotting is pending.",
        "blocks": [
            {
                "kind": "table",
                "caption": "Synthetic measurement",
                "columns": ["Score"],
                "rows": [[{"metric": {"name": "score", "variant": "synthetic"}}]],
            }
        ],
    }
    version = store.working_report(eid, document, "synthetic-partial-report")
    assert store.working_report(eid, document, "synthetic-partial-report") == version
    await wf.advance_reports()
    assert store.one("SELECT state FROM report_versions WHERE version=?", (version,))["state"] == "ready"
    assert store.experiment(eid)["phase"] != "complete"
    await until(wf, store, eid, lambda e: e["phase"] == "failed")
    projection = store.projection(eid)
    assert projection["partial_results"]["tasks"]["evaluate"]["result"]["metrics"][0]["value"] == 42
    assert not any(s["id"] == "validate" for s in projection["stages"])
    original_jobs = store.rows("SELECT intent_id FROM jobs WHERE stage_id IN ('train','evaluate')")
    original = store.rows("SELECT * FROM output_refs WHERE experiment_id=?", (eid,))
    assert len(original) == 3
    assert scheduler.submissions == 3
    diagnostic = wf.controller.status(eid)[0]
    assert "EXPERIMENT_INPUTS" in diagnostic["command"]
    assert diagnostic["attempt"] == 1
    assert diagnostic["declared_outputs"]["context"] == "acacia"
    assert store.one("SELECT COUNT(*) n FROM output_bindings")["n"] == 3
    scheduler.preflight = AsyncMock()
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    for still_broken in (True, False):
        wf.config.mode = "live"  # Exercise production repair policy with synthetic adapters.
        wf.cluster_snapshot = {"fixture": True}
        agent.fail_plot = still_broken
        await wf.repair_failed_experiments()
        await wf.close()
        wf = Workflow(wf.config, store, scheduler, agent)
        wf.config.mode = "demo"  # Hold automatic repair so each restart is explicit in this fixture.
        await until(
            wf, store, eid, lambda e, broken=still_broken: e["phase"] == ("failed" if broken else "complete")
        )
    assert scheduler.submissions == 5
    assert store.rows("SELECT intent_id FROM jobs WHERE stage_id IN ('train','evaluate')") == original_jobs
    assert store.rows("SELECT * FROM output_refs WHERE kind='directory'") == [
        r for r in original if r["kind"] == "directory"
    ]
    assert store.projection(eid)["partial_results"] == projection["partial_results"]
    plot = store.one("SELECT intent_id FROM jobs WHERE stage_id='plot' AND state='complete'")
    bindings = store.rows(
        "SELECT output_id FROM output_bindings WHERE consumer_intent=?", (plot["intent_id"],)
    )
    assert bindings == [{"output_id": next(r["id"] for r in original if r["name"] == "metrics.json")}]
    assert not (wf.projects.base_root(eid) / "repairs").exists()
    reporting.verify_report(
        store.experiment(eid),
        wf.root(eid),
        store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)),
        version,
        document,
    )
    assert len(store.projection(eid)["report_versions"]) >= 3
    old_report = store.one("SELECT id FROM artifacts WHERE name='report-v1.json'")["id"]
    original_begin = agent.begin

    async def discuss(thread_id, prompt, schema, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        if payload["phase"] != "discuss":
            return await original_begin(thread_id, prompt, schema, **kwargs)
        catalog = payload.get("artifacts")
        if catalog is None:
            text = await asyncio.to_thread(Path(payload["context"]["artifacts_detail"]["path"]).read_text)
            catalog = json.loads(text)
        assert old_report in {r["id"] for r in catalog}
        turn = "archived-report-discussion"
        agent.results[(thread_id, turn)] = {
            "reply": "The earlier working report retained the original measurement.",
            "evidence_ids": [old_report],
        }
        return turn

    monkeypatch.setattr(agent, "begin", discuss)
    request = str(uuid4())
    store.command(request, "discuss", {"text": "Explain the earlier working report."}, eid=eid)
    await until(
        wf,
        store,
        eid,
        lambda e: store.one("SELECT state FROM discussions WHERE id=?", (request,))["state"] == "completed",
    )
    assert json.loads(
        store.one("SELECT evidence_ids FROM discussions WHERE id=?", (request,))["evidence_ids"]
    ) == [old_report]
    await wf.close()


def test_directory_registration_never_reads_or_enumerates_scientific_bytes(tmp_path, monkeypatch):
    root = tmp_path / "attempt"
    root.mkdir()
    (root / "checkpoint").mkdir()
    (root / "checkpoint/large").write_text("do not read")
    monkeypatch.setattr(Path, "iterdir", lambda *a: pytest.fail("directory enumeration"))
    monkeypatch.setattr(artifacts, "sha256", lambda *a: pytest.fail("scientific hash"))
    assert iteration.metadata(root, "checkpoint")["kind"] == "directory"
    with pytest.raises(ValueError, match="missing or inaccessible declared output"):
        iteration.metadata(root, "missing")
    monkeypatch.setattr(remote_worker, "digest", lambda *a: pytest.fail("remote scientific hash"))
    remote_worker.iteration_operation(
        "seal",
        root,
        "train",
        "intent",
        {
            "intent_id": "intent",
            "snapshot_hash": "control",
            "plan": {"stages": [{"id": "train", "outputs": [], "local_outputs": ["checkpoint"]}]},
        },
    )
    assert json.loads((root / "remote-receipt-train-intent.json").read_text())["provenance_bytes_read"] == 0


def test_graph_has_no_final_barrier_and_detects_disconnected_cycles():
    plan = {**PLAN, "stages": [{**s, "needs": []} for s in PLAN["stages"]]}
    Implementation.model_validate(plan)
    with pytest.raises(ValueError, match="cycle"):
        Implementation.model_validate(
            {
                **PLAN,
                "stages": [
                    {**s, "needs": ["evaluate"] if s["id"] == "train" else s["needs"]} for s in PLAN["stages"]
                ],
            }
        )


def test_source_capture_is_bounded_declared_and_preserves_packages(tmp_path, monkeypatch):
    work = tmp_path / "work"
    work.mkdir()
    (work / "pkg").mkdir()
    (work / "pkg/__init__.py").write_text("VALUE=42")
    (work / "ignored-dataset").mkdir()
    (work / "ignored-dataset/large").write_text("not source")
    monkeypatch.setattr(Path, "iterdir", lambda *a: pytest.fail("source traversal"))
    plan = Implementation.model_validate({**PLAN, "files": ["recipe.py", "pkg/__init__.py"]})
    (work / "recipe.py").write_text("from pkg import VALUE")
    one = tmp_path / "one"
    one.mkdir()
    files, _, count = iteration.capture_sources(one, plan, work, None)
    assert set(files) == {"recipe.py", "pkg/__init__.py"} and count < 100
    assert iteration.capture_sources(one, plan, work, None)[0] == files
    (work / "pkg/__init__.py").write_text("VALUE=43")
    two = tmp_path / "two"
    two.mkdir()
    iteration.capture_sources(two, plan, work, None)
    assert (one / "snapshot/pkg/__init__.py").read_text() == "VALUE=42"
    assert (two / "snapshot/pkg/__init__.py").read_text() == "VALUE=43"


async def test_lost_submission_response_reconciles_exact_attempt_without_retry(runtime):
    store, scheduler, _, wf, eid = await prepared(runtime)
    submit = scheduler.submit
    reconcile = scheduler.reconcile
    lost = False

    async def lose_once(*args, **kwargs):
        nonlocal lost
        value = await submit(*args, **kwargs)
        if not lost:
            lost = True
            raise SubmissionUncertain("synthetic lost response after acceptance")
        return value

    scheduler.submit = lose_once
    scheduler.reconcile = AsyncMock(return_value=None)
    await until(wf, store, eid, lambda e: e["hold_reason"] == "submission_uncertain")
    intent = store.one("SELECT * FROM jobs WHERE experiment_id=?", (eid,))
    for _ in range(3):
        await wf.tick(force_observe=True)
    assert scheduler.submissions == 1
    scheduler.reconcile = reconcile
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    assert scheduler.submissions == 3
    assert (
        store.one("SELECT job_id FROM jobs WHERE intent_id=?", (intent["intent_id"],))["job_id"]
        == "demo-" + intent["intent_id"]
    )
    await wf.close()


async def test_remote_attempt_does_not_seal_or_sync_project_and_delivery_retries_only(
    runtime, tmp_path, monkeypatch
):
    from test_remote_execution import Machines
    from test_remote_execution import prepared as remote_prepared

    wf, exp, _ = remote_prepared(runtime, tmp_path)
    root = wf.root(exp["id"])
    work = root / "work"
    work.mkdir()
    (work / "train.sbatch").write_text(
        (root / "snapshot/train.sbatch").read_text()
        + '\nln -s /inaccessible-scientific-input "$EXPERIMENT_ROOT/checkpoint/never-inspect"\n'
    )
    manifest = exp["snapshot"]
    manifest["plan"]["schema_version"] = 3
    manifest["plan"]["stages"][0]["result_file"] = "results.json"
    wf.store.update(exp["id"], snapshot=manifest)
    exp = wf.store.experiment(exp["id"])
    machines = Machines(monkeypatch, wf.store)
    submitted = await wf.submit_prepared_stage(exp, "measure")
    assert submitted["state"] == "submitted"
    machines.fail_download = True
    await wf.observe_jobs()
    assert wf.store.experiment(exp["id"])["hold_reason"] == "results_transfer_pending"
    wf.store.db.execute("UPDATE jobs SET remote_polled_at=0")
    await wf.observe_jobs()
    assert wf.store.one("SELECT COUNT(*) n FROM execution_transfers WHERE direction='down'")["n"] == 1
    machines.fail_download = False
    wf.store.db.execute("UPDATE jobs SET remote_polled_at=0")
    await wf.observe_jobs()
    assert len(machines.jobs) == 1
    ref = wf.store.one("SELECT * FROM output_refs WHERE kind='directory'")
    assert ref["context"] == "jean-zay" and ref["host"] == "jz"
    assert not (
        Path(wf.store.one("SELECT execution_root FROM jobs")["execution_root"]) / "checkpoint"
    ).exists()
    assert not any(call[0] == "rsync" and str(tmp_path / "source") + "/" in call for call in machines.calls)
    assert not wf.store.rows("PRAGMA foreign_key_check")


@pytest.mark.parametrize(
    "change, message",
    [
        ({"sample_count": 11}, "sample count"),
        (
            {"metrics": [{"name": "wrong", "variant": "synthetic", "value": 42, "unit": ""}]},
            "expected metrics",
        ),
        (
            {"metrics": [{"name": "score", "variant": "synthetic", "value": float("nan"), "unit": ""}]},
            "non-finite",
        ),
    ],
)
def test_bounded_results_reject_bad_counts_missing_metrics_and_nonfinite(tmp_path, change, message):
    value = {
        "schema_version": 1,
        "protocol": PLAN["protocol"],
        "sample_count": 12,
        "metrics": [{"name": "score", "variant": "synthetic", "value": 42, "unit": ""}],
    }
    value.update(change)
    (tmp_path / "metrics.json").write_text(json.dumps(value))
    with pytest.raises(ValueError, match=message):
        iteration.check_stage(tmp_path, "evaluate", {"plan": PLAN})


async def test_missing_original_output_is_actionable_without_new_compute(runtime):
    store, scheduler, _, wf, eid = await prepared(runtime, True)
    await until(wf, store, eid, lambda e: e["phase"] == "failed")
    output = store.one("SELECT * FROM output_refs WHERE name='model.json'")
    await asyncio.to_thread(Path(output["path"]).unlink)
    selected = Implementation.model_validate(
        {
            **PLAN,
            "reuse_stages": ["train", "evaluate"],
            "reuse_rationale": "Select original results for plot repair.",
        }
    )
    with pytest.raises(ValueError, match="missing output " + output["id"]):
        iteration.reuse(wf, store.experiment(eid), selected)
    assert scheduler.submissions == 3
    assert (
        store.projection(eid)["partial_results"]["tasks"]["evaluate"]["result"]["metrics"][0]["value"] == 42
    )
    await wf.close()


async def test_failed_shared_editors_repair_serially_without_ownership_deadlock(runtime):
    store, scheduler, agent, wf, first = await prepared(runtime, True)
    await until(wf, store, first, lambda e: e["phase"] == "failed")
    _, _, _, second_wf, second = await prepared(runtime, True)
    await until(second_wf, store, second, lambda e: e["phase"] == "failed")
    agent.fail_plot = False
    wf.config.mode = "live"
    scheduler.discover = AsyncMock(return_value={"fixture": True})
    scheduler.preflight = AsyncMock()
    wf.cluster_snapshot = {"fixture": True}
    await wf.repair_failed_experiments()
    phases = [store.experiment(eid)["phase"] for eid in (first, second)]
    assert sorted(phases) == ["failed", "preparing"]
    await until(wf, store, first, lambda e: e["phase"] == "complete")
    await until(wf, store, second, lambda e: e["phase"] == "complete")
    assert scheduler.submissions == 8  # Two initial graphs and exactly two plot retries.
    await wf.close()


async def test_final_report_render_correction_never_queues_compute_repair(runtime, monkeypatch):
    store, scheduler, agent, wf, eid = await prepared(runtime)
    original = reporting.build_report
    failures = 0

    def render(exp, root, records, document, version, **kwargs):
        nonlocal failures
        if document["title"] == "Synthetic result" and not failures:
            failures += 1
            raise ValueError("synthetic final rendering error")
        return original(exp, root, records, document, version, **kwargs)

    monkeypatch.setattr(reporting, "build_report", render)
    await until(wf, store, eid, lambda e: e["phase"] == "complete")
    assert failures == 1 and scheduler.submissions == 3
    assert store.experiment(eid)["evidence_revision"] == 1
    actions = store.rows("SELECT state,turn_id FROM agent_actions WHERE phase='interpret' ORDER BY rowid")
    assert [a["state"] for a in actions] == ["failed", "completed"]
    assert len({a["turn_id"] for a in actions}) == 2
    assert not store.experiment(eid)["contract"].get("auto_repair")
    assert store.one("SELECT COUNT(*) n FROM report_versions WHERE state='failed'")["n"] == 1
    assert len(store.rows("SELECT id FROM artifacts WHERE name LIKE 'analysis-%.json'")) == 2
    await wf.close()


def test_incomplete_source_capture_can_be_corrected_without_changing_saved_records(tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    (work / "recipe.py").write_text("version=1")
    plan = Implementation.model_validate({**PLAN, "project_files": ["missing.py"]})
    root = tmp_path / "record"
    root.mkdir()
    with pytest.raises(ValueError, match="project checkout"):
        iteration.prepare_manifest(
            root, plan, python=sys.executable, project=None, workspace=work, reused={}, remote_execution=False
        )
    (work / "recipe.py").write_text("version=2")
    fixed = Implementation.model_validate(PLAN)
    saved = iteration.prepare_manifest(
        root, fixed, python=sys.executable, project=None, workspace=work, reused={}, remote_execution=False
    )
    assert (root / "snapshot/recipe.py").read_text() == "version=2"
    (work / "recipe.py").write_text("version=3")
    assert (
        iteration.prepare_manifest(
            root,
            fixed,
            python=sys.executable,
            project=None,
            workspace=work,
            reused={},
            remote_execution=False,
        )
        == saved
    )
    assert (root / "snapshot/recipe.py").read_text() == "version=2"


def test_output_declarations_cannot_replace_attempt_sources_or_bindings():
    for path in ("snapshot", "snapshot/package", "inputs.json", "remote-worker.py"):
        stages = [
            {**stage, "local_outputs": [path]} if stage["id"] == "train" else stage
            for stage in PLAN["stages"]
        ]
        with pytest.raises(ValueError, match="protected attempt"):
            Implementation.model_validate({**PLAN, "stages": stages})
