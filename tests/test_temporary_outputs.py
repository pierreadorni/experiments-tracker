"""Disposable intermediates stay available through repair, without evidence hashing."""

import asyncio
import copy
import time
from uuid import uuid4

import pytest
from test_generic import PLAN, SOURCE

from experimenttracker import artifacts
from experimenttracker.models import Implementation
from experimenttracker.remote_worker import cleanup_temporary
from experimenttracker.store import encode
from experimenttracker.workflow import Workflow


@pytest.fixture
def temporary_run(runtime):
    store, scheduler, agent, workflow = runtime
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic cache lifecycle", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    root = workflow.root(eid)
    (root / "work").mkdir(parents=True)
    (root / "work/recipe.py").write_text(SOURCE)
    plan = Implementation.model_validate(PLAN)
    plan.stages[0].temporary_outputs = ["temporary/features"]
    plan.stages[0].local_outputs = ["checkpoint"]
    manifest = artifacts.freeze_implementation(root, plan, "")
    store.resolve_implementation(eid, manifest)
    (root / "temporary/features").mkdir(parents=True)
    (root / "temporary/features/data.pt").write_bytes(b"regenerable features")
    (root / "checkpoint").write_bytes(b"valuable model")
    (root / "raw.json").write_text('{"value":42}')
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest) "
            "VALUES(?,?,'z_measure','complete','12345','script','{}',?,?,?,?)",
            (str(uuid4()), eid, manifest["snapshot_hash"], time.time(), str(root), encode(manifest)),
        )
    return store, scheduler, agent, workflow, eid, root, manifest


@pytest.mark.parametrize(
    "paths",
    [
        ["../elsewhere"],
        ["/temporary/cache"],
        ["temporary/../snapshot"],
        ["cache"],
        ["temporary"],
        ["temporary/cache", "temporary/cache"],
        ["temporary/cache", "temporary/cache/nested"],
    ],
)
def test_temporary_declarations_reject_unsafe_or_ambiguous_ownership(paths):
    plan = copy.deepcopy(PLAN)
    plan["stages"][0]["temporary_outputs"] = paths
    with pytest.raises(ValueError):
        Implementation.model_validate(plan)


@pytest.mark.parametrize("retained", ["temporary/cache", "temporary/cache/checkpoint", "temporary"])
def test_temporary_declarations_cannot_delete_retained_outputs(retained):
    plan = copy.deepcopy(PLAN)
    plan["stages"][0].update(temporary_outputs=["temporary/cache"], local_outputs=[retained])
    with pytest.raises(ValueError, match="temporary output"):
        Implementation.model_validate(plan)


def test_cache_is_never_hashed_but_checkpoint_changes_are_detected(temporary_run, monkeypatch):
    store, _, _, _, eid, root, manifest = temporary_run
    digest = artifacts.sha256
    reads = []

    def tracked(path):
        reads.append(path)
        return digest(path)

    monkeypatch.setattr(artifacts, "sha256", tracked)
    artifacts.validate_stage(root, "z_measure", manifest, seal_outputs=True)
    assert root / "checkpoint" in reads
    assert not any("temporary" in p.parts for p in reads)
    reads.clear()
    (root / "checkpoint").write_bytes(b"changed")
    artifacts.validate_stage(root, "z_measure", manifest, verify_local_outputs=False)
    assert root / "checkpoint" not in reads
    with pytest.raises(ValueError, match="checkpoint changed"):
        artifacts.validate_stage(root, "z_measure", manifest)
    # The small scientific output still must be valid JSON in ordinary mode.
    (root / "raw.json").write_text("invalid")
    with pytest.raises(ValueError):
        artifacts.validate_stage(root, "z_measure", manifest, verify_local_outputs=False)


async def test_cleanup_waits_for_result_ready_and_replays_after_restart(temporary_run):
    store, scheduler, agent, workflow, eid, root, manifest = temporary_run
    cache = root / "temporary/features"
    for phase in ("training", "validating", "interpreting", "publishing", "failed"):
        store.update(eid, phase=phase)
        await workflow.advance_cleanup()
        assert not workflow.cleanup_tasks and cache.exists()
    store.update(eid, phase="complete")
    # Simulate a crash after filesystem deletion but before the completion receipt.
    cleanup_temporary(root, manifest)
    assert not cache.exists()
    fresh = Workflow(workflow.config, store, scheduler, agent)
    await fresh.advance_cleanup()
    await asyncio.gather(*fresh.cleanup_tasks.values())
    await fresh.advance_cleanup()
    assert not fresh.cleanup_tasks
    assert (root / "checkpoint").read_bytes() == b"valuable model"
    assert (root / "raw.json").exists() and (root / "snapshot/manifest.json").exists()
    assert len(store.rows("SELECT * FROM events WHERE kind='temporary_outputs_cleaned'")) == 1
    assert not store.rows("PRAGMA foreign_key_check")
    await fresh.close()


@pytest.mark.parametrize("unsafe", ["target", "parent"])
def test_cleanup_rejects_symlink_targets_and_parents(temporary_run, tmp_path, unsafe):
    _, _, _, _, _, root, manifest = temporary_run
    cleanup_temporary(root, manifest)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.pt").write_bytes(b"keep")
    if unsafe == "target":
        (root / "temporary/features").symlink_to(outside, target_is_directory=True)
    else:
        (root / "temporary").rmdir()
        (root / "temporary").symlink_to(outside, target_is_directory=True)
    with pytest.raises(ValueError, match="symlink"):
        cleanup_temporary(root, manifest)
    assert (outside / "keep.pt").read_bytes() == b"keep"


async def test_cleanup_failure_retains_ready_result_and_retries(temporary_run, monkeypatch):
    store, _, _, workflow, eid, root, _ = temporary_run
    store.update(eid, phase="complete")
    original = cleanup_temporary

    def denied(*args):
        raise PermissionError("synthetic filesystem failure")

    monkeypatch.setattr("experimenttracker.workflow.cleanup_temporary", denied)
    await workflow.advance_cleanup()
    await asyncio.gather(*workflow.cleanup_tasks.values(), return_exceptions=True)
    await workflow.advance_cleanup()
    assert store.experiment(eid)["phase"] == "complete"
    assert (root / "temporary/features").exists()
    assert not store.rows("SELECT * FROM events WHERE kind='temporary_outputs_cleaned'")
    monkeypatch.setattr("experimenttracker.workflow.cleanup_temporary", original)
    workflow.last_cleanup_attempt.clear()
    await workflow.advance_cleanup()
    await asyncio.gather(*workflow.cleanup_tasks.values())
    assert not (root / "temporary/features").exists()
    await workflow.close()


async def test_remote_cleanup_uses_exact_saved_root_and_snapshot(temporary_run, monkeypatch):
    store, _, _, workflow, eid, root, manifest = temporary_run
    manifest = {**manifest, "remote_execution": True, "execution": {"layout": {"remote_run": "/saved/run"}}}
    with store.tx() as db:
        db.execute("UPDATE jobs SET execution_manifest=? WHERE experiment_id=?", (encode(manifest), eid))
    calls = []

    class Remote:
        async def remote_command(self, *args, **kwargs):
            calls.append(args)

    monkeypatch.setattr(workflow, "scheduler_for", lambda exp, job: Remote())
    store.update(eid, phase="complete")
    await workflow.advance_cleanup()
    await asyncio.gather(*workflow.cleanup_tasks.values())
    assert calls == [
        (
            "/usr/bin/python3",
            "/saved/run/remote-worker.py",
            "cleanup",
            "/saved/run",
            "z_measure",
            manifest["snapshot_hash"],
        )
    ]
    assert not (root / "temporary/features").exists()
    await workflow.close()


@pytest.mark.parametrize("strict", [False, True])
def test_interpretation_prompt_reuses_completed_checks(temporary_run, monkeypatch, strict):
    import json

    store, _, _, workflow, eid, root, manifest = temporary_run
    (root / "results.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "protocol": manifest["plan"]["protocol"],
                "metrics": [{"name": "measurement", "variant": "requested method", "value": 42, "unit": ""}],
            }
        )
    )
    (root / "validation.json").write_text(
        json.dumps(
            {
                "schema_version": 1,
                "passed": True,
                "checks": [{"name": "synthetic check", "passed": True, "detail": "fixture"}],
            }
        )
    )
    (root / "frontier.svg").write_text('<svg xmlns="http://www.w3.org/2000/svg"/>')
    artifacts.validate_stage(root, "z_measure", manifest, seal_outputs=True)
    exp = store.experiment(eid)
    exp["contract"]["strict_reproducibility"] = strict
    reads = []
    digest = artifacts.sha256

    def tracked(path):
        reads.append(path)
        return digest(path)

    monkeypatch.setattr(artifacts, "sha256", tracked)
    prompt = workflow.prompt(exp, "interpret")
    assert '"passed":true' in prompt
    assert (root / "checkpoint" in reads) is strict
    assert not any("temporary" in p.parts for p in reads)


def test_cleanup_does_not_follow_child_symlinks(temporary_run, tmp_path):
    _, _, _, _, _, root, manifest = temporary_run
    outside = tmp_path / "keep.pt"
    outside.write_bytes(b"keep")
    (root / "temporary/features/link").symlink_to(outside)
    cleanup_temporary(root, manifest)
    assert outside.read_bytes() == b"keep"
    assert not (root / "temporary/features").exists()


def test_failed_final_validation_does_not_leave_stage_running(temporary_run):
    store, _, _, workflow, eid, _, _ = temporary_run
    store.start_validation(eid)
    workflow.fail(eid, "validation_failed", "synthetic scientific check failure")
    assert store.experiment(eid)["phase"] == "failed"
    assert store.one("SELECT state,error FROM stages WHERE experiment_id=? AND id='validate'", (eid,)) == {
        "state": "failed",
        "error": "synthetic scientific check failure",
    }
