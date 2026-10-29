"""Directory provenance and recovery of an exact successful validation-failed job."""

import asyncio
import os
import threading
import time
from uuid import uuid4

import pytest
from test_generic import PLAN, SOURCE

from experimenttracker import artifacts
from experimenttracker.models import Implementation
from experimenttracker.slurm import Observation
from experimenttracker.workflow import Workflow


def manifest_for(*names):
    return {"plan": {"stages": [{"id": "features", "local_outputs": list(names)}]}}


def test_directory_outputs_seal_nested_large_files_and_empty_directories(tmp_path):
    cache = tmp_path / "cache"
    (cache / "nested").mkdir(parents=True)
    (cache / "empty").mkdir()
    (cache / "nested" / "features.pt").write_bytes(b"features")
    with (cache / "large.pt").open("wb") as stream:
        stream.truncate(artifacts.MAX_ARTIFACT_BYTES + 1)
    manifest = manifest_for("cache")
    assert artifacts.seal_local_outputs(tmp_path, "features", manifest) == ["local-output-features.json"]
    saved = artifacts.read_json(tmp_path / "local-output-features.json", tmp_path)
    output = saved["outputs"][0]
    assert output["type"] == "directory"
    assert output["directories"] == ["cache", "cache/empty", "cache/nested"]
    assert [f["name"] for f in output["files"]] == ["cache/large.pt", "cache/nested/features.pt"]
    assert output["size"] == artifacts.MAX_ARTIFACT_BYTES + 1 + len(b"features")
    assert output["files"][1]["sha256"] == artifacts.sha256(cache / "nested/features.pt")
    assert artifacts.local_output_manifest(tmp_path, "features", manifest) == saved


@pytest.mark.parametrize("change", ["modify", "add", "delete", "rename", "empty_directory"])
def test_sealed_directory_detects_content_and_membership_changes(tmp_path, change):
    cache = tmp_path / "cache"
    cache.mkdir()
    file = cache / "data.pt"
    file.write_bytes(b"before")
    manifest = manifest_for("cache")
    artifacts.seal_local_outputs(tmp_path, "features", manifest)
    if change == "modify":
        file.write_bytes(b"after!")
    elif change == "add":
        (cache / "extra.pt").write_bytes(b"extra")
    elif change == "delete":
        file.unlink()
    elif change == "rename":
        file.rename(cache / "renamed.pt")
    else:
        (cache / "empty").mkdir()
    with pytest.raises(ValueError, match="registered local checkpoint changed"):
        artifacts.local_output_manifest(tmp_path, "features", manifest)


@pytest.mark.parametrize("kind", ["file_link", "directory_link", "parent_link", "fifo", "missing", "escape"])
def test_directory_provenance_rejects_unsafe_or_missing_entries(tmp_path, kind):
    cache = tmp_path / "cache"
    cache.mkdir()
    target = tmp_path / "target"
    target.mkdir()
    (target / "data.pt").write_bytes(b"data")
    name = "cache"
    if kind == "file_link":
        (cache / "link").symlink_to(target / "data.pt")
    elif kind == "directory_link":
        (cache / "link").symlink_to(target, target_is_directory=True)
    elif kind == "parent_link":
        (cache / "link").symlink_to(target, target_is_directory=True)
        name = "cache/link/data.pt"
    elif kind == "fifo":
        os.mkfifo(cache / "pipe")
    elif kind == "missing":
        name = "missing"
    else:
        name = "../outside"
    with pytest.raises(ValueError, match="local output"):
        artifacts.seal_local_outputs(tmp_path, "features", manifest_for(name))
    assert not (tmp_path / "local-output-features.json").exists()


def test_directory_provenance_stays_bounded_and_published_outputs_stay_files(tmp_path, monkeypatch):
    (tmp_path / "cache").mkdir()
    monkeypatch.setattr(artifacts, "MAX_ARTIFACT_BYTES", 10)
    with pytest.raises(ValueError, match="provenance exceeds"):
        artifacts.seal_local_outputs(tmp_path, "features", manifest_for("cache"))
    with pytest.raises(ValueError, match="regular file"):
        artifacts.allowed_path(tmp_path / "cache", tmp_path)


@pytest.fixture
def failed_directory_run(runtime):
    store, _, agent, workflow = runtime
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic recovery check", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    root = workflow.root(eid)
    (root / "work").mkdir(parents=True)
    (root / "work/recipe.py").write_text(SOURCE)
    plan = Implementation.model_validate(PLAN)
    plan.stages[0].local_outputs = ["cache"]
    manifest = artifacts.freeze_implementation(root, plan, "")
    store.resolve_implementation(eid, manifest)
    store.stage(eid, "prepare", "complete")
    (root / "raw.json").write_text('{"value":42}')
    (root / "cache").mkdir()
    (root / "cache/data.pt").write_bytes(b"saved features")
    error = "artifact must be a regular file inside the registered root"
    intent = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,snapshot_hash,created_at,error) VALUES(?,?,?,'failed','12345','script','{}',?,?,?)",
            (intent, eid, "z_measure", manifest["snapshot_hash"], time.time(), error),
        )
    store.stage(eid, "z_measure", "failed", error=error)
    workflow.fail(eid, "validation_failed", error)

    class SavedScheduler:
        identity = "12345"
        outcome = Observation("COMPLETED", "0:0")

        async def reconcile(self, saved_intent):
            assert saved_intent == intent
            return self.identity

        async def observe(self, job_id):
            assert job_id == "12345"
            return self.outcome

        async def submit(self, *args, **kwargs):
            pytest.fail("recovery must not submit a job")

    workflow.scheduler = SavedScheduler()
    return store, agent, workflow, eid, root


async def test_recovery_adopts_saved_success_once_and_keeps_compute_paused(failed_directory_run):
    store, agent, workflow, eid, root = failed_directory_run
    result = await workflow.reconcile_outputs(eid)
    assert result == await workflow.reconcile_outputs(eid)
    exp = store.experiment(eid)
    assert exp["phase"] == "training" and exp["hold_reason"] == "new_compute_paused"
    assert exp["pause_compute"] and exp["error"] is None
    assert store.one("SELECT state,error FROM jobs WHERE experiment_id=?", (eid,)) == {
        "state": "complete",
        "error": None,
    }
    assert (
        store.one("SELECT state FROM stages WHERE experiment_id=? AND id='z_measure'", (eid,))["state"]
        == "complete"
    )
    assert {a["name"] for a in store.rows("SELECT name FROM artifacts WHERE experiment_id=?", (eid,))} == {
        "raw.json",
        "local-output-z_measure.json",
    }
    assert len(store.rows("SELECT * FROM events WHERE kind='output_validation_reconciled'")) == 1
    assert (
        artifacts.read_json(root / "local-output-z_measure.json", root)["outputs"][0]["type"] == "directory"
    )
    await workflow.advance(eid)
    assert agent.calls == 0
    assert len(store.rows("SELECT * FROM jobs")) == 1
    await workflow.close()


@pytest.fixture
def checking_directory_run(failed_directory_run):
    store, agent, workflow, eid, root = failed_directory_run
    store.update(eid, phase="training", hold_reason="accounting_pending", error=None)
    store.stage(eid, "plan", "complete")
    store.stage(eid, "z_measure", "running", error=None)
    with store.tx() as db:
        db.execute(
            "UPDATE jobs SET state='submitted',scheduler_state='RUNNING',error=NULL WHERE experiment_id=?",
            (eid,),
        )
    return store, agent, workflow, eid, root


async def test_slow_checks_publish_terminal_accounting_and_do_not_block_observation(
    checking_directory_run, monkeypatch
):
    store, agent, workflow, eid, root = checking_directory_run
    entered, release = threading.Event(), threading.Event()
    identity = artifacts.local_output_identity
    calls = []

    def slow_identity(*args):
        calls.append(args)
        entered.set()
        assert release.wait(5), "test did not release checkpoint verification"
        return identity(*args)

    monkeypatch.setattr(artifacts, "local_output_identity", slow_identity)
    other = store.command(uuid4(), "create", {"brief": "Other synthetic job", "recipe_id": "toy_compare_v1"})[
        "experiment_id"
    ]
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,snapshot_hash,created_at) "
            "VALUES(?,?,'train_linear','submitted','other','script','{}','hash',?)",
            (str(uuid4()), other, time.time()),
        )
    observed = []

    async def observe(job_id):
        observed.append(job_id)
        return Observation("RUNNING") if job_id == "other" else Observation("COMPLETED", "0:0", ended_at=123)

    monkeypatch.setattr(workflow.scheduler, "observe", observe)
    try:
        await asyncio.wait_for(workflow.observe_jobs(), 1)
        assert await asyncio.to_thread(entered.wait, 1)
        projection = store.projection(eid)
        job = projection["jobs"][0]
        assert job["scheduler_state"] == "COMPLETED" and job["exit_code"] == "0:0"
        assert job["state"] == "submitted" and job["ended_at"] == 123
        assert store.experiment(eid)["hold_reason"] is None
        assert (
            store.one("SELECT state FROM stages WHERE experiment_id=? AND id='z_measure'", (eid,))["state"]
            == "running"
        )
        assert "other" in observed
        await asyncio.wait_for(workflow.observe_jobs(), 1)
        await asyncio.wait_for(workflow.advance(eid), 1)
        assert len(calls) == 1 and observed.count("12345") == 1
        assert not store.rows("SELECT * FROM artifacts WHERE name='local-output-z_measure.json'")
        assert not store.rows("SELECT * FROM jobs WHERE stage_id='a_verify'")
        assert agent.calls == 0
        release.set()
        await asyncio.gather(*(task for task, _ in workflow.output_checks.values()))
        await workflow.observe_jobs()
        assert store.one("SELECT state FROM jobs WHERE experiment_id=?", (eid,))["state"] == "complete"
        assert (
            store.one("SELECT state FROM stages WHERE experiment_id=? AND id='z_measure'", (eid,))["state"]
            == "complete"
        )
        assert len(calls) == 1  # Validation and sealing share one checkpoint hash pass.
        revision = store.experiment(eid)["revision"]
        await workflow.observe_jobs()
        assert store.experiment(eid)["revision"] == revision
        assert not store.rows("PRAGMA foreign_key_check")
    finally:
        release.set()
        await workflow.close()


@pytest.mark.parametrize("corrupt", [False, True])
async def test_restart_rechecks_saved_terminal_job_without_resubmission(
    checking_directory_run, monkeypatch, corrupt
):
    store, agent, old, eid, root = checking_directory_run
    await old.observe_jobs()
    await asyncio.gather(*(task for task, _ in old.output_checks.values()))
    # Simulate shutdown before the owner has committed the worker's result.
    await old.close()
    assert store.one("SELECT state,scheduler_state FROM jobs WHERE experiment_id=?", (eid,)) == {
        "state": "submitted",
        "scheduler_state": "COMPLETED",
    }
    if corrupt:
        (root / "cache/data.pt").write_bytes(b"changed after seal")
    restarted = Workflow(old.config, store, old.scheduler, agent)
    try:
        await restarted.observe_jobs()
        await asyncio.gather(*(task for task, _ in restarted.output_checks.values()), return_exceptions=True)
        await restarted.observe_jobs()
        assert store.one("SELECT state FROM jobs WHERE experiment_id=?", (eid,))["state"] == (
            "failed" if corrupt else "complete"
        )
        assert len(store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))) == 1
        assert agent.calls == 0
        assert len(store.rows("SELECT * FROM artifacts WHERE name='local-output-z_measure.json'")) == (
            0 if corrupt else 1
        )
    finally:
        await restarted.close()


async def test_checkpoint_verification_releases_compute_slot_for_another_experiment(
    checking_directory_run, monkeypatch
):
    store, _, workflow, eid, root = checking_directory_run
    workflow.config.slurm.max_concurrent_jobs = 1
    entered, release = threading.Event(), threading.Event()
    validate = artifacts.validate_stage

    def slow_validate(*args, **kwargs):
        entered.set()
        assert release.wait(5)
        return validate(*args, **kwargs)

    monkeypatch.setattr(artifacts, "validate_stage", slow_validate)
    other = store.command(
        uuid4(), "create", {"brief": "Independent synthetic run", "recipe_id": "toy_compare_v1"}
    )["experiment_id"]
    manifest = artifacts.freeze_recipe(
        workflow.config.recipe_dir, workflow.root(other), store.experiment(other)["contract"]
    )
    store.update(other, snapshot=manifest, phase="training")
    store.stage(other, "prepare", "complete")
    submitted = []

    async def submit(intent_id, *args, **kwargs):
        assert store.one("SELECT state FROM jobs WHERE intent_id=?", (intent_id,))["state"] == "submitting"
        submitted.append(intent_id)
        return "54321"

    monkeypatch.setattr(workflow.scheduler, "submit", submit)
    try:
        await workflow.observe_jobs()
        assert await asyncio.to_thread(entered.wait, 1)
        await asyncio.wait_for(workflow.advance(other), 1)
        assert len(submitted) == 1
        assert (
            store.one("SELECT state FROM stages WHERE experiment_id=? AND id='z_measure'", (eid,))["state"]
            == "running"
        )
    finally:
        release.set()
        await workflow.close()


@pytest.mark.parametrize("strict", [False, True])
async def test_final_checkpoint_checks_do_not_block_advancement_loop(
    checking_directory_run, monkeypatch, strict
):
    store, agent, workflow, eid, root = checking_directory_run
    contract = store.experiment(eid)["contract"]
    contract["strict_reproducibility"] = strict
    from experimenttracker.store import encode

    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    for stage in workflow.compute_stages(store.experiment(eid)):
        store.stage(eid, stage, "complete")
    with store.tx() as db:
        db.execute("UPDATE jobs SET state='complete' WHERE experiment_id=?", (eid,))
    entered, release = threading.Event(), threading.Event()
    calls = []

    def slow_validate(root, stage, manifest, **kwargs):
        assert kwargs == {"verify_local_outputs": strict}
        calls.append(stage)
        entered.set()
        assert release.wait(5)
        return []

    monkeypatch.setattr(artifacts, "validate_stage", slow_validate)
    try:
        await asyncio.wait_for(workflow.advance(eid), 1)
        assert await asyncio.to_thread(entered.wait, 1)
        await asyncio.wait_for(workflow.advance(eid), 1)
        assert store.experiment(eid)["phase"] == "validating"
        assert (
            store.one("SELECT state FROM stages WHERE experiment_id=? AND id='validate'", (eid,))["state"]
            == "running"
        )
        assert len(calls) == 1 and agent.calls == 0
        assert (
            len(
                store.rows("SELECT * FROM events WHERE experiment_id=? AND kind='validation_started'", (eid,))
            )
            == 1
        )
        release.set()
        await asyncio.gather(*workflow.evidence_checks.values())
        await workflow.advance(eid)
        assert (
            store.one("SELECT state FROM stages WHERE experiment_id=? AND id='validate'", (eid,))["state"]
            == "complete"
        )
        assert calls == workflow.compute_stages(store.experiment(eid))
    finally:
        release.set()
        await workflow.close()


@pytest.mark.parametrize("failure", ["identity", "running", "exit", "invalid_output", "symlink"])
async def test_recovery_refuses_unconfirmed_or_invalid_saved_work(failed_directory_run, failure):
    store, agent, workflow, eid, root = failed_directory_run
    if failure == "identity":
        workflow.scheduler.identity = "different"
    elif failure == "running":
        workflow.scheduler.outcome = Observation("RUNNING")
    elif failure == "exit":
        workflow.scheduler.outcome = Observation("COMPLETED", "1:0")
    elif failure == "invalid_output":
        (root / "raw.json").write_text("invalid JSON")
    else:
        (root / "cache/link").symlink_to(root / "raw.json")
    with pytest.raises(ValueError):
        await workflow.reconcile_outputs(eid)
    assert store.experiment(eid)["phase"] == "failed"
    assert store.one("SELECT state FROM jobs WHERE experiment_id=?", (eid,))["state"] == "failed"
    assert not store.rows("SELECT * FROM artifacts")
    assert agent.calls == 0
    await workflow.close()


async def test_recovery_rolls_back_registration_and_states_on_transaction_failure(
    failed_directory_run, monkeypatch
):
    store, agent, workflow, eid, root = failed_directory_run
    emit = store.emit

    def fail_commit(db, eid, kind, payload=None):
        if kind == "output_validation_reconciled":
            raise RuntimeError("simulated transaction failure")
        return emit(db, eid, kind, payload)

    monkeypatch.setattr(store, "emit", fail_commit)
    with pytest.raises(RuntimeError, match="transaction failure"):
        await workflow.reconcile_outputs(eid)
    assert store.experiment(eid)["phase"] == "failed"
    assert store.one("SELECT state FROM jobs WHERE experiment_id=?", (eid,))["state"] == "failed"
    assert not store.rows("SELECT * FROM artifacts")
    assert (root / "local-output-z_measure.json").exists()
    monkeypatch.setattr(store, "emit", emit)
    await workflow.reconcile_outputs(eid)
    assert store.experiment(eid)["pause_compute"]
    assert agent.calls == 0
    await workflow.close()
