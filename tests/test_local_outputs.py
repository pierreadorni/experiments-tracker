"""Directory provenance and recovery of an exact successful validation-failed job."""

import asyncio
import time
from uuid import uuid4

import pytest
from test_generic import PLAN, SOURCE

from experimenttracker import artifacts
from experimenttracker.models import Implementation
from experimenttracker.slurm import Observation
from experimenttracker.store import encode
from experimenttracker.workflow import Workflow

SEALED_RECORD = (
    '{"outputs":[{"directories":["cache"],"files":[{"name":"cache/data.pt","sha256":'
    '"6db7d803e74f1ffa7d8f5adc0bf95b3e15bf4c8373fffadf546227cc6c6742cb","size":6}],'
    '"name":"cache","sha256":"ee68e6156ba2908d16286aa6052684dff50b7a0cfacdd4bc91386f51c4614740",'
    '"size":6,"type":"directory"}],"schema_version":1,"stage":"features","storage":"executor-only"}'
)


def manifest_for(*names):
    return {"plan": {"stages": [{"id": "features", "local_outputs": list(names)}]}}


@pytest.mark.parametrize("change", ["modify", "add", "delete", "rename", "empty_directory"])
def test_sealed_directory_detects_content_and_membership_changes(tmp_path, change):
    cache = tmp_path / "cache"
    cache.mkdir()
    file = cache / "data.pt"
    file.write_bytes(b"before")
    manifest = manifest_for("cache")
    # A historical sealed record kept verbatim: verification must not need the writer.
    (tmp_path / "local-output-features.json").write_text(SEALED_RECORD)
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


@pytest.fixture
def failed_directory_run(runtime):
    store, _, agent, workflow = runtime
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic recovery check", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    # Explicit static legacy state: this fixture never advances the experiment, so the
    # saved contract must not depend on the global downgrade removing the flag.
    contract = store.experiment(eid)["contract"]
    contract.pop("execution_contract", None)
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
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


@pytest.mark.parametrize("corrupt", [True])
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
