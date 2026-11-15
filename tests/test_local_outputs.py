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


@pytest.mark.parametrize(
    ("phase", "hold"),
    [("training", "accounting_pending"), ("failed", "validation_failed"), ("complete", None)],
)
async def test_archived_saved_job_settles_without_reading_checkpoint_bytes(
    checking_directory_run, phase, hold
):
    store, agent, workflow, eid, root = checking_directory_run
    (root / "cache/data.pt").write_bytes(b"changed after the saved run")
    store.update(eid, phase=phase, hold_reason=hold)
    store.archive_legacy_experiments()
    before = store.experiment(eid)
    stages = store.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY id", (eid,))
    artifacts_before = store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,))
    outstanding = asyncio.get_running_loop().create_future()
    intent = store.one("SELECT intent_id FROM jobs WHERE experiment_id=?", (eid,))["intent_id"]
    workflow.output_checks[intent] = (outstanding, Observation("COMPLETED", "0:0"))

    await workflow.observe_jobs()

    job = store.one("SELECT state,scheduler_state,exit_code FROM jobs WHERE experiment_id=?", (eid,))
    assert job == {"state": "complete", "scheduler_state": "COMPLETED", "exit_code": "0:0"}
    after = store.experiment(eid)
    assert (after["phase"], after["hold_reason"]) == (before["phase"], before["hold_reason"])
    assert store.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY id", (eid,)) == stages
    assert store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)) == artifacts_before
    assert agent.calls == 0
    assert len(store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,))) == 1
    outstanding.cancel()


async def test_archived_legacy_output_reconciliation_refuses_without_state_change(failed_directory_run):
    store, agent, workflow, eid, _ = failed_directory_run
    store.archive_legacy_experiments()

    async def forbidden(*args, **kwargs):
        pytest.fail("archived recovery must not query the scheduler")

    workflow.scheduler.reconcile = forbidden
    workflow.scheduler.observe = forbidden
    before = (
        store.experiment(eid),
        store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)),
        store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)),
        store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)),
        store.rows("SELECT kind,payload FROM events WHERE experiment_id=? ORDER BY seq", (eid,)),
    )
    with pytest.raises(ValueError, match="archived legacy"):
        await workflow.reconcile_outputs(eid)
    after = (
        store.experiment(eid),
        store.rows("SELECT * FROM jobs WHERE experiment_id=?", (eid,)),
        store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)),
        store.rows("SELECT * FROM artifacts WHERE experiment_id=?", (eid,)),
        store.rows("SELECT kind,payload FROM events WHERE experiment_id=? ORDER BY seq", (eid,)),
    )
    assert after == before
    assert agent.calls == 0
    await workflow.close()
