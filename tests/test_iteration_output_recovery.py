"""Contract-3 recovery of a failed saved attempt through the production observer.

Every case drives the real synthetic demo recipe: the plan and prepare turns run,
``DemoScheduler`` executes the real commands until the evaluate job is saved as
``submitted``, and only then is its bounded metrics file corrupted and restored.
The validation failure is therefore produced by the production observer, never by
inserted rows or an edited demo state file.
"""

import json
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pytest
from helpers import prepared, wait_until

from experimenttracker import artifacts
from experimenttracker.slurm import DemoScheduler, Observation
from experimenttracker.store import encode
from experimenttracker.workflow import Workflow

CORRUPT_METRICS = b'{"schema_version":1,"protocol":"truncated'
LEGACY_HOOKS = (
    "verify_snapshot",
    "local_output_identity",
    "local_output_manifest",
)


def evaluate_job(store, eid):
    return store.one(
        "SELECT * FROM jobs WHERE experiment_id=? AND stage_id='evaluate' ORDER BY created_at DESC LIMIT 1",
        (eid,),
    )


def persisted(store, eid):
    """Every saved row a recovery attempt may touch, in a stable order."""
    exp = store.experiment(eid)
    return {
        "experiment": {
            key: exp[key]
            for key in ("phase", "hold_reason", "error", "pause_compute", "evidence_revision", "revision")
        },
        "partial_results": exp["partial_results"],
        "jobs": store.rows(
            "SELECT intent_id,stage_id,attempt,state,job_id,scheduler_state,exit_code,error "
            "FROM jobs WHERE experiment_id=? ORDER BY intent_id",
            (eid,),
        ),
        "stages": store.rows(
            "SELECT id,state,error,output_manifest FROM stages WHERE experiment_id=? ORDER BY id", (eid,)
        ),
        "artifacts": store.rows(
            "SELECT id,name,stage_id,path,size,evidence_revision FROM artifacts "
            "WHERE experiment_id=? ORDER BY id",
            (eid,),
        ),
        "output_refs": store.rows(
            "SELECT id,intent_id,name,path,kind,context,host FROM output_refs "
            "WHERE experiment_id=? ORDER BY id",
            (eid,),
        ),
        "stage_producers": store.rows(
            "SELECT stage_id,intent_id,rationale FROM stage_producers WHERE experiment_id=? ORDER BY stage_id",
            (eid,),
        ),
        "output_bindings": store.rows("SELECT consumer_intent,output_id FROM output_bindings"),
        "artifact_registrations": store.rows("SELECT artifact_id,event_id FROM artifact_registrations"),
        "report_versions": store.rows(
            "SELECT version,request_id,state FROM report_versions WHERE experiment_id=? ORDER BY version",
            (eid,),
        ),
        "events": store.rows("SELECT kind,payload FROM events WHERE experiment_id=? ORDER BY seq", (eid,)),
        "outbox": store.rows("SELECT id FROM outbox ORDER BY id"),
    }


def forbid_experiment_root_traversal(patch):
    """Legacy recovery re-reads frozen sources and the main folder; an attempt must not."""

    def forbidden(*args, **kwargs):
        raise AssertionError("contract-3 recovery must not re-validate the experiment root")

    def no_iterdir(self):
        raise AssertionError("contract-3 recovery must not walk a directory")

    for name in LEGACY_HOOKS:
        patch.setattr(artifacts, name, forbidden)
    patch.setattr(Path, "iterdir", no_iterdir)


class FailedEvaluate:
    """A genuine demo run stopped at the saved attempt that failed output validation."""

    def __init__(self, store, scheduler, agent, workflow, eid):
        self.store, self.scheduler, self.agent = store, scheduler, agent
        self.workflow, self.eid = workflow, eid
        self.job = evaluate_job(store, eid)
        self.attempt_root = Path(self.job["execution_root"])
        self.metrics = self.attempt_root / "metrics.json"
        self.metrics_bytes = self.metrics.read_bytes()

    def result(self):
        return {
            "experiment_id": self.eid,
            "job_id": self.job["job_id"],
            "stage": "evaluate",
            "pause_compute": True,
        }


@pytest.fixture
async def failed_evaluate(runtime):
    store, scheduler, agent, workflow, eid = await prepared(runtime)

    def submitted():
        job = evaluate_job(store, eid)
        return job if job and job["state"] == "submitted" else None

    async def tick():
        await workflow.tick(force_observe=True)

    job = await wait_until(submitted, ticks=tick, describe=lambda: persisted(store, eid))
    metrics = Path(job["execution_root"]) / "metrics.json"
    saved_bytes = metrics.read_bytes()
    assert 0 < len(saved_bytes) <= artifacts.MAX_ARTIFACT_BYTES
    metrics.write_bytes(CORRUPT_METRICS)
    await wait_until(
        lambda: store.experiment(eid)["phase"] == "failed",
        ticks=tick,
        describe=lambda: persisted(store, eid),
    )
    # Restore the genuine bounded bytes: recovery is measured against where the
    # outputs are, not against a corrupt file.
    metrics.write_bytes(saved_bytes)
    assert metrics.read_bytes() == saved_bytes
    failed, exp = evaluate_job(store, eid), store.experiment(eid)
    assert (failed["state"], exp["hold_reason"]) == ("failed", "validation_failed")
    assert failed["error"] == exp["error"]
    assert (failed["scheduler_state"], failed["exit_code"]) == ("COMPLETED", "0:0")
    obligation = store.one(
        "SELECT state,owner,wait FROM obligations WHERE id LIKE ? ORDER BY id DESC LIMIT 1",
        (f"job:{failed['intent_id']}:%",),
    )
    assert obligation == {"state": "satisfied", "owner": "external", "wait": f"job:{failed['job_id']}"}
    assert not store.rows("SELECT * FROM output_refs WHERE intent_id=?", (failed["intent_id"],))
    assert exp["partial_results"] is None
    assert (scheduler.submissions, agent.calls) == (2, 2)
    yield FailedEvaluate(store, scheduler, agent, workflow, eid)
    await workflow.close()


@pytest.mark.usefixtures("contract_three_engine")
async def test_recovery_adopts_failed_attempt_outputs_once(failed_evaluate, monkeypatch):
    case = failed_evaluate
    store, workflow, eid, job = case.store, case.workflow, case.eid, case.job
    before = persisted(store, eid)
    external = store.one("SELECT * FROM obligations WHERE id=?", (f"job:{job['intent_id']}:1",))
    assert (external["owner"], external["kind"], external["wait"], external["state"]) == (
        "external",
        "job",
        f"job:{job['job_id']}",
        "satisfied",
    )
    with monkeypatch.context() as patch:
        forbid_experiment_root_traversal(patch)
        assert await workflow.reconcile_outputs(eid) == case.result()
        assert await workflow.reconcile_outputs(eid) == case.result()
    after = persisted(store, eid)
    exp = store.experiment(eid)
    assert (exp["phase"], exp["hold_reason"], exp["pause_compute"]) == (
        "training",
        "new_compute_paused",
        1,
    )
    assert exp["error"] is None
    saved = evaluate_job(store, eid)
    assert (saved["job_id"], saved["error"]) == (job["job_id"], None)
    assert (saved["state"], saved["scheduler_state"], saved["exit_code"]) == ("complete", "COMPLETED", "0:0")
    settled = store.one("SELECT * FROM obligations WHERE id=?", (external["id"],))
    assert settled["state"] == "satisfied"
    assert store.one(
        "SELECT state,error,output_manifest FROM stages WHERE experiment_id=? AND id='evaluate'", (eid,)
    ) == {"state": "complete", "error": None, "output_manifest": encode(["metrics.json"])}
    [ref] = store.rows("SELECT * FROM output_refs WHERE intent_id=? ORDER BY name", (job["intent_id"],))
    assert ref["id"] == str(uuid5(NAMESPACE_URL, job["intent_id"] + ":metrics.json"))
    assert ref["path"] == str(case.attempt_root / "metrics.json")
    assert (ref["name"], ref["kind"], ref["context"]) == ("metrics.json", "file", "acacia")
    assert ref["host"] == workflow.config.executor_id
    assert store.one(
        "SELECT intent_id,rationale FROM stage_producers WHERE experiment_id=? AND stage_id='evaluate'",
        (eid,),
    ) == {"intent_id": job["intent_id"], "rationale": "successful declared task outputs"}
    partial = json.loads(exp["partial_results"])
    assert list(partial["tasks"]) == ["evaluate"]
    [metric] = partial["tasks"]["evaluate"]["result"]["metrics"]
    assert (metric["name"], metric["value"]) == ("score", 42)
    [report] = store.rows(
        "SELECT request_id,state,document FROM report_versions WHERE experiment_id=?", (eid,)
    )
    assert (report["request_id"], report["state"]) == ("working-results:" + job["intent_id"], "queued")
    remaining = next(
        block["text"]
        for block in json.loads(report["document"])["blocks"]
        if block.get("kind") == "paragraph"
    )
    assert "Synthetic plot" in remaining and "Synthetic evaluation" not in remaining
    added = [row["kind"] for row in after["events"][len(before["events"]) :]]
    assert sorted(added) == ["artifacts_registered", "output_validation_reconciled", "report_requested"]
    assert len(after["outbox"]) == len(before["outbox"]) + 3
    trained = [row for row in before["output_refs"] if row["intent_id"] != job["intent_id"]]
    assert {row["name"] for row in trained} == {"model.json", "checkpoints"}
    assert [row for row in after["output_refs"] if row["intent_id"] != job["intent_id"]] == trained
    assert [row for row in after["jobs"] if row["stage_id"] != "evaluate"] == [
        row for row in before["jobs"] if row["stage_id"] != "evaluate"
    ]
    known = {row["id"] for row in before["artifacts"]}
    assert [row["name"] for row in after["artifacts"] if row["id"] not in known] == ["metrics.json"]
    assert not store.rows("SELECT * FROM jobs WHERE experiment_id=? AND stage_id='plot'", (eid,))
    await workflow.advance(eid)
    assert (case.scheduler.submissions, case.agent.calls) == (2, 2)
    assert store.experiment(eid)["hold_reason"] == "new_compute_paused"
    settled = persisted(store, eid)
    restarted = Workflow(workflow.config, store, workflow.scheduler, case.agent)
    try:
        assert await restarted.reconcile_outputs(eid) == case.result()
        assert persisted(store, eid) == settled
    finally:
        await restarted.close()


@pytest.mark.parametrize(
    ("failure", "reason"),
    [
        ("identity", "scheduler identity differs"),
        ("running", "confirmed successful terminal accounting"),
        ("exit", "confirmed successful terminal accounting"),
        ("invalid_output", "Unterminated string"),
        ("symlink", "regular file inside the registered root"),
    ],
)
@pytest.mark.usefixtures("contract_three_engine")
async def test_recovery_refuses_unconfirmed_or_invalid_saved_attempt(
    failed_evaluate, monkeypatch, failure, reason
):
    case = failed_evaluate
    before = persisted(case.store, case.eid)
    if failure == "identity":

        async def reconcile(self, saved_intent):
            return "demo-other"

        monkeypatch.setattr(DemoScheduler, "reconcile", reconcile)
    elif failure in ("running", "exit"):

        async def observe(self, job_id):
            return Observation("RUNNING") if failure == "running" else Observation("COMPLETED", "1:0")

        monkeypatch.setattr(DemoScheduler, "observe", observe)
    elif failure == "invalid_output":
        case.metrics.write_bytes(CORRUPT_METRICS)
    else:
        declared = case.attempt_root / "declared-metrics.json"
        declared.write_bytes(case.metrics_bytes)
        case.metrics.unlink()
        case.metrics.symlink_to(declared)
    with pytest.raises(ValueError, match=reason):
        await case.workflow.reconcile_outputs(case.eid)
    assert persisted(case.store, case.eid) == before
    assert case.scheduler.submissions == 2


@pytest.mark.usefixtures("contract_three_engine")
async def test_recovery_rolls_back_attempt_adoption_on_transaction_failure(failed_evaluate, monkeypatch):
    case = failed_evaluate
    store, workflow, eid, job = case.store, case.workflow, case.eid, case.job
    before = persisted(store, eid)
    obligations_before = store.rows("SELECT * FROM obligations WHERE experiment_id=? AND kind='job'", (eid,))
    published = workflow.root(eid) / "metrics.json"
    assert not published.exists()
    emit, failing = store.emit, {"on": True}

    def guarded_emit(db, exp_id, kind, payload=None):
        if failing["on"] and kind == "output_validation_reconciled":
            raise RuntimeError("simulated transaction failure")
        return emit(db, exp_id, kind, payload)

    monkeypatch.setattr(store, "emit", guarded_emit)
    with pytest.raises(RuntimeError, match="transaction failure"):
        await workflow.reconcile_outputs(eid)
    assert persisted(store, eid) == before
    assert (
        store.rows("SELECT * FROM obligations WHERE experiment_id=? AND kind='job'", (eid,))
        == obligations_before
    )
    assert not store.rows("PRAGMA foreign_key_check")
    # The small published copy may survive the rolled-back attempt; it is
    # unregistered and a retry with the same bytes is a no-op, not cleanup.
    assert published.read_bytes() == case.metrics_bytes
    assert not store.rows("SELECT * FROM artifacts WHERE experiment_id=? AND name='metrics.json'", (eid,))
    failing["on"] = False
    assert await workflow.reconcile_outputs(eid) == case.result()
    assert published.read_bytes() == case.metrics_bytes
    assert len(store.rows("SELECT * FROM output_refs WHERE intent_id=?", (job["intent_id"],))) == 1
    assert (
        len(store.rows("SELECT * FROM artifacts WHERE experiment_id=? AND name='metrics.json'", (eid,))) == 1
    )
    assert len(store.rows("SELECT * FROM report_versions WHERE experiment_id=?", (eid,))) == 1
    assert (
        len(
            store.rows(
                "SELECT * FROM events WHERE experiment_id=? AND kind='output_validation_reconciled'", (eid,)
            )
        )
        == 1
    )
    assert store.experiment(eid)["pause_compute"]
    assert not store.rows("PRAGMA foreign_key_check")


@pytest.mark.usefixtures("contract_three_engine")
async def test_restarted_observation_adopts_saved_terminal_attempt_once(runtime):
    store, scheduler, agent, workflow, eid = await prepared(runtime)

    def submitted():
        job = evaluate_job(store, eid)
        return job if job and job["state"] == "submitted" else None

    async def tick():
        await workflow.tick(force_observe=True)

    job = await wait_until(submitted, ticks=tick, describe=lambda: persisted(store, eid))
    assert not store.rows("SELECT * FROM output_refs WHERE intent_id=?", (job["intent_id"],))
    await workflow.close()
    restarted = Workflow(workflow.config, store, scheduler, agent)
    try:
        await restarted.observe_jobs()
        observed, revision = persisted(store, eid), store.experiment(eid)["revision"]
        [ref] = store.rows("SELECT * FROM output_refs WHERE intent_id=?", (job["intent_id"],))
        assert evaluate_job(store, eid)["state"] == "complete"
        assert ref["id"] == str(uuid5(NAMESPACE_URL, job["intent_id"] + ":metrics.json"))
        assert ref["name"] == "metrics.json"
        await restarted.observe_jobs()
        assert persisted(store, eid) == observed
        assert store.experiment(eid)["revision"] == revision
        assert (scheduler.submissions, agent.calls) == (2, 2)
    finally:
        await restarted.close()
