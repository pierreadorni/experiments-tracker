"""Focused contract 4 scheduler observation tests using manually saved jobs."""

import json
import struct
from types import SimpleNamespace

import pytest
from helpers import seed_saved_contract4
from tensorboard.compat.proto.event_pb2 import Event
from tensorboard.compat.proto.summary_pb2 import Summary
from tensorboard.compat.tensorflow_stub.pywrap_tensorflow import masked_crc32c

from experimenttracker.research import job_observe, wakeups
from experimenttracker.store import encode


class FakeScheduler:
    def __init__(self, observed):
        self.observed = observed
        self.calls = []

    async def observe(self, job_id):
        self.calls.append(job_id)
        if isinstance(self.observed, Exception):
            raise self.observed
        return self.observed


def saved_job(store, *, pending=False):
    eid = seed_saved_contract4(store, "synthetic observer")
    intent = "manual-observe-job"
    with store.tx() as db:
        db.execute("INSERT INTO stages(experiment_id,id,updated_at) VALUES(?,'research',1)", (eid,))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at,execution_manifest) VALUES(?,?,?,1,?,?,'manual','{}','synthetic',1,?)",
            (
                intent,
                eid,
                "research",
                "submitting" if pending else "submitted",
                None if pending else "42",
                encode({"executor": "slurm"}),
            ),
        )
        from experimenttracker.core import job_obligations

        job_obligations.sync_job(db, intent)
    return store.experiment(eid), store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))


@pytest.mark.asyncio
async def test_terminal_observation_saves_job_obligation_and_wakeup_atomically(runtime):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    scheduler = FakeScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    workflow.scheduler_for = lambda *_: scheduler

    result = await job_observe.observe(workflow, exp, job)

    assert scheduler.calls == ["42"]
    assert result["state"] == "complete"
    assert result["scheduler_state"] == "COMPLETED"
    wake = store.one("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))
    assert json.loads(wake["reason"]) == {"event": wakeups.JOB_TERMINAL, "source_id": job["intent_id"]}
    with store.tx() as db:
        assert (
            db.execute(
                "SELECT state FROM obligations WHERE id LIKE ?", (f"job:{job['intent_id']}:%",)
            ).fetchone()[0]
            == "satisfied"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "observed",
    [
        SimpleNamespace(state="RUNNING", exit_code=None, ended_at=None),
        SimpleNamespace(state="COMPLETED", exit_code=None, ended_at=None),
    ],
)
async def test_healthy_or_accounting_pending_job_does_not_wake(runtime, observed):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    workflow.scheduler_for = lambda *_: FakeScheduler(observed)

    assert await job_observe.observe(workflow, exp, job) is None
    assert store.rows("SELECT * FROM agent_wakeups") == []
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "submitted"


@pytest.mark.asyncio
async def test_workflow_routes_contract4_saved_jobs_before_contract3_logic(runtime, monkeypatch):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    scheduler = FakeScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    workflow.scheduler_for = lambda *_: scheduler
    monkeypatch.setattr("experimenttracker.iteration.enabled", lambda *_: pytest.fail("C3 logic reached"))

    await workflow.observe_jobs()
    await workflow.observe_jobs()

    assert scheduler.calls == ["42"]
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "complete"
    assert len(store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


@pytest.mark.asyncio
async def test_workflow_routes_healthy_contract4_without_wakeup(runtime):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    workflow.scheduler_for = lambda *_: FakeScheduler(
        SimpleNamespace(state="RUNNING", exit_code=None, ended_at=None)
    )

    await workflow.observe_jobs()

    assert store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],)) == []
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "submitted"


@pytest.mark.asyncio
async def test_healthy_local_c4_observation_persists_tensorboard_projection_without_wake(runtime, tmp_path):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    root = tmp_path / "attempt"
    event_dir = root / "tensorboard" / "fit"
    event_dir.mkdir(parents=True)
    event = Event(
        wall_time=10,
        step=3,
        summary=Summary(value=[Summary.Value(tag="loss/train", simple_value=0.25)]),
    )
    payload = event.SerializeToString()
    size = struct.pack("<Q", len(payload))
    (event_dir / "events.out.tfevents.test").write_bytes(
        size + struct.pack("<I", masked_crc32c(size)) + payload + struct.pack("<I", masked_crc32c(payload))
    )
    manifest = {
        "executor": "slurm",
        "research_request": {
            "tensorboard": {
                "log_dir": "tensorboard/fit",
                "scalar_tag": "loss/train",
                "image_tag": None,
            }
        },
    }
    with store.tx() as db:
        db.execute(
            "UPDATE jobs SET execution_root=?,execution_manifest=? WHERE intent_id=?",
            (str(root), encode(manifest), job["intent_id"]),
        )
    workflow.scheduler_for = lambda *_: FakeScheduler(
        SimpleNamespace(state="RUNNING", exit_code=None, ended_at=None)
    )

    await workflow.observe_jobs()

    projected = store.projection(exp["id"])["jobs"][0]
    assert projected["tensorboard"]["scalar"] == {"tag": "loss/train", "points": [[3, 0.25]]}
    assert projected["state"] == "submitted"
    assert store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],)) == []


async def test_missing_local_job_id_is_adopted_before_terminal_observation(runtime, monkeypatch):
    store, _, _, workflow = runtime
    exp, original = saved_job(store, pending=True)
    scheduler = FakeScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    reconciled = []

    async def reconcile(intent_id):
        reconciled.append(intent_id)
        return "42"

    scheduler.reconcile = reconcile
    workflow.scheduler_for = lambda *_: scheduler
    monkeypatch.setattr("experimenttracker.iteration.enabled", lambda *_: pytest.fail("C3 logic reached"))

    workflow.submissions_in_flight.add(original["intent_id"])
    await workflow.observe_jobs()
    assert not reconciled
    workflow.submissions_in_flight.remove(original["intent_id"])
    await workflow.observe_jobs()
    await workflow.observe_jobs()

    assert reconciled == [original["intent_id"]]
    assert scheduler.calls == ["42"]
    assert store.one("SELECT state,job_id FROM jobs WHERE intent_id=?", (original["intent_id"],)) == {
        "state": "complete",
        "job_id": "42",
    }
    assert len(store.rows("SELECT id FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


class FakeRemoteScheduler(FakeScheduler):
    def __init__(self, observed=None, delivery=None):
        super().__init__(observed)
        self.delivery = delivery
        self.workflow = None
        self.delivery_calls = 0
        self.due_calls = 0
        self.is_due = True
        self.preview_calls = 0

    async def fetch_event_preview(self, job):
        self.preview_calls += 1
        return {
            "scalar": {"tag": "loss", "points": [[1, 2.0]]},
            "image": None,
            "image_pending": False,
            "updated_at": 1,
            "limited": False,
        }

    def due(self, job, now):
        self.due_calls += 1
        return self.is_due

    async def fetch_result(self, job):
        self.delivery_calls += 1
        if isinstance(self.delivery, Exception):
            raise self.delivery


async def remote_job(store, workflow, *, pending=False):
    exp, job = saved_job(store, pending=pending)
    contract = dict(
        exp["contract"],
        execution={
            **exp["contract"].get("execution", {}),
            "context": "jean-zay",
            "ssh_host": "remote",
            "remote_project": "/remote/project",
        },
    )
    root = workflow.projects.base_root(exp["id"]) / "attempts" / job["intent_id"]
    root.mkdir(parents=True)
    remote_prefix = f"/remote/project/.experimenttracker/executions/{exp['id']}/{job['intent_id']}"
    layout = {
        "host": "remote",
        "remote_base": "/remote/project",
        "remote_run": remote_prefix + "/run",
        "remote_project": remote_prefix + "/project",
        "local_project": str(workflow.working_directory(exp)),
        "local_run": str(root),
    }
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), exp["id"]))
        db.execute(
            "UPDATE jobs SET execution_root=?,execution_manifest=?,scheduler_state=NULL,exit_code=NULL,ended_at=NULL "
            "WHERE intent_id=?",
            (
                str(root),
                encode(
                    {
                        "executor": "slurm",
                        "execution": {
                            "context": "jean-zay",
                            "settings": contract["execution"],
                            "layout": layout,
                            "memory_allocation": "cluster-assigned; --mem is not submitted; memory_mb_per_job is a planning estimate",
                        },
                    }
                ),
                job["intent_id"],
            ),
        )
    return store.experiment(exp["id"]), store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))


@pytest.mark.asyncio
async def test_remote_success_persists_accounting_then_delivers_and_wakes_once(runtime):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    scheduler = FakeRemoteScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    scheduler.workflow = workflow
    workflow.scheduler_for = lambda *_: scheduler

    await job_observe.observe_remote(workflow, exp, job)
    assert store.one(
        "SELECT state,scheduler_state,ended_at FROM jobs WHERE intent_id=?", (job["intent_id"],)
    ) == {"state": "complete", "scheduler_state": "COMPLETED", "ended_at": 9}
    assert scheduler.calls == ["42"]
    assert scheduler.delivery_calls == 1
    assert scheduler.preview_calls == 0
    await job_observe.observe_remote(
        workflow, exp, store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    )
    assert scheduler.delivery_calls == 1
    assert len(store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


@pytest.mark.asyncio
async def test_remote_running_preview_persists_without_waking(runtime):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    scheduler = FakeRemoteScheduler(SimpleNamespace(state="RUNNING", exit_code=None, ended_at=None))
    scheduler.workflow = workflow
    workflow.scheduler_for = lambda *_: scheduler

    await job_observe.observe_remote(workflow, exp, job)

    saved = store.one("SELECT tensorboard,state FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved["state"] == "submitted"
    assert json.loads(saved["tensorboard"])["scalar"]["points"] == [[1, 2.0]]
    assert scheduler.preview_calls == 1
    assert store.rows("SELECT * FROM agent_wakeups") == []
    assert len(store.rows("SELECT * FROM events WHERE kind='job_progress'")) == 1


@pytest.mark.asyncio
async def test_remote_pending_delivery_restarts_from_saved_accounting_without_observation(runtime):
    from experimenttracker.remote import TransferPending

    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    scheduler = FakeRemoteScheduler(
        SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9), TransferPending("later")
    )
    workflow.scheduler_for = lambda *_: scheduler

    await job_observe.observe_remote(workflow, exp, job)
    saved = store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved["state"] == "submitted" and saved["ended_at"] == 9
    assert store.rows("SELECT * FROM agent_wakeups") == []
    scheduler.delivery = None
    await job_observe.observe_remote(workflow, exp, saved)
    assert scheduler.calls == ["42"]
    assert scheduler.delivery_calls == 2
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "complete"
    assert len(store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


@pytest.mark.asyncio
async def test_remote_completed_nonzero_exit_is_saved_as_failure(runtime):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    scheduler = FakeRemoteScheduler(SimpleNamespace(state="COMPLETED", exit_code="2:0", ended_at=10))
    workflow.scheduler_for = lambda *_: scheduler

    await job_observe.observe_remote(workflow, exp, job)

    saved = store.one("SELECT state,error,ended_at FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved == {"state": "failed", "error": "job exit 2:0", "ended_at": 10}
    assert scheduler.delivery_calls == 0
    assert len(store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


@pytest.mark.asyncio
async def test_remote_failed_job_wakes_once_without_delivery(runtime):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    scheduler = FakeRemoteScheduler(SimpleNamespace(state="CANCELLED", exit_code=None, ended_at=10))
    workflow.scheduler_for = lambda *_: scheduler

    await job_observe.observe_remote(workflow, exp, job)
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "failed"
    assert scheduler.delivery_calls == 0
    assert len(store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


@pytest.mark.asyncio
async def test_workflow_routes_remote_c4_through_cadenced_result_delivery(runtime, monkeypatch):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    scheduler = FakeRemoteScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    workflow.scheduler_for = lambda *_: scheduler
    monkeypatch.setattr("experimenttracker.iteration.enabled", lambda *_: pytest.fail("C3 logic reached"))

    await workflow.observe_jobs()
    await workflow.observe_jobs()

    assert scheduler.calls == ["42"]
    assert scheduler.delivery_calls == 1
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "complete"
    assert len(store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


async def test_remote_result_validation_failure_holds_without_marking_job_failed(runtime):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    scheduler = FakeRemoteScheduler(
        SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9), ValueError("saved route mismatch")
    )
    workflow.scheduler_for = lambda *_: scheduler

    await workflow.observe_jobs()

    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "submitted"
    assert store.experiment(exp["id"])["hold_reason"] == "executor_unavailable"
    assert store.rows("SELECT * FROM agent_wakeups") == []


async def test_remote_workflow_resolves_missing_id_at_poll_cadence(runtime, monkeypatch):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow, pending=True)
    scheduler = FakeRemoteScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    workflow.scheduler_for = lambda *_: scheduler
    reconciled = []

    async def resolve(_workflow, _exp, unresolved):
        reconciled.append(unresolved["intent_id"])
        from experimenttracker.core import job_obligations

        with store.tx() as db:
            before = job_obligations.locked_job(db, unresolved, "state")
            db.execute(
                "UPDATE jobs SET state='submitted',job_id='42' WHERE intent_id=?",
                (unresolved["intent_id"],),
            )
            job_obligations.sync_backend(db, unresolved["intent_id"], before=before)
        return store.one("SELECT * FROM jobs WHERE intent_id=?", (unresolved["intent_id"],))

    monkeypatch.setattr("experimenttracker.resolver.resolve", resolve)
    await workflow.observe_jobs()

    assert reconciled == [job["intent_id"]]
    assert scheduler.due_calls == 1
    assert scheduler.calls == ["42"]
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "complete"


async def test_workflow_cadence_suppresses_pending_delivery_until_one_terminal_wake(runtime):
    from experimenttracker.remote import TransferPending

    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    scheduler = FakeRemoteScheduler(
        SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9), TransferPending("later")
    )
    workflow.scheduler_for = lambda *_: scheduler

    await workflow.observe_jobs()
    assert scheduler.calls == ["42"]
    assert scheduler.delivery_calls == 1
    assert store.rows("SELECT * FROM agent_wakeups") == []

    scheduler.is_due = False
    scheduler.delivery = None
    await workflow.observe_jobs()
    assert scheduler.calls == ["42"]
    assert scheduler.delivery_calls == 1
    assert store.rows("SELECT * FROM agent_wakeups") == []

    scheduler.is_due = True
    await workflow.observe_jobs()
    await workflow.observe_jobs()
    assert scheduler.calls == ["42"]
    assert scheduler.delivery_calls == 2
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "complete"
    assert len(store.rows("SELECT * FROM agent_wakeups WHERE experiment_id=?", (exp["id"],))) == 1


async def test_workflow_rejects_invalid_saved_remote_route_without_rpc_or_c3(runtime, monkeypatch):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    with store.tx() as db:
        db.execute(
            "UPDATE jobs SET execution_manifest=? WHERE intent_id=?",
            (encode({"executor": "slurm"}), job["intent_id"]),
        )
    scheduler = FakeRemoteScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=9))
    workflow.scheduler_for = lambda *_: scheduler
    monkeypatch.setattr("experimenttracker.iteration.enabled", lambda *_: pytest.fail("C3 logic reached"))

    await workflow.observe_jobs()

    assert scheduler.calls == []
    assert scheduler.delivery_calls == 0
    saved = store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))
    assert saved["state"] == "submitted"
    assert store.experiment(exp["id"])["hold_reason"] == "executor_unavailable"
    assert store.rows("SELECT * FROM agent_wakeups") == []


async def test_remote_success_without_ended_at_is_accounting_lag(runtime):
    store, _, _, workflow = runtime
    exp, job = await remote_job(store, workflow)
    with store.tx() as db:
        db.execute(
            "UPDATE jobs SET scheduler_state=NULL,exit_code=NULL,ended_at=NULL WHERE intent_id=?",
            (job["intent_id"],),
        )
    job = store.one("SELECT * FROM jobs WHERE intent_id=?", (job["intent_id"],))
    scheduler = FakeRemoteScheduler(SimpleNamespace(state="COMPLETED", exit_code="0:0", ended_at=None))
    workflow.scheduler_for = lambda *_: scheduler

    await job_observe.observe_remote(workflow, exp, job)
    saved = store.one(
        "SELECT state,scheduler_state,ended_at FROM jobs WHERE intent_id=?", (job["intent_id"],)
    )
    assert saved == {"state": "submitted", "scheduler_state": None, "ended_at": None}
    assert scheduler.delivery_calls == 0
    assert store.rows("SELECT * FROM agent_wakeups") == []


async def test_scheduler_uncertainty_is_not_a_terminal_retry(runtime):
    store, _, _, workflow = runtime
    exp, job = saved_job(store)
    workflow.scheduler_for = lambda *_: FakeScheduler(TimeoutError("unreadable"))

    with pytest.raises(TimeoutError):
        await job_observe.observe(workflow, exp, job)
    assert store.rows("SELECT * FROM agent_wakeups") == []
    assert store.one("SELECT state FROM jobs WHERE intent_id=?", (job["intent_id"],))["state"] == "submitted"
