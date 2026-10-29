import json
import os
import time
from uuid import uuid4

import pytest

from experimenttracker.progress import LOG_TAIL_BYTES, read_job_progress
from experimenttracker.slurm import Observation
from experimenttracker.store import Store, encode


def sample(job, now, **extra):
    return {
        "stage": job["stage_id"],
        "completed": 25,
        "total": 100,
        "timestamp": now,
        "eta_seconds": 300,
        **extra,
    }


def line(value):
    return "ET_PROGRESS " + json.dumps(value) + "\n"


@pytest.fixture
def log_job(tmp_path):
    now = time.time()
    job = {"intent_id": str(uuid4()), "job_id": "123", "stage_id": "fit", "started_at": now - 60}
    log = tmp_path / f"et-{job['intent_id']}-{job['job_id']}.out"
    return tmp_path, job, log, now


def test_exact_identity_bounded_tail_complete_lines_and_numeric_only(log_job):
    root, job, log, now = log_job
    assert read_job_progress(root, job, now=now) is None
    log.write_text(
        "private output\n" * LOG_TAIL_BYTES
        + line(sample(job, now, credential="private", command="never published"))
        + line(sample(job, now + 1, stage="other"))
        + "ET_PROGRESS {invalid}\n"
        + "ET_PROGRESS "
        + "[" * 1500
        + "\n"
        + line(sample(job, now + 2, completed=75)).rstrip("\n")
    )
    result = read_job_progress(root, job, now=now)
    assert result == {"completed": 25, "total": 100, "reported_at": now, "eta_seconds": 300}
    assert "private" not in json.dumps(result)
    assert read_job_progress(root, {**job, "job_id": "124"}, now=now) is None
    assert read_job_progress(root, {**job, "intent_id": str(uuid4())}, now=now) is None
    with log.open("a") as stream:
        stream.write("\n")
    assert read_job_progress(root, job, now=now)["completed"] == 75


@pytest.mark.parametrize(
    "fields",
    [
        {"total": 0},
        {"completed": -1},
        {"completed": 101},
        {"completed": True},
        {"completed": "25"},
        {"total": float("nan")},
        {"completed": float("inf")},
        {"eta_seconds": -1},
        {"eta_seconds": True},
        {"eta_seconds": float("inf")},
        {"timestamp": float("nan")},
        {"timestamp": 0},
        {"timestamp": 1e30},
        {"completed": 10**500},
    ],
)
def test_invalid_telemetry_is_ignored_without_affecting_execution(log_job, fields):
    root, job, log, now = log_job
    log.write_text(line(sample(job, now, **fields)))
    assert read_job_progress(root, job, now=now) is None


def test_missing_eta_old_attempt_future_sample_and_unsafe_files(log_job):
    root, job, log, now = log_job
    log.write_text(line(sample(job, now, eta_seconds=None)))
    assert read_job_progress(root, job, now=now)["eta_seconds"] is None
    log.write_text(line(sample(job, now + 61)))
    assert read_job_progress(root, job, now=now) is None
    log.write_text(line(sample(job, job["started_at"] - 61)))
    assert read_job_progress(root, job, now=now) is None
    log.rename(root / "private.log")
    log.symlink_to(root / "private.log")
    assert read_job_progress(root, job, now=now) is None
    log.unlink()
    os.mkfifo(log)
    assert read_job_progress(root, job, now=now) is None


async def test_progress_publication_restart_replay_and_readiness(runtime, monkeypatch):
    store, scheduler, agent, workflow = runtime
    eid = store.command(uuid4(), "create", {"brief": "Synthetic progress", "recipe_id": "toy_compare_v1"})[
        "experiment_id"
    ]
    now = time.time()
    intent = str(uuid4())
    store.stage(eid, "train_linear", "running")
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,snapshot_hash,"
            "created_at,job_id,scheduler_state,started_at) VALUES(?,?,?,'submitted','hash',?,'hash',?,'123','RUNNING',?)",
            (
                intent,
                eid,
                "train_linear",
                encode(workflow.config.slurm.defaults().model_dump()),
                now - 60,
                now - 60,
            ),
        )
    root = workflow.root(eid)
    root.mkdir(parents=True, exist_ok=True)
    log = root / f"et-{intent}-123.out"
    job = store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    log.write_text(line(sample(job, now)))

    async def observe(job_id):
        assert job_id == "123"
        return Observation("RUNNING", started_at=now - 60)

    monkeypatch.setattr(scheduler, "observe", observe)
    await workflow.observe_jobs()
    projection = store.projection(eid)
    assert projection["jobs"][0]["progress"]["completed"] == 25
    event = store.one("SELECT * FROM events WHERE experiment_id=? AND kind='job_progress'", (eid,))
    outbox = json.loads(store.one("SELECT payload FROM outbox WHERE id=?", (event["id"],))["payload"])
    assert outbox["experiment"]["jobs"][0]["progress"] == projection["jobs"][0]["progress"]
    assert event["revision"] == projection["revision"]
    reopened = Store(store.path)
    workflow.store = reopened
    try:
        await workflow.observe_jobs()
        assert reopened.experiment(eid)["revision"] == projection["revision"]
        # Truncation/old telemetry never refreshes or erases the saved sample.
        log.write_text(line(sample(job, now - 1)))
        await workflow.observe_jobs()
        assert reopened.projection(eid)["jobs"][0]["progress"] == projection["jobs"][0]["progress"]
        log.write_text(line(sample(job, now + 1, completed=100)))
        await workflow.observe_jobs()
        assert reopened.projection(eid)["jobs"][0]["progress"]["completed"] == 100
        assert (
            reopened.one("SELECT state FROM stages WHERE experiment_id=? AND id='train_linear'", (eid,))[
                "state"
            ]
            == "running"
        )
        assert reopened.one("SELECT state FROM jobs WHERE intent_id=?", (intent,))["state"] == "submitted"
        assert scheduler.submissions == agent.calls == 0
        revision = reopened.experiment(eid)["revision"]
        log.write_text(line(sample(job, now + 2, completed=101)))
        await workflow.observe_jobs()
        assert reopened.experiment(eid)["revision"] == revision
        assert not reopened.rows("PRAGMA foreign_key_check")
    finally:
        workflow.store = store
        reopened.close()


async def test_progress_and_outbox_roll_back_together(runtime, monkeypatch):
    store, scheduler, _, workflow = runtime
    eid = store.command(uuid4(), "create", {"brief": "Synthetic progress", "recipe_id": "toy_compare_v1"})[
        "experiment_id"
    ]
    now, intent = time.time(), str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,snapshot_hash,"
            "created_at,job_id,scheduler_state) VALUES(?,?,?,'submitted','hash','{}','hash',?,'123','RUNNING')",
            (intent, eid, "train_linear", now),
        )
    job = store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    root = workflow.root(eid)
    root.mkdir(parents=True, exist_ok=True)
    (root / f"et-{intent}-123.out").write_text(line(sample(job, now)))

    async def observe(_):
        return Observation("RUNNING")

    class PublicationInterrupted(Exception):
        pass

    def interrupt(*args, **kwargs):
        raise PublicationInterrupted

    monkeypatch.setattr(scheduler, "observe", observe)
    monkeypatch.setattr(store, "emit", interrupt)
    revision = store.experiment(eid)["revision"]
    with pytest.raises(PublicationInterrupted):
        await workflow.observe_jobs()
    assert store.one("SELECT progress FROM jobs WHERE intent_id=?", (intent,))["progress"] is None
    assert store.experiment(eid)["revision"] == revision
    assert not store.rows("SELECT * FROM events WHERE kind='job_progress'")
