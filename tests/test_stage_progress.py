import copy
import json
import os
import sys
import time
from datetime import UTC, datetime, timedelta, timezone
from uuid import uuid4

import pytest
from helpers import PLAN, seed_saved_contract3

from experimenttracker import artifacts
from experimenttracker.models import Implementation
from experimenttracker.progress import LOG_TAIL_BYTES, read_job_progress


def progress_experiment(store, workflow):
    eid = seed_saved_contract3(store, "Synthetic progress")
    root = workflow.root(eid)
    work = root / "work"
    work.mkdir(parents=True, exist_ok=True)
    (work / "recipe.py").write_text("pass\n")
    aliases = {"train": "train_linear", "evaluate": "train_quadratic", "plot": "evaluate_and_plot"}
    payload = copy.deepcopy(PLAN)
    payload["stages"] = [
        {
            **stage,
            "id": aliases[stage["id"]],
            "needs": [aliases[need] for need in stage["needs"]],
        }
        for stage in payload["stages"]
    ]
    plan = Implementation.model_validate(payload)
    manifest = artifacts.freeze_implementation(
        root, plan, python=sys.executable, project=None, workspace=work
    )
    store.resolve_implementation(eid, manifest)
    return eid


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


def test_iso_timestamps_are_normalized_and_subject_to_same_bounds(log_job):
    root, job, log, now = log_job

    def iso(timestamp, tz=UTC):
        return datetime.fromtimestamp(timestamp, tz).isoformat()

    log.write_text(
        line(sample(job, now, timestamp=iso(now - 1)))
        + line(sample(job, now, completed=50, timestamp=iso(now, timezone(timedelta(hours=2)))))
    )
    result = read_job_progress(root, job, now=now)
    assert result["completed"] == 50
    assert result["reported_at"] == pytest.approx(now, abs=0.000001)
    log.write_text(line(sample(job, now, timestamp=iso(now).replace("+00:00", "Z"))))
    assert read_job_progress(root, job, now=now) is not None
    for timestamp in ("invalid", "2026-11-04T06:00:00", iso(now + 61), iso(job["started_at"] - 61)):
        log.write_text(line(sample(job, now, timestamp=timestamp)))
        assert read_job_progress(root, job, now=now) is None
