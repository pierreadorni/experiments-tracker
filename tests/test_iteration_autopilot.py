import json
from uuid import uuid4

import pytest

from experimenttracker.autopilot import Autopilot, calendar
from experimenttracker.autopilot_budget import WEEK, window
from experimenttracker.autopilot_models import AutopilotPolicy
from experimenttracker.config import JobResources
from experimenttracker.slurm import Observation
from experimenttracker.store import Conflict


def policy(now=1000000, **overrides):
    return AutopilotPolicy(
        subject="Synthetic software acceptance",
        scope="Infrastructure only",
        publication_goal="Researcher review",
        venue="Configurable venue",
        start_at=now,
        handoff_at=now + 10 * WEEK,
        submission_at=now + 11 * WEEK,
        weekly_gpu_hours=100,
        anchor_at=now,
        **overrides,
    )


def command(service, kind, **fields):
    run = service.view("test-project")["run"]
    return service.command(
        "test-project",
        {
            "request_id": str(uuid4()),
            "kind": kind,
            "expected_revision": run["revision"] if run else 0,
            **fields,
        },
    )


def activated(runtime, monkeypatch):
    store, _, _, wf = runtime
    monkeypatch.setattr("time.time", lambda: 1000000)
    service = Autopilot(store, wf.config)
    command(service, "configure", policy=policy().model_dump())
    command(service, "activate")
    return service, service.run("test-project")


def linked(service, run, gpus=1, minutes=2400):
    store = service.store
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic acceptance only", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    intent = str(uuid4())
    resources = JobResources(partition="test", gpus_per_job=gpus, walltime_minutes_per_job=minutes)
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
            (eid, run["id"], run["project_id"], 1000000),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,snapshot_hash,created_at,job_id) VALUES(?,?,'prepare','submitted','test',?,'test',?,?)",
            (intent, eid, json.dumps(resources.model_dump()), 1000000, "123"),
        )
        held = service.admission(db, store.experiment(eid), "prepare", resources, intent)
    return eid, intent, held


def support(service):
    path = service.config.state_dir / "autopilot-accounting.json"
    path.write_text(
        json.dumps({"acacia": {"verified_at": 1000000, "no_requeue": True, "allocation_intervals": "single"}})
    )


def test_fractional_calendar_borrowing_and_dst():
    p = policy()
    c = calendar(p, p.start_at)
    assert c["boundaries"]["foundation"] == p.start_at + WEEK / 10
    assert calendar(p, p.start_at + WEEK)["phase"] == "main_argument"
    assert calendar(p, p.start_at + 6 * WEEK, 5)["phase"] == "main_argument"
    assert calendar(p, p.handoff_at)["handoff_due"]
    assert window(p.anchor_at, p.anchor_at + WEEK) == (p.anchor_at + WEEK, p.anchor_at + 2 * WEEK)


def test_draft_no_work_revision_replay_and_authority(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    assert not service.store.rows("SELECT * FROM agent_actions")
    request = {"request_id": str(uuid4()), "kind": "pause", "expected_revision": run["revision"]}
    result = service.command("test-project", request)
    assert service.command("test-project", request) == result
    with pytest.raises(Conflict, match="identity"):
        service.command("test-project", {**request, "kind": "resume"})
    with pytest.raises(Conflict, match="stale"):
        command(service, "resume", expected_revision=0)
    command(service, "resume")
    revised = policy().model_dump()
    revised["anchor_at"] += 10
    with pytest.raises(Conflict, match="immutable"):
        command(service, "revise", policy=revised)


def test_budget_six_40_hour_jobs_and_reset(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    support(service)
    outcomes = [linked(service, run)[2] for _ in range(6)]
    assert outcomes[:2] == [None, None]
    assert all(o["reason"] == "autopilot_weekly_gpu_limit" for o in outcomes[2:])
    assert len(service.store.rows("SELECT * FROM autopilot_allocations")) == 2
    budget = service.budget.view(run["project_id"], policy(), 1000000 + WEEK)
    assert budget["reserved_seconds"] == 80 * 3600
    assert budget["available_seconds"] == 20 * 3600


def test_settlement_cross_week_and_replay(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    support(service)
    _, first, _ = linked(service, run, minutes=3600)
    linked(service, run)
    job = service.store.one("SELECT * FROM jobs WHERE intent_id=?", (first,))
    service.budget.observe(
        job,
        Observation("COMPLETED", started_at=1000000, ended_at=1000000 + 20 * 3600),
        "test",
        now=1000000 + 20 * 3600,
    )
    assert service.budget.view(run["project_id"], policy(), 1000000)["available_seconds"] == 40 * 3600
    service.budget.observe(
        job,
        Observation("COMPLETED", started_at=1000000, ended_at=1000000 + 20 * 3600),
        "test",
        now=1000000 + 20 * 3600,
    )
    assert len(service.store.rows("SELECT * FROM autopilot_accounting")) == 1
    assert linked(service, run)[2] is None


def test_unknown_terminal_no_refund_and_allocation_total(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    support(service)
    _, intent, _ = linked(service, run, gpus=8, minutes=600)
    job = service.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    service.budget.observe(job, Observation("CANCELLED"), "test", now=1000100)
    budget = service.budget.view(run["project_id"], policy())
    assert budget["reserved_seconds"] == 80 * 3600
    assert budget["unknown_seconds"] == 80 * 3600
    assert linked(service, run)[2]["reason"] == "autopilot_accounting_unknown"


def test_deadline_without_quota_and_foundation_not_implicit(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    service.tick(1000000 + WEEK)
    view = service.view("test-project", 1000000 + WEEK)
    assert view["run"]["foundation"] == "pending"
    assert view["calendar"]["phase"] == "main_argument"
    service.tick(policy().handoff_at)
    assert service.run("test-project")["status"] == "handoff_incomplete"
    assert json.loads(service.run("test-project")["handoff"])["incomplete"]
    count = len(service.store.rows("SELECT * FROM autopilot_transitions"))
    service.tick(policy().handoff_at + 1)
    assert len(service.store.rows("SELECT * FROM autopilot_transitions")) == count


def test_unverified_accounting_and_cpu(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    assert linked(service, run)[2]["reason"] == "autopilot_accounting_prerequisite"
    assert linked(service, run, gpus=0)[2] is None
