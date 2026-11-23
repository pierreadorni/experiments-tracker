import json
from uuid import uuid4

import pytest
from helpers import lead_ready, seed_saved_contract3

from experimenttracker import creation_policy
from experimenttracker.autopilot import Autopilot
from experimenttracker.autopilot_budget import WEEK, window
from experimenttracker.autopilot_models import AutopilotPolicy
from experimenttracker.autopilot_policy import calendar, lead_id
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
    monkeypatch.setattr("experimenttracker.clock.now", lambda: 1000000)
    lead_ready(store)
    service = Autopilot(store, wf.config)
    command(service, "configure", policy=policy().model_dump())
    command(service, "activate")
    return service, service.run("test-project")


def linked(service, run, gpus=1, minutes=2400):
    store = service.store
    eid = seed_saved_contract3(store, "Synthetic acceptance only")
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
        required = resources.gpus_per_job * resources.walltime_minutes_per_job * 60
        db.execute(
            "INSERT INTO autopilot_allocations(intent_id,project_id,run_id,requested_gpus,reserved_seconds,state) VALUES(?,?,?,?,?,?)",
            (
                intent,
                run["project_id"],
                run["id"],
                resources.gpus_per_job,
                required,
                "reserved" if required else "settled",
            ),
        )
    return eid, intent


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


def test_reserve_revision_preserves_borrowing_and_changes_admission(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    with service.store.tx() as db:
        db.execute("UPDATE autopilot_runs SET borrowed_percent=3 WHERE id=?", (run["id"],))
    gate = runtime[3].gate
    gate.update(
        {
            "ordinaryUsageAllowed": True,
            "rateLimits": {
                "primary": {"windowDurationMins": 300, "usedPercent": 60},
                "secondary": {"windowDurationMins": 10080, "usedPercent": 10},
            },
        }
    )
    assert not service.quota(gate, run=service.run("test-project")).admitted
    revised = service.policy(service.run("test-project")).model_dump()
    revised["five_hour_reserve"] = 30
    command(service, "revise", policy=revised)
    run = service.run("test-project")
    assert run["borrowed_percent"] == 3
    assert service.quota(gate, run=run).admitted
    assert service.policy(run).five_hour_reserve == 30


def test_budget_six_40_hour_jobs_and_reset(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    for _ in range(2):
        linked(service, run)
    assert len(service.store.rows("SELECT * FROM autopilot_allocations")) == 2
    budget = service.budget.view(run["project_id"], policy(), 1000000 + WEEK)
    assert budget["reserved_seconds"] == 80 * 3600
    assert budget["available_seconds"] == 20 * 3600


def test_settlement_cross_week_and_replay(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    _, first = linked(service, run, minutes=3600)
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


def test_unknown_terminal_no_refund_and_allocation_total(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    _, intent = linked(service, run, gpus=8, minutes=600)
    job = service.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    service.budget.observe(job, Observation("CANCELLED"), "test", now=1000100)
    budget = service.budget.view(run["project_id"], policy())
    assert budget["reserved_seconds"] == 80 * 3600
    assert budget["unknown_seconds"] == 80 * 3600


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


def action(service, run, experiment_id=None):
    """A running research action of the run's lead, snapshotting its control revision."""
    identity = str(uuid4())
    with service.store.tx() as db:
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,"
            "created_at,updated_at,autopilot_control_revision) "
            "VALUES(?,?,'research',0,'running','lead-thread','lead-turn','{}',1000000,1000000,?)",
            (
                identity,
                experiment_id or lead_id(run["id"]),
                service.run(run_id=run["id"])["control_revision"],
            ),
        )
    return identity


def tool(service, identity, name, /, **arguments):
    return service.invoke(identity, name, {"request_id": str(uuid4()), **arguments})


def create(service, identity, **overrides):
    arguments = {"brief": "Synthetic fixture", "purpose": "exploration", "estimated_finish_at": 1003600}
    return tool(service, identity, "create_experiment", **{**arguments, **overrides})["experiment_id"]


def test_lead_tools_replay_scoping_and_stale_documents(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    arguments = {
        "request_id": str(uuid4()),
        "brief": "Synthetic scientific work is not authorized by this fixture.",
        "purpose": "exploration",
        "estimated_finish_at": 1000000 + 3600,
    }
    first = service.invoke(identity, "create_experiment", arguments)
    # A lost reply replays the saved receipt and creates nothing more.
    assert service.invoke(identity, "create_experiment", arguments) == first
    assert len(service.store.rows("SELECT * FROM experiments")) == 2
    assert service.store.experiment(first["experiment_id"])["contract"]["execution_contract"] == 4
    member = service.member(first["experiment_id"])
    assert (member["run_id"], member["purpose"]) == (run["id"], "exploration")
    with pytest.raises(Conflict, match="different payload"):
        service.invoke(identity, "create_experiment", {**arguments, "brief": "Different"})
    with pytest.raises(Conflict, match="handoff"):
        create(service, identity, estimated_finish_at=policy().handoff_at + 1)
    document = {"name": "paper.tex", "base_version": 0, "text": "Synthetic draft. TODO: no research."}
    assert tool(service, identity, "update_document", **document)["versions"] == {"paper.tex": 1}
    with pytest.raises(Conflict, match="stale"):
        tool(service, identity, "update_document", **{**document, "text": "Overwrite old draft"})
    assert service.document(run["id"], "paper.tex", 1)["content"].startswith("Synthetic")
    status = tool(service, identity, "project_status", document="paper.tex")
    assert status["documents"] == {"paper.tex": 1} and status["document"]["version"] == 1
    for retired in ("update_backlog", "record_decision", "compile_manuscript", "submit_job"):
        with pytest.raises(Conflict, match="allowlist"):
            service.invoke(identity, retired, {})
    with pytest.raises(Conflict, match="owned"):
        tool(
            service,
            identity,
            "control_experiment",
            experiment_id=str(uuid4()),
            operation="abandon",
            reason="x",
        )
    with pytest.raises(Conflict, match="not a member"):
        tool(service, identity, "message_experiment", experiment_id=str(uuid4()), text="Unlinked")
    steer = tool(service, identity, "message_experiment", experiment_id=first["experiment_id"], text="Focus")
    assert service.store.one("SELECT text FROM steering WHERE request_id=?", (steer["request_id"],)) == {
        "text": "Focus"
    }
    # A member's own research action never carries the lead's authority.
    with pytest.raises(Conflict, match="lead action"):
        tool(service, action(service, run, first["experiment_id"]), "project_status")


def test_lead_approval_hands_off_and_a_policy_revision_makes_it_stale(runtime, monkeypatch):
    from experimenttracker.research.autopilot_approval import approved
    from experimenttracker.research.owner_tool import AskOwnerQuestion, ask_owner

    service, run = activated(runtime, monkeypatch)
    store = service.store
    identity = action(service, run)
    eid = create(service, identity)
    research_action = action(service, run, eid)
    store.update(eid, thread_id="lead-thread")
    objective = "Run the bounded synthetic CPU comparison."
    approval_id = str(uuid4())
    ask_owner(
        store,
        store.one("SELECT * FROM agent_actions WHERE id=?", (research_action,)),
        AskOwnerQuestion(
            request_id=approval_id, question="May it proceed?", kind="approval", objective=objective
        ),
    )
    assert tool(service, identity, "project_status")["pending_objectives"] == [
        {"experiment_id": eid, "approval_request_id": approval_id, "objective": objective}
    ]
    arguments = {"experiment_id": eid, "approval_request_id": approval_id, "objective": objective}
    with pytest.raises(Conflict, match="not currently pending"):
        tool(service, identity, "approve_experiment_objective", **{**arguments, "objective": "altered"})
    receipt = tool(service, identity, "approve_experiment_objective", **arguments)
    assert receipt["approval"]["objective"] == objective
    pending = store.experiment(eid)["pending_request"]
    assert pending["answer_source"] == {"kind": "orchestrator", "run_id": run["id"], "action_id": identity}
    assert store.one(
        "SELECT 1 FROM agent_wakeups WHERE experiment_id=? AND json_extract(reason,'$.event')=?",
        (eid, "orchestrator_approval"),
    )
    assert approved(store, eid, approval_id, objective).objective == objective

    revised = policy().model_dump()
    revised["weekly_reserve"] = 30
    command(service, "revise", policy=revised)
    # The saved proof and the lead's turn are both stale after a policy revision.
    assert approved(store, eid, approval_id, objective) is None
    with pytest.raises(Conflict, match="authority changed"):
        create(service, identity)


def test_scoped_defer_user_pause_and_cancellation_replay(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    eid = create(service, identity)
    # The lead's own control keeps its turn's authority.
    tool(service, identity, "control_experiment", experiment_id=eid, operation="defer", reason="Idle")
    tool(service, identity, "control_experiment", experiment_id=eid, operation="resume", reason="Ready")
    service.store.update(eid, pause_agent=True, pause_compute=True)
    command(service, "control", experiment_id=eid, control="defer", text="Keep history and release idle slot")
    assert service.authority(eid) == "autopilot_deferred"
    # Late lead tools cannot undo a researcher control.
    with pytest.raises(Conflict, match="authority changed"):
        tool(
            service,
            identity,
            "control_experiment",
            experiment_id=eid,
            operation="resume",
            reason="Stale controller",
        )
    command(service, "control", experiment_id=eid, control="resume", text="Resume separately from user flags")
    exp = service.store.experiment(eid)
    assert exp["pause_agent"] and exp["pause_compute"]
    request = {
        "request_id": str(uuid4()),
        "expected_revision": service.run("test-project")["revision"],
        "kind": "control",
        "experiment_id": eid,
        "control": "abandon",
        "text": "Stop exact linked work",
    }
    first = service.command("test-project", request)
    assert service.command("test-project", request) == first
    assert service.store.experiment(eid)["phase"] == "cancelled"
    with pytest.raises(Conflict, match="terminal"):
        command(service, "control", experiment_id=eid, control="resume", text="Cannot resurrect")


def test_quota_policy_hysteresis_does_not_mutate_owner(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    gate = runtime[3].gate
    payload = {
        "ordinaryUsageAllowed": True,
        "rateLimits": {
            "primary": {"windowDurationMins": 300, "usedPercent": 51},
            "secondary": {"windowDurationMins": 10080, "usedPercent": 10},
        },
    }
    gate.update(payload)
    assert gate.decide().admitted
    assert not service.quota(gate, run=run).admitted
    payload["rateLimits"]["primary"]["usedPercent"] = 48
    gate.update(payload)
    assert gate.decide().admitted
    assert not service.quota(gate, run=run).admitted
    payload["rateLimits"]["primary"]["usedPercent"] = 44
    gate.update(payload)
    assert service.quota(gate, run=run).admitted
    assert gate.config.five_hour_min_remaining == 25


def test_cross_week_running_corrections_duplicate_steps_and_oversize(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    _, intent = linked(service, run, gpus=1, minutes=3600)
    job = service.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    start = 1000000 + WEEK - 10 * 3600
    now = 1000000 + WEEK + 5 * 3600
    service.budget.observe(job, Observation("RUNNING", started_at=start, observed_at=now), "fixture", now)
    current = service.budget.view(run["project_id"], policy(), now)
    assert current["consumed_seconds"] == 5 * 3600
    assert current["reserved_seconds"] == 45 * 3600
    assert current["available_seconds"] == 50 * 3600
    previous = service.budget.view(run["project_id"], policy(), 1000000 + 1)
    assert previous["consumed_seconds"] == 10 * 3600
    # A stale observation cannot refund elapsed allocation; late authoritative terminal correction can.
    service.budget.observe(
        job,
        Observation("COMPLETED", started_at=start, ended_at=start + 3600, observed_at=now - 1),
        "fixture",
        now,
    )
    assert service.budget.view(run["project_id"], policy(), now)["reserved_seconds"] == 45 * 3600
    service.budget.observe(
        job,
        Observation("COMPLETED", started_at=start, ended_at=1000000 + WEEK + 2 * 3600, observed_at=now + 1),
        "fixture",
        now + 1,
    )
    current = service.budget.view(run["project_id"], policy(), now + 1)
    assert current["reserved_seconds"] == 0 and current["consumed_seconds"] == 2 * 3600
    from experimenttracker.config import SlurmConfig
    from experimenttracker.slurm import SlurmScheduler

    assert "--no-requeue" not in SlurmScheduler(SlurmConfig()).submission_args(
        "manual", service.config.state_dir, JobResources()
    )


def test_pause_survives_deadline_and_revised_deadline_resumes_only_explicitly(runtime, monkeypatch):
    service, _ = activated(runtime, monkeypatch)
    command(service, "pause")
    service.tick(policy().handoff_at)
    assert service.run("test-project")["status"] == "paused"
    assert service.run("test-project")["handoff"]
    changed = policy().model_dump()
    changed["handoff_at"] += WEEK
    changed["submission_at"] += WEEK
    command(service, "revise", policy=changed)
    assert service.run("test-project")["status"] == "paused"


def test_cpu_terminal_without_timestamps_cannot_hold_gpu_admission(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    _, intent = linked(service, run, gpus=0)
    job = service.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    service.budget.observe(job, Observation("COMPLETED"), "synthetic")
    assert service.budget.view(run["project_id"], policy())["unknown_jobs"] == 0


def test_project_guidance_change_wakes_and_invalidates_stale_authority(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    wf = runtime[3]
    wf.projects.enqueue_settings(
        "test-project",
        {
            "request_id": str(uuid4()),
            "instructions": "Retain negative results; focus on the specified protocol.",
        },
    )
    wf.projects.settings_tick()
    changed = service.run("test-project")
    assert changed["control_revision"] > run["control_revision"]
    assert service.store.one(
        "SELECT 1 FROM agent_wakeups WHERE experiment_id=? AND json_extract(reason,'$.source_id')=?",
        (lead_id(run["id"]), "guidance:1"),
    )
    with pytest.raises(Conflict, match="authority changed"):
        tool(service, identity, "update_document", name="claims", base_version=0, text="Old context")


def test_request_identity_cannot_cross_project_and_experiment_commands(runtime, monkeypatch):
    service, _ = activated(runtime, monkeypatch)
    # The lead is created at activation; legacy mode applies to the later member creates.
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    request = str(uuid4())
    payload = {
        "request_id": request,
        "kind": "pause",
        "expected_revision": service.run("test-project")["revision"],
    }
    service.command("test-project", payload)
    with pytest.raises(Conflict, match="Autopilot"):
        service.store.command(
            request,
            "create",
            {"brief": "Cross-domain identity must conflict", "recipe_id": "agent_experiment_v1"},
        )
    old = str(uuid4())
    eid = service.store.command(
        old, "create", {"brief": "Synthetic fixture", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    assert service.store.experiment(eid)["contract"]["execution_contract"] == 3
    with pytest.raises(Conflict, match="another domain"):
        service.command(
            "test-project",
            {
                "request_id": old,
                "kind": "resume",
                "expected_revision": service.run("test-project")["revision"],
            },
        )


def test_synthetic_agent_evidence_cannot_become_a_research_claim(runtime, monkeypatch):
    from experimenttracker.autopilot_tools import Source

    service, run = activated(runtime, monkeypatch)
    eid = seed_saved_contract3(service.store, "Synthetic agent fixture")
    service.store.update(eid, snapshot={"plan": {"synthetic_demo": True}})
    with pytest.raises(Conflict, match="synthetic"):
        service.validate_sources(run, [Source(kind="experiment", id=eid)])
