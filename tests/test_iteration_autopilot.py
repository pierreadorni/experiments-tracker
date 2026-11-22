import json
from uuid import uuid4

import pytest
from helpers import role_step, seed_saved_contract3

from experimenttracker import creation_policy
from experimenttracker.agent_pool import project_owner
from experimenttracker.autopilot import Autopilot, calendar
from experimenttracker.autopilot_budget import WEEK, window
from experimenttracker.autopilot_models import AutopilotPolicy
from experimenttracker.config import JobResources, ProjectConfig
from experimenttracker.slurm import Observation
from experimenttracker.store import Conflict
from experimenttracker.workspaces import initialize_project


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


def action(service, run):
    identity = str(uuid4())
    with service.store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_actions(id,run_id,cursor,control_revision,state,created_at,updated_at) VALUES(?,?,?,?,'running',?,?)",
            (identity, run["id"], run["input_cursor"], run["control_revision"], 1000000, 1000000),
        )
    return identity


def tool(service, identity, name, **arguments):
    row = service.store.one("SELECT run_id FROM autopilot_actions WHERE id=?", (identity,))
    run = service.run(run_id=row["run_id"])
    return service.invoke(
        identity, name, {"request_id": str(uuid4()), "expected_revision": run["revision"], **arguments}
    )


def foundation(service, identity):
    docs = []
    for name in (
        "research_plan",
        "literature",
        "protocol",
        "claims",
        "backlog",
        "paper.tex",
        "references.bib",
    ):
        docs.append(
            {
                "name": name,
                "base_version": 0,
                "content": "Synthetic software fixture. TODO: real research is not performed.",
                "sources": [
                    {
                        "kind": "literature",
                        "id": "https://example.org/synthetic-fixture",
                        "title": "Explicit synthetic retrieval adapter fixture",
                        "access": "abstract",
                        "retrieved_at": 1000000,
                    }
                ]
                if name == "literature"
                else [],
            }
        )
    tool(service, identity, "publish_documents", documents=docs)
    tool(
        service,
        identity,
        "update_backlog",
        item_revision=0,
        item={
            "id": "question",
            "question": "Does synthetic admission preserve one experiment?",
            "purpose": "exploration",
            "decision_if_positive": "Continue software test",
            "decision_if_negative": "Fix infrastructure",
            "stop_condition": "Stop after fixture",
            "review_at": 1000000 + WEEK,
            "max_attempts": 2,
            "priority": 3,
            "resource_estimate": "Synthetic accounting, no real GPU use",
        },
    )
    tool(
        service,
        identity,
        "set_foundation",
        state="ready",
        gaps="All literature and measurements here are synthetic test fixtures.",
    )


def test_project_tools_lost_response_scoping_and_stale_documents(runtime, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    with pytest.raises(Conflict, match="not ready"):
        tool(
            service,
            identity,
            "create_experiment",
            brief="Synthetic experiment",
            backlog_id="question",
            scope_rationale="Synthetic infrastructure test",
            estimated_finish_at=1000000 + 3600,
        )
    foundation(service, identity)
    run = service.run("test-project")
    arguments = {
        "request_id": str(uuid4()),
        "expected_revision": run["revision"],
        "brief": "Synthetic scientific work is not authorized by this fixture.",
        "backlog_id": "question",
        "scope_rationale": "Test delegation only",
        "estimated_finish_at": 1000000 + 3600,
    }
    first = service.invoke(identity, "create_experiment", arguments)
    assert service.invoke(identity, "create_experiment", arguments) == first
    assert len(service.store.rows("SELECT * FROM experiments")) == 1
    assert service.store.experiment(first["experiment_id"])["contract"]["execution_contract"] == 3
    assert len(service.store.rows("SELECT * FROM autopilot_members")) == 1
    with pytest.raises(Conflict, match="different payload"):
        service.invoke(identity, "create_experiment", {**arguments, "brief": "Different"})
    with pytest.raises(Conflict, match="stale"):
        tool(
            service,
            identity,
            "publish_documents",
            documents=[{"name": "paper.tex", "base_version": 0, "content": "Overwrite old draft"}],
        )
    assert service.document(run["id"], "paper.tex", 1)["content"].startswith("Synthetic")
    with pytest.raises(Conflict, match="allowlist"):
        service.invoke(identity, "revise_policy", {})
    with pytest.raises(Conflict, match="owned"):
        tool(
            service,
            identity,
            "control_experiment",
            experiment_id=str(uuid4()),
            operation="abandon",
            reason="Not linked",
        )


def test_candidate_autopilot_create_links_c4_and_routes_objective_approval(
    runtime, config, tmp_path, monkeypatch
):
    store = runtime[0]
    checkout = tmp_path / "project"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="test-project", name="Synthetic project", checkout=checkout)
    config.projects.append(project)
    store.projects[project.id] = project
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    foundation(service, identity)
    monkeypatch.setattr(creation_policy, "MODE", "candidate")

    arguments = {
        "request_id": str(uuid4()),
        "expected_revision": service.run("test-project")["revision"],
        "brief": "Synthetic software acceptance only; no scientific work.",
        "backlog_id": "question",
        "scope_rationale": "Verify candidate create binding and replay.",
        "estimated_finish_at": 1003600,
    }
    created = service.invoke(identity, "create_experiment", arguments)
    eid = created["experiment_id"]
    assert service.invoke(identity, "create_experiment", arguments) == created
    assert store.experiment(eid)["contract"]["execution_contract"] == 4
    member = store.one("SELECT * FROM autopilot_members WHERE experiment_id=?", (eid,))
    assert member["run_id"] == run["id"]
    assert member["project_id"] == "test-project"
    assert [row["id"] for row in store.rows("SELECT id FROM experiments")] == [eid]

    research_action = str(uuid4())
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id='synthetic-thread' WHERE id=?", (eid,))
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,created_at,updated_at) "
            "VALUES(?,?,'research',0,'running','synthetic-thread','synthetic-turn','{}',1000000,1000000)",
            (research_action, eid),
        )
    from experimenttracker.research.owner_tool import AskOwnerQuestion, ask_owner

    objective = "Run the bounded synthetic CPU comparison."
    approval_id = uuid4()
    ask_owner(
        store,
        store.one("SELECT * FROM agent_actions WHERE id=?", (research_action,)),
        AskOwnerQuestion(
            request_id=approval_id,
            question="May the synthetic comparison proceed?",
            kind="approval",
            objective=objective,
        ),
    )
    prompt = runtime[3].research.prompt(store.one("SELECT * FROM autopilot_actions WHERE id=?", (identity,)))
    assert "approve_experiment_objective" in prompt
    assert "review_experiment_plan is only for saved contract-3 plans" in prompt
    approval_args = {
        "request_id": str(uuid4()),
        "expected_revision": service.run("test-project")["revision"],
        "experiment_id": eid,
        "approval_request_id": str(approval_id),
        "objective": objective,
    }
    approved = service.invoke(identity, "approve_experiment_objective", approval_args)
    assert approved["approval"]["objective"] == objective
    with pytest.raises(Conflict, match="contract 3"):
        tool(
            service,
            identity,
            "review_experiment_plan",
            experiment_id=eid,
            plan_revision=1,
            scope_rationale="Must not route C4 through plan review.",
            comparable_protocol="Synthetic protocol",
            estimated_finish_at=1003600,
        )


def test_scoped_defer_user_pause_and_cancellation_replay(runtime, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    foundation(service, identity)
    eid = tool(
        service,
        identity,
        "create_experiment",
        brief="Synthetic fixture",
        backlog_id="question",
        scope_rationale="Software check",
        estimated_finish_at=1003600,
    )["experiment_id"]
    service.store.update(eid, pause_agent=True, pause_compute=True)
    command(service, "control", experiment_id=eid, control="defer", text="Keep history and release idle slot")
    assert service.authority(eid) == "autopilot_deferred"
    # Late child/project tools cannot undo a researcher control.
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


def test_investigation_review_and_cumulative_borrowing(runtime, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    foundation(service, identity)
    for _ in range(2):
        created = tool(
            service,
            identity,
            "create_experiment",
            brief="Synthetic fixture",
            backlog_id="question",
            scope_rationale="Software check",
            estimated_finish_at=1003600,
        )
        service.store.update(created["experiment_id"], phase="complete")
    with pytest.raises(Conflict, match="review point"):
        tool(
            service,
            identity,
            "create_experiment",
            brief="Rename cannot bypass attempts",
            backlog_id="question",
            scope_rationale="test",
            estimated_finish_at=1003600,
        )
    tool(
        service,
        identity,
        "record_decision",
        outcome="narrow",
        backlog_id="question",
        rationale="Repeated inconclusive fixture; narrow before spending",
        next_review_at=1000000 + WEEK,
        next_max_attempts=3,
    )
    tool(
        service,
        identity,
        "record_decision",
        outcome="borrow",
        rationale="Synthetic schedule test",
        borrow_percent=3,
        dropped_work="Drop low-value variant",
        remaining_ablations="One control remains",
    )
    with pytest.raises(Conflict, match="borrowing"):
        tool(
            service,
            identity,
            "record_decision",
            outcome="borrow",
            rationale="Second small request cannot evade total cap",
            borrow_percent=3,
            dropped_work="Drop more",
            remaining_ablations="Keep controls",
        )
    assert service.run("test-project")["borrowed_percent"] == 3


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
    support(service)
    _, intent, held = linked(service, run, gpus=1, minutes=3600)
    assert held is None
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
    assert linked(service, run, minutes=6060)[2]["reason"] == "autopilot_job_exceeds_weekly_cap"
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


async def test_controller_coalesces_late_inputs_and_quarantines_unknown_turn(runtime, monkeypatch):
    from experimenttracker.autopilot_models import ProjectDecision

    service, run = activated(runtime, monkeypatch)
    store, _, agent, wf = runtime
    wf.agent_ready = True
    wf.gate.update(await agent.quota())
    agent.resolved_model = "demo"
    agent.models = [{"id": "demo", "reasoning_efforts": ["medium"], "default_effort": "medium"}]
    original_begin = agent.begin

    async def begin(thread, prompt, schema, **kwargs):
        assert schema == ProjectDecision.model_json_schema()
        agent.calls += 1
        identity = "project-turn"
        agent.results[(thread, identity)] = {
            "summary": "Synthetic checkpoint",
            "central_argument": "No real science in fixture",
            "uncertainty": "Real retrieval remains a live prerequisite",
            "outcome": "wait",
        }
        # Result arrives during the turn; it must remain behind the saved input cursor.
        with store.tx() as db:
            service.wake(db, run["id"], "late-synthetic-result", "experiment_change", {"synthetic": True})
        return identity

    monkeypatch.setattr(agent, "begin", begin)
    await role_step(wf.research)
    await wf.research.task
    await role_step(wf.research)
    saved = service.run("test-project")
    assert saved["consumed_cursor"] < saved["input_cursor"]
    if wf.research.task:
        await wf.research.task
        await role_step(wf.research)
    await wf.research.close()
    # A turn-start reply loss has no saved identity; restart must not issue another begin.
    with store.tx() as db:
        db.execute("UPDATE autopilot_actions SET state='completed'")
        db.execute(
            "INSERT INTO autopilot_actions(id,run_id,cursor,control_revision,state,thread_id,created_at,updated_at) VALUES(?,?,1,0,'dispatching','known-thread',?,?)",
            (str(uuid4()), run["id"], 1000000, 1000000),
        )
    wf.research.task = None
    calls = agent.calls
    await role_step(wf.research)
    assert store.one("SELECT state FROM autopilot_actions WHERE state='uncertain'")
    assert agent.calls == calls
    monkeypatch.setattr(agent, "begin", original_begin)


async def test_gateway_autopilot_lost_ack_and_durable_refusal(runtime, config, tmp_path, monkeypatch):
    import httpx

    from experimenttracker.config import GatewayConfig
    from experimenttracker.gateway_client import GatewayClient

    service, _ = activated(runtime, monkeypatch)
    token = tmp_path / "machine-token"
    token.write_text("e" * 40)
    config.gateway = GatewayConfig(url="https://gateway.example", credential_file=token)
    payload = {
        "schema_version": 1,
        "request_id": str(uuid4()),
        "kind": "pause",
        "expected_revision": service.run("test-project")["revision"],
    }
    row = {"request_id": payload["request_id"], "project_id": "test-project", "payload": payload}
    attempts = []

    async def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json={"schema_version": 1, "commands": [row]})
        if request.url.path.endswith("autopilot-commands"):
            attempts.append(json.loads(request.content))
            if len(attempts) == 1:
                raise httpx.ConnectError("lost command receipt")
            return httpx.Response(200, json={"schema_version": 1, "accepted": payload["request_id"]})
        return httpx.Response(200, json={"schema_version": 1, "accepted": True})

    client = GatewayClient(config, service.store, transport=httpx.MockTransport(handler))
    with pytest.raises(httpx.ConnectError):
        await client.autopilot()
    await client.autopilot()
    assert attempts[0] == attempts[1]
    assert service.run("test-project")["status"] == "paused"
    payload.update(request_id=str(uuid4()), expected_revision=0, kind="resume")
    row["request_id"] = payload["request_id"]
    await client.autopilot()
    assert attempts[-1]["state"] == "rejected"
    assert (
        service.store.one(
            "SELECT state FROM autopilot_requests WHERE request_id=?", (payload["request_id"],)
        )["state"]
        == "rejected"
    )
    await client.close()


async def test_managed_project_input_is_durable_and_scoped(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    wf = runtime[3]
    with service.store.tx() as db:
        db.execute(
            "UPDATE autopilot_actions SET thread_id='project-thread',turn_id='project-turn' WHERE id=?",
            (identity,),
        )
    wf.agent_pool.claim(
        role="research",
        source_table="autopilot_actions",
        row=service.store.one("SELECT * FROM autopilot_actions WHERE id=?", (identity,)),
        owner=project_owner("test-project"),
        slot=wf.agent_pool.primary,
    )
    await wf._request(
        wf.agent_pool.primary,
        {
            "id": 123,
            "method": "item/tool/requestUserInput",
            "params": {
                "threadId": "project-thread",
                "turnId": "project-turn",
                "questions": [{"id": "data", "question": "Synthetic fixture: which accessible data source?"}],
            },
        },
        wf.agent.start_count,
    )
    saved = service.store.one("SELECT pending_request FROM autopilot_actions WHERE id=?", (identity,))
    pending = json.loads(saved["pending_request"])
    command(
        service, "answer", input_id=pending["id"], text="Keep this a synthetic fixture; no live data work."
    )
    answered = json.loads(
        service.store.one("SELECT pending_request FROM autopilot_actions WHERE id=?", (identity,))[
            "pending_request"
        ]
    )
    assert answered["answer"].startswith("Keep this")
    assert service.run("test-project")["control_revision"] == run["control_revision"]
    assert service.store.one("SELECT resolved_at FROM autopilot_notices WHERE id=?", (pending["id"],))[
        "resolved_at"
    ]


def test_cpu_terminal_without_timestamps_cannot_hold_gpu_admission(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    support(service)
    _, intent, _ = linked(service, run, gpus=0)
    job = service.store.one("SELECT * FROM jobs WHERE intent_id=?", (intent,))
    service.budget.observe(job, Observation("COMPLETED"), "synthetic")
    assert service.budget.view(run["project_id"], policy())["unknown_jobs"] == 0
    assert linked(service, run)[2] is None


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
    assert service.context(changed)["project_guidance"]["instructions"].startswith("Retain negative")
    assert service.store.one(
        "SELECT 1 FROM autopilot_inputs WHERE run_id=? AND kind='project_guidance'", (run["id"],)
    )
    with pytest.raises(Conflict, match="authority changed"):
        tool(
            service,
            identity,
            "record_decision",
            outcome="stop",
            rationale="Old context must not overwrite guidance",
        )


def test_request_identity_cannot_cross_project_and_experiment_commands(runtime, monkeypatch):
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    service, _ = activated(runtime, monkeypatch)
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


def test_project_bridge_explicitly_authorizes_only_its_declared_tools(runtime, monkeypatch):
    from experimenttracker.autopilot_tools import MODELS

    _, _, _, wf = runtime
    monkeypatch.setattr(wf.controller, "server", object())
    config = wf.research.config("synthetic-action")
    assert list(config) == ["mcp_servers.experimenttracker_project"]
    server = config["mcp_servers.experimenttracker_project"]
    assert server["required"] is True
    assert server["enabled_tools"] == list(MODELS)
    assert server["tools"] == {name: {"approval_mode": "approve"} for name in MODELS}
    assert server["args"][-1] == "synthetic-action"


def guidance_result():
    return {
        "summary": "Synthetic fixture: project tools were rejected before a question could be saved.",
        "central_argument": "No scientific claim in this software fixture.",
        "uncertainty": "Which synthetic source should the fixture use?",
        "outcome": "request_guidance",
    }


def finish_guidance(wf, identity):
    wf.research.state(identity, "received", result=json.dumps(guidance_result()))
    wf.research.complete(wf.store.one("SELECT * FROM autopilot_actions WHERE id=?", (identity,)))


async def test_rejected_question_becomes_durable_visible_wait_and_answer_resumes(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    store, _, agent, wf = runtime
    identity = action(service, run)
    finish_guidance(wf, identity)
    saved = service.run("test-project")
    assert saved["hold_reason"] == "autopilot_researcher_input"
    assert saved["consumed_cursor"] == saved["input_cursor"]
    notice = store.one("SELECT * FROM autopilot_notices WHERE run_id=?", (run["id"],))
    assert guidance_result()["summary"] in notice["text"]
    assert guidance_result()["uncertainty"] in notice["text"]
    before = saved["revision"]
    wf.research.recover_guidance()
    wf.research.recover_guidance()
    assert service.run("test-project")["revision"] == before
    assert len(store.rows("SELECT * FROM autopilot_notices")) == 1
    assert agent.calls == 0
    command(service, "answer", input_id=notice["id"], text="Use only synthetic sources.")
    assert service.run("test-project")["input_cursor"] > saved["consumed_cursor"]
    assert store.one("SELECT resolved_at FROM autopilot_notices WHERE id=?", (notice["id"],))["resolved_at"]
    wf.agent_ready = True
    wf.gate.update(await agent.quota())
    agent.resolved_model = "demo"
    agent.models = [{"id": "demo", "reasoning_efforts": ["medium"], "default_effort": "medium"}]

    async def begin(thread, prompt, schema, **kwargs):
        agent.calls += 1
        assert "Use only synthetic sources." in prompt
        agent.results[(thread, "synthetic-continuation")] = {**guidance_result(), "outcome": "wait"}
        return "synthetic-continuation"

    monkeypatch.setattr(agent, "begin", begin)
    await role_step(wf.research)
    await wf.research.task
    await role_step(wf.research)
    assert agent.calls == 1
    assert service.run("test-project")["hold_reason"] is None
    assert store.one("SELECT state FROM autopilot_actions WHERE id=?", (identity,))["state"] == "completed"
    await wf.research.close()


@pytest.mark.parametrize("resolved", [False, True])
def test_guidance_completion_preserves_existing_question_without_duplicate(runtime, monkeypatch, resolved):
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    receipt = tool(service, identity, "request_researcher_input", question="Synthetic question?")
    if resolved:
        command(service, "answer", input_id=receipt["request_id"], text="Synthetic answer.")
    finish_guidance(runtime[3], identity)
    assert len(service.store.rows("SELECT * FROM autopilot_notices")) == 1
    assert service.run("test-project")["hold_reason"] == (None if resolved else "autopilot_researcher_input")


def test_startup_recovers_old_guidance_without_replaying_turn_and_respects_new_input(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    identity = action(service, run)
    wf = runtime[3]
    wf.research.state(identity, "completed", result=json.dumps(guidance_result()))
    with service.store.tx() as db:
        db.execute("UPDATE autopilot_runs SET consumed_cursor=input_cursor WHERE id=?", (run["id"],))
    wf.research.recover_guidance()
    wf.research.recover_guidance()
    assert len(service.store.rows("SELECT * FROM autopilot_notices")) == 1
    assert service.run("test-project")["hold_reason"] == "autopilot_researcher_input"
    assert runtime[2].calls == 0
    assert (
        service.store.one("SELECT state FROM autopilot_actions WHERE id=?", (identity,))["state"]
        == "completed"
    )
    command(service, "message", text="New synthetic guidance supersedes the old turn.")
    revision = service.run("test-project")["revision"]
    wf.research.recover_guidance()
    assert service.run("test-project")["revision"] == revision
