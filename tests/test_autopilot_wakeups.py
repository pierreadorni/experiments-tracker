"""T5.1d: run events wake the Autopilot lead with one debounced ``project_update``."""

import json
from uuid import uuid4

from helpers import seed_saved_contract4
from scenario import FakeClock
from test_iteration_autopilot import activated, command, policy
from test_research_wakeups import saved_job

from experimenttracker.autopilot import Autopilot
from experimenttracker.autopilot_budget import WEEK
from experimenttracker.autopilot_policy import lead_id
from experimenttracker.research import settlement, wakeups
from experimenttracker.research.owner_tool import AskOwnerQuestion, ask_owner
from experimenttracker.research.session import first_turn, follow_up_turn
from experimenttracker.store import Store, encode


def settled_lead(runtime, monkeypatch):
    """An active run whose lead finished one turn waiting for project updates."""
    service, run = activated(runtime, monkeypatch)
    store, lead = service.store, lead_id(run["id"])
    action = first_turn(store, lead)
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id='lead-thread' WHERE id=?", (lead,))
        db.execute(
            "UPDATE agent_actions SET state='completed',thread_id='lead-thread',turn_id='lead-turn',result=? "
            "WHERE id=?",
            (encode({"status": "wait", "summary": "Waiting", "next": "member updates"}), action["id"]),
        )
    clock = FakeClock()
    clock.wall = 1_000_000
    clock.install(monkeypatch)
    return service, run, lead, clock


def member(service, run):
    """A member experiment with a running research turn."""
    store = service.store
    eid = seed_saved_contract4(store, "Synthetic member fixture")
    action_id = str(uuid4())
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id='member-thread' WHERE id=?", (eid,))
        db.execute(
            "INSERT INTO autopilot_members VALUES(?,?,?,'question','exploration','test',0,1,0,NULL,'[]',?)",
            (eid, run["id"], "test-project", 1_000_000),
        )
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,input,"
            "created_at,updated_at) VALUES(?,?,'research',0,'running','member-thread','member-turn','{}',"
            "1000000,1000000)",
            (action_id, eid),
        )
    return eid, store.one("SELECT * FROM agent_actions WHERE id=?", (action_id,))


def updates(store, lead):
    return {
        json.loads(row["reason"])["source_id"]: row
        for row in store.rows(
            "SELECT * FROM agent_wakeups WHERE experiment_id=? AND json_extract(reason,'$.event')=?",
            (lead, wakeups.PROJECT_UPDATE),
        )
    }


def test_a_burst_of_member_events_costs_one_debounced_lead_turn(runtime, monkeypatch):
    service, run, lead, clock = settled_lead(runtime, monkeypatch)
    store = service.store
    approving, approving_action = member(service, run)
    failing, failing_action = member(service, run)
    request_id = str(uuid4())
    ask_owner(
        store,
        approving_action,
        AskOwnerQuestion(
            request_id=request_id, question="May it proceed?", kind="approval", objective="Run the CPU check."
        ),
    )
    clock.advance(20)
    settlement._settle(
        store, failing_action, turn_id="member-turn", state="failed", result=None, error="rejected end"
    )
    clock.advance(20)
    message = command(service, "message", text="Prefer the smaller synthetic comparison.")["request_id"]

    saved = updates(store, lead)
    assert set(saved) == {
        f"approval:{approving}:{request_id}",
        f"failed:{failing}:{failing_action['id']}",
        f"message:{message}",
    }
    # Every arrival shares the first arrival's window.
    assert {row["due_at"] for row in saved.values()} == {1_000_060}
    clock.advance(19)
    assert follow_up_turn(store, lead) is None
    clock.advance(1)
    action = follow_up_turn(store, lead)
    assert f"Member {approving} requests approval {request_id}" in action["input"]
    assert f"Member {failing} research turn {failing_action['id']} failed" in action["input"]
    assert "Owner message to the run: Prefer the smaller synthetic comparison." in action["input"]
    assert "You lead this project's Autopilot run" in action["input"]
    assert {row["action_id"] for row in updates(store, lead).values()} == {action["id"]}
    assert follow_up_turn(store, lead) is None
    assert len(store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (lead,))) == 2
    assert service.project_status(service.run(run_id=run["id"]))["messages"] == [
        "Prefer the smaller synthetic comparison."
    ]


def test_healthy_member_jobs_cost_no_lead_turn(runtime, monkeypatch):
    service, run, lead, clock = settled_lead(runtime, monkeypatch)
    store = service.store
    eid, _ = member(service, run)
    saved_job(store, eid, state="submitted")
    intent = saved_job(store, eid)
    wakeups.record(store, eid, wakeups.JOB_TERMINAL, intent)
    clock.advance(120)
    assert not updates(store, lead)
    assert follow_up_turn(store, lead) is None


def test_a_calendar_boundary_wakes_once_per_policy_revision_across_restart(runtime, monkeypatch):
    service, run, lead, clock = settled_lead(runtime, monkeypatch)
    after_foundation = policy().start_at + WEEK + 1
    service.tick(after_foundation)
    service.tick(after_foundation + 1)
    Autopilot(Store(service.store.path)).tick(after_foundation + 2)
    first = {"phase:1:foundation", f"report:{policy().start_at + WEEK}"}
    assert set(updates(service.store, lead)) == first

    revised = policy().model_dump()
    revised["weekly_reserve"] = 30
    command(service, "revise", policy=revised)
    service.tick(after_foundation + 3)
    Autopilot(Store(service.store.path)).tick(after_foundation + 4)
    assert set(updates(service.store, lead)) == first | {"policy:2", "phase:2:foundation"}


def test_a_paused_runs_updates_stay_unconsumed_until_resume(runtime, monkeypatch):
    service, run, lead, clock = settled_lead(runtime, monkeypatch)
    store = service.store
    command(service, "pause")
    command(service, "message", text="Read this after the pause.")
    clock.advance(600)
    assert follow_up_turn(store, lead) is None
    assert [row["state"] for row in updates(store, lead).values()] == ["pending"]
    (shown,) = service.view("test-project")["messages"]
    assert json.loads(shown["document"])["text"] == "Read this after the pause."

    command(service, "resume")
    action = follow_up_turn(store, lead)
    assert "Read this after the pause." in action["input"]
    assert [row["state"] for row in updates(store, lead).values()] == ["queued"]

    # A stopped run records nothing for its lead.
    command(service, "stop")
    eid, _ = member(service, run)
    with store.tx() as db:
        assert wakeups.member_update(store, db, eid, f"complete:{eid}") is None
    assert len(updates(store, lead)) == 1
