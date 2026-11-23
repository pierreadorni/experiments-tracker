"""T5.1e: the retired project-turn engine leaves saved runs to their leads (OWNER 8)."""

import json
from uuid import uuid4

import pytest
from helpers import lead_ready
from test_iteration_autopilot import activated, command, policy

from experimenttracker.autopilot import CUTOVER_HOLD, Autopilot
from experimenttracker.autopilot_policy import lead_id
from experimenttracker.research import wakeups
from experimenttracker.research.checkpoints import latest_checkpoint
from experimenttracker.research.prompt import DEFERRED_CHECKPOINT, with_checkpoint
from experimenttracker.research.session import first_turn
from experimenttracker.store import Conflict

NOW = 1000000
UNSETTLED = ("pending", "dispatching", "running", "uncertain", "received")


def other_project(store):
    """A second configured project with its own writable checkout."""
    from experimenttracker.config import ProjectConfig
    from experimenttracker.workspaces import initialize_project

    folder = store.path.parent / "other-project-checkout"
    folder.mkdir()
    initialize_project(folder)
    store.projects["other-project"] = ProjectConfig(id="other-project", name="Other project", checkout=folder)
    return "other-project"


def saved_run(store, *, hold="autopilot_researcher_input", status="active", project="test-project"):
    """An open run saved by the project-turn engine before leads existed."""
    run_id = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_runs(id,project_id,status,control_revision,input_cursor,consumed_cursor,"
            "central_argument,uncertainty,hold_reason,created_at,updated_at) "
            "VALUES(?,?,?,3,4,3,'Synthetic argument','Synthetic uncertainty',?,?,?)",
            (run_id, project, status, hold, NOW, NOW),
        )
        db.execute(
            "INSERT INTO autopilot_policies VALUES(?,1,?,'researcher',?)",
            (run_id, json.dumps(policy().model_dump()), NOW),
        )
    return run_id


def project_turn(store, run_id, state, result=None):
    identity = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_actions(id,run_id,cursor,control_revision,state,result,thread_id,turn_id,"
            "created_at,updated_at) VALUES(?,?,3,3,?,?,'old-thread','old-turn',?,?)",
            (identity, run_id, state, result, NOW, NOW),
        )
    return identity


def snapshot(store):
    return {
        table: store.rows(f"SELECT * FROM {table} ORDER BY rowid")
        for table in ("experiments", "events", "autopilot_runs", "autopilot_actions", "autopilot_events")
    }


async def test_startup_gives_an_old_run_one_seeded_lead_without_replaying_turns(runtime, monkeypatch):
    store, _, agent, wf = runtime
    monkeypatch.setattr("experimenttracker.clock.now", lambda: NOW)
    lead_ready(store)
    run_id = saved_run(store)
    result = json.dumps({"summary": "Old decision", "outcome": "wait"})
    old = project_turn(store, run_id, "completed", result)
    project_turn(store, run_id, "failed")
    await wf.start()
    lead = lead_id(run_id)
    experiment = store.experiment(lead)
    assert experiment["contract"]["autopilot_lead"] == {"run_id": run_id}
    run = store.one("SELECT * FROM autopilot_runs WHERE id=?", (run_id,))
    # The old engine's hold and cursors stay history; no old input becomes a turn.
    assert run["hold_reason"] is None
    assert (run["input_cursor"], run["consumed_cursor"]) == (4, 3)
    assert agent.calls == 0
    checkpoint = latest_checkpoint(store, lead)
    assert checkpoint["version"] == 1 and checkpoint["action_id"] is None
    assert "Synthetic argument" in checkpoint["markdown"]
    assert "Synthetic uncertainty" in checkpoint["markdown"]
    # The lead's first turn starts from that checkpoint; the saved input carries no copy.
    action = store.one("SELECT * FROM agent_actions WHERE experiment_id=?", (lead,)) or first_turn(
        store, lead
    )
    assert DEFERRED_CHECKPOINT in action["input"]
    assert "Synthetic argument" not in action["input"]
    assert "Synthetic argument" in with_checkpoint(action["input"], checkpoint["markdown"])
    # Historical project turns stay visible in the Autopilot view.
    view = wf.research.service.view("test-project")
    assert {row["id"] for row in view["actions"]} == {old} | {
        row["id"] for row in store.rows("SELECT id FROM autopilot_actions WHERE state='failed'")
    }
    assert [row["id"] for row in view["responses"]] == [old]
    await wf.close()

    # A second startup, with only terminal saved turns and the lead in place, changes nothing.
    before = snapshot(store)
    wf.research.service.cutover()
    assert snapshot(store) == before


@pytest.mark.parametrize("state", UNSETTLED)
def test_an_unsettled_project_turn_holds_its_run_without_a_lead(runtime, monkeypatch, state):
    store = runtime[0]
    monkeypatch.setattr("experimenttracker.clock.now", lambda: NOW)
    lead_ready(store)
    service = Autopilot(store, runtime[3].config)
    held = saved_run(store)
    other = saved_run(store, hold=None, status="paused", project=other_project(store))
    # The schema allows one project turn per run in flight; the other run is drained.
    turn = project_turn(store, held, state)
    project_turn(store, other, "completed")
    service.cutover()
    service.cutover()
    assert not store.one("SELECT 1 FROM experiments WHERE id=?", (lead_id(held),))
    assert (
        store.one("SELECT hold_reason FROM autopilot_runs WHERE id=?", (held,))["hold_reason"] == CUTOVER_HOLD
    )
    (event,) = store.rows(
        "SELECT * FROM autopilot_events WHERE run_id=? AND kind='lead_cutover_held'", (held,)
    )
    assert json.loads(event["payload"]) == {"action_ids": [turn]}
    assert store.one("SELECT state FROM autopilot_actions WHERE id=?", (turn,))["state"] == state
    # A drained run in the same installation still gets its lead.
    assert store.experiment(lead_id(other))["contract"]["autopilot_lead"] == {"run_id": other}
    # Once a release that still reads the turn settles it, the next startup gives the lead.
    with store.tx() as db:
        db.execute("UPDATE autopilot_actions SET state='completed' WHERE id=?", (turn,))
    service.cutover()
    assert store.experiment(lead_id(held))
    assert store.one("SELECT hold_reason FROM autopilot_runs WHERE id=?", (held,))["hold_reason"] is None


def test_an_answer_to_an_old_question_reaches_the_lead(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    store = service.store
    notice = str(uuid4())
    with store.tx() as db:
        db.execute(
            "INSERT INTO autopilot_notices VALUES(?,?,'researcher_input','Synthetic question?',NULL,?)",
            (notice, run["id"], NOW),
        )
    receipt = command(service, "answer", input_id=notice, text="Synthetic answer only.")
    assert store.one("SELECT resolved_at FROM autopilot_notices WHERE id=?", (notice,))["resolved_at"]
    saved = service.run(run_id=run["id"])
    assert saved["input_cursor"] == run["input_cursor"]
    assert not store.rows("SELECT * FROM autopilot_inputs")
    (wake,) = store.rows("SELECT reason FROM agent_wakeups WHERE experiment_id=?", (lead_id(run["id"]),))
    assert json.loads(wake["reason"]) == {
        "event": wakeups.PROJECT_UPDATE,
        "source_id": "message:" + receipt["request_id"],
    }
    assert service.project_status(saved)["messages"] == ["Synthetic answer only."]
    (message,) = service.view("test-project")["messages"]
    assert message["kind"] == "answer"
    with pytest.raises(Conflict, match="not pending"):
        command(service, "answer", input_id=notice, text="Second answer.")
