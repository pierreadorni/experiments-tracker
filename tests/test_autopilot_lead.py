"""T5.1b: the Autopilot lead is a contract 4 experiment held by its run's policy."""

import json
from uuid import uuid4

import pytest
from test_iteration_autopilot import activated, command

from experimenttracker.autopilot_policy import lead_id, lead_request_id
from experimenttracker.research.prompt import MAX_PROMPT_BYTES, turn_prompt
from experimenttracker.research.session import first_turn
from experimenttracker.store import Conflict


def admit(gate):
    gate.update(
        {
            "ordinaryUsageAllowed": True,
            "rateLimits": {
                "primary": {"windowDurationMins": 300, "usedPercent": 10},
                "secondary": {"windowDurationMins": 10080, "usedPercent": 10},
            },
        }
    )


def test_activation_creates_one_lead_and_replay_creates_none(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    store = service.store
    lead = lead_id(run["id"])
    experiment = store.experiment(lead)
    assert experiment["contract"]["execution_contract"] == 4
    assert experiment["contract"]["autopilot_lead"] == {"run_id": run["id"]}
    assert experiment["contract"]["branch_workspace"] is True
    assert "Synthetic software acceptance" in experiment["brief"]
    assert store.one("SELECT origin FROM commands WHERE request_id=?", (lead_request_id(run["id"]),)) == {
        "origin": "autopilot"
    }
    # The lead is not a member, so it never counts toward max_open_experiments.
    assert not store.rows("SELECT * FROM autopilot_members")
    service.require_slot(service.run(run_id=run["id"]))

    request = {"request_id": str(uuid4()), "kind": "pause", "expected_revision": run["revision"]}
    service.command("test-project", request)
    service.command("test-project", request)
    assert [row["id"] for row in store.rows("SELECT id FROM experiments")] == [lead]
    # The activation itself cannot be applied twice to one run.
    with pytest.raises(Conflict):
        command(service, "activate")
    assert len(store.rows("SELECT id FROM experiments")) == 1


def test_activation_replay_returns_the_saved_receipt(runtime, monkeypatch):
    service, _ = activated(runtime, monkeypatch)
    store = service.store
    run = service.run("test-project")
    before = store.rows("SELECT id FROM experiments")
    receipt = store.one(
        "SELECT request_id,payload,author FROM autopilot_requests WHERE run_id=? AND payload LIKE ?",
        (run["id"], '%"activate"%'),
    )
    replayed = service.command("test-project", json.loads(receipt["payload"]), origin=receipt["author"])
    assert replayed["request_id"] == receipt["request_id"]
    assert store.rows("SELECT id FROM experiments") == before


def test_only_the_autopilot_service_marks_a_lead(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    payload = {
        "brief": "Forged lead",
        "recipe_id": "agent_experiment_v1",
        "project_id": "test-project",
        "execution_contract": 4,
        "autopilot_lead": {"run_id": run["id"]},
    }
    with pytest.raises(Conflict, match="Autopilot service"):
        service.store.command(uuid4(), "create", payload, experiment_id=lead_id(run["id"]))
    with pytest.raises(Conflict, match="Autopilot service"):
        service.store.command(uuid4(), "create", payload, origin="autopilot", experiment_id=str(uuid4()))


def test_a_paused_run_holds_the_leads_next_turn(runtime, monkeypatch):
    service, run = activated(runtime, monkeypatch)
    store, gate = service.store, runtime[3].gate
    admit(gate)
    lead = lead_id(run["id"])
    assert service.quota(gate, eid=lead).admitted
    revision = service.run(run_id=run["id"])["control_revision"]
    command(service, "pause")
    paused = service.run(run_id=run["id"])
    assert paused["control_revision"] == revision + 1
    assert first_turn(store, lead) is None
    assert not store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (lead,))
    denied = service.quota(gate, eid=lead)
    assert not denied.admitted and denied.reason == "autopilot_paused"

    command(service, "resume")
    resumed = service.run(run_id=run["id"])
    assert resumed["control_revision"] == revision + 2
    action = first_turn(store, lead)
    assert action["autopilot_control_revision"] == resumed["control_revision"]
    assert "\nPROJECT STATUS\n" in action["input"]
    assert "(the lead is not counted)" in action["input"]
    assert len(action["input"].encode()) < MAX_PROMPT_BYTES
    assert service.quota(gate, eid=lead).admitted

    command(service, "stop")
    assert not service.quota(gate, eid=lead).admitted


def test_project_status_block_is_bounded():
    prompt = turn_prompt(brief="b" * 5000, checkpoint="c" * 9000, project_status="s" * 20000)
    assert "\nPROJECT STATUS\n" in prompt and "[truncated]" in prompt
    assert len(prompt.encode()) < MAX_PROMPT_BYTES
    assert "PROJECT STATUS" not in turn_prompt(brief="member")
