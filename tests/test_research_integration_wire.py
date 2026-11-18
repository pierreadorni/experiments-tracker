"""Contract-4 request_integration through the MCP bridge: queue, replay, never merge (T4.2j).

The tool advertises exactly ``RequestIntegration``, reuses the existing ``integrate``
command for this experiment's saved named branch, and returns queued rather than
merged. The worker still owns approval and Git; the controller route performs no
live side effect beyond the saved queue entry.
"""

from pathlib import Path
from uuid import uuid4

import pytest
from test_integration_commands import queued
from test_research_settlement import setup
from test_research_tools import saved_contract4
from test_research_write_tools import add_research_turn

from experimenttracker import controller
from experimenttracker.research.integration_tool import RequestIntegration, request_integration
from experimenttracker.store import Conflict

REASON = "keep the measured improvement"


def request(request_id=None, reason=REASON):
    return {"request_id": str(request_id or uuid4()), "reason": reason}


def test_request_integration_schema_is_advertised_from_the_actual_model():
    served = {item["name"]: item for item in controller.served_tools(research=True)}
    schema = served["request_integration"]["inputSchema"]
    assert schema == RequestIntegration.model_json_schema()
    assert schema["additionalProperties"] is False
    assert schema["required"] == ["request_id", "reason"]
    assert set(schema["properties"]) == {"request_id", "reason"}
    assert schema["properties"]["request_id"]["format"] == "uuid"


async def test_request_integration_queues_the_saved_branch_without_git_or_live_effects(
    runtime, tmp_path, monkeypatch
):
    store, scheduler, agent, _ = runtime
    _, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    branch = workflow.workspaces.branch(eid)
    head = branch["head_commit"]
    asked = request()

    result = await workflow.controller.call(eid, "request_integration", asked)

    assert result == {
        "request_id": asked["request_id"],
        "experiment_id": eid,
        "queued": True,
        "merged": False,
        "reason": REASON,
    }
    entries = queued(store)
    assert len(entries) == 1
    row, detail = entries[0]
    assert row["experiment_id"] == eid and row["state"] == "open"
    assert detail["repo"] == branch["repo"]
    assert detail["stage"] == "queued"
    assert detail["request"] == {
        "schema_version": 1,
        "kind": "merge",
        "experiment_id": eid,
        "branch": branch["branch"],
        "base": "main",
        "reason": REASON,
    }
    # Queueing is the whole effect: no base move, integration worktree or live call.
    assert workflow.workspaces.branch(eid)["head_commit"] == head
    assert workflow.workspaces.branch(eid)["state"] == "active"
    assert not (Path(branch["repo"]) / ".worktrees" / "_integration").exists()
    assert scheduler.submissions == 0 and agent.calls == 0


async def test_repeating_the_request_uuid_replays_and_changes_conflict(runtime, tmp_path, monkeypatch):
    store, _, _, _ = runtime
    _, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    asked = request()

    first = await workflow.controller.call(eid, "request_integration", asked)

    assert await workflow.controller.call(eid, "request_integration", asked) == first
    assert len(queued(store)) == 1
    with pytest.raises(Conflict, match="different command"):
        await workflow.controller.call(
            eid, "request_integration", {"request_id": asked["request_id"], "reason": "other"}
        )
    with pytest.raises(Conflict, match="already queued"):
        await workflow.controller.call(eid, "request_integration", request())
    assert workflow.workspaces.branch(eid)["state"] == "active"


async def test_a_foreign_action_is_refused_before_queueing(runtime, tmp_path, monkeypatch):
    store, _, _, _ = runtime
    _, eid, workflow, _ = setup(runtime, tmp_path, monkeypatch)
    foreign = add_research_turn(store, saved_contract4(store))

    with pytest.raises(ValueError, match="active research turn"):
        request_integration(
            store, store.experiment(eid), foreign, RequestIntegration(request_id=uuid4(), reason=REASON)
        )
    assert queued(store) == []
    assert workflow.workspaces.branch(eid)["state"] == "active"
