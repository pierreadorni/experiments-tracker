"""Contract-4 request_integration queues the saved C6 integration without merging.

These tests prove the strict input model, the exact named-branch queue entry and
reason, the UUID replay, and that the existing live-research, maintenance and
terminal/branch gates are reused instead of reimplemented. Git work belongs to the
integration worker, so a tool result is queued, never merged.
"""

from uuid import uuid4

import pytest
from pydantic import ValidationError
from test_integration_commands import branch_experiment, queued
from test_research_tools import add_action, saved_contract4
from test_tick_steps import block_maintenance

from experimenttracker.research.integration_tool import RequestIntegration, request_integration
from experimenttracker.store import Conflict, encode


async def research_branch(runtime, tmp_path, monkeypatch):
    """A real named branch whose saved execution contract is the research loop."""
    source, store, workflow, eid = await branch_experiment(runtime, tmp_path, monkeypatch)
    contract = {**store.experiment(eid)["contract"], "execution_contract": 4}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    add_action(store, eid)
    return source, store, workflow, eid


def request(reason="keep the measured improvement"):
    return RequestIntegration(request_id=uuid4(), reason=reason)


def seed_research_branch(store, eid, repo, *, state="active", merged_commit=None):
    """Record the named branch a research experiment's integration must bind."""
    with store.tx() as db:
        db.execute(
            "INSERT INTO experiment_branches(experiment_id,repo,branch,worktree,base_commit,head_commit,"
            "state,merged_commit,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (
                eid,
                str(repo.resolve()),
                f"exp/20260101-{eid}",
                str(repo / eid),
                "a" * 40,
                "b" * 40,
                state,
                merged_commit,
                0,
                0,
            ),
        )


def test_request_integration_input_requires_a_uuid_and_a_bounded_reason():
    request_id = uuid4()
    parsed = RequestIntegration.model_validate({"request_id": str(request_id), "reason": "keep"})
    assert parsed.request_id == request_id and parsed.reason == "keep"
    for payload in (
        {"request_id": str(request_id)},
        {"reason": "keep"},
        {"request_id": "not-a-uuid", "reason": "keep"},
        {"request_id": str(request_id), "reason": ""},
        {"request_id": str(request_id), "reason": "   "},
        {"request_id": str(request_id), "reason": "r" * 2001},
        {"request_id": str(request_id), "reason": "keep", "extra": 1},
    ):
        with pytest.raises(ValidationError):
            RequestIntegration.model_validate(payload)


async def test_request_integration_queues_the_exact_named_branch_and_never_merges(
    runtime, tmp_path, monkeypatch
):
    source, store, workflow, eid = await research_branch(runtime, tmp_path, monkeypatch)
    head = workflow.workspaces.branch(eid)["head_commit"]
    payload = request("  keep the measured improvement  ")

    result = request_integration(workflow, store.experiment(eid), payload)

    assert result == {
        "request_id": str(payload.request_id),
        "experiment_id": eid,
        "queued": True,
        "merged": False,
        # The reason is forwarded exactly: no trimming, no invented schema.
        "reason": "  keep the measured improvement  ",
    }
    entries = queued(store)
    assert len(entries) == 1
    row, detail = entries[0]
    branch = workflow.workspaces.branch(eid)
    assert row["experiment_id"] == eid and row["state"] == "open"
    assert detail["repo"] == str(source.resolve())
    assert detail["stage"] == "queued"
    assert detail["request"] == {
        "schema_version": 1,
        "kind": "merge",
        "experiment_id": eid,
        "branch": branch["branch"],
        "base": "main",
        "reason": "  keep the measured improvement  ",
    }
    # Queueing is the whole effect: no base move and no integration worktree.
    assert workflow.workspaces.branch(eid)["head_commit"] == head
    assert workflow.workspaces.branch(eid)["state"] == "active"
    assert not (source / ".worktrees" / "_integration").exists()


async def test_repeating_the_request_uuid_replays_the_saved_queue_entry(runtime, tmp_path, monkeypatch):
    _, store, workflow, eid = await research_branch(runtime, tmp_path, monkeypatch)
    exp = store.experiment(eid)
    payload = request()

    first = request_integration(workflow, exp, payload)
    second = request_integration(workflow, exp, payload)

    assert second == first
    assert len(queued(store)) == 1
    with pytest.raises(Conflict, match="different command"):
        request_integration(workflow, exp, RequestIntegration(request_id=payload.request_id, reason="other"))
    with pytest.raises(Conflict, match="already queued"):
        request_integration(workflow, exp, request())


async def test_request_integration_reuses_the_existing_named_branch_gates(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    add_action(store, eid)
    with pytest.raises(Conflict, match="named experiment branch"):
        request_integration(workflow, store.experiment(eid), request())

    exp = store.experiment(eid)
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=? WHERE id=?",
            (encode({**exp["contract"], "dont_merge": True}), eid),
        )
    with pytest.raises(Conflict, match="Don't merge"):
        request_integration(workflow, store.experiment(eid), request())

    other = saved_contract4(store)
    add_action(store, other)
    seed_research_branch(store, other, tmp_path, state="merged", merged_commit="c" * 40)
    with pytest.raises(Conflict, match="active experiment branch"):
        request_integration(workflow, store.experiment(other), request())
    assert queued(store) == []


@pytest.mark.parametrize(
    "phase,state,pause_agent,contract,message",
    [
        ("plan", "running", False, 4, "active research turn"),
        ("research", "pending", False, 4, "active research turn"),
        ("research", "running", True, 4, "live research experiment"),
        ("complete", "running", False, 4, "live research experiment"),
        ("research", "running", False, 3, "execution contract 4"),
    ],
)
async def test_request_integration_requires_the_owned_live_research_turn(
    runtime, tmp_path, phase, state, pause_agent, contract, message
):
    store, _, _, workflow = runtime
    eid = saved_contract4(store, phase=phase, pause_agent=pause_agent)
    seed_research_branch(store, eid, tmp_path)
    value = dict(store.experiment(eid)["contract"])
    value["execution_contract"] = contract
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(value), eid))
    add_action(store, eid, phase=phase if phase in ("plan", "research") else "research", state=state)
    with pytest.raises(ValueError, match=message):
        request_integration(workflow, store.experiment(eid), request())
    assert queued(store) == []


async def test_request_integration_requires_an_idle_installation(runtime, tmp_path):
    store, _, _, workflow = runtime
    eid = saved_contract4(store)
    seed_research_branch(store, eid, tmp_path)
    add_action(store, eid)
    block_maintenance(store)

    with pytest.raises(ValueError, match="idle installation"):
        request_integration(workflow, store.experiment(eid), request())
    assert queued(store) == []
