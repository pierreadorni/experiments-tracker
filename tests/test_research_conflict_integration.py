"""Two real contract-4 branches resolve a saved C6 integration conflict (T4.6)."""

import sys
from pathlib import Path
from uuid import uuid4

import pytest
from test_integration_resolver import ScriptedAgent, head, scripted
from test_research_create import create, git_project

from experimenttracker.engine import gitworkspace, integration_runtime
from experimenttracker.engine import integration_queue as queue
from experimenttracker.research.session import first_turn
from experimenttracker.research.settlement import complete
from experimenttracker.workspaces import git


def _running_action(store, eid):
    action = first_turn(store, eid)
    with store.tx() as db:
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id='thread',turn_id='turn',attempts=1 WHERE id=?",
            (action["id"],),
        )
        db.execute("UPDATE experiments SET thread_id='thread' WHERE id=?", (eid,))
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))


def _done_turn():
    return {
        "status": "done",
        "summary": "The synthetic comparison is complete.",
        "next": "None",
        "integrate": {"value": True, "reason": "Keep the synthetic change."},
    }


def _saved(store, request_id):
    with store.tx() as db:
        return queue.saved(db, request_id)


@pytest.mark.asyncio
async def test_two_contract4_done_requests_resolve_and_apply_the_fifo_git_conflict(
    runtime, config, tmp_path, monkeypatch
):
    store, _, _, workflow = runtime
    project, checkout = git_project(config, store, tmp_path, research_loop=True)
    (checkout / "shared.py").write_text("value = 0\n")
    git(checkout, "add", "shared.py")
    git(checkout, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "baseline")
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    project.check_command = [
        sys.executable,
        "-c",
        "from pathlib import Path; text=Path('shared.py').read_text(); assert '<<<<<<<' not in text and text.strip() in {'value = 1', 'value = 3'}",
    ]
    store.projects[project.id] = project

    branches = []
    requests = []
    for value in (1, 2):
        _, _, receipt = create(store)
        eid = receipt["experiment_id"]
        workflow.workspaces.ensure_branch(eid)
        branch = workflow.workspaces.branch(eid)
        branches.append(branch)
        (Path(branch["worktree"]) / "shared.py").write_text(f"value = {value}\n")
        action = _running_action(store, eid)
        store.report_state(eid, store.working_report(eid, "# Synthetic result", str(uuid4())), "ready")
        result = await complete(workflow, action, _done_turn())
        assert result["state"] == "completed"
        assert store.experiment(eid)["contract"]["execution_contract"] == 4
        with store.tx() as db:
            saved = db.execute(
                "SELECT id,detail FROM obligations WHERE kind='integration' AND experiment_id=?", (eid,)
            ).fetchone()
        requests.append(saved["id"].split(":")[1])

    assert branches[0]["base_commit"] == branches[1]["base_commit"]
    assert (checkout / "shared.py").read_text() == "value = 0\n"
    base = head(checkout, "main")

    first = integration_runtime.run(workflow.workspaces)
    assert len(first) == 1 and first[0]["outcome"] == "applied"
    assert _saved(store, requests[0])["stage"] == "applied"
    assert head(checkout, "main") != base
    assert (checkout / "shared.py").read_text() == "value = 1\n"

    conflict = integration_runtime.run(workflow.workspaces)
    assert len(conflict) == 1 and conflict[0]["stage"] == "conflict"
    second = _saved(store, requests[1])
    candidate = second["candidate"]
    assert second["stage"] == "conflict"
    assert candidate["conflicts"] == ["shared.py"]
    assert candidate["base_commit"] == head(checkout, "main")
    assert candidate["base_commit"] != branches[1]["base_commit"]

    integrator = scripted(
        workflow,
        ScriptedAgent(
            edit=lambda worktree: (worktree / "shared.py").write_text("value = 3\n"),
            result={"summary": "preserved both branch changes in the resolved candidate"},
        ),
    )
    await workflow.integration_resolver.advance()
    workflow.agent_pool.admit()
    task = workflow.agent_pool.primary.agent_task
    assert task is not None
    await task
    workflow.agent_pool.primary.agent_task = None

    resolved = _saved(store, requests[1])
    resolved_candidate = resolved["candidate"]
    assert (resolved["stage"], resolved_candidate["state"], resolved_candidate["conflicts"]) == (
        "claimed",
        "ready",
        [],
    )
    assert (Path(resolved_candidate["worktree"]) / "shared.py").read_text() == "value = 3\n"
    assert head(checkout, "main") == candidate["base_commit"]

    applied = integration_runtime.run(workflow.workspaces)
    assert len(applied) == 1 and applied[0]["outcome"] == "applied"
    assert _saved(store, requests[1])["stage"] == "applied"
    assert (checkout / "shared.py").read_text() == "value = 3\n"
    assert resolved_candidate["candidate_commit"] == head(checkout, "main")
    assert len([call for call in integrator.calls if call[0] == "begin"]) == 1
    assert git(checkout, "rev-list", "--count", "main").decode().strip() == "6"

    # Completed requests are inert on repeated control passes: no extra turn or merge.
    final_head = head(checkout, "main")
    assert integration_runtime.run(workflow.workspaces) == []
    await workflow.integration_resolver.advance()
    workflow.agent_pool.admit()
    assert workflow.agent_pool.primary.agent_task is None
    assert len([call for call in integrator.calls if call[0] == "begin"]) == 1
    assert head(checkout, "main") == final_head
    assert git(checkout, "rev-list", "--count", "main").decode().strip() == "6"
