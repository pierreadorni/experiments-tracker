"""C7 turn-end settlement: exact identity, evidence, one commit and one obligation (T4.1f-1)."""

import json
from pathlib import Path
from uuid import uuid4

import pytest
from test_projects import register

from experimenttracker.codex_client import NeedsAttention
from experimenttracker.core import action_obligations
from experimenttracker.engine import gitworkspace
from experimenttracker.research.session import first_turn
from experimenttracker.research.settlement import commit_message, complete
from experimenttracker.workspaces import git, initialize_project

BRIEF = "Compare two toy optimizers under one fixed compute budget."
COMMIT_KINDS = ("workspace_commit_intent", "workspace_commit_settled")


def setup(runtime, tmp_path, monkeypatch):
    """A live contract 4 experiment with one saved running research turn."""
    store, _, _, workflow = runtime
    source = register(workflow.config, store, tmp_path)
    initialize_project(source)
    (source / "shared.py").write_text("value = 1\n")
    git(source, "add", "--", "shared.py")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Source")
    workflow.projects.reload()
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = store.command(
        uuid4(),
        "create",
        {"brief": BRIEF, "recipe_id": "agent_experiment_v1", "project_id": "anyup-rs"},
    )["experiment_id"]
    contract = store.experiment(eid)["contract"]
    contract["execution_contract"] = 4
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    workflow.workspaces.ensure_branch(eid)
    action = first_turn(store, eid)
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action["id"],)).fetchone())
        db.execute(
            "UPDATE agent_actions SET state='running',thread_id='thread-1',turn_id='turn-1',attempts=1 "
            "WHERE id=?",
            (action["id"],),
        )
        db.execute("UPDATE experiments SET thread_id='thread-1' WHERE id=?", (eid,))
        action_obligations.sync_action(db, action["id"], before=before)
    return store, eid, workflow, store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))


def turn(status, **extra):
    value = {"status": status, "summary": "Measured the first condition.", "next": "Continue the comparison."}
    if status == "done":
        value["integrate"] = {"value": True, "reason": "The report supports the comparison."}
    value.update(extra)
    return value


def branch_head(workflow, eid):
    return workflow.workspaces.branch(eid)["head_commit"]


def events(store, eid, kinds=COMMIT_KINDS):
    return store.rows(
        f"SELECT kind,payload FROM events WHERE experiment_id=? AND kind IN ({','.join('?' * len(kinds))}) "
        "ORDER BY seq",
        (eid, *kinds),
    )


def saved(store, action):
    return store.one("SELECT * FROM agent_actions WHERE id=?", (action["id"],))


def add_job(store, eid):
    stage = store.one("SELECT id FROM stages WHERE experiment_id=? LIMIT 1", (eid,))["id"]
    with store.tx() as db:
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at) VALUES(?,?,?,99,'submitted','synthetic','source','{}','snapshot',0)",
            (str(uuid4()), eid, stage),
        )


def ask_owner(store, eid, *, expired=False):
    store.update(
        eid,
        pending_request={
            "id": "question",
            "method": "item/tool/requestUserInput",
            "params": {"questions": [{"id": "dataset", "question": "Which dataset?"}]},
            "expired": expired,
        },
    )


@pytest.mark.asyncio
async def test_a_valid_turn_is_committed_once_and_settles_its_saved_obligation(
    runtime, tmp_path, monkeypatch
):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    worktree = Path(workflow.workspaces.branch(eid)["worktree"])
    (worktree / "notes.md").write_text("first measurement\n")
    before_head = branch_head(workflow, eid)
    before_stages = store.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY id", (eid,))

    settled = await complete(workflow, action, turn("continue"))

    assert (settled["state"], settled["thread_id"], settled["turn_id"]) == (
        "completed",
        "thread-1",
        "turn-1",
    )
    assert json.loads(settled["result"])["status"] == "continue"
    assert (
        store.one("SELECT state FROM obligations WHERE target=? ORDER BY rowid DESC", (action["id"],))[
            "state"
        ]
        == "satisfied"
    )
    assert store.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY id", (eid,)) == before_stages
    rows = events(store, eid)
    assert [row["kind"] for row in rows] == ["workspace_commit_intent", "workspace_commit_settled"]
    commit = json.loads(rows[1]["payload"])["commit"]
    assert commit == branch_head(workflow, eid) != before_head
    assert git(worktree, "show", f"{commit}:notes.md").decode() == "first measurement\n"
    assert commit_message(action["id"]) in git(worktree, "log", "-1", "--format=%s", commit).decode()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "prepare"),
    [
        ("wait", lambda store, eid: add_job(store, eid)),
        ("needs_owner", lambda store, eid: ask_owner(store, eid)),
        (
            "done",
            lambda store, eid: store.report_state(
                eid, store.working_report(eid, "# Result", str(uuid4())), "ready"
            ),
        ),
    ],
)
async def test_valid_turn_ends_need_real_saved_evidence(runtime, tmp_path, monkeypatch, status, prepare):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    prepare(store, eid)

    assert (await complete(workflow, action, turn(status)))["state"] == "completed"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "reason"),
    [
        ("wait", "wait needs"),
        ("needs_owner", "open ask_owner"),
        ("done", "published report"),
    ],
)
async def test_invalid_turn_end_keeps_the_returned_identity_without_a_commit(
    runtime, tmp_path, monkeypatch, status, reason
):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    worktree = Path(workflow.workspaces.branch(eid)["worktree"])
    (worktree / "uncommitted.md").write_text("rejected edit\n")
    before_head = branch_head(workflow, eid)

    rejected = await complete(workflow, action, turn(status))

    assert rejected["state"] == "failed"
    assert (rejected["thread_id"], rejected["turn_id"]) == ("thread-1", "turn-1")
    assert reason in rejected["error"]
    assert json.loads(rejected["result"])["status"] == status
    assert events(store, eid) == []
    assert branch_head(workflow, eid) == before_head


@pytest.mark.asyncio
async def test_a_persisted_settled_receipt_is_reused_and_later_edits_stay_uncommitted(
    runtime, tmp_path, monkeypatch
):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    worktree = Path(workflow.workspaces.branch(eid)["worktree"])
    (worktree / "first.md").write_text("committed\n")
    receipt = workflow.workspaces.commit_branch(eid, commit_message(action["id"]))["commit"]
    (worktree / "later.md").write_text("must stay local\n")

    def refuse(*_args, **_kwargs):
        raise AssertionError("a settled receipt must not call commit_branch again")

    monkeypatch.setattr(workflow.workspaces, "commit_branch", refuse)
    settled = await complete(workflow, action, turn("continue"))

    assert settled["state"] == "completed"
    assert branch_head(workflow, eid) == receipt
    assert git(worktree, "show", f"{receipt}:first.md").decode() == "committed\n"
    committed = git(worktree, "ls-tree", "--name-only", receipt).decode().splitlines()
    assert "first.md" in committed and "later.md" not in committed
    assert (worktree / "later.md").read_text() == "must stay local\n"


@pytest.mark.asyncio
async def test_an_unknown_commit_intent_is_left_for_inspection_and_never_replayed(
    runtime, tmp_path, monkeypatch
):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    before_head = branch_head(workflow, eid)
    intent = {"parent": before_head, "message": commit_message(action["id"]), "id": str(uuid4())}
    with store.tx() as db:
        store.emit(db, eid, "workspace_commit_intent", intent)

    def refuse(*_args, **_kwargs):
        raise AssertionError("an unknown commit effect must not be replayed")

    monkeypatch.setattr(workflow.workspaces, "commit_branch", refuse)
    with pytest.raises(NeedsAttention, match="needs inspection"):
        await complete(workflow, action, turn("continue"))

    assert saved(store, action)["state"] == "running"
    assert branch_head(workflow, eid) == before_head
    assert [json.loads(row["payload"]) for row in events(store, eid)] == [intent]


@pytest.mark.asyncio
async def test_a_stale_expected_action_fails_closed_before_any_effect(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    with store.tx() as db:
        db.execute("UPDATE agent_actions SET state='superseded' WHERE id=?", (action["id"],))

    def refuse(*_args, **_kwargs):
        raise AssertionError("a stale row must not commit")

    monkeypatch.setattr(workflow.workspaces, "commit_branch", refuse)
    with pytest.raises(NeedsAttention, match="changed during reconciliation"):
        await complete(workflow, action, turn("continue"))

    assert saved(store, action)["state"] == "superseded"
    assert events(store, eid) == []


@pytest.mark.asyncio
async def test_a_row_changed_while_git_awaited_is_not_settled(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    real = workflow.workspaces.commit_branch

    def supersede_during_commit(*args, **kwargs):
        outcome = real(*args, **kwargs)
        with store.tx() as db:
            db.execute("UPDATE agent_actions SET state='superseded' WHERE id=?", (action["id"],))
        return outcome

    monkeypatch.setattr(workflow.workspaces, "commit_branch", supersede_during_commit)
    with pytest.raises(NeedsAttention, match="changed during reconciliation"):
        await complete(workflow, action, turn("continue"))

    assert saved(store, action)["state"] == "superseded"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", [("phase", "cancelled"), ("phase", "complete"), ("phase", "failed"), ("deleted_at", 1.0)]
)
async def test_terminal_cancelled_or_deleted_runs_save_the_outcome_without_committing(
    runtime, tmp_path, monkeypatch, field
):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    before_head = branch_head(workflow, eid)
    with store.tx() as db:
        db.execute(f"UPDATE experiments SET {field[0]}=? WHERE id=?", (field[1], eid))

    settled = await complete(workflow, action, turn("continue"))

    assert settled["state"] == "completed"
    assert json.loads(settled["result"]) == turn("continue")
    assert branch_head(workflow, eid) == before_head
    assert events(store, eid) == []
    assert store.experiment(eid)[field[0]] == field[1]


@pytest.mark.asyncio
async def test_read_back_adopts_the_exact_turn_a_lost_start_left_unknown(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action["id"],)).fetchone())
        db.execute("UPDATE agent_actions SET state='uncertain',turn_id=NULL WHERE id=?", (action["id"],))
        action_obligations.sync_action(db, action["id"], before=before)
    expected = saved(store, action)

    settled = await complete(workflow, expected, turn("continue"), recovered_turn_id="turn-9")

    assert (settled["state"], settled["turn_id"], settled["thread_id"]) == (
        "completed",
        "turn-9",
        "thread-1",
    )


@pytest.mark.asyncio
async def test_a_different_read_back_turn_or_contract_is_refused(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    with pytest.raises(NeedsAttention, match="differs from the saved"):
        await complete(workflow, action, turn("continue"), recovered_turn_id="turn-2")

    contract = store.experiment(eid)["contract"]
    contract["execution_contract"] = 3
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (json.dumps(contract), eid))
    with pytest.raises(NeedsAttention, match="contract 3 action"):
        await complete(workflow, action, turn("continue"))

    assert saved(store, action)["state"] == "running"
    assert events(store, eid) == []


@pytest.mark.asyncio
async def test_a_different_saved_experiment_thread_refuses_before_git(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    with store.tx() as db:
        db.execute("UPDATE experiments SET thread_id='thread-2' WHERE id=?", (eid,))

    def refuse(*_args, **_kwargs):
        raise AssertionError("a mismatched experiment thread must not commit")

    monkeypatch.setattr(workflow.workspaces, "commit_branch", refuse)
    with pytest.raises(NeedsAttention, match="thread differs"):
        await complete(workflow, action, turn("continue"))

    assert saved(store, action)["state"] == "running"
    assert events(store, eid) == []


@pytest.mark.asyncio
async def test_a_pending_request_without_an_open_owner_obligation_is_not_evidence(
    runtime, tmp_path, monkeypatch
):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    ask_owner(store, eid)
    with store.tx() as db:
        db.execute("DELETE FROM obligations WHERE experiment_id=? AND id LIKE 'request:%'", (eid,))

    rejected = await complete(workflow, action, turn("needs_owner"))

    assert rejected["state"] == "failed"
    assert "open ask_owner" in rejected["error"]


@pytest.mark.asyncio
async def test_multiple_action_commit_receipts_fail_closed(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    workflow.workspaces.commit_branch(eid, commit_message(action["id"]))
    before_head = branch_head(workflow, eid)
    with store.tx() as db:
        store.emit(
            db,
            eid,
            "workspace_commit_intent",
            {"parent": before_head, "message": commit_message(action["id"]), "id": str(uuid4())},
        )

    def refuse(*_args, **_kwargs):
        raise AssertionError("duplicate receipts must not commit")

    monkeypatch.setattr(workflow.workspaces, "commit_branch", refuse)
    with pytest.raises(NeedsAttention, match="multiple workspace commit receipts"):
        await complete(workflow, action, turn("continue"))

    current = saved(store, action)
    assert (current["state"], current["turn_id"]) == ("running", "turn-1")
    assert branch_head(workflow, eid) == before_head


@pytest.mark.asyncio
async def test_an_uncertain_git_outcome_keeps_the_adopted_read_back_turn(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    before_head = branch_head(workflow, eid)
    with store.tx() as db:
        before = dict(db.execute("SELECT * FROM agent_actions WHERE id=?", (action["id"],)).fetchone())
        db.execute("UPDATE agent_actions SET state='uncertain',turn_id=NULL WHERE id=?", (action["id"],))
        action_obligations.sync_action(db, action["id"], before=before)
        store.emit(
            db,
            eid,
            "workspace_commit_intent",
            {"parent": before_head, "message": commit_message(action["id"]), "id": str(uuid4())},
        )

    def refuse(*_args, **_kwargs):
        raise AssertionError("an unknown commit effect must not be replayed")

    monkeypatch.setattr(workflow.workspaces, "commit_branch", refuse)
    with pytest.raises(NeedsAttention, match="needs inspection"):
        await complete(workflow, saved(store, action), turn("continue"), recovered_turn_id="turn-9")

    current = saved(store, action)
    assert (current["state"], current["turn_id"]) == ("uncertain", "turn-9")


@pytest.mark.asyncio
async def test_evidence_is_rechecked_after_git_before_settlement(runtime, tmp_path, monkeypatch):
    store, eid, workflow, action = setup(runtime, tmp_path, monkeypatch)
    store.report_state(eid, store.working_report(eid, "# Result", str(uuid4())), "ready")
    worktree = Path(workflow.workspaces.branch(eid)["worktree"])
    (worktree / "recheck.md").write_text("measured\n")
    before_head = branch_head(workflow, eid)
    real = workflow.workspaces.commit_branch

    def drop_report(*args, **kwargs):
        outcome = real(*args, **kwargs)
        with store.tx() as db:
            db.execute("DELETE FROM report_versions WHERE experiment_id=?", (eid,))
        return outcome

    monkeypatch.setattr(workflow.workspaces, "commit_branch", drop_report)
    with pytest.raises(NeedsAttention, match="evidence changed"):
        await complete(workflow, action, turn("done"))

    assert saved(store, action)["state"] == "running"
    assert branch_head(workflow, eid) != before_head
