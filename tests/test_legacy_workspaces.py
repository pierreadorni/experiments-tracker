"""The adopt-legacy plan reports and applies exact detached worktrees (T3.7)."""

import pytest

from experimenttracker.engine import legacy_workspaces
from experimenttracker.store import Store, encode
from experimenttracker.workspaces import Workspaces, WorkspaceUnavailable, git

EID = "00000000-0000-4000-8000-000000000001"
BRANCH = "exp/legacy-00000000"


def checkout(tmp_path, phase="complete"):
    repo = tmp_path / "repo"
    repo.mkdir()
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@localhost")
    (repo / "a.py").write_text("value = 1\n")
    git(repo, "add", "--", "a.py")
    git(repo, "commit", "-m", "Source")
    head = git(repo, "rev-parse", "HEAD").decode().strip()
    folder = tmp_path / "workspaces" / EID / "checkout"
    folder.parent.mkdir(parents=True)
    git(repo, "worktree", "add", "--detach", "--lock", "--reason", "experimenttracker:" + EID, str(folder), head)  # fmt: skip
    common = (repo / git(repo, "rev-parse", "--git-common-dir").decode().strip()).resolve()
    store = Store(tmp_path / "state" / "workflow.sqlite")
    with store.tx() as db:
        db.execute("INSERT INTO experiments(id,brief,recipe_id,contract,phase,created_at,updated_at) VALUES(?,?,?,?,?,0,0)", (EID, "Brief", "agent_experiment_v1", encode({"execution_contract": 3}), phase))  # fmt: skip
        db.execute("INSERT INTO isolated_workspaces(experiment_id,source,folder,head,baseline,common_dir,git_device,git_inode,state,device,inode,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,0)", (EID, str(repo), str(folder), head, encode({}), str(common), common.stat().st_dev, common.stat().st_ino, "ready", folder.stat().st_dev, folder.stat().st_ino))  # fmt: skip
    return store, repo, folder


def test_plan_reports_a_dirty_exact_worktree_read_only_then_blocks_drift(tmp_path):
    store, repo, folder = checkout(tmp_path)
    (folder / "a.py").write_text("value = 2\n")
    report = legacy_workspaces.plan(store)
    item = report["worktrees"][0]
    assert (report["apply"], report["ok"], item["state"], item["branch"], item["dirty"], item["reasons"]) == (False, True, "ready", BRANCH, True, [])  # fmt: skip
    assert git(folder, "diff", "--cached", "--name-only") == b""
    assert git(folder, "rev-parse", "--abbrev-ref", "HEAD").strip() == b"HEAD"
    assert store.rows("SELECT * FROM experiment_branches") == []
    git(folder, "switch", "-c", "other")
    assert "the saved checkout is not detached" in legacy_workspaces.plan(store)["worktrees"][0]["reasons"]
    git(folder, "switch", "--detach", "HEAD")
    git(repo, "branch", BRANCH, "HEAD")
    assert (
        "the planned legacy branch already exists" in legacy_workspaces.plan(store)["worktrees"][0]["reasons"]
    )


def test_apply_attaches_the_planned_branch_and_commits_pending_edits_in_place(tmp_path):
    store, repo, folder = checkout(tmp_path)
    main_head, main_status = git(repo, "rev-parse", "main"), git(repo, "status", "--porcelain", "-z")
    (folder / "a.py").write_text("value = 2\n")
    (folder / "notes.txt").write_text("pending\n")
    report = legacy_workspaces.apply(store)
    item = report["worktrees"][0]
    assert (report["apply"], report["ok"], item["state"], item["branch"], item["dirty"], item["reasons"]) == (True, True, "adopted", BRANCH, True, [])  # fmt: skip
    assert item["skipped"] == [] and item["head"] == git(folder, "rev-parse", "HEAD").decode().strip()
    assert git(folder, "rev-parse", "--abbrev-ref", "HEAD").strip() == BRANCH.encode()
    assert git(folder, "show", "HEAD:a.py") == b"value = 2\n"
    assert git(folder, "show", "HEAD:notes.txt") == b"pending\n"
    assert git(repo, "rev-parse", "main") == main_head
    assert git(repo, "status", "--porcelain", "-z") == main_status
    saved = store.one("SELECT * FROM isolated_workspaces WHERE experiment_id=?", (EID,))
    assert (saved["state"], saved["head"]) == ("ready", main_head.decode().strip())
    branch = store.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (EID,))
    assert (branch["repo"], branch["branch"], branch["worktree"], branch["state"]) == (str(repo), BRANCH, str(folder), "active")  # fmt: skip
    assert (branch["base_commit"], branch["head_commit"]) == (saved["head"], item["head"])
    adopted = Workspaces(type("Projects", (), {"store": store})())
    assert adopted.checkout(EID) == adopted.branch_checkout(EID) == str(folder)
    assert legacy_workspaces.apply(store)["worktrees"][0]["reasons"] == [
        "an experiment branch identity is already saved for this workspace"
    ]
    git(folder, "commit", "--allow-empty", "-m", "Drift")
    with pytest.raises(WorkspaceUnavailable, match="saved branch"):
        adopted.checkout(EID)


def test_apply_blocks_unsettled_and_drifted_rows_without_recording_branches(tmp_path):
    store, repo, folder = checkout(tmp_path, phase="running")
    report = legacy_workspaces.apply(store)
    item = report["worktrees"][0]
    assert (report["ok"], item["state"]) == (False, "blocked")
    assert "experiment phase running is not terminal" in item["reasons"]
    assert not git(repo, "for-each-ref", "--format=%(refname)", f"refs/heads/{BRANCH}")
    assert store.rows("SELECT * FROM experiment_branches") == []
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='complete'")
        db.execute("UPDATE isolated_workspaces SET head=?", ("0" * 40,))
    assert "HEAD differs from the saved baseline" in legacy_workspaces.apply(store)["worktrees"][0]["reasons"][0]  # fmt: skip
    assert git(folder, "rev-parse", "--abbrev-ref", "HEAD").strip() == b"HEAD"


def test_apply_fails_closed_when_the_branch_identity_is_already_saved(tmp_path):
    store, repo, folder = checkout(tmp_path)
    with store.tx() as db:
        db.execute(
            "INSERT INTO experiment_branches(experiment_id,repo,branch,worktree,base_commit,head_commit,state,created_at,updated_at) VALUES(?,?,?,?,?,?,'active',0,0)",
            (EID, str(repo), BRANCH, str(folder), "a" * 40, "a" * 40),
        )
    item = legacy_workspaces.apply(store)["worktrees"][0]
    assert (item["state"], item["reasons"]) == ("blocked", ["an experiment branch identity is already saved for this workspace"])  # fmt: skip
    assert git(folder, "rev-parse", "--abbrev-ref", "HEAD").strip() == b"HEAD"
    assert not git(repo, "for-each-ref", "--format=%(refname)", f"refs/heads/{BRANCH}")
    assert store.one("SELECT head_commit FROM experiment_branches WHERE experiment_id=?", (EID,))["head_commit"] == "a" * 40  # fmt: skip


def test_apply_leaves_the_saved_intent_when_the_bounded_commit_fails(tmp_path, monkeypatch):
    store, repo, folder = checkout(tmp_path)
    (folder / "a.py").write_text("value = 2\n")

    def lost(folder, message, max_untracked_mb=5):
        raise WorkspaceUnavailable("bounded commit outcome lost")

    monkeypatch.setattr(legacy_workspaces.gitworkspace, "commit", lost)
    item = legacy_workspaces.apply(store)["worktrees"][0]
    assert (item["state"], item["reasons"]) == ("blocked", ["bounded commit outcome lost"])
    saved = store.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (EID,))
    assert (saved["branch"], saved["state"]) == (BRANCH, "active")
    assert saved["head_commit"] == store.one("SELECT head FROM isolated_workspaces WHERE experiment_id=?", (EID,))["head"]  # fmt: skip
    assert git(folder, "rev-parse", "--abbrev-ref", "HEAD").strip() == BRANCH.encode()
    assert (
        "an experiment branch identity is already saved for this workspace"
        in legacy_workspaces.apply(store)["worktrees"][0]["reasons"]
    )


def test_plan_blocks_unsettled_work_and_an_unready_checkout(tmp_path):
    store, _, _ = checkout(tmp_path, phase="running")
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,ordinal,state,updated_at) VALUES(?,'run',1,'running',0)",
            (EID,),
        )
        db.execute("INSERT INTO jobs(intent_id,experiment_id,stage_id,state,script_hash,resources,snapshot_hash,created_at) VALUES('job',?,'run','submitted','s','{}','x',0)", (EID,))  # fmt: skip
        db.execute("INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,input,created_at,updated_at) VALUES('action',?,'prepare',1,'running','{}',0,0)", (EID,))  # fmt: skip
    item = legacy_workspaces.plan(store)["worktrees"][0]
    assert item["state"] == "blocked"
    assert {"experiment phase running is not terminal", "saved jobs are not terminal", "saved agent actions are not terminal"} <= set(item["reasons"])  # fmt: skip
    with store.tx() as db:
        db.execute("UPDATE experiments SET phase='complete'")
        db.execute("DELETE FROM jobs")
        db.execute("DELETE FROM agent_actions")
        db.execute("UPDATE isolated_workspaces SET state='creating'")
    assert "not ready" in legacy_workspaces.plan(store)["worktrees"][0]["reasons"][0]
