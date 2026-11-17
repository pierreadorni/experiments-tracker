"""Real Git proves named branches, bounded commits, exports and durable pins (T3.1)."""

from pathlib import Path

import pytest

from experimenttracker.engine import gitworkspace
from experimenttracker.engine.gitworkspace import commit, create, export, pin
from experimenttracker.store import Store
from experimenttracker.workspaces import WorkspaceUnavailable, git


def make_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@localhost")
    (repo / "tracked.txt").write_text("value = 1\n")
    (repo / "script.sh").write_text("#!/bin/sh\necho ok\n")
    (repo / "script.sh").chmod(0o755)
    git(repo, "add", "--", "tracked.txt", "script.sh")
    git(repo, "commit", "-m", "Source")
    return repo


def test_create_uses_named_branch_and_exact_worktree(tmp_path, monkeypatch):
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    repo = make_repo(tmp_path)
    planned = gitworkspace.plan(repo, "demo")
    assert not Path(planned["worktree"]).exists()
    created = create(repo, "demo")
    assert planned == created
    assert gitworkspace.verify(repo, planned) == created
    worktree = Path(created["worktree"])
    assert created["repo"] == str(repo.resolve())
    assert created["branch"] == "exp/20260101-demo"
    assert worktree == repo / ".worktrees" / "demo"
    assert (
        git(repo, "rev-parse", f"refs/heads/{created['branch']}").decode().strip() == created["base_commit"]
    )
    assert "/.worktrees/" in (repo / ".git/info/exclude").read_text().splitlines()
    with pytest.raises(WorkspaceUnavailable, match="already exists"):
        create(repo, "demo")


def test_create_refuses_unsafe_or_occupied_identities(tmp_path, monkeypatch):
    repo = make_repo(tmp_path)
    with pytest.raises(WorkspaceUnavailable, match="safe path"):
        create(repo, "../escape")
    assert not (repo / ".worktrees").exists()

    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    git(repo, "branch", "exp/20260101-taken", "main")
    with pytest.raises(WorkspaceUnavailable, match="already exists"):
        create(repo, "taken")

    (repo / ".worktrees" / "occupied").mkdir(parents=True)
    with pytest.raises(WorkspaceUnavailable, match="occupied"):
        create(repo, "occupied")

    linked_repo = make_repo(tmp_path / "linked")
    foreign = tmp_path / "foreign"
    foreign.mkdir()
    (linked_repo / ".worktrees").symlink_to(foreign)
    with pytest.raises(WorkspaceUnavailable, match="parent"):
        create(linked_repo, "demo")


def test_commit_keeps_tracked_edits_and_skips_excluded_untracked(tmp_path):
    repo = make_repo(tmp_path)
    created = create(repo, "commit")
    worktree = Path(created["worktree"])
    (worktree / "tracked.txt").write_text("value = 2\n")
    (worktree / "small.txt").write_text("small\n")
    (worktree / ":(glob)*.txt").write_text("literal path\n")
    (worktree / "large.bin").write_bytes(b"x" * 2048)
    (worktree / ".env").write_text("TOKEN=private\n")
    (worktree / ".ssh").mkdir()
    (worktree / ".ssh" / "id").write_text("private\n")
    git(worktree, "add", "--", ".env")
    with pytest.raises(WorkspaceUnavailable, match="commit boundary"):
        commit(worktree, "Pre-staged bypass", max_untracked_mb=0.001)
    assert git(worktree, "diff", "--cached", "--name-only").decode().strip() == ".env"
    git(worktree, "reset", "--", ".env")

    sha, skipped = commit(worktree, "Record work", max_untracked_mb=0.001)
    assert skipped == [".env", ".ssh/id", "large.bin"]
    assert git(worktree, "show", f"{sha}:tracked.txt").decode() == "value = 2\n"
    assert git(worktree, "show", f"{sha}:small.txt").decode() == "small\n"
    assert git(worktree, "show", "-s", "--format=%an", sha).decode().strip() == "ExperimentTracker Agent"
    assert set(git(worktree, "ls-tree", "-r", "--name-only", sha).decode().splitlines()) == {
        ":(glob)*.txt",
        "script.sh",
        "small.txt",
        "tracked.txt",
    }

    (worktree / "tracked.txt").write_text("v" * 2048)
    sha2, skipped2 = commit(worktree, "Grow tracked file", max_untracked_mb=0.001)
    assert git(worktree, "show", f"{sha2}:tracked.txt").decode() == "v" * 2048
    assert commit(worktree, "No changes", max_untracked_mb=0.001) == (sha2, skipped2)


def test_export_extracts_exact_commit_and_refuses_occupied_targets(tmp_path):
    repo = make_repo(tmp_path)
    worktree = Path(create(repo, "export")["worktree"])
    (worktree / "tracked.txt").write_text("first\n")
    first, _ = commit(worktree, "First")
    (worktree / "later.txt").write_text("later\n")
    commit(worktree, "Second")

    target = tmp_path / "attempt" / "src"
    assert export(repo, first, target) == target
    assert (target / "tracked.txt").read_text() == "first\n"
    assert not (target / "later.txt").exists()

    occupied = tmp_path / "occupied"
    occupied.mkdir()
    (occupied / "sentinel").write_text("keep\n")
    with pytest.raises(WorkspaceUnavailable, match="occupied"):
        export(repo, first, occupied)
    assert (occupied / "sentinel").read_text() == "keep\n"

    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises(WorkspaceUnavailable, match="symlink"):
        export(repo, first, link)

    missing = tmp_path / "missing"
    with pytest.raises(WorkspaceUnavailable, match="commit"):
        export(repo, "0" * 40, missing)
    assert not missing.exists()


def test_pin_keeps_orphaned_commit_reachable_through_gc(tmp_path):
    repo = make_repo(tmp_path)
    created = create(repo, "pinned")
    worktree = Path(created["worktree"])
    (worktree / "work.txt").write_text("keep\n")
    sha, _ = commit(worktree, "Work")
    git(repo, "worktree", "remove", "--force", str(worktree))
    git(repo, "branch", "-D", created["branch"])

    ref = "refs/et/jobs/job-1"
    assert pin(repo, sha, ref) == ref
    assert git(repo, "rev-parse", ref).decode().strip() == sha
    git(repo, "gc", "--prune=now")
    git(repo, "cat-file", "-e", sha)
    assert git(repo, "show", f"{sha}:work.txt").decode() == "keep\n"

    with pytest.raises(WorkspaceUnavailable, match="different commit"):
        pin(repo, created["base_commit"], ref)
    with pytest.raises(WorkspaceUnavailable, match="refs/et/"):
        pin(repo, sha, "refs/heads/main")
    with pytest.raises(WorkspaceUnavailable, match="safe"):
        pin(repo, sha, "refs/et/../escape")


def test_schema_adds_experiment_branches(tmp_path):
    store = Store(tmp_path / "workflow.sqlite")
    try:
        assert store.one("SELECT MAX(version) AS version FROM schema_version")["version"] == 25
        columns = [row["name"] for row in store.rows("PRAGMA table_info(experiment_branches)")]
        assert (
            columns
            == "experiment_id repo branch worktree base_commit head_commit state merged_commit created_at updated_at".split()
        )
    finally:
        store.close()
