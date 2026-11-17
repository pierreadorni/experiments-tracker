"""Real Git proves only a verified resolution becomes the pinned merge candidate (T3.5a)."""

from pathlib import Path

import pytest

from experimenttracker.engine import integration_resolution
from experimenttracker.engine.gitworkspace import commit, create
from experimenttracker.engine.integration import apply, discard, prepare
from experimenttracker.engine.integration_resolution import finalize
from experimenttracker.workspaces import WorkspaceUnavailable, git


def make_repo(tmp_path, name="tracked.txt"):
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@localhost")
    (repo / name).parent.mkdir(parents=True, exist_ok=True)
    (repo / name).write_text("base\n")
    git(repo, "add", "--", name)
    git(repo, "commit", "-m", "Source")
    return repo


def conflicted(tmp_path, name="tracked.txt"):
    """A prepared ``--no-ff`` merge whose only conflict is ``name``."""
    repo = make_repo(tmp_path, name)
    worktree = Path(create(repo, "demo")["worktree"])
    (worktree / name).write_text("experiment\n")
    (worktree / "added.txt").write_text("added\n")
    head_commit = commit(worktree, "Experiment work")[0]
    (repo / name).write_text("main\n")
    git(repo, "add", "--", name)
    git(repo, "commit", "-m", "Move main")
    return repo, prepare(repo, "conflict", head_commit)


def head(repo, ref="HEAD"):
    return git(repo, "rev-parse", ref).decode().strip()


def ref_exists(repo, ref):
    return bool(git(repo, "for-each-ref", "--format=%(refname)", ref).decode().strip())


def test_finalize_commits_the_exact_resolved_merge_and_stays_applyable(tmp_path):
    repo, candidate = conflicted(tmp_path)
    assert candidate["state"] == "conflict"
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")

    ready = finalize(repo, candidate)

    assert ready["state"] == "ready"
    assert ready["conflicts"] == []
    assert candidate["candidate_commit"] is None and candidate["state"] == "conflict"
    commit_sha = ready["candidate_commit"]
    assert commit_sha not in (None, candidate["base_commit"], candidate["head_commit"])
    assert git(repo, "rev-list", "--parents", "-n", "1", commit_sha).decode().split() == [
        commit_sha,
        candidate["base_commit"],
        candidate["head_commit"],
    ]
    assert head(repo, ready["ref"]) == commit_sha
    # The base branch and the main checkout are untouched until apply fast-forwards them.
    assert head(repo) == candidate["base_commit"]
    assert (repo / "tracked.txt").read_text() == "main\n"
    assert git(repo, "status", "--porcelain").decode() == ""
    # The candidate worktree stays locked, detached and at the pinned merge commit.
    assert head(worktree) == commit_sha
    assert (worktree / "tracked.txt").read_text() == "resolved\n"
    with pytest.raises(WorkspaceUnavailable):
        git(worktree, "symbolic-ref", "--quiet", "HEAD")
    with pytest.raises(WorkspaceUnavailable, match="Only a saved integration conflict"):
        finalize(repo, ready)

    applied = apply(repo, ready)

    assert applied["merged_commit"] == commit_sha
    assert head(repo) == commit_sha
    assert (repo / "tracked.txt").read_text() == "resolved\n"
    assert (repo / "added.txt").read_text() == "added\n"
    discard(repo, ready)
    assert not worktree.exists()
    assert not ref_exists(repo, ready["ref"])


def test_finalize_refuses_leftover_markers_and_keeps_the_conflict(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("<<<<<<< HEAD\nmain\n=======\nexperiment\n>>>>>>> demo\n")

    with pytest.raises(WorkspaceUnavailable, match="conflict markers"):
        finalize(repo, candidate)

    assert not ref_exists(repo, candidate["ref"])
    assert head(repo) == candidate["base_commit"]
    assert (repo / "tracked.txt").read_text() == "main\n"
    assert git(worktree, "diff", "--name-only", "--diff-filter=U").decode().split() == ["tracked.txt"]
    assert head(worktree) == candidate["base_commit"]


def test_finalize_refuses_whitespace_errors_from_diff_check(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved   \n")

    with pytest.raises(WorkspaceUnavailable, match="whitespace|markers"):
        finalize(repo, candidate)

    assert not ref_exists(repo, candidate["ref"])
    assert head(repo) == candidate["base_commit"]
    assert head(worktree) == candidate["base_commit"]

    # No commit exists yet, so the same conflict can be finished once the file is fixed.
    (worktree / "tracked.txt").write_text("resolved\n")
    assert finalize(repo, candidate)["state"] == "ready"


def test_finalize_refuses_edits_outside_the_permitted_conflict_paths(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")

    (worktree / "notes.txt").write_text("scratch\n")
    with pytest.raises(WorkspaceUnavailable, match="untracked"):
        finalize(repo, candidate)
    (worktree / "notes.txt").unlink()

    (worktree / "added.txt").write_text("hacked\n")
    with pytest.raises(WorkspaceUnavailable, match="outside the conflicted files"):
        finalize(repo, candidate)
    (worktree / "added.txt").write_text("added\n")

    # Every refusal left the exact conflict usable, so the resolver's own edit can finish it.
    assert finalize(repo, candidate)["state"] == "ready"
    assert head(repo) == candidate["base_commit"]


def test_finalize_refuses_wrong_identity_and_unverifiable_merge_state(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")

    with pytest.raises(WorkspaceUnavailable, match="merges another commit"):
        finalize(repo, {**candidate, "head_commit": candidate["base_commit"]})
    with pytest.raises(WorkspaceUnavailable, match="Only a saved integration conflict"):
        finalize(repo, {**candidate, "candidate_commit": candidate["base_commit"]})
    with pytest.raises(WorkspaceUnavailable, match="not at the saved conflict"):
        finalize(repo, {**candidate, "base_commit": candidate["head_commit"]})
    with pytest.raises(WorkspaceUnavailable, match="safe key"):
        finalize(repo, {**candidate, "key": "../escape", "ref": "refs/et/candidates/../escape"})
    with pytest.raises(WorkspaceUnavailable, match="not safe"):
        finalize(repo, {**candidate, "repo": str(tmp_path)})
    with pytest.raises(WorkspaceUnavailable, match="specialized resolution path"):
        finalize(repo, {**candidate, "kind": "revert"})
    with pytest.raises(WorkspaceUnavailable, match="not safe"):
        finalize(repo, {**candidate, "conflicts": ["../escape.txt"]})
    with pytest.raises(WorkspaceUnavailable, match="not exact"):
        finalize(repo, {**candidate, "conflicts": ["tracked.txt", "tracked.txt"]})
    with pytest.raises(WorkspaceUnavailable, match="path list is required"):
        finalize(repo, {**candidate, "conflicts": []})
    with pytest.raises(WorkspaceUnavailable, match="Only a saved integration conflict"):
        finalize(repo, {**candidate, "state": "preparing"})

    auto = git(worktree, "rev-parse", "AUTO_MERGE").decode().strip()
    git(worktree, "update-ref", "-d", "AUTO_MERGE")
    with pytest.raises(WorkspaceUnavailable, match="did not record the merge"):
        finalize(repo, candidate)
    git(worktree, "update-ref", "AUTO_MERGE", auto)

    git(repo, "update-ref", "refs/et/candidates/conflict", candidate["base_commit"])
    with pytest.raises(WorkspaceUnavailable, match="already uses this key"):
        finalize(repo, candidate)
    git(repo, "update-ref", "-d", "refs/et/candidates/conflict")

    git(repo, "worktree", "remove", "--force", "--force", str(worktree))
    with pytest.raises(WorkspaceUnavailable, match="worktree is missing"):
        finalize(repo, candidate)
    assert head(repo) == candidate["base_commit"]


def test_finalize_never_retries_a_worktree_that_left_the_conflict_commit(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")
    git(worktree, "--literal-pathspecs", "add", "-A", "--", "tracked.txt")
    git(worktree, "commit", "-m", "Manual merge")
    manual = head(worktree)
    assert manual != candidate["base_commit"]

    with pytest.raises(WorkspaceUnavailable, match="unpinned merge was kept"):
        finalize(repo, candidate)

    assert head(worktree) == manual
    assert (worktree / "tracked.txt").read_text() == "resolved\n"
    assert not ref_exists(repo, candidate["ref"])
    assert head(repo) == candidate["base_commit"]


def test_finalize_refuses_a_pre_staged_change_outside_the_conflict_paths(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")
    # The prepared merge stages its own clean result, so this edit is invisible to a
    # worktree-only scope check once it is in the index.
    (worktree / "added.txt").write_text("changed\n")
    git(worktree, "add", "--", "added.txt")

    with pytest.raises(WorkspaceUnavailable, match="staged resolution changed a path outside"):
        finalize(repo, candidate)

    assert not ref_exists(repo, candidate["ref"])
    assert head(worktree) == candidate["base_commit"]
    assert head(repo) == candidate["base_commit"]


def test_finalize_refuses_a_pre_staged_untracked_path_and_accepts_the_recovery(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")
    (worktree / "notes.txt").write_text("scratch\n")
    git(worktree, "add", "--", "notes.txt")

    with pytest.raises(WorkspaceUnavailable, match="staged resolution changed a path outside"):
        finalize(repo, candidate)

    git(worktree, "rm", "--cached", "--force", "--", "notes.txt")
    (worktree / "notes.txt").unlink()
    assert finalize(repo, candidate)["state"] == "ready"
    assert head(repo) == candidate["base_commit"]


def test_finalize_preserves_a_deletion_resolution(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").unlink()

    ready = finalize(repo, candidate)

    assert ready["state"] == "ready"
    assert not (worktree / "tracked.txt").exists()
    apply(repo, ready)
    assert not (repo / "tracked.txt").exists()
    assert (repo / "added.txt").read_text() == "added\n"


def test_finalize_never_reads_a_symlinked_conflict_path(tmp_path):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    outside = tmp_path / "outside.txt"
    outside.write_text("<<<<<<< HEAD\nleaked\n")
    (worktree / "tracked.txt").unlink()
    (worktree / "tracked.txt").symlink_to(outside)

    with pytest.raises(WorkspaceUnavailable, match="is a symlink"):
        finalize(repo, candidate)
    assert (worktree / "tracked.txt").is_symlink()


def test_finalize_refuses_a_conflict_path_that_resolves_outside_the_worktree(tmp_path):
    repo, candidate = conflicted(tmp_path, "sub/tracked.txt")
    worktree = Path(candidate["worktree"])
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "tracked.txt").write_text("<<<<<<< HEAD\nleaked\n")
    (worktree / "sub" / "tracked.txt").unlink()
    (worktree / "sub").rmdir()
    (worktree / "sub").symlink_to(outside)

    with pytest.raises(WorkspaceUnavailable, match="resolves outside the worktree"):
        finalize(repo, candidate)

    assert not ref_exists(repo, candidate["ref"])
    assert head(repo) == candidate["base_commit"]


def test_finalize_reports_a_lost_commit_response_as_unknown(tmp_path, monkeypatch):
    repo, candidate = conflicted(tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")
    real = integration_resolution.git

    def lost(folder, *args):
        if "commit" in args:
            raise WorkspaceUnavailable("Git could not establish the isolated checkout")
        return real(folder, *args)

    monkeypatch.setattr(integration_resolution, "git", lost)
    with pytest.raises(WorkspaceUnavailable, match="outcome is unknown"):
        finalize(repo, candidate)

    assert not ref_exists(repo, candidate["ref"])
    assert head(worktree) == candidate["base_commit"]
    assert (worktree / "tracked.txt").read_text() == "resolved\n"
    assert head(repo) == candidate["base_commit"]
