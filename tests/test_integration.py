"""Real Git proves integration candidates stay local and apply only by fast-forward (T3.4a)."""

from pathlib import Path

import pytest

from experimenttracker.engine.gitworkspace import commit, create
from experimenttracker.engine.integration import apply, discard, prepare, prepare_revert
from experimenttracker.workspaces import WorkspaceUnavailable, git


def make_repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir(parents=True)
    git(repo, "init", "--initial-branch=main")
    git(repo, "config", "user.name", "Test")
    git(repo, "config", "user.email", "test@localhost")
    (repo / "tracked.txt").write_text("base\n")
    git(repo, "add", "--", "tracked.txt")
    git(repo, "commit", "-m", "Source")
    return repo


def experiment(repo, slug="demo", content="experiment\n"):
    worktree = Path(create(repo, slug)["worktree"])
    (worktree / "tracked.txt").write_text(content)
    (worktree / "added.txt").write_text("added\n")
    return commit(worktree, "Experiment work")[0]


def head(repo, ref="main"):
    return git(repo, "rev-parse", ref).decode().strip()


def ref_exists(repo, ref):
    return bool(git(repo, "for-each-ref", "--format=%(refname)", ref).decode().strip())


def test_prepare_merges_exact_commit_without_moving_base(tmp_path):
    repo = make_repo(tmp_path)
    base = head(repo)
    experiment_sha = experiment(repo)

    candidate = prepare(repo, "exp-1", experiment_sha[:12])

    assert candidate["kind"] == "merge"
    assert candidate["state"] == "ready"
    assert candidate["conflicts"] == []
    assert (candidate["base_commit"], candidate["head_commit"]) == (base, experiment_sha)
    assert candidate["candidate_commit"] not in (None, base, experiment_sha)
    assert candidate["worktree"] == str(repo.resolve() / ".worktrees" / "_integration")
    assert candidate["ref"] == "refs/et/candidates/exp-1"
    assert git(repo, "rev-list", "--parents", "-n", "1", candidate["candidate_commit"]).decode().split() == [
        candidate["candidate_commit"],
        base,
        experiment_sha,
    ]
    # The base branch and the main checkout are untouched by candidate preparation.
    assert head(repo) == base
    assert (repo / "tracked.txt").read_text() == "base\n"
    assert head(repo, candidate["ref"]) == candidate["candidate_commit"]
    worktree = Path(candidate["worktree"])
    assert (worktree / "tracked.txt").read_text() == "experiment\n"
    assert (worktree / "added.txt").read_text() == "added\n"
    assert git(repo, "status", "--porcelain").decode() == ""


def test_apply_fast_forwards_exactly_then_discard_removes_only_the_candidate(tmp_path):
    repo = make_repo(tmp_path)
    candidate = prepare(repo, "exp-1", experiment(repo))

    applied = apply(repo, candidate)

    assert applied["merged_commit"] == candidate["candidate_commit"]
    assert applied["previous_base_commit"] == candidate["base_commit"]
    assert head(repo) == candidate["candidate_commit"]
    assert (repo / "tracked.txt").read_text() == "experiment\n"
    with pytest.raises(WorkspaceUnavailable, match="moved"):
        apply(repo, candidate)

    assert discard(repo, candidate)["state"] == "discarded"
    assert not Path(candidate["worktree"]).exists()
    assert not ref_exists(repo, candidate["ref"])
    assert (repo / "tracked.txt").read_text() == "experiment\n"


def test_prepare_preserves_conflicts_without_touching_base(tmp_path):
    repo = make_repo(tmp_path)
    experiment_sha = experiment(repo)
    (repo / "tracked.txt").write_text("main\n")
    git(repo, "add", "--", "tracked.txt")
    git(repo, "commit", "-m", "Move main")
    base = head(repo)

    candidate = prepare(repo, "conflict", experiment_sha)

    assert candidate["state"] == "conflict"
    assert candidate["candidate_commit"] is None
    assert candidate["conflicts"] == ["tracked.txt"]
    assert head(repo) == base
    assert (repo / "tracked.txt").read_text() == "main\n"
    assert "<<<<<<<" in (Path(candidate["worktree"]) / "tracked.txt").read_text()
    assert not ref_exists(repo, candidate["ref"])
    with pytest.raises(WorkspaceUnavailable, match="already registered"):
        prepare(repo, "other", experiment_sha)

    discard(repo, candidate)
    assert not Path(candidate["worktree"]).exists()
    assert head(repo) == base


def test_apply_rejects_dirty_wrong_checkout_and_stale_base(tmp_path):
    repo = make_repo(tmp_path)
    experiment_sha = experiment(repo)
    candidate = prepare(repo, "exp-1", experiment_sha)

    (repo / "tracked.txt").write_text("uncommitted\n")
    with pytest.raises(WorkspaceUnavailable, match="uncommitted"):
        apply(repo, candidate)
    git(repo, "checkout", "--", "tracked.txt")

    git(repo, "checkout", "--detach", "main")
    try:
        with pytest.raises(WorkspaceUnavailable, match="base branch"):
            apply(repo, candidate)
    finally:
        git(repo, "checkout", "main")

    with pytest.raises(WorkspaceUnavailable, match="main checkout"):
        apply(repo / ".worktrees" / "demo", candidate)

    (repo / "other.txt").write_text("moved\n")
    git(repo, "add", "--", "other.txt")
    git(repo, "commit", "-m", "Move base")
    with pytest.raises(WorkspaceUnavailable, match="moved"):
        apply(repo, candidate)

    discard(repo, candidate)
    fresh = prepare(repo, "exp-2", experiment_sha)
    assert fresh["base_commit"] == head(repo)
    apply(repo, fresh)
    assert (repo / "other.txt").read_text() == "moved\n"
    assert (repo / "tracked.txt").read_text() == "experiment\n"


def test_prepare_revert_undoes_an_applied_merge(tmp_path):
    repo = make_repo(tmp_path)
    experiment_sha = experiment(repo)
    merged_candidate = prepare(repo, "exp-1", experiment_sha)
    merged = apply(repo, merged_candidate)["merged_commit"]
    discard(repo, merged_candidate)

    revert = prepare_revert(repo, "revert-1", merged)

    assert revert["kind"] == "revert"
    assert revert["state"] == "ready"
    assert (revert["base_commit"], revert["head_commit"]) == (merged, merged)
    assert git(repo, "rev-list", "--parents", "-n", "1", revert["candidate_commit"]).decode().split() == [
        revert["candidate_commit"],
        merged,
    ]
    assert (repo / "tracked.txt").read_text() == "experiment\n"

    assert apply(repo, revert)["merged_commit"] == revert["candidate_commit"]
    assert head(repo) == revert["candidate_commit"]
    assert (repo / "tracked.txt").read_text() == "base\n"
    assert not (repo / "added.txt").exists()
    with pytest.raises(WorkspaceUnavailable, match="merge commit"):
        prepare_revert(repo, "bad", experiment_sha)


def test_recovery_refuses_to_remove_an_unrelated_worktree(tmp_path):
    repo = make_repo(tmp_path)
    other = repo / ".worktrees" / "_integration"
    other.parent.mkdir(parents=True)
    git(repo, "worktree", "add", "-b", "other", str(other), "main")
    (other / "sentinel.txt").write_text("keep\n")
    experiment_sha = experiment(repo)
    forged = {
        "repo": str(repo.resolve()),
        "key": "exp-1",
        "kind": "merge",
        "base": "main",
        "base_commit": head(repo),
        "head_commit": experiment_sha,
        "worktree": str(other),
        "ref": "refs/et/candidates/exp-1",
        "candidate_commit": None,
        "state": "conflict",
    }

    with pytest.raises(WorkspaceUnavailable, match="already registered"):
        prepare(repo, "exp-1", experiment_sha)
    with pytest.raises(WorkspaceUnavailable, match="does not belong"):
        discard(repo, forged)
    assert (other / "sentinel.txt").read_text() == "keep\n"
    assert head(repo, "other") == forged["base_commit"]

    with pytest.raises(WorkspaceUnavailable, match="not safe"):
        discard(repo, {**forged, "worktree": str(tmp_path / "elsewhere")})
    with pytest.raises(WorkspaceUnavailable, match="safe key"):
        discard(repo, {**forged, "key": "../escape"})
    assert other.exists()


def test_discard_refuses_stale_foreign_and_symlinked_worktrees(tmp_path):
    repo = make_repo(tmp_path)
    stale = prepare(repo, "a", experiment(repo, "a"))
    git(repo, "worktree", "remove", "--force", "--force", stale["worktree"])
    git(repo, "update-ref", "-d", stale["ref"])
    current = prepare(repo, "a", experiment(repo, "b", "different\n"))

    with pytest.raises(WorkspaceUnavailable, match="does not belong"):
        discard(repo, stale)
    assert Path(current["worktree"]).exists()
    assert head(repo, current["ref"]) == current["candidate_commit"]
    discard(repo, current)

    other = repo / ".worktrees" / "_integration"
    git(repo, "worktree", "add", "--detach", str(other), "main")
    git(repo, "worktree", "lock", "--reason", "experimenttracker:integration:other", str(other))
    forged = {**current, "candidate_commit": None, "state": "conflict"}
    with pytest.raises(WorkspaceUnavailable, match="does not belong"):
        discard(repo, forged)

    git(repo, "worktree", "remove", "--force", "--force", str(other))
    other.symlink_to(tmp_path)
    with pytest.raises(WorkspaceUnavailable, match="unsafe"):
        discard(repo, forged)
    assert other.is_symlink()
