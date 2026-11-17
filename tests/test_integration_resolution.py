"""Real Git proves the bounded resolver seam and its exact pinned merge (T3.5a-b)."""

import json
from pathlib import Path

import pytest

from experimenttracker.engine import integration_queue as queue
from experimenttracker.engine import integration_resolution
from experimenttracker.engine import integration_worker as worker
from experimenttracker.engine.gitworkspace import commit, create
from experimenttracker.engine.integration import apply, discard, prepare
from experimenttracker.engine.integration_resolution import accept_resolution, finalize, resolver_input
from experimenttracker.store import Store
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


PROJECT = {"base": "main", "check_command": ["python", "-V"], "check_timeout": 30, "require_approval": False}


@pytest.fixture
def store(tmp_path):
    value = Store(tmp_path / "state" / "workflow.sqlite")
    yield value
    value.close()


def queued_conflict(store, tmp_path):
    """A real saved conflict request, exactly as the driver leaves it for the resolver."""
    repo, candidate = conflicted(tmp_path)
    with store.tx() as db:
        queue.enqueue(db, repo, "r1", {"kind": "merge"})
        queue.claim(db, repo)
        saved = queue.checkpoint(db, "r1", "conflict", candidate=candidate, status="integration conflict")
    return repo, saved["candidate"]


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


def test_resolver_input_is_the_exact_conflict_with_bounded_redacted_logs(tmp_path):
    repo = make_repo(tmp_path)
    worktree = Path(create(repo, "demo")["worktree"])
    for index in range(24):
        (worktree / "tracked.txt").write_text(f"experiment {index}\n")
        commit(worktree, f"head {index} Bearer master-secret sk-live-{index} " + "h" * 200)
    (worktree / "tracked.txt").write_text("experiment\n")
    head_commit = commit(worktree, "head final")[0]
    for index in range(24):
        (repo / "tracked.txt").write_text(f"main {index}\n")
        git(repo, "add", "--", "tracked.txt")
        git(repo, "commit", "-m", f"base {index} Bearer base-secret ghp_{index} " + "b" * 200)
    (repo / "tracked.txt").write_text("main\n")
    git(repo, "add", "--", "tracked.txt")
    git(repo, "commit", "-m", "Move main")
    candidate = prepare(repo, "conflict", head_commit)
    experiment = {
        "title": "Conflict sk-title",
        "snapshot": {"plan": {"summary": "plan Bearer plan-secret " + "x" * 4000}},
    }

    value = resolver_input(repo, candidate, experiment)

    assert value["key"] == candidate["key"]
    assert (value["kind"], value["base"]) == ("merge", "main")
    assert value["base_commit"] == candidate["base_commit"]
    assert value["head_commit"] == candidate["head_commit"] == head_commit
    assert (value["worktree"], value["ref"]) == (candidate["worktree"], candidate["ref"])
    assert value["conflicts"] == ["tracked.txt"]
    assert value["head_log"][0].endswith("head final")
    assert value["base_log"][0].endswith("Move main")
    for rows in (value["head_log"], value["base_log"]):
        assert len(rows) <= integration_resolution.MAX_LOG_COMMITS + 1
        assert sum(len(row) + 1 for row in rows) <= (
            integration_resolution.MAX_LOG_CHARS + len("[older commits omitted]")
        )
        assert rows[-1] == "[older commits omitted]"
    for line in value["head_log"]:
        assert "master-secret" not in line and "sk-live" not in line
    assert any("Bearer [redacted]" in line for line in value["head_log"])
    for line in value["base_log"]:
        assert "base-secret" not in line and "ghp_" not in line
    summary = value["experiment_summary"]
    assert summary.startswith("Conflict [redacted]")
    assert "plan Bearer [redacted]" in summary and "plan-secret" not in summary
    assert len(summary) <= integration_resolution.MAX_SUMMARY_CHARS
    assert len(json.dumps(value, ensure_ascii=False).encode()) <= integration_resolution.MAX_INPUT_BYTES


def test_resolver_input_refuses_a_stale_or_unknown_conflict(tmp_path):
    repo, candidate = conflicted(tmp_path)
    with pytest.raises(WorkspaceUnavailable, match="experiment summary"):
        resolver_input(repo, candidate, {})
    with pytest.raises(WorkspaceUnavailable, match="Only a saved integration conflict"):
        resolver_input(repo, {**candidate, "state": "ready"}, {"title": "demo"})
    with pytest.raises(WorkspaceUnavailable, match="merges another commit"):
        resolver_input(repo, {**candidate, "head_commit": candidate["base_commit"]}, {"title": "demo"})
    with pytest.raises(WorkspaceUnavailable, match="not safe"):
        resolver_input(repo, {**candidate, "conflicts": ["../escape.txt"]}, {"title": "demo"})

    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")
    git(worktree, "--literal-pathspecs", "add", "-A", "--", "tracked.txt")
    with pytest.raises(WorkspaceUnavailable, match="no longer matches"):
        resolver_input(repo, candidate, {"title": "demo"})


def test_resolver_input_refuses_a_missing_worktree(tmp_path):
    repo, candidate = conflicted(tmp_path)
    git(repo, "worktree", "remove", "--force", "--force", candidate["worktree"])

    with pytest.raises(WorkspaceUnavailable, match="worktree is missing"):
        resolver_input(repo, candidate, {"title": "demo"})


def test_accept_resolution_returns_the_pinned_merge_to_check_and_approval(store, tmp_path):
    repo, candidate = queued_conflict(store, tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")
    base = head(repo)

    view = accept_resolution(store, "r1", candidate)

    finalized = view["candidate"]
    assert (view["stage"], view["state"], view["owner"]) == ("claimed", "open", "backend")
    assert (finalized["state"], finalized["conflicts"]) == ("ready", [])
    assert head(repo) == base
    assert head(repo, finalized["ref"]) == finalized["candidate_commit"]
    checked = []
    project = {**PROJECT, "require_approval": True}

    def check(cwd, command, timeout):
        checked.append(Path(cwd))
        return True, "ok"

    waiting = worker.drive(store, "r1", project=project, check=check, approval=lambda *_: None)

    assert waiting["outcome"] == "awaiting_approval"
    assert checked == [worktree]
    assert head(repo) == base
    applied = worker.drive(store, "r1", project=project, check=check, approval=lambda *_: True)
    assert applied["outcome"] == "applied"
    assert head(repo) == finalized["candidate_commit"]


def test_accept_resolution_keeps_the_saved_conflict_when_git_is_uncertain(store, tmp_path, monkeypatch):
    repo, candidate = queued_conflict(store, tmp_path)
    worktree = Path(candidate["worktree"])
    (worktree / "tracked.txt").write_text("resolved\n")
    real = integration_resolution.git

    def lost(folder, *args):
        if "commit" in args:
            raise WorkspaceUnavailable("Git could not establish the isolated checkout")
        return real(folder, *args)

    monkeypatch.setattr(integration_resolution, "git", lost)
    with pytest.raises(WorkspaceUnavailable, match="outcome is unknown"):
        accept_resolution(store, "r1", candidate)

    with store.tx() as db:
        saved = queue.saved(db, "r1")
    assert (saved["stage"], saved["candidate"], saved["status"]) == (
        "conflict",
        candidate,
        "integration conflict",
    )
    assert head(repo) == candidate["base_commit"]
    assert head(worktree) == candidate["base_commit"]


def test_accept_resolution_refuses_anything_but_the_exact_open_conflict(store, tmp_path):
    repo, candidate = queued_conflict(store, tmp_path)
    with pytest.raises(ValueError, match="unknown"):
        accept_resolution(store, "missing", candidate)
    with pytest.raises(ValueError, match="exact saved"):
        accept_resolution(store, "r1", {**candidate, "head_commit": candidate["base_commit"]})

    (Path(candidate["worktree"]) / "tracked.txt").write_text("resolved\n")
    accept_resolution(store, "r1", candidate)
    with pytest.raises(ValueError, match="open saved integration conflict"):
        accept_resolution(store, "r1", candidate)
    assert head(repo) == candidate["base_commit"]
