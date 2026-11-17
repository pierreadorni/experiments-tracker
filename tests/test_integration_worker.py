"""Real Git and SQLite prove the integration driver's exact saved boundaries (T3.4b-2)."""

from pathlib import Path

import pytest

from experimenttracker.engine import integration
from experimenttracker.engine import integration_queue as queue
from experimenttracker.engine import integration_worker as worker
from experimenttracker.engine.gitworkspace import commit, create
from experimenttracker.store import Store
from experimenttracker.workspaces import WorkspaceUnavailable, git

PROJECT = {"base": "main", "check_command": ["pytest", "-q"], "check_timeout": 30, "require_approval": False}


@pytest.fixture
def store(tmp_path):
    value = Store(tmp_path / "state" / "workflow.sqlite")
    yield value
    value.close()


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


def experiment(repo, content="experiment\n"):
    worktree = Path(create(repo, "demo")["worktree"])
    (worktree / "tracked.txt").write_text(content)
    (worktree / "added.txt").write_text("added\n")
    return commit(worktree, "Experiment work")[0]


def head(repo, ref="main"):
    return git(repo, "rev-parse", ref).decode().strip()


def ref_exists(repo, ref):
    return bool(git(repo, "for-each-ref", "--format=%(refname)", ref).decode().strip())


def claim(store, repo, request=None, request_id="r1"):
    with store.tx() as db:
        queue.enqueue(db, repo, request_id, request or {"kind": "merge"})
        return queue.claim(db, repo)


def saved(store, request_id="r1"):
    with store.tx() as db:
        return queue.saved(db, request_id)


def ok(*_):
    return True, "ok"


def owner_approved(store, request_id="r1"):
    """Save the transition the authenticated owner approval command performs."""
    with store.tx() as db:
        return queue.checkpoint(db, request_id, "ready", status="owner approved")


def requested(store, tmp_path, **kwargs):
    repo = make_repo(tmp_path)
    sha = experiment(repo)
    claim(store, repo, **kwargs)
    return repo, sha


def test_clean_merge_then_revert_use_saved_identities(store, tmp_path):
    repo, sha = requested(store, tmp_path)
    seen = []

    def check(cwd, command, timeout):
        seen.append((Path(cwd), command, timeout))
        return True, "ok"

    result = worker.drive(store, "r1", head=sha, project=PROJECT, check=check)

    view = saved(store)
    candidate = view["candidate"]
    assert (result["outcome"], view["stage"], view["state"]) == ("applied", "applied", "satisfied")
    assert head(repo) == candidate["candidate_commit"]
    assert seen == [(Path(candidate["worktree"]), ["pytest", "-q"], 30)]
    assert not ref_exists(repo, candidate["ref"])

    merged = candidate["candidate_commit"]
    with store.tx() as db:
        queue.enqueue(db, repo, "r2", {"kind": "revert", "merge_commit": merged})
        queue.claim(db, repo)
    reverted = worker.drive(store, "r2", project=PROJECT, check=ok)
    view = saved(store, "r2")
    assert reverted["outcome"] == "applied"
    assert (view["candidate"]["kind"], view["candidate"]["head_commit"]) == ("revert", merged)
    assert head(repo) == view["candidate"]["candidate_commit"]
    assert (repo / "tracked.txt").read_text() == "base\n"


@pytest.mark.parametrize("mode", ["failed", "timeout", "dirty_before", "dirty_after"])
def test_check_failures_never_apply_and_keep_the_exact_candidate(store, tmp_path, mode):
    repo, sha = requested(store, tmp_path)
    base = head(repo)

    def check(cwd, command, timeout):
        if mode == "failed":
            return False, "nope"
        if mode == "timeout":
            raise TimeoutError
        if mode == "dirty_before":
            pytest.fail("a dirty candidate must fail before the project check runs")
        (Path(cwd) / "tracked.txt").write_text("changed\n")
        return True, "ok"

    prepare = integration.prepare
    if mode == "dirty_before":

        def prepare(repo_, key, head_, base_="main"):
            candidate = integration.prepare(repo_, key, head_, base_)
            (Path(candidate["worktree"]) / "tracked.txt").write_text("dirty\n")
            return candidate

    result = worker.drive(store, "r1", head=sha, project=PROJECT, check=check, prepare=prepare)

    view = saved(store)
    assert result["outcome"] == "check_failed"
    assert view["stage"] == "check_failed"
    assert head(repo) == base
    assert head(repo, view["candidate"]["ref"]) == view["candidate"]["candidate_commit"]


def test_conflict_is_saved_and_never_checked_or_applied(store, tmp_path):
    repo = make_repo(tmp_path)
    sha = experiment(repo)
    (repo / "tracked.txt").write_text("main\n")
    git(repo, "add", "--", "tracked.txt")
    git(repo, "commit", "-m", "Move main")
    base = head(repo)
    claim(store, repo)

    result = worker.drive(
        store, "r1", head=sha, project=PROJECT, check=lambda *_: pytest.fail("no check on conflict")
    )

    view = saved(store)
    assert result["outcome"] == "conflict"
    assert view["stage"] == "conflict"
    assert view["candidate"]["conflicts"] == ["tracked.txt"]
    assert head(repo) == base
    assert "<<<<<<<" in (Path(view["candidate"]["worktree"]) / "tracked.txt").read_text()


def test_approval_waits_then_applies(store, tmp_path):
    repo, sha = requested(store, tmp_path)
    project = {**PROJECT, "require_approval": True}

    waiting = worker.drive(store, "r1", head=sha, project=project, check=ok, approval=lambda *_: None)
    assert waiting["outcome"] == "awaiting_approval"
    applied = worker.drive(store, "r1", head=sha, project=project, check=ok, approval=lambda *_: True)
    assert applied["outcome"] == "applied"


def test_saved_owner_approval_applies_without_another_check_or_approval(store, tmp_path):
    repo, sha = requested(store, tmp_path)
    project = {**PROJECT, "require_approval": True}
    waiting = worker.drive(store, "r1", head=sha, project=project, check=ok, approval=lambda *_: None)
    assert waiting["outcome"] == "awaiting_approval"
    candidate = saved(store)["candidate"]
    owner_approved(store)

    def never(*_):
        raise AssertionError("an external owner approval applies without a fresh check or approval")

    result = worker.drive(store, "r1", project=project, check=never, approval=never)

    assert result["outcome"] == "applied"
    assert head(repo) == candidate["candidate_commit"]
    assert not ref_exists(repo, candidate["ref"])


@pytest.mark.parametrize("drift", ["dirty", "moved"])
def test_saved_owner_approval_still_revalidates_the_exact_checkout(store, tmp_path, drift):
    repo, sha = requested(store, tmp_path)
    project = {**PROJECT, "require_approval": True}
    worker.drive(store, "r1", head=sha, project=project, check=ok, approval=lambda *_: None)
    candidate = saved(store)["candidate"]
    owner_approved(store)
    if drift == "dirty":
        (repo / "tracked.txt").write_text("owner edit\n")
        reason = "uncommitted changes"
    else:
        (repo / "other.txt").write_text("moved\n")
        git(repo, "add", "--", "other.txt")
        git(repo, "commit", "-m", "Move main")
        reason = "base branch moved"
    current = head(repo)

    result = worker.drive(store, "r1", project=project, check=ok)

    view = saved(store)
    assert (result["outcome"], view["stage"]) == ("waiting", "ready")
    assert reason in view["status"]
    assert head(repo) == current
    assert head(repo, candidate["ref"]) == candidate["candidate_commit"]


@pytest.mark.parametrize("drift", [False, True])
def test_saved_prepare_intent_waits_without_recreating_the_candidate(store, tmp_path, drift):
    repo, sha = requested(store, tmp_path)
    base = head(repo)
    calls = []

    def prepare(repo_, key, head_, base_="main"):
        calls.append(key)
        if drift:
            moved_repo = Path(repo_)
            (moved_repo / "other.txt").write_text("moved\n")
            git(moved_repo, "add", "--", "other.txt")
            git(moved_repo, "commit", "-m", "Move base")
            return integration.prepare(repo_, key, head_, base_)
        integration.prepare(repo_, key, head_, base_)
        raise RuntimeError("crash after prepare")

    if drift:
        result = worker.drive(
            store,
            "r1",
            head=sha,
            project=PROJECT,
            prepare=prepare,
            check=lambda *_: pytest.fail("a drifted candidate must not be checked"),
        )
        assert result["outcome"] == "waiting"
    else:
        with pytest.raises(RuntimeError, match="crash"):
            worker.drive(store, "r1", head=sha, project=PROJECT, prepare=prepare, check=ok)

    view = saved(store)
    expected = head(repo)
    assert (view["stage"], view["candidate"]["state"]) == ("claimed", "preparing")
    assert view["candidate"]["base_commit"] == base
    assert ref_exists(repo, view["candidate"]["ref"])
    assert Path(view["candidate"]["worktree"]).exists()
    assert ("base_commit" in view["status"] and expected != base) if drift else expected == base

    def never(*_):
        raise AssertionError("a saved prepare intent must never be repeated")

    again = worker.drive(store, "r1", head=sha, project=PROJECT, prepare=never, check=ok)
    assert again["outcome"] == "waiting"
    assert len(calls) == 1
    assert head(repo) == expected


@pytest.mark.parametrize("mode", ["crash", "lost", "dirty"])
def test_uncertain_apply_waits_and_keeps_the_exact_candidate(store, tmp_path, mode):
    repo, sha = requested(store, tmp_path)

    if mode == "dirty":
        (repo / "tracked.txt").write_text("user edit\n")
        result = worker.drive(store, "r1", head=sha, project=PROJECT, check=ok)
        assert result["outcome"] == "waiting"
        applied = saved(store)["candidate"]["base_commit"]
    else:

        def crashing_apply(repo_, candidate):
            integration.apply(repo_, candidate)
            if mode == "lost":
                raise WorkspaceUnavailable("lost response")
            raise RuntimeError("crash after apply")

        if mode == "lost":
            result = worker.drive(store, "r1", head=sha, project=PROJECT, check=ok, apply=crashing_apply)
            assert result["outcome"] == "waiting"
        else:
            with pytest.raises(RuntimeError, match="crash"):
                worker.drive(store, "r1", head=sha, project=PROJECT, check=ok, apply=crashing_apply)
        applied = saved(store)["candidate"]["candidate_commit"]

    def never(*_):
        raise AssertionError("an uncertain apply must never be repeated")

    result = worker.drive(store, "r1", head=sha, project=PROJECT, check=ok, apply=never)

    view = saved(store)
    assert result["outcome"] == "waiting"
    assert view["status"].startswith("apply outcome unknown")
    assert head(repo) == applied
    assert head(repo, view["candidate"]["ref"]) == view["candidate"]["candidate_commit"]
