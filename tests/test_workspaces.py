"""Real Git worktrees preserve source baselines and exact restart identities."""

import json
from pathlib import Path
from uuid import uuid4

import pytest
from helpers import seed_saved_contract3
from test_projects import register

from experimenttracker.workspaces import Workspaces, WorkspaceUnavailable, git, initialize_project


def experiment(store):
    """Seed saved C3 state without invoking today's create policy."""
    return seed_saved_contract3(store, project_id="anyup-rs")


def source_repo(config, store, tmp_path):
    source = register(config, store, tmp_path)
    initialize_project(source)
    (source / "tracked.py").write_text("value = 1\n")
    (source / "deleted.py").write_text("value = 0\n")
    git(source, "add", "--", "tracked.py", "deleted.py")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Source")
    (source / "tracked.py").write_text("value = 2\n")
    (source / "untracked.py").write_text("value = 3\n")
    git(source, "rm", "--", "deleted.py")
    (source / ".venv").mkdir()
    (source / ".venv/token").write_text("private runtime sentinel")
    return source


def test_uncertain_worktree_creation_adopts_only_exact_identity(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    source = source_repo(workflow.config, store, tmp_path)
    workflow.projects.reload()
    eid = experiment(store)
    import experimenttracker.workspaces as module

    original = module.git
    calls = 0

    def lost_response(folder, *args):
        nonlocal calls
        result = original(folder, *args)
        if args[:2] == ("worktree", "add"):
            calls += 1
            raise WorkspaceUnavailable("saved creation response lost")
        return result

    monkeypatch.setattr(module, "git", lost_response)
    with pytest.raises(WorkspaceUnavailable):
        workflow.workspaces.ensure(eid)
    assert workflow.workspaces.row(eid)["state"] == "creating"
    target = Path(workflow.workspaces.row(eid)["folder"])
    (target / ".gitignore").unlink()  # Simulate an interrupted checkout of a clean file.
    workflow.workspaces.ensure(eid)
    assert (target / ".gitignore").read_text() == (source / ".gitignore").read_text()
    assert calls == 1
    assert workflow.workspaces.row(eid)["state"] == "ready"
    assert git(source, "rev-parse", "HEAD").decode().strip() == workflow.workspaces.row(eid)["head"]
    target = Path(workflow.workspaces.checkout(eid))
    target.rename(target.with_name("saved-original"))
    target.mkdir()
    with pytest.raises(WorkspaceUnavailable, match="identity changed"):
        Workspaces(workflow.projects).checkout(eid)


def test_non_git_import_remains_untouched_with_clear_reason(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = register(workflow.config, store, tmp_path)
    (source / "source.py").write_text("value = 1\n")
    workflow.projects.reload()
    eid = experiment(store)
    with pytest.raises(WorkspaceUnavailable, match="initial commit"):
        workflow.workspaces.ensure(eid)
    assert not (source / ".git").exists()
    assert (source / "source.py").read_text() == "value = 1\n"
    assert not workflow.workspaces.row(eid)
    assert not next(p for p in workflow.projects.catalog() if p["id"] == "anyup-rs")["supports_worktrees"]
    # User initialization unblocks the same experiment without a new identity.
    initialize_project(source)
    workflow.workspaces.ensure(eid)
    assert workflow.workspaces.row(eid)["state"] == "ready"


def test_existing_target_is_never_overwritten(runtime, tmp_path):
    store, _, _, workflow = runtime
    source_repo(workflow.config, store, tmp_path)
    workflow.projects.reload()
    eid = experiment(store)
    root = workflow.root(eid)
    (root / "checkout").mkdir(parents=True)
    (root / "checkout/sentinel").write_text("untouched")
    with pytest.raises(WorkspaceUnavailable, match="occupied"):
        workflow.workspaces.ensure(eid)
    assert (root / "checkout/sentinel").read_text() == "untouched"
    assert not store.rows("SELECT * FROM agent_actions WHERE experiment_id=?", (eid,))


def test_new_tracker_project_has_git_and_initial_commit(runtime):
    store, _, _, workflow = runtime
    request = uuid4()
    workflow.projects.enqueue(
        {"request_id": str(request), "name": "Fresh research", "mode": "new", "allow_edits": True}
    )
    workflow.projects.tick()
    receipt = workflow.projects.receipt(request)
    assert receipt["state"] == "applied"
    folder = Path(receipt["project"]["folder"])
    head = git(folder, "rev-parse", "HEAD")
    assert head.strip()
    assert git(folder, "ls-tree", "--name-only", "HEAD").decode().split() == [".gitignore", "AGENTS.md"]
    assert "experiments/<slug>/" in (folder / "AGENTS.md").read_text()
    assert not receipt["project"]["non_empty"]
    workflow.projects.tick()
    assert git(folder, "rev-parse", "HEAD") == head
    assert next(p for p in workflow.projects.catalog() if p["id"] == receipt["project_id"])[
        "supports_worktrees"
    ]


def test_source_git_identity_change_does_not_recreate_checkout(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = source_repo(workflow.config, store, tmp_path)
    workflow.projects.reload()
    eid = experiment(store)
    workflow.workspaces.ensure(eid)
    checkout = Path(workflow.workspaces.checkout(eid))
    (checkout / "untracked.py").write_text("keep agent changes\n")
    (source / ".git").rename(source / ".git-before")
    (source / ".git").mkdir()
    with pytest.raises(WorkspaceUnavailable, match="Git repository identity changed"):
        workflow.workspaces.ensure(eid)
    assert (checkout / "untracked.py").read_text() == "keep agent changes\n"


def test_symlink_source_is_not_copied_into_writable_checkout(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = source_repo(workflow.config, store, tmp_path)
    workflow.projects.reload()
    (source / "external.py").symlink_to(tmp_path / "outside.py")
    (tmp_path / "outside.py").write_text("unrelated source\n")
    eid = experiment(store)
    with pytest.raises(WorkspaceUnavailable, match="symlinks"):
        workflow.workspaces.ensure(eid)
    assert not workflow.workspaces.row(eid)
    assert (tmp_path / "outside.py").read_text() == "unrelated source\n"


def test_clean_committed_files_do_not_count_against_edit_capture_limit(runtime, tmp_path, monkeypatch):
    import experimenttracker.workspaces as module

    store, _, _, workflow = runtime
    source = register(workflow.config, store, tmp_path)
    initialize_project(source)
    (source / "committed.txt").write_text("This committed source is larger than the simulated edit limit.\n")
    git(source, "add", "--", "committed.txt")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Clean source")
    monkeypatch.setattr(module, "LIMIT", 1)
    workflow.projects.reload()
    eid = experiment(store)
    workflow.workspaces.ensure(eid)
    assert json.loads(workflow.workspaces.row(eid)["baseline"])["files"] == {}
    assert (Path(workflow.workspaces.checkout(eid)) / "committed.txt").read_text() == (
        source / "committed.txt"
    ).read_text()


def test_untracked_embedded_repository_remains_explicit_readonly_input(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = source_repo(workflow.config, store, tmp_path)
    nested = source / "JAFAR"
    nested.mkdir()
    initialize_project(nested)
    (nested / "model.py").write_text("source = 'external'\n")
    workflow.projects.reload()
    eid = experiment(store)
    workflow.workspaces.ensure(eid)
    target = Path(workflow.workspaces.checkout(eid))
    assert not (target / "JAFAR").exists()
    context = workflow.project_context(store.experiment(eid))
    assert context["read_only_repositories"] == ["JAFAR"]
    assert (nested / "model.py").read_text() == "source = 'external'\n"
