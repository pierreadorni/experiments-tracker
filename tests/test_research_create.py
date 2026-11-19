"""T4.1g-1: the owner research-loop flag selects a saved contract-4 create (C7)."""

from pathlib import Path
from uuid import uuid4

import pytest

from experimenttracker import creation_policy
from experimenttracker.config import ProjectConfig
from experimenttracker.store import Conflict, Store
from experimenttracker.workspaces import initialize_project


def git_project(config, store, tmp_path, *, research_loop):
    checkout = tmp_path / "research"
    checkout.mkdir()
    initialize_project(checkout)
    project = ProjectConfig(id="anyup-rs", name="anyup-rs", checkout=checkout, research_loop=research_loop)
    config.projects.append(project)
    store.projects[project.id] = project
    return project, checkout


def create(store, **extra):
    request_id = str(uuid4())
    payload = {
        "brief": "Measure a project-specific method.",
        "recipe_id": "agent_experiment_v1",
        "project_id": "anyup-rs",
        **extra,
    }
    return request_id, payload, store.command(request_id, "create", payload)


def test_research_loop_defaults_off():
    project = ProjectConfig(id="anyup-rs", name="anyup-rs", checkout=Path("/tmp/research"))
    assert project.research_loop is False


def test_status_advertises_candidate_create_default_without_changing_legacy_opt_ins(
    config, runtime, tmp_path, monkeypatch
):
    _, _, _, workflow = runtime
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    config.projects.extend(
        [
            ProjectConfig(id="anyup-rs", name="Research", checkout=tmp_path / "research", research_loop=True),
            ProjectConfig(id="legacy", name="Legacy", checkout=tmp_path / "legacy"),
        ]
    )
    status = workflow.status(None)
    assert status["execution_contract_create"] == 4
    assert "execution_contract_create_default" not in status
    assert status["research_loop_projects"] == ["anyup-rs"]

    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    candidate = workflow.status(None)
    assert candidate["execution_contract_create_default"] == 4
    assert candidate["research_loop_projects"] == ["anyup-rs"]


def test_unflagged_project_keeps_the_contract_3_stage_create(runtime, config, tmp_path, monkeypatch):
    store, _, _, _ = runtime
    _, checkout = git_project(config, store, tmp_path, research_loop=False)
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    _, _, receipt = create(store)
    exp = store.projection(receipt["experiment_id"])
    assert exp["contract"]["execution_contract"] == 3
    assert exp["contract"]["planning_version"] == 1
    assert exp["contract"]["branch_workspace"] is True
    assert [stage["id"] for stage in exp["stages"]] == [
        "plan",
        "prepare",
        "interpret",
        "publish",
    ]
    workspace = store.one(
        "SELECT * FROM experiment_workspaces WHERE experiment_id=?", (receipt["experiment_id"],)
    )
    assert workspace["folder"] == str(checkout.resolve())


def test_create_policy_candidate_selects_c4_only_for_eligible_named_repo(
    runtime, config, tmp_path, monkeypatch
):
    store, _, _, _ = runtime
    git_project(config, store, tmp_path, research_loop=False)
    monkeypatch.setattr(creation_policy, "MODE", "candidate")

    _, _, receipt = create(store)
    assert store.projection(receipt["experiment_id"])["contract"]["execution_contract"] == 4


def test_candidate_create_refuses_local_project_without_c4_eligibility(runtime, monkeypatch):
    store, _, _, _ = runtime
    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    with pytest.raises(Conflict, match="writable Git project checkout"):
        store.command(
            uuid4(),
            "create",
            {"brief": "Create against an incompatible local project.", "recipe_id": "agent_experiment_v1"},
        )
    assert store.rows("SELECT * FROM experiments") == []


def test_saved_c3_create_replays_unchanged_after_candidate_policy(runtime, monkeypatch):
    store, _, _, _ = runtime
    request_id = str(uuid4())
    payload = {
        "brief": "Preserve a previously saved contract-three create.",
        "recipe_id": "agent_experiment_v1",
    }
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    original = store.command(request_id, "create", payload)
    eid = original["experiment_id"]
    assert store.experiment(eid)["contract"]["execution_contract"] == 3

    monkeypatch.setattr(creation_policy, "MODE", "candidate")
    assert store.command(request_id, "create", payload) == original
    assert store.experiment(eid)["contract"]["execution_contract"] == 3
    assert len(store.rows("SELECT * FROM experiments")) == 1


def test_explicit_contract4_is_exact_and_requires_project_opt_in(runtime, config, tmp_path, monkeypatch):
    store, _, _, _ = runtime
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    # An explicit C4 request must not fall back on an opted-out project.
    with pytest.raises(Conflict, match="not enabled and eligible"):
        create(store, project_id="test-project", execution_contract=4)
    assert store.rows("SELECT * FROM experiments") == []


def test_flagged_eligible_project_saves_contract_4_without_stage_rows(runtime, config, tmp_path):
    store, _, _, _ = runtime
    _, checkout = git_project(config, store, tmp_path, research_loop=True)
    request_id, payload, receipt = create(store, execution_contract=4)
    eid = receipt["experiment_id"]
    exp = store.projection(eid)
    contract = exp["contract"]
    assert contract["execution_contract"] == 4
    assert contract["branch_workspace"] is True
    assert "planning_version" not in contract
    assert "reference" not in contract
    assert exp["stages"] == []
    assert store.rows("SELECT * FROM stages WHERE experiment_id=?", (eid,)) == []
    workspace = store.one("SELECT * FROM experiment_workspaces WHERE experiment_id=?", (eid,))
    info = checkout.resolve().stat()
    assert workspace["folder"] == str(checkout.resolve())
    assert (workspace["device"], workspace["inode"]) == (info.st_dev, info.st_ino)
    assert store.command(request_id, "create", payload) == receipt


def test_flagged_create_uses_one_authoritative_eligibility_result(runtime, config, tmp_path, monkeypatch):
    store, _, _, _ = runtime
    git_project(config, store, tmp_path, research_loop=True)
    calls = []

    def eligible_once(payload):
        calls.append("probe")
        return len(calls) == 1

    monkeypatch.setattr(store, "branch_workspace_for", eligible_once)
    _, _, receipt = create(store)
    # A later eligibility change cannot leave a contract-4 create without its worktree.
    assert calls == ["probe"]
    contract = store.projection(receipt["experiment_id"])["contract"]
    assert contract["execution_contract"] == 4
    assert contract["branch_workspace"] is True

    calls.clear()
    monkeypatch.setattr(store, "branch_workspace_for", lambda payload: calls.append("probe") or False)
    with pytest.raises(Conflict, match="main Git checkout"):
        create(store)
    assert calls == ["probe"]
    assert len(store.rows("SELECT * FROM experiments")) == 1


def test_flagged_project_refuses_a_reference_snapshot_path(runtime, config, tmp_path):
    store, _, _, _ = runtime
    project, _ = git_project(config, store, tmp_path, research_loop=False)
    _, _, parent_receipt = create(store)
    parent = parent_receipt["experiment_id"]
    store.update(parent, phase="complete", snapshot={"snapshot_hash": "test"})
    project.research_loop = True
    store.projects[project.id] = project
    with pytest.raises(Conflict, match="reference experiment snapshot"):
        create(store, execution_contract=4, reference_experiment_id=parent)
    assert [row["id"] for row in store.rows("SELECT id FROM experiments")] == [parent]


def test_flagged_project_refuses_shared_or_ineligible_creates(runtime, config, tmp_path):
    store, _, _, _ = runtime
    git_project(config, store, tmp_path, research_loop=True)
    with pytest.raises(Conflict, match="named branch workspace"):
        create(store, execution_contract=4, workspace_mode="shared")
    assert store.rows("SELECT * FROM experiments") == []

    plain = tmp_path / "plain"
    plain.mkdir()
    project = ProjectConfig(id="anyup-rs", name="anyup-rs", checkout=plain, research_loop=True)
    config.projects[-1] = project
    store.projects[project.id] = project
    with pytest.raises(Conflict, match="main Git checkout"):
        create(store, execution_contract=4)
    # A client-supplied path never becomes project eligibility.
    eligible = tmp_path / "eligible"
    eligible.mkdir()
    initialize_project(eligible)
    with pytest.raises(Conflict, match="main Git checkout"):
        create(store, execution_contract=4, checkout=str(eligible), folder=str(eligible))
    # A symlink is not the verified normal checkout path.
    project.checkout = tmp_path / "linked"
    (tmp_path / "linked").symlink_to(eligible)
    with pytest.raises(Conflict, match="main Git checkout"):
        create(store, execution_contract=4)
    assert store.rows("SELECT * FROM experiments") == []
    assert store.rows("SELECT * FROM commands") == []


def test_flagged_project_without_a_checkout_is_refused(runtime):
    store, _, _, _ = runtime
    store.projects["test-project"] = ProjectConfig(id="test-project", name="Test project", research_loop=True)
    with pytest.raises(Conflict, match="writable Git project checkout"):
        store.command(
            uuid4(),
            "create",
            {
                "brief": "Measure a project-specific method.",
                "recipe_id": "agent_experiment_v1",
                "execution_contract": 4,
            },
        )
    assert store.rows("SELECT * FROM experiments") == []


def test_restart_with_the_flag_removed_keeps_the_saved_contract_4(runtime, config, tmp_path, monkeypatch):
    store, _, _, _ = runtime
    monkeypatch.setattr(creation_policy, "MODE", "legacy")
    project, _ = git_project(config, store, tmp_path, research_loop=True)
    request_id, payload, receipt = create(store)
    eid = receipt["experiment_id"]
    store.close()

    project.research_loop = False
    reopened = Store(config.state_dir / "workflow.sqlite")
    try:
        reopened.projects = {item.id: item for item in config.projects}
        exp = reopened.projection(eid)
        assert exp["contract"]["execution_contract"] == 4
        assert exp["stages"] == []
        assert reopened.command(request_id, "create", payload) == receipt
        _, _, new_receipt = create(reopened)
        assert reopened.projection(new_receipt["experiment_id"])["contract"]["execution_contract"] == 3
    finally:
        reopened.close()
