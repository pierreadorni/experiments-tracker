"""Integration commands queue saved branch scope without running Git (T3.4c).

The store owns the exact experiment/repository/branch identity; the local API and
CLI only carry a request ID and an optional reason. These tests prove the queue
entry, its idempotent replay and the live/archive/branch boundaries. Queued-to-run
Git work, project checks and approval decisions belong to later cards.
"""

import json
import sys
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from test_branch_workspaces import create, git_project

from experimenttracker import clock
from experimenttracker.api import create_app
from experimenttracker.engine import integration_queue
from experimenttracker.store import Conflict, encode
from experimenttracker.workspaces import git


def queued(store):
    rows = store.rows("SELECT * FROM obligations WHERE kind='integration' ORDER BY id")
    return [(row, json.loads(row["detail"])) for row in rows]


def branch_row(eid, repo, *, state="active", merged_commit=None):
    return (
        eid,
        str(repo.resolve()),
        f"exp/20260101-{eid}",
        str(repo.resolve() / ".worktrees" / eid),
        "a" * 40,
        "b" * 40,
        state,
        merged_commit,
        clock.now(),
        clock.now(),
    )


def seed_branch(store, eid, repo, *, state="active", merged_commit=None):
    with store.tx() as db:
        db.execute(
            "INSERT INTO experiment_branches(experiment_id,repo,branch,worktree,base_commit,head_commit,"
            "state,merged_commit,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            branch_row(eid, repo, state=state, merged_commit=merged_commit),
        )


def make_experiment(store):
    return store.command(
        uuid4(),
        "create",
        {
            "brief": "Test an entirely new method and show its frontier.",
            "recipe_id": "agent_experiment_v1",
        },
    )["experiment_id"]


async def branch_experiment(runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    source = git_project(workflow, store, tmp_path)
    monkeypatch.setattr("experimenttracker.engine.gitworkspace._today", lambda: "20260101")
    eid = create(store)
    assert await workflow.prepare_workspace(store.experiment(eid))
    return source, store, workflow, eid


async def test_integrate_queues_the_saved_branch_and_never_runs_git(runtime, tmp_path, monkeypatch):
    source, store, workflow, eid = await branch_experiment(runtime, tmp_path, monkeypatch)
    before = git(source, "rev-parse", "HEAD")
    request_id = uuid4()

    result = store.command(request_id, "integrate", {}, eid=eid)

    assert result == {
        "schema_version": 1,
        "experiment_id": eid,
        "request_id": str(request_id),
        "state": "applied",
    }
    entries = queued(store)
    assert len(entries) == 1
    row, detail = entries[0]
    branch = workflow.workspaces.branch(eid)
    assert row["experiment_id"] == eid and row["project_id"] == "anyup-rs"
    assert detail["repo"] == str(source.resolve())
    assert detail["stage"] == "queued"
    assert detail["request"] == {
        "schema_version": 1,
        "kind": "merge",
        "experiment_id": eid,
        "branch": branch["branch"],
        "base": "main",
    }
    # The command queue is the only side effect: no base move, no candidate worktree.
    assert git(source, "rev-parse", "HEAD") == before
    assert not (source / ".worktrees" / "_integration").exists()

    # A replay of the stable request ID returns the saved result and adds nothing.
    assert store.command(request_id, "integrate", {}, eid=eid) == result
    assert len(queued(store)) == 1
    with pytest.raises(Conflict, match="different command"):
        store.command(request_id, "integrate", {"reason": "keep"}, eid=eid)
    with pytest.raises(Conflict, match="already queued"):
        store.command(uuid4(), "integrate", {}, eid=eid)


async def test_integrate_saves_a_bounded_reason_on_the_request(runtime, tmp_path, monkeypatch):
    _, store, _, eid = await branch_experiment(runtime, tmp_path, monkeypatch)
    store.command(uuid4(), "integrate", {"reason": "  keep the measured improvement  "}, eid=eid)
    _, detail = queued(store)[0]
    assert detail["request"]["reason"] == "  keep the measured improvement  "
    with pytest.raises(Conflict, match="bounded text"):
        store.command(uuid4(), "integrate", {"reason": "   "}, eid=eid)


def test_integrate_refuses_archived_deleted_cancelled_and_branchless_experiments(runtime, tmp_path):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    with pytest.raises(Conflict, match="named experiment branch"):
        store.command(uuid4(), "integrate", {}, eid=eid)

    seed_branch(store, eid, tmp_path / "repo")
    exp = store.experiment(eid)
    frozen = {**exp["contract"], "execution_contract": 2}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(frozen), eid))
    with pytest.raises(Conflict, match="contract-3"):
        store.command(uuid4(), "integrate", {}, eid=eid)

    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,deleted_at=? WHERE id=?", (encode(exp["contract"]), 1.0, eid)
        )
    with pytest.raises(Conflict, match="deleted"):
        store.command(uuid4(), "integrate", {}, eid=eid)

    with store.tx() as db:
        db.execute("UPDATE experiments SET deleted_at=NULL,phase='cancelled' WHERE id=?", (eid,))
    with pytest.raises(Conflict, match="cancelled"):
        store.command(uuid4(), "integrate", {}, eid=eid)


def test_integrate_refuses_a_branch_that_is_already_integrating(runtime, tmp_path):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    seed_branch(store, eid, tmp_path / "repo", state="merged", merged_commit="c" * 40)
    with pytest.raises(Conflict, match="active experiment branch"):
        store.command(uuid4(), "integrate", {}, eid=eid)


def test_revert_queues_the_exact_saved_merge_commit(runtime, tmp_path):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    merged = "c" * 40
    seed_branch(store, eid, tmp_path / "repo", state="merged", merged_commit=merged)

    store.command(uuid4(), "revert_integration", {"head": "d" * 40, "repo": "/elsewhere"}, eid=eid)

    _, detail = queued(store)[0]
    assert detail["request"] == {
        "schema_version": 1,
        "kind": "revert",
        "experiment_id": eid,
        "branch": f"exp/20260101-{eid}",
        "base": "main",
        "merge_commit": merged,
    }
    with pytest.raises(Conflict, match="already queued"):
        store.command(uuid4(), "revert_integration", {}, eid=eid)


def test_revert_requires_an_applied_integration_with_an_exact_commit(runtime, tmp_path):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    seed_branch(store, eid, tmp_path / "repo")
    with pytest.raises(Conflict, match="only an applied integration"):
        store.command(uuid4(), "revert_integration", {}, eid=eid)

    with store.tx() as db:
        db.execute(
            "UPDATE experiment_branches SET state='merged',merged_commit='not-a-sha' WHERE experiment_id=?",
            (eid,),
        )
    with pytest.raises(Conflict, match="not exact"):
        store.command(uuid4(), "revert_integration", {}, eid=eid)
    assert queued(store) == []


def test_dont_merge_blocks_and_abandons_queued_merges(runtime, tmp_path):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    seed_branch(store, eid, tmp_path / "repo")
    store.command(uuid4(), "integrate", {}, eid=eid)
    assert queued(store)[0][0]["state"] == "open"

    store.command(uuid4(), "dont_merge", {"value": True}, eid=eid)

    assert store.experiment(eid)["contract"]["dont_merge"] is True
    assert queued(store)[0][0]["state"] == "abandoned"
    with pytest.raises(Conflict, match="Don't merge"):
        store.command(uuid4(), "integrate", {}, eid=eid)

    store.command(uuid4(), "dont_merge", {"value": False}, eid=eid)
    assert "dont_merge" not in store.experiment(eid)["contract"]
    store.command(uuid4(), "integrate", {}, eid=eid)
    assert sorted(row["state"] for row, _ in queued(store)) == ["abandoned", "open"]


def test_dont_merge_fails_closed_when_a_merge_is_already_being_applied(runtime, tmp_path):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    repo = tmp_path / "repo"
    seed_branch(store, eid, repo)
    store.command(uuid4(), "integrate", {}, eid=eid)
    with store.tx() as db:
        assert integration_queue.claim(db, str(repo))["stage"] == "claimed"

    with pytest.raises(Conflict, match="already being applied"):
        store.command(uuid4(), "dont_merge", {"value": True}, eid=eid)

    assert "dont_merge" not in store.experiment(eid)["contract"]
    assert queued(store)[0][0]["state"] == "open"


def test_dont_merge_refuses_a_non_boolean_or_archived_experiment(runtime, tmp_path):
    store, _, _, _ = runtime
    eid = make_experiment(store)
    with pytest.raises(Conflict, match="boolean"):
        store.command(uuid4(), "dont_merge", {"value": "yes"}, eid=eid)
    exp = store.experiment(eid)
    frozen = {**exp["contract"], "execution_contract": 2}
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(frozen), eid))
    with pytest.raises(Conflict, match="contract-3"):
        store.command(uuid4(), "dont_merge", {"value": True}, eid=eid)


class IdleRunner:
    """Keep the API test about command wiring, not the periodic engine."""

    def __init__(self, workflow):
        self.workflow = workflow

    def health(self):
        return clock.now(), None

    async def start(self):
        return None

    async def stop(self):
        return None


def test_local_api_commands_carry_only_identity_and_saved_scope(config, runtime, tmp_path, monkeypatch):
    store, _, _, workflow = runtime
    monkeypatch.setattr("experimenttracker.api.Runner", IdleRunner)
    eid = make_experiment(store)
    seed_branch(store, eid, tmp_path / "repo")
    app = create_app(config, components=(None, store, workflow))
    headers = {"Authorization": "Bearer " + "t" * 40}
    with TestClient(app) as client:
        assert client.post(f"/api/experiments/{eid}/integrate", json={}).status_code == 401
        sparse = client.post(
            f"/api/experiments/{eid}/integrate",
            headers=headers,
            json={"request_id": str(uuid4()), "head_commit": "d" * 40, "repo": "/elsewhere"},
        )
        assert sparse.status_code == 422
        request_id = str(uuid4())
        response = client.post(
            f"/api/experiments/{eid}/integrate",
            headers=headers,
            json={"request_id": request_id, "reason": "keep"},
        )
        assert response.status_code == 200
        assert client.get(f"/api/commands/{request_id}", headers=headers).json()["state"] == "applied"
        assert (
            client.post(
                f"/api/experiments/{eid}/dont-merge",
                headers=headers,
                json={"request_id": str(uuid4()), "value": "yes"},
            ).status_code
            == 422
        )
        assert (
            client.post(
                f"/api/experiments/{eid}/dont-merge",
                headers=headers,
                json={"request_id": str(uuid4()), "value": True},
            ).status_code
            == 200
        )
        # The queued merge is abandoned, so a new integrate is refused by the flag.
        assert (
            client.post(
                f"/api/experiments/{eid}/integrate",
                headers=headers,
                json={"request_id": str(uuid4())},
            ).status_code
            == 409
        )
        with store.tx() as db:
            db.execute(
                "UPDATE experiment_branches SET state='merged',merged_commit=? WHERE experiment_id=?",
                ("c" * 40, eid),
            )
        # A client-supplied merge SHA is forbidden; the exact saved one is used instead.
        assert (
            client.post(
                f"/api/experiments/{eid}/integrations/revert",
                headers=headers,
                json={"request_id": str(uuid4()), "merge_commit": "e" * 40},
            ).status_code
            == 422
        )
        assert (
            client.post(
                f"/api/experiments/{eid}/integrations/revert",
                headers=headers,
                json={"request_id": str(uuid4())},
            ).status_code
            == 200
        )
        assert any(
            detail["request"]["kind"] == "revert" and detail["request"]["merge_commit"] == "c" * 40
            for _, detail in queued(store)
        )


def test_cli_integrate_posts_to_the_running_owner(config, monkeypatch, capsys):
    from experimenttracker import cli

    eid = uuid4()
    calls = {}

    class Response:
        def raise_for_status(self):
            return None

        def json(self):
            return {"schema_version": 1, "experiment_id": str(eid), "state": "applied"}

    def post(url, **kwargs):
        calls.update(url=url, **kwargs)
        return Response()

    monkeypatch.setattr(cli, "load_config", lambda path: config)
    monkeypatch.setattr("httpx.post", post)
    monkeypatch.setattr(sys, "argv", ["experimenttracker", "--config", "unused.yaml", "integrate", str(eid)])
    cli.main()

    assert calls["url"].endswith(f"/api/experiments/{eid}/integrate")
    body = calls["json"]
    assert set(body) == {"schema_version", "request_id"}
    assert calls["headers"]["Authorization"] == "Bearer " + "t" * 40
    assert json.loads(capsys.readouterr().out)["state"] == "applied"
