"""Public experiment projections expose branch and integration identity only (T3.6b)."""

import json
from uuid import uuid4

from test_integration_commands import awaiting, make_experiment, seed_branch

from experimenttracker.store import encode


def add_job(store, eid):
    """Save one non-agent job whose manifest carries a commit and private diagnostics."""
    with store.tx() as db:
        db.execute(
            "INSERT INTO stages(experiment_id,id,state,updated_at) VALUES(?,'train','complete',1.0)",
            (eid,),
        )
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,script_hash,resources,"
            "snapshot_hash,created_at,execution_root,execution_manifest) "
            "VALUES(?,?,'train',1,'complete','hash','{}','snapshot',1.0,?,?)",
            (
                str(uuid4()),
                eid,
                "/private/execution/root",
                encode(
                    {
                        "commit": "d" * 40,
                        "command": "python /private/work/script.py",
                        "repo": "/private/repository",
                    }
                ),
            ),
        )


def test_projection_exposes_exact_branch_head_job_commit_and_owner_candidate(runtime, tmp_path):
    store, *_ = runtime
    eid = make_experiment(store)
    repo = tmp_path / "repo"
    seed_branch(store, eid, repo)
    request_id, candidate = awaiting(store, eid, repo, reason="/private command text")
    add_job(store, eid)

    view = store.projection(eid)

    assert (view["branch"], view["head_commit"]) == (f"exp/20260101-{eid}", "b" * 40)
    assert view["jobs"][0]["commit"] == "d" * 40
    assert view["integration"] == {
        "stage": "awaiting_approval",
        "request_id": request_id,
        "candidate_commit": candidate["candidate_commit"],
    }
    assert "execution_manifest" not in view["jobs"][0]
    assert "execution_root" not in view["jobs"][0]
    assert "command" not in view["jobs"][0]
    published = json.dumps(view)
    assert str(repo.resolve()) not in published
    assert "awaiting owner approval" not in published
    assert "/private command text" not in published
    assert "/private" not in published


def test_archived_projection_omits_branch_integration_and_job_commit(runtime, tmp_path):
    store, *_ = runtime
    eid = make_experiment(store)
    repo = tmp_path / "repo"
    seed_branch(store, eid, repo)
    awaiting(store, eid, repo)
    add_job(store, eid)
    exp = store.experiment(eid)
    with store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=? WHERE id=?",
            (encode({**exp["contract"], "execution_contract": 2}), eid),
        )

    view = store.projection(eid)

    assert "branch" not in view and "head_commit" not in view and "integration" not in view
    assert "commit" not in view["jobs"][0]


def test_projection_stays_readable_without_workspace_tables(runtime):
    store, *_ = runtime
    eid = make_experiment(store)
    with store.tx() as db:
        db.execute("DROP TABLE experiment_branches")
        db.execute("DROP TABLE obligations")

    view = store.projection(eid)

    assert "branch" not in view and "head_commit" not in view and "integration" not in view
