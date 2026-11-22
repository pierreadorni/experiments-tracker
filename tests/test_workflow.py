from uuid import uuid4

import pytest
from helpers import seed_saved_contract3
from test_research_create import git_project

from experimenttracker import artifacts
from experimenttracker.store import Conflict, Store


def submit(store):
    return seed_saved_contract3(store, "Compare linear and quadratic models on shared synthetic data.")


def test_commands_are_idempotent_and_conflicts_rejected(runtime, config, tmp_path):
    store, _, _, _ = runtime
    project, _ = git_project(config, store, tmp_path, research_loop=True)
    request_id = uuid4()
    payload = {
        "brief": "Compare the two synthetic models.",
        "recipe_id": "agent_experiment_v1",
        "execution_contract": 4,
        "project_id": project.id,
    }
    first = store.command(request_id, "create", payload)
    assert store.command(request_id, "create", payload) == first
    assert store.experiment(first["experiment_id"])["contract"]["execution_contract"] == 4
    assert len(store.rows("SELECT * FROM experiments")) == 1
    with pytest.raises(Conflict):
        store.command(request_id, "create", {**payload, "brief": "Different brief"})
    note_id = uuid4()
    store.command(note_id, "note", {"text": "Researcher text"}, eid=first["experiment_id"])
    store.command(note_id, "note", {"text": "Researcher text"}, eid=first["experiment_id"])
    assert len(store.rows("SELECT * FROM notes")) == 1
    with pytest.raises(Conflict):
        store.command(request_id, "create", payload, origin="gateway", experiment_id=str(uuid4()))


def test_online_backup_is_self_contained(runtime, config):
    store, _, _, _ = runtime
    eid = submit(store)
    stages = store.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY ordinal,id", (eid,))
    store.backup(config.backup_dir)
    recovered = Store(config.backup_dir / "workflow.sqlite")
    assert recovered.experiment(eid)["id"] == eid
    assert recovered.rows("SELECT * FROM stages WHERE experiment_id=? ORDER BY ordinal,id", (eid,)) == stages
    recovered.close()


def test_artifact_paths_and_nonfinite_json_rejected(tmp_path):
    root = tmp_path / "allowed"
    root.mkdir()
    outside = tmp_path / "outside.json"
    outside.write_text('{"metric": NaN}')
    (root / "link.json").symlink_to(outside)
    with pytest.raises(ValueError):
        artifacts.allowed_path(root / "link.json", root)
    inside = root / "bad.json"
    inside.write_text(outside.read_text())
    with pytest.raises(ValueError):
        artifacts.read_json(inside, root)
