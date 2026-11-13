"""Bounded checks on the frozen legacy fixture loader: a loaded run stays
read-only under repeated observation and never dispatches, and a malformed
fixture is rejected before any row is written.
"""

import json
import shutil

import pytest
from legacy_fixture import FIXTURE_DIR, load_legacy_completed


def copied(tmp_path):
    source = tmp_path / "legacy_completed"
    shutil.copytree(FIXTURE_DIR, source)
    return source


def saved(source):
    return json.loads((source / "rows.json").read_text())


async def test_loaded_legacy_run_is_read_only_and_never_dispatches(runtime):
    store, scheduler, agent, workflow = runtime
    eid = load_legacy_completed(store, workflow)
    revision = store.experiment(eid)["revision"]
    counts = {
        table: len(store.rows(f"SELECT * FROM {table} WHERE experiment_id=?", (eid,)))
        for table in ("stages", "jobs", "agent_actions", "artifacts", "inbox")
    }
    for _ in range(3):
        await workflow.tick(force_observe=True)
    exp = store.experiment(eid)
    assert exp["phase"] == "complete" and exp["hold_reason"] is None and exp["revision"] == revision
    assert scheduler.submissions == 0 and agent.calls == 0
    for table, count in counts.items():
        assert len(store.rows(f"SELECT * FROM {table} WHERE experiment_id=?", (eid,))) == count
    assert counts["jobs"] == 3 and counts["agent_actions"] == 2 and counts["artifacts"] == 13
    assert not store.rows("SELECT * FROM events") and not store.rows("SELECT * FROM outbox")
    await workflow.close()


def test_loader_rejects_an_unknown_column_before_writing(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = copied(tmp_path)
    rows = saved(source)
    rows["tables"]["stages"][0]["invented_column"] = 1
    (source / "rows.json").write_text(json.dumps(rows))
    with pytest.raises(ValueError, match="unknown columns"):
        load_legacy_completed(store, workflow, directory=source)
    assert not workflow.projects.base_root(rows["experiment_id"]).exists()
    assert not store.rows("SELECT * FROM experiments")


def test_loader_rejects_a_missing_fixture_artifact(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = copied(tmp_path)
    rows = saved(source)
    (source / "root/results.json").unlink()
    with pytest.raises(ValueError, match="artifact is missing"):
        load_legacy_completed(store, workflow, directory=source)
    assert not store.rows("SELECT * FROM experiments WHERE id=?", (rows["experiment_id"],))


def test_loader_rejects_a_saved_histories_artifact(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = copied(tmp_path)
    rows = saved(source)
    rows["tables"]["artifacts"][0]["name"] = "histories.json"
    (source / "rows.json").write_text(json.dumps(rows))
    with pytest.raises(ValueError, match="histories.json as an artifact"):
        load_legacy_completed(store, workflow, directory=source)


def test_loader_rejects_the_root_token_outside_a_saved_path(runtime, tmp_path):
    store, _, _, workflow = runtime
    source = copied(tmp_path)
    rows = saved(source)
    rows["tables"]["inbox"][0]["text"] += " @ROOT@/leaked"
    (source / "rows.json").write_text(json.dumps(rows))
    with pytest.raises(ValueError, match="@ROOT@ outside a path"):
        load_legacy_completed(store, workflow, directory=source)
