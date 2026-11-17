"""Focused SQLite-only report publication responsibility tests."""

from uuid import uuid4

import pytest

from experimenttracker import artifacts, clock
from experimenttracker.core import report_obligations
from experimenttracker.store import Conflict


def document(text="Synthetic result"):
    return {"title": text, "standfirst": "Bounded synthetic result.", "blocks": []}


def create(store):
    eid = store.command(
        uuid4(), "create", {"brief": "Synthetic report obligation", "recipe_id": "agent_experiment_v1"}
    )["experiment_id"]
    return eid


def episode(store, eid, version, index=0):
    return store.one("SELECT * FROM obligations WHERE id=?", (f"report-version:{eid}:{version}:{index}",))


def history(store, eid, version):
    return store.rows(
        "SELECT * FROM obligations WHERE id LIKE ? ORDER BY id", (f"report-version:{eid}:{version}:%",)
    )


def counts(store, eid):
    return (
        store.one("SELECT COUNT(*) n FROM events WHERE experiment_id=?", (eid,))["n"],
        store.one("SELECT COUNT(*) n FROM outbox")["n"],
    )


def test_request_creation_opens_one_episode_and_replay_keeps_it(runtime):
    store, _, _, _ = runtime
    eid = create(store)
    version = store.working_report(eid, document(), "synthetic-report")
    saved = episode(store, eid, version)
    assert saved["state"] == "open"
    assert (saved["owner"], saved["kind"]) == ("backend", "publication")
    assert saved["target"] == f"{eid}:{version}"
    assert saved["wait"] == f"report:{eid}:{version}"
    assert saved["detail"] == "request=synthetic-report"
    assert saved["experiment_id"] == eid
    assert saved["project_id"] == store.experiment(eid)["contract"]["project"]["id"]

    assert store.working_report(eid, document(), "synthetic-report") == version
    replayed = episode(store, eid, version)
    assert (replayed["id"], replayed["created_at"]) == (saved["id"], saved["created_at"])
    with pytest.raises(Conflict, match="another document"):
        store.working_report(eid, document("changed"), "synthetic-report")
    assert len(history(store, eid, version)) == 1


def test_initial_report_opens_version_one_episode(runtime):
    store, _, _, _ = runtime
    eid = create(store)
    store.initial_report(eid, document())
    saved = episode(store, eid, 1)
    assert saved["state"] == "open"
    assert saved["detail"] == f"request=initial-report:{eid}"
    store.initial_report(eid, document())
    assert len(history(store, eid, 1)) == 1


def test_report_progress_keeps_one_episode_and_ready_satisfies_once(runtime):
    store, _, _, _ = runtime
    eid = create(store)
    store.working_report(eid, document("first"), "first-report")
    version = store.working_report(eid, document("second"), "second-report")
    opened = episode(store, eid, version)
    store.report_state(eid, version, "building")
    store.report_state(eid, version, "publishing")
    live = episode(store, eid, version)
    assert (live["id"], live["created_at"], live["state"]) == (
        opened["id"],
        opened["created_at"],
        "open",
    )
    store.report_state(eid, version, "ready")
    assert episode(store, eid, version)["state"] == "satisfied"
    store.report_state(eid, version, "ready")
    assert [row["state"] for row in history(store, eid, version)] == ["satisfied"]
    inbox = "SELECT COUNT(*) n FROM inbox WHERE experiment_id=? AND kind='report_ready'"
    assert store.one(inbox, (eid,))["n"] == 1
    assert episode(store, eid, 1)["state"] == "open"


def test_failure_abandons_open_episode_and_terminal_row_without_history_stays(runtime):
    store, _, _, _ = runtime
    eid = create(store)
    version = store.working_report(eid, document(), "synthetic-report")
    store.report_state(eid, version, "failed", "synthetic render error")
    assert episode(store, eid, version)["state"] == "abandoned"
    with store.tx() as db:
        db.execute(
            "INSERT INTO report_versions(experiment_id,version,state,document,request_id,created_at,updated_at) "
            "VALUES(?, 2, 'queued', '{}', 'historical-report', 0, 0)",
            (eid,),
        )
    store.report_state(eid, 2, "failed", "historical failure")
    saved = store.one("SELECT state FROM report_versions WHERE experiment_id=? AND version=2", (eid,))
    assert saved["state"] == "failed"
    assert not history(store, eid, 2)


def test_artifact_registration_keeps_publication_open_until_ready(runtime):
    store, _, _, wf = runtime
    eid = create(store)
    version = store.working_report(eid, document(), "synthetic-report")
    root = wf.root(eid)
    root.mkdir(parents=True, exist_ok=True)
    (root / "report-v2.html").write_text("Synthetic report")
    artifacts.register(
        store,
        eid,
        "publish",
        root,
        ["report-v2.html"],
        report_version=version,
        report_name="report-v2.html",
    )
    saved = store.one("SELECT state FROM report_versions WHERE experiment_id=? AND version=?", (eid, version))
    assert saved["state"] == "publishing"
    assert episode(store, eid, version)["state"] == "open"
    store.report_state(eid, version, "ready")
    assert episode(store, eid, version)["state"] == "satisfied"


def test_cancellation_abandons_every_live_version(runtime):
    store, _, _, _ = runtime
    eid = create(store)
    settled = store.working_report(eid, document("first"), "first-report")
    store.report_state(eid, settled, "ready")
    live = store.working_report(eid, document("second"), "second-report")
    store.report_state(eid, live, "building")
    with store.tx() as db:
        store.cancel_experiment(db, eid, clock.now())
    states = {
        row["version"]: row["state"]
        for row in store.rows("SELECT version,state FROM report_versions WHERE experiment_id=?", (eid,))
    }
    assert states == {settled: "ready", live: "failed"}
    assert episode(store, eid, settled)["state"] == "satisfied"
    assert episode(store, eid, live)["state"] == "abandoned"


def test_creation_rollback_keeps_row_event_and_obligation_together(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = create(store)
    events, outbox = counts(store, eid)

    def fail_sync(*args, **kwargs):
        raise RuntimeError("synthetic obligation failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(report_obligations, "sync_report", fail_sync)
        with pytest.raises(RuntimeError, match="synthetic obligation failure"):
            store.working_report(eid, document(), "synthetic-report")
    assert not store.one("SELECT 1 FROM report_versions WHERE experiment_id=?", (eid,))
    assert counts(store, eid) == (events, outbox)
    assert not store.one(
        "SELECT 1 FROM obligations WHERE experiment_id=? AND id LIKE 'report-version:%'", (eid,)
    )


def test_state_and_cancellation_rollback_keep_rows_and_events_together(runtime, monkeypatch):
    store, _, _, _ = runtime
    eid = create(store)
    version = store.working_report(eid, document(), "synthetic-report")
    events, outbox = counts(store, eid)

    def fail_sync(*args, **kwargs):
        raise RuntimeError("synthetic obligation failure")

    with monkeypatch.context() as patcher:
        patcher.setattr(report_obligations, "sync_report", fail_sync)
        with pytest.raises(RuntimeError, match="synthetic obligation failure"):
            store.report_state(eid, version, "building")
        with pytest.raises(RuntimeError, match="synthetic obligation failure"):
            with store.tx() as db:
                store.cancel_experiment(db, eid, clock.now())
    saved = store.one("SELECT state FROM report_versions WHERE experiment_id=? AND version=?", (eid, version))
    assert saved["state"] == "queued"
    assert store.experiment(eid)["phase"] != "cancelled"
    assert counts(store, eid) == (events, outbox)
    assert episode(store, eid, version)["state"] == "open"
