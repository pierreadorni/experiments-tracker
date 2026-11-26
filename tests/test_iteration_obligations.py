"""C2 obligations schema/API evidence on synthetic state; no live records are read."""

import contextlib
import sqlite3

import pytest
from helpers import seed_saved_contract3

from experimenttracker import clock
from experimenttracker.core import obligations
from experimenttracker.store import Store

SAVED_TABLES = ("experiments", "agent_actions", "jobs", "events", "outbox", "obligations")


def saved_rows(store):
    return {name: store.rows(f"SELECT * FROM {name}") for name in SAVED_TABLES}


def own_open(store, **scope):
    """Open rows this test opened; creation's lifetime episode is not one of them."""
    return [
        row for row in obligations.open_for(store.db, **scope) if not row["id"].startswith("advancement:")
    ]


class Injected(RuntimeError):
    """Failure a test injects inside an open transaction."""


def experiment(store, brief="Synthetic obligations experiment"):
    return seed_saved_contract3(store, brief)


def saved_fixture(store):
    """One experiment with a submitted job id, a running action turn, event and outbox row."""
    eid = experiment(store)
    with store.tx() as db:
        db.execute("INSERT INTO stages(experiment_id,id,updated_at) VALUES(?,?,1.0)", (eid, "train"))
        db.execute(
            "INSERT INTO jobs(intent_id,experiment_id,stage_id,state,job_id,script_hash,resources,"
            "snapshot_hash,created_at) VALUES(?,?,'train','submitted','4242','h','{}','s',1.0)",
            (f"{eid}/job", eid),
        )
        db.execute(
            "INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,turn_id,"
            "input,created_at,updated_at) VALUES(?,?,'agent:plan',1,'running','thread-1','turn-1','{}',1.0,1.0)",
            (f"{eid}/action", eid),
        )
        store.emit(db, eid, "job_submitted")
    return eid


def test_upgrade_preserves_saved_rows_and_backfills_no_obligations(runtime):
    store = runtime[0]
    eid = saved_fixture(store)
    view = store.projection(eid)
    hold = store.experiment(eid)["hold_reason"]
    assert store.one("SELECT MAX(version) AS version FROM schema_version")["version"] == 26
    assert store.rows("SELECT * FROM obligations WHERE id NOT LIKE 'advancement:%'") == []
    before = saved_rows(store)
    # The dropped table is a schema-23 store, so its saved rows cannot survive; the
    # migration must add none of them back.
    before["obligations"] = []
    # No historical schema-23 database is checked in, so this fixture is a fresh
    # store downgraded in place by dropping the newest migration, not a real
    # schema-23 snapshot with legacy rows.
    with store.tx() as db:
        db.execute("ALTER TABLE jobs DROP COLUMN observation_error")
        db.execute("DROP TABLE experiment_branches")
        db.execute("DROP TABLE obligations")
        db.execute("DELETE FROM schema_version WHERE version>=24")
    migrated = Store(store.path)
    try:
        assert migrated.one("SELECT MAX(version) AS version FROM schema_version")["version"] == 26
        assert migrated.rows("SELECT * FROM obligations") == []
        assert saved_rows(migrated) == before
        assert migrated.rows("PRAGMA foreign_key_check") == []
        assert migrated.projection(eid) == view
        assert migrated.experiment(eid)["hold_reason"] == hold
        reopened = Store(store.path)
        reopened.close()
        assert saved_rows(migrated) == before
        reader = Store.read_only(store.path)
        try:
            assert reader.rows("SELECT * FROM obligations") == []
        finally:
            reader.close()
    finally:
        migrated.close()


def test_failed_upgrade_transaction_rolls_back_schema_and_version(runtime, monkeypatch):
    store = runtime[0]
    saved_fixture(store)
    # Schema 23 has no obligations table, so only the older saved tables can be compared.
    before = {name: store.rows(f"SELECT * FROM {name}") for name in SAVED_TABLES if name != "obligations"}
    with store.tx() as db:
        db.execute("ALTER TABLE jobs DROP COLUMN observation_error")
        db.execute("DROP TABLE experiment_branches")
        db.execute("DROP TABLE obligations")
        db.execute("DELETE FROM schema_version WHERE version>=24")
    original = Store.tx

    @contextlib.contextmanager
    def failing_tx(self):
        with original(self) as db:
            yield db
            raise Injected("injected migration failure before commit")

    with monkeypatch.context() as patch:
        patch.setattr(Store, "tx", failing_tx)
        with pytest.raises(Injected):
            Store(store.path)
    reader = Store.read_only(store.path)
    try:
        assert reader.one("SELECT MAX(version) AS version FROM schema_version")["version"] == 23
        assert reader.rows("SELECT name FROM sqlite_master WHERE name LIKE 'obligations%'") == []
        before_v23 = {
            **before,
            "jobs": [
                {key: value for key, value in job.items() if key != "observation_error"}
                for job in before["jobs"]
            ],
        }
        assert {name: reader.rows(f"SELECT * FROM {name}") for name in before} == before_v23
    finally:
        reader.close()
    migrated = Store(store.path)
    try:
        assert migrated.one("SELECT MAX(version) AS version FROM schema_version")["version"] == 26
        assert migrated.rows("SELECT * FROM obligations") == []
        assert {name: migrated.rows(f"SELECT * FROM {name}") for name in before} == before
    finally:
        migrated.close()


def test_open_replay_keeps_identity_and_never_reopens_terminal_work(runtime, monkeypatch):
    store = runtime[0]
    identity = {
        "experiment_id": None,
        "project_id": "project-1",
        "kind": "job",
        "target": "job-1",
        "owner": "external",
    }
    monkeypatch.setattr(clock, "now", lambda: 100.0)
    with store.tx() as db:
        first = obligations.open(db, "gen-1/job", **identity, wait="job:job-1", detail="queued", due_at=500.0)
    assert (first["state"], first["created_at"], first["updated_at"]) == ("open", 100.0, 100.0)

    monkeypatch.setattr(clock, "now", lambda: 200.0)
    with store.tx() as db:
        replay = obligations.open(db, "gen-1/job", **identity, wait="quota", detail="queued", due_at=500.0)
    assert (replay["wait"], replay["created_at"], replay["updated_at"]) == ("quota", 100.0, 200.0)

    monkeypatch.setattr(clock, "now", lambda: 300.0)
    with store.tx() as db:
        same = obligations.open(db, "gen-1/job", **identity, wait="quota", detail="queued", due_at=500.0)
    assert same["updated_at"] == 200.0
    assert store.rows("SELECT id FROM obligations") == [{"id": "gen-1/job"}]

    with store.tx() as db:
        assert obligations.satisfy(db, "gen-1/job")["state"] == "satisfied"
    with store.tx() as db:
        terminal = obligations.open(db, "gen-1/job", **identity, wait="other", detail=None, due_at=None)
    assert (terminal["state"], terminal["wait"], terminal["updated_at"]) == ("satisfied", "quota", 300.0)

    for field, value in (("owner", "owner"), ("kind", "timer"), ("target", "job-2"), ("project_id", "p2")):
        with pytest.raises(ValueError):
            with store.tx() as db:
                obligations.open(db, "gen-1/job", **{**identity, field: value})
    assert store.rows("SELECT owner,kind,target FROM obligations") == [
        {"owner": "external", "kind": "job", "target": "job-1"}
    ]


def test_terminal_transitions_are_first_saved_and_unknown_ids_return_none(runtime, monkeypatch):
    store = runtime[0]
    monkeypatch.setattr(clock, "now", lambda: 10.0)
    with store.tx() as db:
        for oid in ("gen-1/done", "gen-2/dropped"):
            obligations.open(db, oid, kind="reconcile", owner="backend")
    with store.tx() as db:
        assert obligations.satisfy(db, "gen-1/done")["state"] == "satisfied"
        assert obligations.abandon(db, "gen-2/dropped")["state"] == "abandoned"
    monkeypatch.setattr(clock, "now", lambda: 20.0)
    with store.tx() as db:
        kept = obligations.abandon(db, "gen-1/done")
        assert (kept["state"], kept["updated_at"]) == ("satisfied", 10.0)
        kept = obligations.satisfy(db, "gen-2/dropped")
        assert (kept["state"], kept["updated_at"]) == ("abandoned", 10.0)
        assert obligations.satisfy(db, "gen-unknown") is None
        assert obligations.abandon(db, "gen-unknown") is None
    assert len(store.rows("SELECT id FROM obligations")) == 2


def test_mutations_require_the_callers_transaction_and_roll_back_with_it(runtime):
    store = runtime[0]
    eid = saved_fixture(store)
    with pytest.raises(RuntimeError):
        obligations.open(store.db, f"{eid}/job", experiment_id=eid, kind="job", owner="external")
    with pytest.raises(RuntimeError):
        obligations.satisfy(store.db, f"{eid}/job")
    assert store.rows("SELECT * FROM obligations WHERE id NOT LIKE 'advancement:%'") == []
    with store.tx() as db:
        obligations.open(db, f"{eid}/job", experiment_id=eid, kind="job", owner="external")
    before = saved_rows(store)

    with pytest.raises(Injected):
        with store.tx() as db:
            db.execute("UPDATE jobs SET state='failed' WHERE intent_id=?", (f"{eid}/job",))
            obligations.satisfy(db, f"{eid}/job")
            obligations.open(db, f"{eid}/timer", experiment_id=eid, kind="timer", owner="backend")
            store.emit(db, eid, "job_failed")
            raise Injected()
    assert saved_rows(store) == before
    assert [row["id"] for row in own_open(store, experiment_id=eid)] == [f"{eid}/job"]


def test_open_for_scopes_and_derived_priority(runtime, monkeypatch):
    store = runtime[0]
    first, second = experiment(store, "Synthetic scope A"), experiment(store, "Synthetic scope B")
    times = iter([1.0, 2.0, 3.0, 4.0, 5.0])
    monkeypatch.setattr(clock, "now", lambda: next(times))
    with store.tx() as db:
        obligations.open(db, "b", experiment_id=first, project_id="p1", kind="job", owner="external")
        obligations.open(db, "a", experiment_id=first, project_id="p2", kind="job", owner="agent")
        obligations.open(db, "c", experiment_id=second, project_id="p1", kind="timer", owner="backend")
        obligations.open(db, "d", experiment_id=second, project_id="p1", kind="job", owner="external")
    with store.tx() as db:
        obligations.satisfy(db, "d")
    assert [row["id"] for row in own_open(store)] == ["b", "a", "c"]
    assert [row["id"] for row in own_open(store, experiment_id=first)] == ["b", "a"]
    assert [row["id"] for row in obligations.open_for(store.db, project_id="p1")] == ["b", "c"]
    assert obligations.open_for(store.db, experiment_id=second, project_id="p2") == []
    assert obligations.derived_status(own_open(store, experiment_id=first))["id"] == "a"


def test_derived_status_ranks_owners_then_due_created_and_id():
    def row(owner, created=0.0, *, due=None, oid="x", state="open"):
        return {"id": oid, "owner": owner, "state": state, "due_at": due, "created_at": created}

    assert obligations.derived_status([]) is None
    assert obligations.derived_status([row("external", state="satisfied")]) is None
    ranked = [row("external"), row("backend"), row("agent"), row("owner")]
    assert obligations.derived_status(ranked)["owner"] == "owner"
    assert obligations.derived_status([row("backend", due=1.0), row("agent", due=9.0)])["owner"] == "agent"
    nulls_last = [row("owner", oid="null-due"), row("owner", due=99.0, oid="due")]
    assert obligations.derived_status(nulls_last)["id"] == "due"
    due_rows = [row("owner", due=9.0, oid="late"), row("owner", due=3.0, oid="early")]
    assert obligations.derived_status(due_rows)["id"] == "early"
    assert (
        obligations.derived_status([row("owner", created=2.0, oid="b"), row("owner", oid="a")])["id"] == "a"
    )


def test_invalid_values_and_database_checks_reject_bad_obligations(runtime):
    store = runtime[0]
    with store.tx() as db:
        for oid, changes in (
            ("", {}),
            ("x", {"kind": "unknown"}),
            ("x", {"owner": "nobody"}),
            ("x", {"wait": 1}),
            ("x", {"detail": {}}),
            ("x", {"due_at": "soon"}),
            ("x", {"due_at": True}),
            ("x", {"due_at": float("nan")}),
            ("x", {"due_at": float("inf")}),
            ("x", {"due_at": -float("inf")}),
            ("x", {"due_at": 10**400}),
        ):
            with pytest.raises(ValueError):
                obligations.open(db, oid, **{"kind": "job", "owner": "external", **changes})
    assert store.rows("SELECT * FROM obligations") == []
    with store.tx() as db:
        for owner, state in (("nobody", "open"), ("external", "waiting")):
            with pytest.raises(sqlite3.IntegrityError):
                db.execute(
                    "INSERT INTO obligations(id,kind,owner,state,created_at,updated_at) VALUES(?,?,?,?,1,1)",
                    (f"raw-{owner}", "job", owner, state),
                )
