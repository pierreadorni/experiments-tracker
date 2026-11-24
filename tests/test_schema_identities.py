"""Every schema step after version 25 keeps saved execution identities (T5.5b).

The frozen fixture replaces the handover's before/after identity comparison: a
migration that rewrote saved job, action or turn identities or pause controls
fails here instead of at an owner's restart.
"""

import sqlite3
from pathlib import Path

from experimenttracker.store import Store

FIXTURE = Path(__file__).parent / "fixtures/schema/version-25.sql"
EXPECTED = {
    "jobs": [("00000000-0000-4000-8000-0000000000a1", "424242")],
    "agent_actions": [("00000000-0000-4000-8000-0000000000b1", "thread-b1", "turn-b1")],
    "project_turns": [("00000000-0000-4000-8000-0000000000c1", "thread-c1", "turn-c1")],
    "autopilot_actions": [("00000000-0000-4000-8000-0000000000d1", "thread-d1", "turn-d1")],
    "pauses": [
        ("00000000-0000-4000-8000-000000000001", 1, 0),
        ("00000000-0000-4000-8000-000000000002", 0, 1),
    ],
}


def saved(store):
    def rows(query):
        return [tuple(row.values()) for row in store.rows(query)]

    return {
        "jobs": rows("SELECT intent_id,job_id FROM jobs ORDER BY intent_id"),
        "agent_actions": rows("SELECT id,thread_id,turn_id FROM agent_actions ORDER BY id"),
        "project_turns": rows("SELECT id,thread_id,turn_id FROM project_turns ORDER BY id"),
        "autopilot_actions": rows("SELECT id,thread_id,turn_id FROM autopilot_actions ORDER BY id"),
        "pauses": rows("SELECT id,pause_agent,pause_compute FROM experiments ORDER BY id"),
    }


def test_migrations_from_version_25_preserve_saved_identities(tmp_path):
    path = tmp_path / "workflow.sqlite"
    with sqlite3.connect(path) as db:
        db.executescript(FIXTURE.read_text())
    store = Store(path)
    try:
        assert saved(store) == EXPECTED
        assert store.db.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert not store.db.execute("PRAGMA foreign_key_check").fetchall()
    finally:
        store.close()
    # Reopening is a no-op migration and keeps them too.
    store = Store(path)
    try:
        assert saved(store) == EXPECTED
    finally:
        store.close()
