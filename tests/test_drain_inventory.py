import sqlite3
from contextlib import closing
from pathlib import Path

import pytest

from experimenttracker.drain_inventory import hosted_inventory, local_inventory
from experimenttracker.store import Store


def test_local_inventory_reads_saved_contracts_and_exact_pending_work(tmp_path):
    store = Store(tmp_path / "owner.sqlite")
    try:
        with store.tx() as db:
            db.executemany(
                """INSERT INTO experiments(id, brief, recipe_id, contract, phase, deleted_at, pause_agent,
                    created_at, updated_at) VALUES(?, 'private brief', 'fixture', ?, ?, ?, ?, 1, 1)""",
                (
                    ("c3-held", '{"execution_contract":3}', "preparing", None, 1),
                    ("c3-deleted", '{"execution_contract":3}', "cancelled", 2, 0),
                    ("c4", '{"execution_contract":4}', "preparing", None, 0),
                    ("old", '{"execution_contract":2}', "complete", None, 0),
                    ("missing", "{}", "complete", None, 0),
                    ("bad-type", '{"execution_contract":"3"}', "complete", None, 0),
                    ("bad-json", "{", "complete", None, 0),
                ),
            )
            db.execute("INSERT INTO stages(experiment_id,id,updated_at) VALUES('c3-deleted','stage',1)")
            db.executemany(
                """INSERT INTO jobs(intent_id,experiment_id,stage_id,attempt,state,job_id,script_hash,
                    snapshot_hash,resources,created_at,scheduler_state,exit_code,error,execution_manifest)
                    VALUES(?,'c3-deleted','stage',?,?,?,'','','{}',1,?,?,?,?)""",
                (
                    ("lag", 1, "failed", "801", None, None, "error", None),
                    ("terminal-unsettled", 2, "submitted", "802", "COMPLETED", "0:0", None, None),
                    ("absent", 3, "failed", None, None, None, "not_submitted: proven absent", None),
                    ("cancelled", 4, "cancelled", "804", "CANCELLED", None, None, None),
                    ("agent", 5, "complete", None, None, None, None, '{"executor":"agent"}'),
                ),
            )
            db.executemany(
                """INSERT INTO agent_actions(id,experiment_id,phase,evidence_revision,state,thread_id,
                    turn_id,input,created_at,updated_at) VALUES(?,?,'prepare',1,?,'th','turn','private',1,1)""",
                (
                    ("uncertain", "c3-deleted", "uncertain"),
                    ("settled", "c3-held", "completed"),
                    ("next", "c4", "pending"),
                ),
            )
            db.execute(
                "INSERT INTO execution_transfers VALUES('transfer','c3-deleted','pull','{}','pending',NULL,1)"
            )
            db.execute(
                "INSERT INTO report_versions VALUES('c3-held',1,'publishing','{}','report','[]',NULL,NULL,1,1)"
            )
            db.execute(
                "INSERT INTO obligations VALUES('integration:req:1','c3-held',NULL,'integration_queue',"
                "'req','backend',NULL,'open',NULL,1,1,NULL)"
            )
            db.execute(
                "INSERT INTO events(id,experiment_id,kind,revision,payload,created_at) "
                "VALUES('evt','c3-deleted','report_queued',1,'private',1)"
            )
            db.execute("INSERT INTO outbox(id,payload,attempts,next_attempt_at) VALUES('evt','private',2,90)")
            db.execute(
                "INSERT INTO output_refs VALUES('result','c3-deleted','agent','metric','acacia','local',"
                "'/private/result','metrics',1)"
            )
            db.execute("INSERT INTO commands VALUES('delayed',NULL,'create','gateway','{}','pending','{}',1)")
            db.execute(
                "INSERT INTO rejected_gateway_commands VALUES('refused','c3-deleted','answer','{}','private',1)"
            )
            db.execute(
                "INSERT INTO project_settings_requests(request_id,project_id,payload,state,created_at,origin) "
                "VALUES('receipt','project','{}','applied',1,'gateway')"
            )
        store.backup(tmp_path / "backup")
    finally:
        store.close()

    backup = tmp_path / "backup" / "workflow.sqlite"
    before = backup.read_bytes()
    report = local_inventory(backup)
    groups = report["experiments_by_saved_contract"]
    assert groups["3"]["nonterminal"][0]["pause_agent"] == 1
    assert groups["3"]["deleted"][0]["id"] == "c3-deleted"
    assert groups["4"]["nonterminal"][0]["id"] == "c4"
    assert set(groups) == {"2", "3", "4", "missing", "malformed"}
    assert len(groups["malformed"]["terminal"]) == 2
    assert {row["intent_id"]: row["settlement"] for row in report["c3_jobs"]} == {
        "absent": "confirmed_absent",
        "agent": "settled_agent",
        "cancelled": "settled_scheduler",
        "lag": "accounting_unconfirmed",
        "terminal-unsettled": "unsettled",
    }
    assert [(row["id"], row["turn_id"]) for row in report["c3_open_actions"]] == [("uncertain", "turn")]
    assert report["c3_open_transfers"][0]["id"] == "transfer"
    assert report["c3_open_obligations"][0]["id"] == "integration:req:1"
    assert report["c3_reports"][0]["state"] == "publishing"
    assert report["c3_undelivered_events"][0]["attempts"] == 2
    assert report["c3_output_refs"][0]["id"] == "result"
    assert report["local_unapplied_commands"][0]["request_id"] == "delayed"
    assert report["local_gateway_refusals"][0]["request_id"] == "refused"
    assert report["local_project_requests"]["project_settings_requests"][0]["request_id"] == "receipt"
    assert "private" not in str(report)
    assert backup.read_bytes() == before


def test_hosted_inventory_reads_all_pending_commands_for_exact_executor(tmp_path):
    backup = tmp_path / "d1.sqlite"
    migrations = Path(__file__).resolve().parents[1] / "gateway" / "drizzle"
    with closing(sqlite3.connect(backup)) as db:
        for migration in sorted(migrations.glob("*.sql")):
            db.executescript(migration.read_text())
        db.execute("INSERT INTO executor_status(id,payload,received_at) VALUES('e','{}',1)")
        # The projection can still be a placeholder; the queue supplies only its target ID.
        db.execute("INSERT INTO projections(id,payload,revision,updated_at) VALUES('target','{}',0,1)")
        db.executemany(
            """INSERT INTO commands(request_id,experiment_id,executor_id,kind,payload,state,received_at)
                VALUES(?, 'target', ?, 'create', 'private', ?, 1)""",
            [(f"pending-{index:02}", "e", "pending") for index in range(23)]
            + [("other-executor", "other", "pending"), ("applied", "e", "applied")],
        )
        db.execute(
            """INSERT INTO project_requests(request_id,project_id,executor_id,payload,received_at)
                VALUES('project-request','project','e','private',1)"""
        )
        db.commit()

    report = hosted_inventory(backup, "e")
    assert [row["request_id"] for row in report["commands"]] == [f"pending-{index:02}" for index in range(23)]
    assert all(row["experiment_id"] == "target" for row in report["commands"])
    assert report["project_requests"][0]["request_id"] == "project-request"
    assert "private" not in str(report)
    with pytest.raises(ValueError, match="no status for the requested executor"):
        hosted_inventory(backup, "wrong-executor")


def test_unreadable_authority_cannot_look_empty(tmp_path):
    missing = tmp_path / "missing.sqlite"
    with pytest.raises(sqlite3.OperationalError):
        local_inventory(missing)
    assert not missing.exists()

    broken = tmp_path / "broken.sqlite"
    with closing(sqlite3.connect(broken)) as db:
        db.executescript(
            "CREATE TABLE p(id PRIMARY KEY); CREATE TABLE c(id REFERENCES p(id)); INSERT INTO c VALUES(1);"
        )
    with pytest.raises(ValueError, match="foreign-key check"):
        hosted_inventory(broken, "e")
