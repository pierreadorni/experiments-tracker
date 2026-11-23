"""Read-only T4.8 and T5.1e drain evidence from offline SQLite backups."""

import argparse
import json
import sqlite3
from contextlib import closing, contextmanager
from pathlib import Path

from .core.job_obligations import not_submitted
from .legacy_archive import TERMINAL_BAD

_C3 = """WITH c3 AS (
    SELECT id FROM experiments
    WHERE CASE WHEN json_valid(contract) THEN
        json_type(contract, '$.execution_contract') = 'integer'
        AND json_extract(contract, '$.execution_contract') = 3
    END
) """
_TERMINAL_PHASES = {"complete", "failed", "cancelled"}


@contextmanager
def _read_backup(path):
    with closing(sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)) as db:
        db.row_factory = sqlite3.Row
        db.execute("BEGIN")
        if [row[0] for row in db.execute("PRAGMA integrity_check")] != ["ok"]:
            raise ValueError("backup failed SQLite integrity check")
        if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ValueError("backup failed SQLite foreign-key check")
        yield db


def _job_status(job):
    if job["state"] not in {"complete", "failed", "cancelled"}:
        return "unsettled"
    if not_submitted(job):
        return "confirmed_absent"
    manifest = json.loads(job["execution_manifest"] or "{}")
    if not isinstance(manifest, dict):
        return "invalid_manifest"
    if manifest.get("executor") == "agent":
        return "settled_agent" if job["job_id"] is None else "invalid_manifest"
    if not job["job_id"] or job["scheduler_state"] not in TERMINAL_BAD | {"COMPLETED"}:
        return "accounting_unconfirmed"
    if job["exit_code"] is None and (job["state"] != "cancelled" or job["scheduler_state"] == "COMPLETED"):
        return "accounting_unconfirmed"
    return "settled_scheduler"


def local_inventory(path: Path):
    with _read_backup(path) as db:

        def rows(query):
            return [dict(row) for row in db.execute(query)]

        groups = {}
        for experiment in rows(
            """SELECT id, phase, deleted_at, hold_reason, pause_agent, pause_compute,
                pending_request IS NOT NULL AS has_pending_request,
                CASE WHEN NOT json_valid(contract) THEN 'malformed'
                     WHEN json_type(contract) != 'object' THEN 'malformed'
                     WHEN json_type(contract, '$.execution_contract') IS NULL THEN 'missing'
                     WHEN json_type(contract, '$.execution_contract') != 'integer' THEN 'malformed'
                     ELSE CAST(json_extract(contract, '$.execution_contract') AS TEXT)
                END AS contract_group
                FROM experiments ORDER BY id"""
        ):
            group = groups.setdefault(experiment.pop("contract_group"), {})
            if experiment["deleted_at"] is not None:
                bucket = "deleted"
            elif experiment["phase"] in _TERMINAL_PHASES:
                bucket = "terminal"
            else:
                bucket = "nonterminal"
            group.setdefault(bucket, []).append(experiment)

        jobs = rows(
            _C3
            + """SELECT j.intent_id, j.experiment_id, j.stage_id, j.attempt, j.state, j.job_id,
                j.scheduler_state, j.exit_code, j.observed_at, j.error, j.execution_manifest
                FROM jobs j JOIN c3 ON c3.id=j.experiment_id ORDER BY j.intent_id"""
        )
        for job in jobs:
            try:
                job["settlement"] = _job_status(job)
            except (TypeError, ValueError):
                job["settlement"] = "invalid_manifest"
            del job["error"], job["execution_manifest"]

        return {
            "experiments_by_saved_contract": groups,
            "c3_jobs": jobs,
            "c3_open_actions": rows(
                _C3
                + """SELECT a.id, a.experiment_id, a.phase, a.state, a.thread_id, a.turn_id
                    FROM agent_actions a JOIN c3 ON c3.id=a.experiment_id
                    WHERE a.state NOT IN ('completed','failed','cancelled','superseded') ORDER BY a.id"""
            ),
            "c3_open_transfers": rows(
                _C3
                + """SELECT t.id, t.experiment_id, t.direction, t.state
                    FROM execution_transfers t JOIN c3 ON c3.id=t.experiment_id
                    WHERE t.state NOT IN ('complete','failed','cancelled') ORDER BY t.id"""
            ),
            "c3_open_steering": rows(
                _C3
                + """SELECT s.request_id, s.experiment_id, s.state
                    FROM steering s JOIN c3 ON c3.id=s.experiment_id
                    WHERE s.state IN ('pending','cancelling') ORDER BY s.request_id"""
            ),
            "c3_open_wakeups": rows(
                _C3
                + """SELECT w.id, w.experiment_id, w.action_id, w.state
                    FROM agent_wakeups w JOIN c3 ON c3.id=w.experiment_id
                    WHERE w.state IN ('pending','queued') ORDER BY w.id"""
            ),
            "c3_open_job_cancellations": rows(
                _C3
                + """SELECT cancellation.key AS request_id, c3.id AS experiment_id,
                    CASE WHEN cancellation.type='object' THEN json_extract(cancellation.value,'$.intent_id') END AS intent_id,
                    CASE WHEN cancellation.type='object' THEN json_extract(cancellation.value,'$.job_id') END AS job_id,
                    CASE WHEN cancellation.type='object' THEN json_extract(cancellation.value,'$.state') END AS state
                    FROM experiments e JOIN c3 ON c3.id=e.id,
                    json_each(CASE WHEN json_valid(e.contract) THEN e.contract END, '$.job_cancellations')
                    AS cancellation
                    WHERE state IS NOT 'terminal' ORDER BY experiment_id,request_id"""
            ),
            "c3_open_obligations": rows(
                _C3
                + """SELECT o.id, o.experiment_id, o.kind, o.target, o.owner
                    FROM obligations o JOIN c3 ON c3.id=o.experiment_id
                    WHERE o.state='open' ORDER BY o.id"""
            ),
            "c3_reports": rows(
                _C3
                + """SELECT r.request_id, r.experiment_id, r.version, r.state, r.report_artifact_id
                    FROM report_versions r JOIN c3 ON c3.id=r.experiment_id ORDER BY r.experiment_id,r.version"""
            ),
            "c3_undelivered_events": rows(
                _C3
                + """SELECT e.id, e.experiment_id, e.kind, e.revision, o.attempts, o.next_attempt_at
                    FROM outbox o JOIN events e ON e.id=o.id JOIN c3 ON c3.id=e.experiment_id
                    WHERE o.delivered_at IS NULL ORDER BY e.seq"""
            ),
            "c3_artifacts": rows(
                _C3
                + """SELECT a.id, a.experiment_id, a.local_valid, a.remote_published
                    FROM artifacts a JOIN c3 ON c3.id=a.experiment_id ORDER BY a.id"""
            ),
            "c3_event_history": rows(
                _C3
                + """SELECT e.id, e.experiment_id, e.kind, e.revision
                    FROM events e JOIN c3 ON c3.id=e.experiment_id ORDER BY e.seq"""
            ),
            "c3_output_refs": rows(
                _C3
                + """SELECT o.id, o.experiment_id, o.intent_id
                    FROM output_refs o JOIN c3 ON c3.id=o.experiment_id ORDER BY o.id"""
            ),
            "c3_branches": rows(
                _C3
                + """SELECT b.experiment_id, b.branch, b.state, b.head_commit, b.merged_commit
                    FROM experiment_branches b JOIN c3 ON c3.id=b.experiment_id ORDER BY b.experiment_id"""
            ),
            "c3_workspaces": rows(
                _C3
                + """SELECT w.experiment_id, 'project' AS kind FROM experiment_workspaces w
                    JOIN c3 ON c3.id=w.experiment_id
                    UNION ALL SELECT w.experiment_id, 'isolated' AS kind FROM isolated_workspaces w
                    JOIN c3 ON c3.id=w.experiment_id ORDER BY experiment_id,kind"""
            ),
            "c3_unsettled_allocations": rows(
                _C3
                + """SELECT a.intent_id, j.experiment_id, a.run_id, a.state FROM autopilot_allocations a
                    JOIN jobs j ON j.intent_id=a.intent_id JOIN c3 ON c3.id=j.experiment_id
                    WHERE a.state!='settled' ORDER BY a.intent_id"""
            ),
            # T5.1e: the project-turn engine has no reconciler, so this list must be
            # empty before that release starts; a run listed here keeps no lead.
            "autopilot_unsettled_actions": rows(
                """SELECT a.id, a.run_id, r.project_id, a.state, a.thread_id, a.turn_id,
                    a.pending_request IS NOT NULL AS has_pending_request, a.updated_at
                    FROM autopilot_actions a JOIN autopilot_runs r ON r.id=a.run_id
                    WHERE a.state IN ('pending','dispatching','running','uncertain','received')
                    ORDER BY a.id"""
            ),
            # Open runs that startup gives a lead, seeded from these saved fields.
            "autopilot_runs_awaiting_lead": rows(
                """SELECT r.id, r.project_id, r.status, r.hold_reason,
                    r.central_argument IS NOT NULL AS has_central_argument,
                    r.uncertainty IS NOT NULL AS has_uncertainty
                    FROM autopilot_runs r
                    WHERE r.status IN ('active','paused','handoff_ready','handoff_incomplete')
                    AND NOT EXISTS (SELECT 1 FROM experiments e WHERE CASE WHEN json_valid(e.contract)
                        THEN json_extract(e.contract,'$.autopilot_lead.run_id') END = r.id)
                    ORDER BY r.id"""
            ),
            "local_unapplied_commands": rows(
                """SELECT request_id, experiment_id, kind, origin, state FROM commands
                    WHERE state!='applied' ORDER BY request_id"""
            ),
            "local_gateway_applied_receipts": rows(
                """SELECT request_id, experiment_id, kind FROM commands
                    WHERE origin='gateway' AND state='applied' ORDER BY request_id"""
            ),
            "local_gateway_refusals": rows(
                """SELECT request_id, experiment_id, kind FROM rejected_gateway_commands ORDER BY request_id"""
            ),
            "local_project_requests": {
                table: rows(
                    f"""SELECT request_id, project_id, origin, state FROM {table}
                        WHERE state='pending' OR (origin='gateway' AND delivered_at IS NULL) ORDER BY request_id"""
                )
                for table in ("project_requests", "project_settings_requests")
            },
        }


def hosted_inventory(path: Path, executor_id: str):
    # A hosted projection may be a create placeholder. Return exact targets for
    # comparison with saved local contract groups and receipts.
    scopes = {
        "commands": "experiment_id, kind,",
        "project_requests": "project_id,",
        "project_settings_requests": "project_id,",
        "settings_commands": "kind,",
        "project_memory_commands": "project_id,",
        "autopilot_commands": "project_id,",
        "maintenance_commands": "release_id,",
    }
    with _read_backup(path) as db:
        if db.execute("SELECT 1 FROM executor_status WHERE id=?", (executor_id,)).fetchone() is None:
            raise ValueError("D1 copy has no status for the requested executor")
        return {
            table: [
                dict(row)
                for row in db.execute(
                    f"""SELECT request_id, {scope} received_at FROM {table}
                        WHERE executor_id=? AND state='pending' ORDER BY request_id""",
                    (executor_id,),
                )
            ]
            for table, scope in scopes.items()
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("backup", type=Path, help="SQLite online backup copy of executor state")
    parser.add_argument("--d1-backup", type=Path, help="SQLite copy imported from a D1 export")
    parser.add_argument("--executor-id", help="exact executor ID to select from the D1 copy")
    args = parser.parse_args()
    if (args.d1_backup is None) != (args.executor_id is None):
        parser.error("--d1-backup and --executor-id must be supplied together")
    try:
        report = {
            "local": local_inventory(args.backup),
            "hosted_pending": hosted_inventory(args.d1_backup, args.executor_id) if args.d1_backup else None,
        }
    except (OSError, sqlite3.Error, ValueError) as exc:
        parser.exit(1, f"Drain inventory failed: {exc}\n")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
