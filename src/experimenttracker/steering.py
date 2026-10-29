"""Durable steering of a running experiment into a corrected replacement."""

import json
import time
from uuid import NAMESPACE_URL, uuid5

from .store import encode

TERMINAL_JOBS = (
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
)


async def process_restarts(workflow):
    store = workflow.store
    for item in store.rows(
        "SELECT * FROM steering WHERE mode='restart' AND state IN ('pending','cancelling') ORDER BY created_at"
    ):
        eid = item["experiment_id"]
        exp = store.experiment(eid)
        if (
            not exp["snapshot"]
            and not store.one("SELECT 1 FROM jobs WHERE experiment_id=?", (eid,))
            and not store.one("SELECT 1 FROM agent_actions WHERE experiment_id=? AND phase='prepare'", (eid,))
        ):
            # The planning loop can revise in place before implementation begins.
            continue
        active_turn = store.one(
            "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
            (eid,),
        )
        if workflow.agent_task and workflow.active_action:
            owned = store.one("SELECT experiment_id FROM agent_actions WHERE id=?", (workflow.active_action,))
            active_turn = active_turn or owned and owned["experiment_id"] == eid
        if active_turn:
            store.update(eid, hold_reason="steering_waiting_for_agent")
            continue
        job = store.one(
            "SELECT * FROM jobs WHERE experiment_id=? AND (state IN ('submitting','submitted') OR "
            "(state='failed' AND job_id IS NOT NULL AND (scheduler_state IS NULL OR scheduler_state NOT IN ("
            + ",".join("?" for _ in TERMINAL_JOBS)
            + ")))) ORDER BY created_at LIMIT 1",
            (eid, *TERMINAL_JOBS),
        )
        if job:
            if not job["job_id"]:
                store.update(eid, hold_reason="steering_waiting_for_job_identity")
                continue
            if item["cancel_job_id"] and item["cancel_job_id"] != job["job_id"]:
                prior = store.one(
                    "SELECT state,scheduler_state FROM jobs WHERE experiment_id=? AND job_id=?",
                    (eid, item["cancel_job_id"]),
                )
                if (
                    not prior
                    or prior["state"] in ("submitting", "submitted")
                    or prior["scheduler_state"] not in TERMINAL_JOBS
                ):
                    store.update(eid, hold_reason="steering_job_identity_changed")
                    continue
                with store.tx() as db:
                    db.execute(
                        "UPDATE steering SET state='pending',cancel_job_id=NULL WHERE request_id=?",
                        (item["request_id"],),
                    )
                    store.emit(db, eid, "next_job_cancellation_pending", {"request_id": item["request_id"]})
                continue
            if job["state"] == "failed":
                # A workflow validation failure may precede scheduler termination.
                try:
                    observed = await workflow.scheduler_for(
                        workflow.store.experiment(job["experiment_id"]), job
                    ).observe(job["job_id"])
                except Exception as exc:
                    store.update(eid, hold_reason="steering_waiting_for_job_identity", error=str(exc))
                    continue
                with store.tx() as db:
                    db.execute(
                        "UPDATE jobs SET scheduler_state=?,exit_code=?,observed_at=? WHERE intent_id=?",
                        (observed.state, observed.exit_code, time.time(), job["intent_id"]),
                    )
                    store.emit(
                        db,
                        eid,
                        "steering_job_observed",
                        {"intent_id": job["intent_id"], "state": observed.state},
                    )
                if observed.state in TERMINAL_JOBS:
                    continue
                if observed.state == "ACCOUNTING_PENDING":
                    store.update(eid, hold_reason="steering_waiting_for_job_identity")
                    continue
            if item["state"] == "pending":
                with store.tx() as db:
                    db.execute(
                        "UPDATE steering SET state='cancelling',cancel_job_id=? WHERE request_id=?",
                        (job["job_id"], item["request_id"]),
                    )
                    store.emit(
                        db,
                        eid,
                        "job_cancellation_intent",
                        {"request_id": item["request_id"], "intent_id": job["intent_id"]},
                    )
            now = time.time()
            if now - workflow.last_cancellation.get(item["request_id"], 0) < 30:
                continue
            workflow.last_cancellation[item["request_id"]] = now
            try:
                await workflow.scheduler_for(workflow.store.experiment(job["experiment_id"]), job).cancel(
                    job["job_id"], job["intent_id"]
                )
                store.update(eid, hold_reason="steering_cancelling_job")
            except Exception as exc:
                store.update(eid, hold_reason="steering_cancellation_uncertain", error=str(exc))
            continue
        _create_replacement(store, exp, item)


def _create_replacement(store, exp, item):
    eid = exp["id"]
    request_id = str(uuid5(NAMESPACE_URL, f"experiment-restart:{item['request_id']}"))
    replacement_id = str(uuid5(NAMESPACE_URL, f"experiment-replacement:{item['request_id']}"))
    correction = "\n\nResearcher correction for this run: " + item["text"]
    if len(correction) >= 4000:
        brief = item["text"][:4000]
    else:
        brief = exp["brief"][: 4000 - len(correction)] + correction
    agent = exp["contract"].get("agent", {})
    payload = {
        "schema_version": 1,
        "request_id": request_id,
        "brief": brief,
        "recipe_id": exp["recipe_id"],
        "workspace_mode": exp["contract"].get("workspace_mode", "auto"),
        **({"execution": exp["contract"]["execution"]} if exp["contract"].get("execution") else {}),
        "project_id": exp["contract"].get("project", {}).get("id", "test-project"),
        **({"model": agent["model"]} if agent.get("model") else {}),
        **({"reasoning_effort": agent["reasoning_effort"]} if agent.get("reasoning_effort") else {}),
        **(
            {"reference_experiment_id": exp["reference_experiment_id"]}
            if exp["reference_experiment_id"]
            else {}
        ),
    }
    with store.tx() as db:
        store.command(request_id, "create", payload, experiment_id=replacement_id, _db=db)
        child = db.execute("SELECT contract FROM experiments WHERE id=?", (replacement_id,)).fetchone()
        contract = json.loads(child["contract"])
        contract["restart_context"] = {
            "experiment_id": eid,
            "original_brief": exp["brief"],
            "previous_plan": exp["plan_review"] or (exp["snapshot"] or {}).get("plan"),
            "snapshot_hash": (exp["snapshot"] or {}).get("snapshot_hash"),
            "guidance": [
                row[0]
                for row in db.execute(
                    "SELECT text FROM steering WHERE experiment_id=? ORDER BY created_at", (eid,)
                )
            ],
        }
        db.execute(
            "UPDATE experiments SET replaces_experiment_id=?,contract=? WHERE id=?",
            (eid, encode(contract), replacement_id),
        )
        db.execute(
            "UPDATE steering SET state='applied',applied_at=?,replacement_experiment_id=? WHERE request_id=?",
            (time.time(), replacement_id, item["request_id"]),
        )
        db.execute(
            "UPDATE experiments SET phase='cancelled',hold_reason=NULL,error=NULL WHERE id=?",
            (eid,),
        )
        db.execute(
            "UPDATE steering SET state='transferred',applied_at=? WHERE experiment_id=? AND mode='later' AND state='pending'",
            (time.time(), eid),
        )
        store.emit(db, eid, "steering_restarted", {"replacement_experiment_id": replacement_id})
        store.emit(db, replacement_id, "replaces_experiment", {"experiment_id": eid})


def queue_late_reports(workflow):
    """Guidance arriving after an interpretation starts becomes a saved result discussion."""
    store = workflow.store
    for item in store.rows(
        "SELECT s.* FROM steering s JOIN experiments e ON e.id=s.experiment_id "
        "WHERE s.mode='later' AND s.state='pending' AND e.phase='complete' AND e.deleted_at IS NULL ORDER BY s.created_at"
    ):
        exp = store.experiment(item["experiment_id"])
        identity = str(uuid5(NAMESPACE_URL, f"steering-discussion:{item['request_id']}"))
        selection = exp["contract"].get("agent", {})
        payload = {
            "schema_version": 1,
            "request_id": identity,
            "text": item["text"],
            **({"model": selection["model"]} if selection.get("model") else {}),
            **(
                {"reasoning_effort": selection["reasoning_effort"]}
                if selection.get("reasoning_effort")
                else {}
            ),
        }
        if (
            store.one(
                "SELECT COUNT(*) AS n FROM discussions WHERE experiment_id=? AND state IN ('queued','waiting','running')",
                (exp["id"],),
            )["n"]
            >= 20
        ):
            continue
        with store.tx() as db:
            store.command(identity, "discuss", payload, eid=exp["id"], _db=db)
            db.execute(
                "UPDATE steering SET state='discussion_queued',applied_at=? WHERE request_id=?",
                (time.time(), item["request_id"]),
            )
            store.emit(
                db,
                exp["id"],
                "steering_discussion_queued",
                {"request_id": item["request_id"], "discussion_id": identity},
            )
