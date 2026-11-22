"""Durable steering of a running experiment into a corrected replacement."""

from uuid import NAMESPACE_URL, uuid5

from . import clock, legacy_archive

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


def queue_late_reports(workflow):
    """Guidance arriving after an interpretation starts becomes a saved result discussion."""
    store = workflow.store
    for item in store.rows(
        "SELECT s.* FROM steering s JOIN experiments e ON e.id=s.experiment_id "
        "WHERE s.mode='later' AND s.state='pending' AND e.phase='complete' AND e.deleted_at IS NULL ORDER BY s.created_at"
    ):
        exp = store.experiment(item["experiment_id"])
        if legacy_archive.execution_frozen(exp):
            continue
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
                (clock.now(), item["request_id"]),
            )
            store.emit(
                db,
                exp["id"],
                "steering_discussion_queued",
                {"request_id": item["request_id"], "discussion_id": identity},
            )
