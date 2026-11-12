"""Allocation GPU-seconds. Outstanding exposure follows an intent across weeks."""

import math
import time

from .store import encode

WEEK = 604800
TERMINAL = {
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "TIMEOUT",
    "OUT_OF_MEMORY",
    "NODE_FAIL",
    "PREEMPTED",
    "BOOT_FAIL",
    "DEADLINE",
    "REVOKED",
}


def window(anchor, now):
    start = anchor + math.floor((now - anchor) / WEEK) * WEEK
    return start, start + WEEK


def seconds_between(start, end, lower, upper):
    # Round exposure upward, never manufacture fractional GPU-second credit.
    return max(0, math.ceil(min(end, upper) - max(start, lower)))


class Budget:
    def __init__(self, store):
        self.store = store

    def view(self, project_id, policy, now=None):
        now = time.time() if now is None else now
        start, end = window(policy.anchor_at, now)
        consumed = reserved = unknown = unknown_jobs = 0
        anomalies = []
        for row in self.store.rows("SELECT * FROM autopilot_allocations WHERE project_id=?", (project_id,)):
            gpus = max(row["requested_gpus"], row["allocated_gpus"] or 0)
            used = 0
            if row["started_at"] is not None and row["accounted_until"] is not None:
                used = gpus * seconds_between(
                    row["started_at"], row["accounted_until"], row["started_at"], row["accounted_until"]
                )
                consumed += gpus * seconds_between(row["started_at"], row["accounted_until"], start, end)
            exposure = 0 if row["state"] == "settled" else max(0, row["reserved_seconds"] - used)
            reserved += exposure
            if row["state"] == "unknown":
                unknown += exposure
                unknown_jobs += 1
            if row["anomaly"]:
                anomalies.append({"intent_id": row["intent_id"], "reason": row["anomaly"]})
        cap = math.floor(policy.weekly_gpu_hours * 3600)
        return {
            "window_start": start,
            "reset_at": end,
            "cap_seconds": cap,
            "consumed_seconds": consumed,
            "reserved_seconds": reserved,
            "unknown_seconds": unknown,
            "unknown_jobs": unknown_jobs,
            "available_seconds": max(0, cap - consumed - reserved),
            "overrun_seconds": max(0, consumed + reserved - cap),
            "anomalies": anomalies,
        }

    def observe(self, job, observation, cluster, now=None):
        now = time.time() if now is None else now
        row = self.store.one("SELECT * FROM autopilot_allocations WHERE intent_id=?", (job["intent_id"],))
        if not row:
            return
        if not row["requested_gpus"] and not getattr(observation, "allocated_gpus", None):
            # CPU-only work has no GPU exposure; absent timestamps cannot create a GPU hold.
            return
        observed_at = getattr(observation, "observed_at", None) or now
        if row["observed_at"] is not None and observed_at <= row["observed_at"]:
            return
        start, end = observation.started_at, observation.ended_at
        allocated = getattr(observation, "allocated_gpus", None)
        restart = getattr(observation, "restarts", None)
        anomaly = row["anomaly"]
        if allocated is not None and allocated != row["requested_gpus"]:
            anomaly = "scheduler allocation differs from authorized request"
        if restart or observation.state in ("PREEMPTED", "SUSPENDED", "REQUEUED", "RESIZING"):
            anomaly = "unsupported repeated, suspended or changed allocation; reconcile exact accounting"
        if start is not None and row["started_at"] is not None and start != row["started_at"]:
            anomaly = "allocation start changed; possible requeue requires reconciliation"
        terminal = observation.state in TERMINAL
        reliable = (
            start is not None and start <= now and (not terminal or end is not None and start <= end <= now)
        )
        if not reliable:
            start, until = row["started_at"], row["accounted_until"]
            state = "unknown" if terminal or observation.state == "ACCOUNTING_PENDING" else row["state"]
            if observation.state in ("PENDING", "CONFIGURING") and start is None and not anomaly:
                state = "reserved"
        else:
            until = end if terminal else min(now, observed_at)
            state = "settled" if terminal and not anomaly else "unknown" if anomaly else "reserved"
            if row["accounted_until"] is not None and until < row["accounted_until"]:
                # Corrections require a terminal authoritative observation; active lag cannot refund.
                if not terminal:
                    until = row["accounted_until"]
            if (
                max(allocated or 0, row["requested_gpus"]) * math.ceil(until - start)
                > row["reserved_seconds"]
            ):
                anomaly = "observed allocation exceeds requested upper bound"
                state = "unknown"
        with self.store.tx() as db:
            db.execute(
                "UPDATE autopilot_allocations SET cluster=?,job_id=?,started_at=?,accounted_until=?,"
                "allocated_gpus=?,state=?,observed_at=?,source=?,anomaly=? WHERE intent_id=?",
                (
                    cluster,
                    job["job_id"],
                    start,
                    until,
                    max(allocated or 0, row["allocated_gpus"] or 0) or None,
                    state,
                    observed_at,
                    getattr(observation, "source", None) or "scheduler",
                    anomaly,
                    job["intent_id"],
                ),
            )
            db.execute(
                "INSERT INTO autopilot_accounting VALUES(?,?,?,?,?)",
                (
                    job["intent_id"],
                    observed_at,
                    cluster,
                    job["job_id"],
                    encode(
                        {
                            "state": observation.state,
                            "start": observation.started_at,
                            "end": end,
                            "gpus": allocated,
                            "restarts": restart,
                        }
                    ),
                ),
            )
