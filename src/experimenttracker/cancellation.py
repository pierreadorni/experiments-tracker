"""Stop queued work immediately; reconcile only exact dispatched identities."""

import time

from .codex_client import TerminalTurnError
from .steering import TERMINAL_JOBS


async def process_cancellations(workflow):
    store = workflow.store
    for row in store.rows("SELECT id FROM experiments WHERE phase='cancelled' AND deleted_at IS NULL"):
        eid = row["id"]
        # A failed connection cannot block local cancellation or resurrect transfers.
        key = ("experiment", eid)
        now = time.time()
        if now - workflow.last_cancellation.get(key, 0) < 5:
            continue
        workflow.last_cancellation[key] = now
        errors = []
        # Background checks can still be using retained files. Let them settle,
        # discard their result, and never publish readiness after cancellation.
        for checks in (workflow.output_checks, workflow.evidence_checks):
            for identity, value in list(checks.items()):
                belongs = (
                    identity[0] == eid
                    if isinstance(identity, tuple)
                    else store.one(
                        "SELECT 1 FROM jobs WHERE intent_id=? AND experiment_id=?", (identity, eid)
                    )
                )
                if not belongs:
                    continue
                task = value[0] if isinstance(value, tuple) else value
                if not task.done():
                    errors.append("Waiting for an existing filesystem check to settle.")
                    continue
                try:
                    task.result()
                except Exception:
                    pass
                checks.pop(identity)

        for action in store.rows(
            "SELECT * FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
            (eid,),
        ):
            lane = next(
                (lane for lane in workflow.pool_owner.lanes if lane.active_action == action["id"]), workflow
            )
            if not action["thread_id"] or not action["turn_id"] or not lane.agent_ready:
                errors.append("An agent dispatch still needs its exact saved outcome reconciled.")
                continue
            try:
                lane.hold_active_action(action, "experiment_cancelled")
                await lane.agent.interrupt(action["thread_id"], action["turn_id"])
                if lane.agent_task and not lane.agent_task.done():
                    errors.append("Waiting for the recorded agent turn to stop.")
                    continue
                try:
                    result = await lane.agent.read_turn(action["thread_id"], action["turn_id"])
                except TerminalTurnError as exc:
                    lane._action_state(action["id"], "failed", error=str(exc))
                else:
                    lane._action_state(action["id"], "completed", result=result)
            except Exception as exc:
                errors.append(str(exc))
        for job in store.rows(
            "SELECT * FROM jobs WHERE experiment_id=? AND (state IN ('submitting','submitted') OR "
            "(state='failed' AND job_id IS NOT NULL))",
            (eid,),
        ):
            try:
                scheduler = workflow.scheduler_for(store.experiment(eid), job)
                job_id = job["job_id"]
                if not job_id:
                    job_id = await scheduler.reconcile(job["intent_id"])
                    if not job_id:
                        errors.append("Submission outcome is unknown; the saved intent needs reconciliation.")
                        continue
                    with store.tx() as db:
                        db.execute(
                            "UPDATE jobs SET job_id=?,state='submitted' WHERE intent_id=?",
                            (job_id, job["intent_id"]),
                        )
                        store.emit(
                            db, eid, "job_reconciled", {"intent_id": job["intent_id"], "job_id": job_id}
                        )
                observed = None
                terminal = job["scheduler_state"] in TERMINAL_JOBS
                if not terminal:
                    observed = await scheduler.observe(job_id)
                    terminal = observed.state in TERMINAL_JOBS
                if terminal:
                    with store.tx() as db:
                        db.execute(
                            "UPDATE jobs SET state='cancelled',scheduler_state=?,exit_code=?,observed_at=? WHERE intent_id=?",
                            (
                                observed.state if observed else job["scheduler_state"],
                                observed.exit_code if observed else job["exit_code"],
                                now,
                                job["intent_id"],
                            ),
                        )
                        store.emit(
                            db,
                            eid,
                            "cancelled_job_settled",
                            {"intent_id": job["intent_id"], "job_id": job_id},
                        )
                    continue
                # scancel is idempotent and verifies this exact intent/job identity.
                with store.tx() as db:
                    store.emit(
                        db, eid, "job_cancellation_intent", {"intent_id": job["intent_id"], "job_id": job_id}
                    )
                await scheduler.cancel(job_id, job["intent_id"])
                errors.append("Waiting for terminal scheduler accounting for the recorded job.")
            except Exception as exc:
                errors.append(str(exc))
        if not store.one(
            "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
            (eid,),
        ):
            with store.tx() as db:
                db.execute(
                    "UPDATE jobs SET state='cancelled' WHERE experiment_id=? AND state='agent'", (eid,)
                )
        store.update(
            eid,
            kind="cancellation_status",
            hold_reason="cancellation_unconfirmed" if errors else None,
            error=" ".join(errors)[:2000] if errors else None,
        )
