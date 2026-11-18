"""Contract 3 selective repair; scheduler boundaries retain exact durable identities."""

import json
import os
from pathlib import Path
from uuid import UUID

from . import artifacts, clock, iteration
from .core import advancement_decisions, job_obligations, review_obligations
from .store import encode

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


def preparing(exp):
    return exp["contract"].get("early_repair", {}).get("state") == "preparing"


def active_predecessor(wf, exp, stage):
    jobs = wf.store.rows(
        "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? AND attempt<? ORDER BY attempt DESC",
        (exp["id"], stage, exp["evidence_revision"]),
    )
    for job in jobs:
        agent = json.loads(job["execution_manifest"] or "{}").get("executor") == "agent"
        if agent:
            if job["state"] not in ("complete", "failed", "cancelled"):
                return job
        elif not job["job_id"] or job["scheduler_state"] not in TERMINAL or job["exit_code"] is None:
            if job_obligations.not_submitted(job):
                # The resolver proved this intent never reached the scheduler, so
                # it has no exact identity left for a later attempt to wait on.
                continue
            return job
    return None


def submission_hold(wf, exp, stage):
    if preparing(exp):
        return {"state": "held", "reason": "repair_preparation"}
    previous = active_predecessor(wf, exp, stage)
    if previous:
        return {"state": "held", "reason": "predecessor_pending", "intent_id": previous["intent_id"]}
    return None


def selected(wf, exp, job):
    """A superseded attempt settles accounting but cannot overwrite readiness."""
    repair = exp["contract"].get("early_repair", {})
    if job["attempt"] >= exp["evidence_revision"] or repair.get("state") != "applied":
        return True
    decision = repair.get("decisions", {}).get(job["stage_id"])
    return not decision or decision["decision"] == "keep"


async def queue(wf, exp):
    if preparing(exp):
        wf.store.update(exp["id"], hold_reason="automatic_repair")
        return
    if not wf.repair_available(exp) or exp["pending_request"]:
        return
    eid = exp["id"]
    if wf.store.one(
        "SELECT 1 FROM steering WHERE experiment_id=? AND state IN ('pending','cancelling')", (eid,)
    ) or wf.store.one(
        "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('running','dispatching','uncertain')",
        (eid,),
    ):
        return
    failed = wf.store.rows("SELECT * FROM jobs WHERE experiment_id=? AND state='failed'", (eid,))
    if not failed:
        return
    # Failures are established by the ordinary exact observer (or saved agent
    # turn). An uncertain dispatch never creates a failure incident.
    revision = exp["evidence_revision"] + 1
    branch = wf.workspaces.branch(eid)
    root = wf.projects.base_root(eid) / "repair-candidates" / str(revision)
    work, project = root / "work", root / "project"
    original_work = wf.working_directory(exp)
    context = wf.project_context(exp)
    plan = exp["snapshot"]["plan"]
    total = 0
    copies = [(plan["files"], original_work, work)]
    if not branch:
        # A named branch worktree is already this experiment's private checkout, so its
        # repair edits stay in the branch the backend commits. A shared project folder
        # needs a candidate copy so the original sources are not edited mid-repair.
        copies.append((plan.get("project_files", []), context["checkout"], project))
    for names, source, target in copies:
        target.mkdir(parents=True, exist_ok=True)
        for name in names:
            value = iteration.bounded_bytes(
                Path(source) / name,
                Path(source),
                min(iteration.SOURCE_BUDGET - total, artifacts.MAX_ARTIFACT_BYTES),
            )
            total += len(value)
            path = target / name
            path.parent.mkdir(parents=True, exist_ok=True)
            if path.exists() and iteration.bounded_bytes(path, target) != value:
                raise ValueError("uncommitted repair candidate differs from its declared source")
            if not path.exists():
                temporary = path.with_name(path.name + ".repair-staging")
                with temporary.open("wb") as stream:
                    stream.write(value)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(temporary, path)
    contract = exp["contract"]
    if contract.get("auto_repair"):
        contract.setdefault("repair_history", []).append(contract["auto_repair"])
    contract["auto_repair"] = {
        "revision": revision,
        "previous_revision": exp["evidence_revision"],
        "previous_root": str(wf.root(eid)),
        "snapshot": exp["snapshot"],
        "error": exp["error"],
        "kind": "selective",
        "reason": exp["hold_reason"],
    }
    contract["early_repair"] = {
        "state": "preparing",
        "revision": revision,
        "failed_intents": [j["intent_id"] for j in failed],
        "error": exp["error"],
        "original_work": str(original_work),
    }
    contract["working_directory"] = str(work)
    if not branch:
        contract["repair_project"] = str(project)
    contract["repair_count"] = contract.get("repair_count", 0) + 1
    review = exp["plan_review"]
    if review:
        review = {
            **review,
            "preparation_revision": max(revision, review.get("preparation_revision", review["revision"]) + 1),
        }
    with wf.store.tx() as db:
        before_review = review_obligations.saved_review(db, eid)
        db.execute(
            "UPDATE experiments SET contract=?,evidence_revision=?,phase='preparing',hold_reason='automatic_repair',plan_review=? WHERE id=?",
            (encode(contract), revision, encode(review) if review else None, eid),
        )
        advancement_decisions.sync_lifetime(db, eid, exp["phase"], "preparing")
        review_obligations.sync_review(db, eid, before=before_review)
        db.execute(
            "UPDATE stages SET state='pending',attempt=?,updated_at=? WHERE experiment_id=? AND id IN ('prepare','interpret','publish')",
            (revision, clock.now(), eid),
        )
        wf.store.emit(db, eid, "selective_repair_queued", contract["early_repair"])


def validate(wf, exp, plan):
    old = exp["snapshot"]["plan"]
    decisions = {d.stage_id: d.model_dump() for d in plan.repair_decisions or []}
    old_stages = {s["id"]: s for s in old["stages"]}
    new_stages = {s.id: s.model_dump() for s in plan.stages}
    if len(decisions) != len(plan.repair_decisions or []) or set(decisions) != set(old_stages):
        raise ValueError(
            "selective repair requires one keep/retry decision with rationale for every existing stage"
        )
    if set(old_stages) - set(new_stages):
        raise ValueError("selective repair must retain existing stages and requested work")
    reused = set(plan.reuse_stages or [])
    for stage, decision in decisions.items():
        latest = wf.store.one(
            "SELECT * FROM jobs WHERE experiment_id=? AND stage_id=? ORDER BY attempt DESC LIMIT 1",
            (exp["id"], stage),
        )
        if decision["decision"] == "keep":
            if new_stages.get(stage) != old_stages[stage]:
                raise ValueError(f"kept stage specification must remain unchanged: {stage}")
            if latest and latest["state"] in ("failed", "cancelled"):
                raise ValueError(f"failed stage requires retry: {stage}")
            if latest and latest["state"] == "complete" and stage not in reused:
                raise ValueError(f"kept successful stage requires explicit reuse: {stage}")
        elif stage in reused:
            raise ValueError(f"retry cannot also reuse {stage}")
        elif latest and latest["state"] in ("submitting", "agent"):
            raise ValueError(f"reconcile exact submission/agent turn before retrying {stage}")
    if reused - {s for s, d in decisions.items() if d["decision"] == "keep"}:
        raise ValueError("reuse must select kept successful stages")


def apply(wf, exp, plan, manifest):
    validate(wf, exp, plan)
    contract = wf.store.experiment(exp["id"])["contract"]
    decisions = {d.stage_id: d.model_dump() for d in plan.repair_decisions}
    contract["early_repair"].update(state="applied", decisions=decisions)
    contract.update(plan=manifest["plan"], parameters=manifest["parameters"])
    with wf.store.tx() as db:
        db.execute(
            "UPDATE experiments SET contract=?,snapshot=?,hold_reason=NULL,error=NULL WHERE id=?",
            (encode(contract), encode(manifest), exp["id"]),
        )
        for ordinal, stage in enumerate(plan.stages, 2):
            keep = decisions.get(stage.id, {}).get("decision") == "keep"
            db.execute(
                "INSERT INTO stages(experiment_id,id,ordinal,attempt,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(experiment_id,id) DO UPDATE SET ordinal=excluded.ordinal",
                (exp["id"], stage.id, ordinal, exp["evidence_revision"], clock.now()),
            )
            if not keep:
                db.execute(
                    "UPDATE stages SET state='pending',attempt=?,output_manifest=NULL,error=NULL,updated_at=? WHERE experiment_id=? AND id=?",
                    (exp["evidence_revision"], clock.now(), exp["id"], stage.id),
                )
        wf.store.emit(db, exp["id"], "selective_repair_applied", {"decisions": decisions})


def save_cancellation(store, db, eid, intent, request, reason):
    """Validate and idempotently save one exact selective cancellation in ``db``.

    Contract-3 selective repair and the contract-4 ``cancel_job`` tool share this
    save: the UUIDs are strict, the job must be this experiment's own saved Slurm
    intent and an exact replay returns the saved decision. It never contacts the
    scheduler; the existing cancellation reconciler proceeds later with the saved
    identity. Returns ``(record, created)``.
    """
    intent, request = str(UUID(str(intent))), str(UUID(str(request)))
    job = db.execute("SELECT * FROM jobs WHERE experiment_id=? AND intent_id=?", (eid, intent)).fetchone()
    if not job or json.loads(job["execution_manifest"] or "{}").get("executor") == "agent":
        raise ValueError("cancellation requires this experiment's exact Slurm intent")
    if not reason.strip() or len(reason) > 2000:
        raise ValueError("provide a cancellation reason of 1–2000 characters")
    contract = json.loads(
        db.execute("SELECT contract FROM experiments WHERE id=?", (eid,)).fetchone()["contract"]
    )
    saved = contract.setdefault("job_cancellations", {}).get(request)
    if saved:
        if (saved["intent_id"], saved["reason"]) != (intent, reason):
            raise ValueError("cancellation request identity belongs to another decision")
        return saved, False
    record = {"intent_id": intent, "reason": reason, "state": "pending"}
    contract["job_cancellations"][request] = record
    db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    store.emit(db, eid, "selective_cancellation_requested", {"request_id": request, **record})
    return record, True


async def cancel(wf, exp, intent, request, reason):
    if not preparing(exp):
        raise ValueError("job cancellation requires an active contract 3 repair")
    with wf.store.tx() as db:
        saved, created = save_cancellation(wf.store, db, exp["id"], intent, request, reason)
    if not created:
        return saved
    await reconcile_cancellations(wf, exp["id"])
    return wf.store.experiment(exp["id"])["contract"]["job_cancellations"][str(UUID(request))]


async def reconcile_cancellations(wf, eid=None):
    for row in wf.store.rows(
        "SELECT id FROM experiments WHERE deleted_at IS NULL"
        if eid is None
        else "SELECT id FROM experiments WHERE id=?",
        () if eid is None else (eid,),
    ):
        exp = wf.store.experiment(row["id"])
        for request, saved in exp["contract"].get("job_cancellations", {}).items():
            if saved["state"] == "terminal":
                continue
            key = ("selective", request)
            if clock.now() - wf.last_cancellation.get(key, 0) < 5:
                continue
            wf.last_cancellation[key] = clock.now()
            job = wf.store.one(
                "SELECT * FROM jobs WHERE experiment_id=? AND intent_id=?", (exp["id"], saved["intent_id"])
            )
            try:
                scheduler = wf.scheduler_for(exp, job)
                identity = await scheduler.reconcile(job["intent_id"])
                if not identity or (job["job_id"] and identity != job["job_id"]):
                    raise ValueError("exact submission identity needs reconciliation")
                observed = await scheduler.observe(identity)
                if observed.state in TERMINAL and observed.exit_code is not None:
                    state = "terminal"
                elif observed.state in ("PENDING", "RUNNING", "CONFIGURING", "COMPLETING", "SUSPENDED"):
                    state = "dispatching"
                else:
                    raise ValueError("waiting for exact scheduler accounting")
                contract = wf.store.experiment(exp["id"])["contract"]
                record = {
                    **saved,
                    "state": state,
                    "job_id": identity,
                    "scheduler_state": observed.state,
                    "error": None,
                }
                contract["job_cancellations"][request] = record
                with wf.store.tx() as db:
                    db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), exp["id"]))
                    wf.store.emit(
                        db, exp["id"], "selective_cancellation_" + state, {"request_id": request, **record}
                    )
                if state == "dispatching":
                    await scheduler.cancel(identity, job["intent_id"])
            except Exception as exc:
                contract = wf.store.experiment(exp["id"])["contract"]
                contract["job_cancellations"][request].update(state="uncertain", error=str(exc)[:2000])
                with wf.store.tx() as db:
                    db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), exp["id"]))
                    wf.store.emit(
                        db,
                        exp["id"],
                        "selective_cancellation_uncertain",
                        {"request_id": request, "error": str(exc)[:2000]},
                    )
