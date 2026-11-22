"""Autopilot policy as data: each check is applied at one admission point (C8).

The calendar, member authority, subscription reserves and the create checks live
here; ``Autopilot`` binds them as methods so its callers keep one copy. The lead
is a contract 4 experiment marked ``contract.autopilot_lead.run_id`` whose id is
derived from the run id; it is never an ``autopilot_members`` row, so it does not
count toward ``max_open_experiments``.
"""

import json
from uuid import NAMESPACE_URL, uuid5

from . import clock
from .autopilot_budget import WEEK
from .quota import Decision
from .store import Conflict

PHASES = ("foundation", "main_argument", "ablations", "writing")
WRITING_PURPOSES = ("correctness", "essential_result", "analysis")
# Run states in which the lead may take its next turn and members may work.
ADMITTING = ("active", "handoff_ready", "handoff_incomplete")
LEAD_ORIGIN = "autopilot"


def calendar(policy, now, borrowed=0):
    total = policy.handoff_at - policy.start_at
    boundaries = []
    percent = 0
    for i, share in enumerate(policy.phase_percent):
        percent += share
        boundaries.append(policy.start_at + total * (percent + (borrowed if i == 1 else 0)) / 100)
    index = next((i for i, at in enumerate(boundaries) if now < at), 3)
    return {
        "phase": PHASES[index],
        "boundaries": dict(zip(PHASES, boundaries, strict=True)),
        "week": max(0, (now - policy.start_at) / WEEK),
        "weeks_remaining": (policy.handoff_at - now) / WEEK,
        "handoff_due": now >= policy.handoff_at,
        "starts_in_seconds": max(0, policy.start_at - now),
    }


def lead_id(run_id):
    """The one lead experiment id of a run; a replayed activation derives the same id."""
    return str(uuid5(NAMESPACE_URL, "autopilot-lead:" + run_id))


def lead_request_id(run_id):
    """The create command identity of a run's lead."""
    return str(uuid5(NAMESPACE_URL, "autopilot-lead-create:" + run_id))


def lead_run_id(contract):
    """The run a saved contract leads, or None for every other experiment."""
    marker = (contract or {}).get("autopilot_lead")
    return marker.get("run_id") if isinstance(marker, dict) else None


def lead_brief(policy):
    parts = [("Subject", policy.subject), ("Scope", policy.scope)]
    parts += [("Exclusions", policy.exclusions), ("Instructions", policy.instructions)]
    body = "\n".join(f"{name}: {text}" for name, text in parts if text.strip())
    return "Autopilot lead for this project's research run.\n" + body


def lead_hold(db, contract):
    """``autopilot_<status>`` while the lead's run does not admit its next turn, else None."""
    run_id = lead_run_id(contract)
    if run_id is None:
        return None
    row = db.execute("SELECT status FROM autopilot_runs WHERE id=?", (run_id,)).fetchone()
    if row is None:
        return "autopilot_missing"
    return None if row["status"] in ADMITTING else "autopilot_" + row["status"]


def project_status(db, run_id, now=None):
    """A compact project status for the lead's turn input; the prompt bounds its size."""
    now = clock.now() if now is None else now
    from .autopilot_models import AutopilotPolicy

    run = db.execute("SELECT * FROM autopilot_runs WHERE id=?", (run_id,)).fetchone()
    policy = AutopilotPolicy.model_validate_json(
        db.execute(
            "SELECT policy FROM autopilot_policies WHERE run_id=? AND revision=?",
            (run_id, run["policy_revision"]),
        ).fetchone()["policy"]
    )
    schedule = calendar(policy, now, run["borrowed_percent"])
    members = db.execute(
        "SELECT m.experiment_id,m.purpose,m.deferred,e.phase FROM autopilot_members m "
        "JOIN experiments e ON e.id=m.experiment_id WHERE m.run_id=? ORDER BY m.priority DESC,m.created_at "
        "LIMIT 20",
        (run_id,),
    ).fetchall()
    lines = [
        f"Run {run_id}: {run['status']}, policy revision {run['policy_revision']}, "
        f"control revision {run['control_revision']}",
        f"Calendar: {schedule['phase']} phase, week {schedule['week']:.1f}, "
        f"{schedule['weeks_remaining']:.1f} weeks to handoff",
        f"Open experiment limit: {policy.max_open_experiments} (the lead is not counted)",
    ]
    lines += [
        f"- {row['experiment_id']}: {row['phase']}, {row['purpose']}"
        + (", deferred" if row["deferred"] else "")
        for row in members
    ] or ["- no member experiments yet"]
    return "\n".join(lines)


def member_authority(service, eid, phase="compute", stage=None, now=None):
    member = service.member(eid)
    if not member:
        return None
    run = service.run(run_id=member["run_id"])
    policy = service.policy(run)
    now = clock.now() if now is None else now
    if (
        run["project_id"] not in service.store.projects
        or not service.store.projects[run["project_id"]].public()["available"]
    ):
        return "autopilot_project_unavailable"
    if run["status"] in ("draft", "paused", "stopped", "completed"):
        return "autopilot_" + run["status"]
    if member["deferred"] or stage in json.loads(member["deferred_stages"]):
        return "autopilot_deferred"
    if now < policy.start_at:
        return "autopilot_not_started"
    if now >= policy.handoff_at and phase != "interpret":
        return "autopilot_handoff"
    if (
        calendar(policy, now, run["borrowed_percent"])["phase"] == "writing"
        and member["purpose"] not in WRITING_PURPOSES
        and phase != "interpret"
    ):
        return "autopilot_writing_scope"
    return None


def reserves(service, gate, eid=None, run=None):
    """The quota decision for a member or lead turn under its run's reserves."""
    member = service.member(eid) if eid else None
    lead = None
    if not run and member:
        run = service.run(run_id=member["run_id"])
    elif not run and eid:
        row = service.store.one(
            "SELECT json_extract(contract,'$.autopilot_lead.run_id') AS run_id FROM experiments WHERE id=?",
            (eid,),
        )
        lead = row["run_id"] if row else None
        run = service.run(run_id=lead) if lead else None
    if not run:
        return gate.decide()
    policy = service.policy(run)
    decision = gate.decide_policy(run["id"], policy.five_hour_reserve, policy.weekly_reserve)
    if lead and run["status"] not in ADMITTING:
        # A paused or stopped run holds the lead's next turn at dispatch too.
        return Decision(False, "autopilot_" + run["status"], decision.remaining, decision.observed_at)
    return decision


def require_slot(service, run):
    active = 0
    for row in service.store.rows(
        "SELECT m.*,e.phase FROM autopilot_members m JOIN experiments e ON e.id=m.experiment_id WHERE m.run_id=?",
        (run["id"],),
    ):
        if row["phase"] in ("complete", "cancelled", "failed"):
            continue
        busy = (
            service.store.one(
                "SELECT 1 FROM jobs WHERE experiment_id=? AND state IN ('submitting','submitted','agent')",
                (row["experiment_id"],),
            )
            or service.store.one(
                "SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN ('dispatching','running','uncertain')",
                (row["experiment_id"],),
            )
            or service.store.one(
                "SELECT 1 FROM execution_transfers WHERE experiment_id=? AND state NOT IN ('complete','cancelled','failed')",
                (row["experiment_id"],),
            )
        )
        if not row["deferred"] or busy:
            active += 1
    if active >= service.policy(run).max_open_experiments:
        raise Conflict("Autopilot open-experiment limit reached")


def create_checks(service, run, policy, now, estimated_finish_at, purpose):
    """Calendar window, finish before handoff, writing-phase purpose and the open slot."""
    if now < policy.start_at or now >= policy.handoff_at:
        raise Conflict("research dispatch is outside authorized calendar")
    if estimated_finish_at > policy.handoff_at or estimated_finish_at <= now:
        raise Conflict("new work must plausibly finish before handoff")
    if (
        calendar(policy, now, run["borrowed_percent"])["phase"] == "writing"
        and purpose not in WRITING_PURPOSES
    ):
        raise Conflict("writing phase rejects new exploratory directions")
    require_slot(service, run)
