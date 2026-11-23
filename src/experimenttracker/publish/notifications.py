"""Notification text for a published experiment view (port of gateway/src/push.ts).

The fingerprint must equal the TypeScript one on gateway/test/fixtures/notification_cases.json.
"""

import json
import re
from urllib.parse import quote


def _pretty(value):
    return value.replace("_", " ")


def _slice(text, limit):
    # JavaScript slices UTF-16 code units, not code points.
    units = text.encode("utf-16-le", "surrogatepass")
    return text if len(units) <= limit * 2 else units[: limit * 2].decode("utf-16-le", "surrogatepass")


def _stringify(value):
    # JSON.stringify writes lone surrogates as escapes; Python would emit them raw.
    return re.sub(
        "[\ud800-\udfff]",
        lambda m: f"\\u{ord(m.group()):04x}",
        json.dumps(value, ensure_ascii=False, separators=(",", ":")),
    )


def _execution(job, state, stage_id, jobs):
    if not job:
        done = any(j["stage_id"] == stage_id and j["state"] == "complete" for j in jobs)
        return "Checking outputs" if done else "Running"
    if job["state"] == "submitting":
        return "Submitting"
    if not state:
        return "Waiting for scheduler"
    if state in ("PENDING", "CONFIGURING"):
        return "Queued"
    if state in ("ACCOUNTING_PENDING", "COMPLETED"):
        return "Waiting for accounting"
    if state == "COMPLETING":
        return "Finishing"
    if state == "RUNNING":
        return "Running"
    return f"Scheduler: {_pretty(state.lower())}"


def _status(exp):
    """Return (status, alert, identity) from explicit published state only."""
    phase, hold = exp["phase"], exp.get("hold_reason")
    request, review = exp.get("pending_request"), exp.get("plan_review")
    if (
        request
        and not request.get("expired")
        and not request.get("answer")
        and phase not in ("failed", "cancelled")
    ):
        return "Your response is needed", True, request["id"]
    if phase == "complete":
        ready = [r["version"] for r in exp.get("report_versions") or [] if r["state"] == "ready"]
        return "Report ready", True, exp.get("active_report_id") or str(max([1, *ready]))
    if phase == "failed":
        return "Experiment failed", True, ""
    if phase == "cancelled":
        return "Experiment cancelled", False, ""
    if review and review["state"] == "awaiting_review":
        return "Plan ready for your approval", True, str(review["revision"])
    if hold:
        return f"On hold · {_pretty(hold)}", False, ""
    jobs, stages = exp.get("jobs") or [], exp.get("stages") or []
    active = [j for j in jobs if j["state"] in ("submitting", "submitted")]

    def first(test):
        return next((j for j in active if test(j)), None)

    # Prefer actual running compute, then queued work, over stage rows.
    job = (
        first(lambda j: j["scheduler_state"] == "RUNNING")
        or first(lambda j: j["scheduler_state"] == "COMPLETING")
        or first(lambda j: j["state"] == "submitting")
        or first(lambda j: (j["scheduler_state"] or "") in ("PENDING", "CONFIGURING"))
        or first(lambda j: not j["scheduler_state"])
        or (active[0] if active else None)
    )
    stage = next((s for s in stages if job and s["id"] == job["stage_id"]), None) or next(
        (s for s in stages if s["state"] in ("running", "dispatching", "submitting")), None
    )
    if not (stage or job):
        return _pretty(phase), False, ""
    stage_id = job["stage_id"] if job else stage["id"]
    plans = ((exp.get("snapshot") or {}).get("plan") or {}).get("stages") or []
    reviews = (review or {}).get("stages") or []
    plan = next((s["title"] for s in plans if s["id"] == stage_id), None)
    review_title = next((s["title"] for s in reviews if s["id"] == stage_id), None)
    title = plan or review_title or _pretty(stage_id)
    return f"{title} · {_execution(job, job and job.get('scheduler_state'), stage_id, jobs)}", False, ""


def experiment_notification(exp):
    status, alert, identity = _status(exp)
    stages = exp.get("stages") or []
    count = (
        f" · {sum(s['state'] == 'complete' for s in stages)}/{len(stages)} stages complete"
        if stages and exp["phase"] != "complete"
        else ""
    )
    demo = "Demo · " if (exp.get("contract") or {}).get("synthetic_demo") else ""
    body = _slice(f"{demo}{status}{count}", 240)
    title = _slice(
        exp.get("title")
        or (exp.get("analysis") or {}).get("title")
        or exp.get("brief")
        or "ExperimentTracker",
        100,
    )
    contract = exp.get("contract") or {}
    project = exp.get("project_id") or (contract.get("project") or {}).get("id") or "test-project"
    safe = "-_.!~*'()"
    return {
        "fingerprint": _stringify([body, identity]),
        "title": title,
        "body": body,
        "url": f"/?project={quote(project, safe=safe)}&experiment={quote(exp['id'], safe=safe)}",
        "alert": alert,
        "targets": [f"experiment:{exp['id']}"],
    }


def autopilot_notification(project_id, view):
    """Attention notices of a project run, for devices following its linked experiments.

    The newest unresolved researcher_input or handoff notice names the notification, so a
    repeated publication of the same notice stays silent.
    """
    if not (view.get("policy") or {}).get("notifications"):
        return None
    notices = [
        n
        for n in view.get("notices") or []
        if not n.get("resolved_at") and n.get("kind") in ("researcher_input", "handoff")
    ]
    targets = [f"experiment:{e['experiment_id']}" for e in view.get("experiments") or []]
    if not notices or not targets:
        return None
    safe = "-_.!~*'()"
    return {
        "fingerprint": _stringify(["autopilot", notices[0]["id"]]),
        "title": "Research Autopilot",
        "body": _slice(str(notices[0]["text"]), 240),
        "url": f"/?project={quote(project_id, safe=safe)}&view=memory",
        "alert": True,
        "targets": targets,
    }
