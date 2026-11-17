"""Read-only adoption plans for tracker-locked detached worktrees (T3.7).

``Workspaces.validate`` owns saved checkout and repository identity. The plan adds only
settled work, detached state, branch availability and dirty-edit presence. It writes
neither Git nor SQLite; applying a plan, including an explicit current-head check for an
adopted row, is later work.
"""

from uuid import UUID

from ..workspaces import Workspaces, WorkspaceUnavailable, git

PREFIX = "exp/legacy-"
TERMINAL = ("complete", "failed", "cancelled")
UNSETTLED = ("pending", "dispatching", "running", "uncertain")


def _unsettled(store, eid):
    exp = store.one("SELECT phase FROM experiments WHERE id=?", (eid,))
    if not exp:
        return ["the experiment record is missing"]
    reasons = []
    if exp["phase"] not in TERMINAL:
        reasons.append(f"experiment phase {exp['phase']} is not terminal")
    if store.one("SELECT 1 FROM jobs WHERE experiment_id=? AND state NOT IN (?,?,?)", (eid, *TERMINAL)):
        reasons.append("saved jobs are not terminal")
    if store.one("SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN (?,?,?,?)", (eid, *UNSETTLED)):  # fmt: skip
        reasons.append("saved agent actions are not terminal")
    return reasons


def _plan(store, row):
    eid = row["experiment_id"]
    item = {"experiment_id": eid, "state": "blocked", "folder": row["folder"], "branch": PREFIX + UUID(eid).hex[:8], "dirty": None, "reasons": []}  # fmt: skip
    if row["state"] != "ready":
        item["reasons"].append(f"the saved workspace is {row['state']}, not ready")
    item["reasons"] += _unsettled(store, eid)
    try:
        # validate reads only the saved row and Git; no project registration is needed.
        Workspaces.validate(Workspaces.__new__(Workspaces), row)
        if git(row["folder"], "--no-optional-locks", "rev-parse", "--abbrev-ref", "HEAD").strip() != b"HEAD":
            item["reasons"].append("the saved checkout is not detached")
        if git(row["source"], "--no-optional-locks", "for-each-ref", "--format=%(refname)", f"refs/heads/{item['branch']}"):  # fmt: skip
            item["reasons"].append("the planned legacy branch already exists")
        item["dirty"] = bool(git(row["folder"], "--no-optional-locks", "status", "--porcelain", "-z"))
    except WorkspaceUnavailable as exc:
        item["reasons"].append(str(exc))
    if not item["reasons"]:
        item["state"] = "ready"
    return item


def plan(store):
    """Return the read-only report; no Git state or saved row changes."""
    tables = {row["name"] for row in store.rows("SELECT name FROM sqlite_master WHERE type='table'")}
    rows = store.rows("SELECT * FROM isolated_workspaces ORDER BY created_at, experiment_id") if "isolated_workspaces" in tables else []  # fmt: skip
    worktrees = [_plan(store, row) for row in rows]
    blocked = sum(item["state"] == "blocked" for item in worktrees)
    summary = {"legacy": len(worktrees), "ready": len(worktrees) - blocked, "blocked": blocked}
    return {"schema_version": 1, "apply": False, "ok": blocked == 0, "summary": summary, "worktrees": worktrees}  # fmt: skip
