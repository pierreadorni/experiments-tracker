"""Dry-run and explicit adoption plans for tracker-locked detached worktrees (T3.7).

``Workspaces.validate`` owns saved checkout and repository identity. The plan adds only
settled work, detached state, branch availability and dirty-edit presence. ``apply``
saves one exact branch intent, attaches it in the saved checkout in place and commits
bounded pending edits; the main checkout and the historical isolated row stay untouched.
"""

from uuid import UUID

from .. import clock
from ..workspaces import Workspaces, WorkspaceUnavailable, git
from . import gitworkspace

PREFIX = "exp/legacy-"
TERMINAL = ("complete", "failed", "cancelled")
UNSETTLED = ("pending", "dispatching", "running", "uncertain")
MESSAGE = "Adopt legacy experiment checkout"


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


def _plan(store, row, recorded):
    eid = row["experiment_id"]
    item = {"experiment_id": eid, "state": "blocked", "folder": row["folder"], "branch": PREFIX + UUID(eid).hex[:8], "dirty": None, "reasons": []}  # fmt: skip
    if row["state"] != "ready":
        item["reasons"].append(f"the saved workspace is {row['state']}, not ready")
    item["reasons"] += _unsettled(store, eid)
    if recorded and store.one("SELECT 1 FROM experiment_branches WHERE experiment_id=?", (eid,)):
        item["reasons"].append("an experiment branch identity is already saved for this workspace")
        return item
    try:
        # validate reads only the saved row and Git; no project registration is needed.
        Workspaces.validate(row)
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


def _saved(store):
    names = {row["name"] for row in store.rows("SELECT name FROM sqlite_master WHERE type='table'")}
    rows = store.rows("SELECT * FROM isolated_workspaces ORDER BY created_at, experiment_id") if "isolated_workspaces" in names else []  # fmt: skip
    return rows, "experiment_branches" in names


def plan(store):
    """Return the read-only report; no Git state or saved row changes."""
    rows, recorded = _saved(store)
    worktrees = [_plan(store, row, recorded) for row in rows]
    blocked = sum(item["state"] == "blocked" for item in worktrees)
    summary = {"legacy": len(worktrees), "ready": len(worktrees) - blocked, "blocked": blocked}
    return {"schema_version": 1, "apply": False, "ok": blocked == 0, "summary": summary, "worktrees": worktrees}  # fmt: skip


def _record(store, row, branch):
    """Save the exact branch identity before Git changes state; a saved row is never rewritten."""
    now = clock.now()
    with store.tx() as db:
        inserted = db.execute(
            "INSERT INTO experiment_branches(experiment_id,repo,branch,worktree,base_commit,head_commit,state,created_at,updated_at) "
            "VALUES(?,?,?,?,?,?,'active',?,?) ON CONFLICT(experiment_id) DO NOTHING",
            (row["experiment_id"], row["source"], branch, row["folder"], row["head"], row["head"], now, now),
        ).rowcount
    if inserted != 1:
        raise WorkspaceUnavailable(
            "The experiment branch identity is already saved; no adoption was repeated"
        )
    return store.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (row["experiment_id"],))


def _adopt(store, row, item):
    """Attach the planned branch in place, commit bounded pending edits, then confirm both."""
    if git(row["folder"], "rev-parse", "HEAD").decode().strip() != row["head"]:
        raise WorkspaceUnavailable("The detached legacy checkout HEAD changed; no adoption was recorded")
    branch = _record(store, row, item["branch"])
    git(row["folder"], "switch", "-c", branch["branch"])
    if git(row["folder"], "rev-parse", "HEAD").decode().strip() != row["head"]:
        raise WorkspaceUnavailable("The attached legacy branch does not start at its saved baseline")
    head, skipped = gitworkspace.commit(row["folder"], MESSAGE)
    Workspaces.validate(row, expected_head=head, branch=branch["branch"])
    with store.tx() as db:
        updated = db.execute(
            "UPDATE experiment_branches SET head_commit=?,updated_at=? "
            "WHERE experiment_id=? AND state='active' AND head_commit=?",
            (head, clock.now(), row["experiment_id"], branch["head_commit"]),
        ).rowcount
    if updated != 1:
        raise WorkspaceUnavailable("The saved legacy branch outcome changed; inspect it before retrying")
    item.update(state="adopted", head=head, skipped=skipped, reasons=[])


def apply(store):
    """Adopt only exact ready worktrees; every other saved state stays untouched and blocked."""
    rows, recorded = _saved(store)
    worktrees = []
    for row in rows:
        item = _plan(store, row, recorded)
        if item["state"] == "ready":
            try:
                _adopt(store, row, item)
            except WorkspaceUnavailable as exc:
                item["state"], item["reasons"] = "blocked", [str(exc)]
        worktrees.append(item)
    adopted = sum(item["state"] == "adopted" for item in worktrees)
    summary = {"legacy": len(worktrees), "adopted": adopted, "blocked": len(worktrees) - adopted}
    return {"schema_version": 1, "apply": True, "ok": adopted == len(worktrees), "summary": summary, "worktrees": worktrees}  # fmt: skip
