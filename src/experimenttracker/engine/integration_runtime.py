"""Periodic driver for already queued integration requests (T3.4c-3).

``integration_queue`` saves the request and ``integration_worker.drive`` runs the exact
prepare, check, approval and apply steps. The running engine supplies the rest: one
driver per repository, the exact saved branch committed through
``Workspaces.commit_branch`` (the only branch head authority), the owner's check argv in
the candidate worktree, and the branch row settled in the request's own transaction. A
merge waits for terminal completion with no agent turn or job that could move its
branch, and the saved scope with the exact pinned head is reverified before the
fast-forward; anything else keeps an explicit saved status.
"""

import logging
import subprocess
from functools import partial
from pathlib import Path

from .. import clock
from ..legacy_archive import execution_frozen
from ..workspaces import WorkspaceUnavailable, git
from . import integration
from . import integration_queue as queue
from .integration_worker import _result, drive

log = logging.getLogger(__name__)

DRIVEN = ("claimed", "ready")
WAITING = "waiting for the experiment's in-flight agent turn or job"
UNFINISHED = "waiting for the experiment to reach terminal completion"
MANUAL = "manual inspection: "
# The backend commits the branch at the end of a turn and before each submission, so a
# saved row in these states proves the eligible experiment branch could still move.
BUSY = (
    "SELECT 1 FROM agent_actions WHERE experiment_id=? "
    "AND state IN ('pending','dispatching','running','uncertain') "
    "UNION ALL SELECT 1 FROM jobs WHERE experiment_id=? AND state IN ('submitting','submitted')"
)


class _Unsafe(Exception):
    """A saved request that must not touch Git; its outcome and text become the status."""

    def __init__(self, outcome, reason):
        super().__init__(reason)
        self.outcome, self.reason = outcome, reason


def _text(value):
    return str(value)[:400]


def open_repos(store):
    """Return the canonical repositories with an open integration request, sorted.

    The queue lists no repositories, so its saved detail is read and canonicalized here.
    """
    rows = store.rows(
        "SELECT DISTINCT json_extract(detail,'$.repo') AS repo FROM obligations "
        "WHERE kind=? AND state='open'",
        (queue.SOURCE,),
    )
    repos = set()
    for row in rows:
        try:
            repos.add(queue.canonical_repo(row["repo"]))
        except ValueError:
            log.warning("integration request has no usable repository path")
    return sorted(repos)


def project_check(cwd, argv, timeout):
    """Run the owner's fixed argv in the candidate worktree, never through a shell.

    The timeout is a hard bound: an expired command is killed and raises
    ``TimeoutError``, so a hung check is saved as failed and never merges.
    """
    try:
        result = subprocess.run(
            list(argv),
            cwd=str(cwd),
            stdin=subprocess.DEVNULL,
            capture_output=True,
            text=True,
            errors="replace",
            timeout=timeout,
        )
    except subprocess.TimeoutExpired:
        raise TimeoutError("the project check timed out") from None
    output = (result.stdout or "").strip() or (result.stderr or "").strip()
    return result.returncode == 0, " ".join(output.split())[:400]


def settle_branch(store, db, view, result):
    """Settle the exact branch row inside the worker's ``queue.finish`` transaction.

    A merge settles the row ``merged`` with the applied merge commit; a revert restores
    it ``active`` without one, because the code left the base branch. Raising
    ``ValueError`` leaves the request unfinishable, so the caller keeps the candidate.
    """
    eid, branch, kind = view["experiment_id"], view["request"].get("branch"), view["candidate"].get("kind")
    row = db.execute("SELECT * FROM experiment_branches WHERE experiment_id=?", (eid,)).fetchone()
    if row is None or row["repo"] != view["repo"] or row["branch"] != branch:
        raise ValueError("the applied integration lost its saved experiment branch")
    now = clock.now()
    if kind == "merge":
        cursor = db.execute(
            "UPDATE experiment_branches SET state='merged',merged_commit=?,updated_at=? "
            "WHERE experiment_id=? AND state='active'",
            (result.get("merged_commit"), now, eid),
        )
        event = "integration_applied"
    elif kind == "revert":
        cursor = db.execute(
            "UPDATE experiment_branches SET state='active',merged_commit=NULL,updated_at=? "
            "WHERE experiment_id=? AND state='merged' AND merged_commit=?",
            (now, eid, view["request"].get("merge_commit")),
        )
        event = "integration_reverted"
    else:
        raise ValueError("the applied integration has no merge or revert kind")
    if cursor.rowcount != 1:
        raise ValueError("the saved experiment branch changed before the integration settled")
    store.emit(
        db,
        eid,
        event,
        {
            "request_id": view["request_id"],
            "kind": kind,
            "branch": branch,
            "commit": result.get("merged_commit"),
            "head_commit": view["candidate"].get("head_commit"),
        },
    )


def run(workspaces, **callbacks):
    """Advance every repository with an open request, one driver at a time."""
    return [drive_repo(workspaces, repo, **callbacks) for repo in open_repos(workspaces.store)]


def drive_repo(workspaces, repo, *, check=project_check, settle=settle_branch, apply=integration.apply):
    """Claim or continue this repository's one request and drive exactly that request.

    The worker keeps its own prepare, revert and discard transactions; this driver adds
    only the owner's check, the settle callback and the pre-apply guard.
    """
    store = workspaces.store
    with store.tx() as db:
        view = queue.active(db, repo) or queue.claim(db, repo)
    if view is None:
        return None
    if view["stage"] not in DRIVEN:
        return _result(view, "waiting", "the next responsible action is not this driver's")
    try:
        head, project = _resolve(workspaces, view)
        return drive(
            store,
            view["request_id"],
            head=head,
            project=project,
            check=check,
            apply=apply,
            settle=partial(settle, store),
            guard=_guard(store),
        )
    except _Unsafe as exc:
        return _save(store, view, exc.outcome, exc.reason)
    except (WorkspaceUnavailable, ValueError) as exc:
        return _save(store, view, "blocked", MANUAL + _text(exc))


def _resolve(workspaces, view):
    """Return ``(head, project)`` for this request, or refuse before any Git work."""
    row, project = _scope(workspaces.store, view)
    if view["request"].get("kind") == "revert" or view["candidate"] is not None:
        return None, project
    head = workspaces.commit_branch(view["experiment_id"], f"Save {row['branch']} for integration")["commit"]
    if _contained(view["repo"], head, view["request"].get("base")):
        raise _Unsafe("blocked", MANUAL + "the branch has no commits outside the base branch")
    return head, project


def _guard(store):
    """Reverify the exact scope and pinned head immediately before the fast-forward.

    The candidate is pinned, so a branch that moved meanwhile waits for inspection.
    """

    def guard(view):
        try:
            row, _project = _scope(store, view)
        except _Unsafe as exc:
            return exc.reason
        saved = row["merged_commit"] if view["request"].get("kind") == "revert" else row["head_commit"]
        if saved != view["candidate"].get("head_commit"):
            return MANUAL + "the saved experiment branch moved before the fast-forward"
        return None

    return guard


def _scope(store, view):
    """Return this request's saved branch row and worker settings, or refuse.

    It also runs right before the fast-forward, so a merge lands only for that scope.
    """
    eid, request = view["experiment_id"], view["request"]
    try:
        exp = store.experiment(eid)
    except KeyError:
        raise _Unsafe("blocked", MANUAL + "the request names no saved experiment") from None
    if request.get("experiment_id") != eid:
        raise _Unsafe("blocked", MANUAL + "the request belongs to another experiment")
    if exp["deleted_at"] or exp["phase"] == "cancelled" or execution_frozen(exp):
        raise _Unsafe("blocked", MANUAL + "the experiment can no longer be integrated")
    row = store.one("SELECT * FROM experiment_branches WHERE experiment_id=?", (eid,))
    if row is None or row["repo"] != view["repo"] or row["branch"] != request.get("branch"):
        raise _Unsafe("blocked", MANUAL + "the saved experiment branch does not match the request")
    if request.get("kind") == "merge":
        if exp["contract"].get("dont_merge"):
            raise _Unsafe("blocked", MANUAL + "the experiment is marked Don't merge")
        if row["state"] != "active":
            raise _Unsafe("blocked", MANUAL + "the saved experiment branch is not active")
        if exp["phase"] != "complete":
            raise _Unsafe("waiting", UNFINISHED)
    elif request.get("kind") == "revert":
        merged = request.get("merge_commit")
        if row["state"] != "merged" or not merged or row["merged_commit"] != merged:
            raise _Unsafe("blocked", MANUAL + "the saved integration merge does not match the revert")
    else:
        raise _Unsafe("blocked", MANUAL + "the request kind is not merge or revert")
    if store.one(BUSY, (eid, eid)):
        raise _Unsafe("waiting", WAITING)
    return row, _project(store, view)


def _project(store, view):
    """Build the worker's project settings from the owner's current declaration."""
    project = store.projects.get(view["project_id"])
    if project is None:
        raise _Unsafe("blocked", MANUAL + "the project settings are missing")
    if project.checkout is None or str(Path(project.checkout).resolve()) != view["repo"]:
        raise _Unsafe("blocked", MANUAL + "the project checkout is no longer this repository")
    return {
        "base": view["request"].get("base"),
        "check_command": project.check_command,
        "require_approval": project.integration.require_approval,
    }


def _contained(repo, head, base):
    """Whether the exact head is already in the base, so no merge commit could exist."""
    if not isinstance(base, str) or not base:
        return False
    try:
        git(repo, "merge-base", "--is-ancestor", head, f"refs/heads/{base}")
    except WorkspaceUnavailable:
        return False
    return True


def _save(store, view, outcome, reason):
    """Save one explicit status for an unsafe or deferred request, then report it."""
    reason = _text(reason)
    if view["status"] != reason:
        try:
            with store.tx() as db:
                view = queue.checkpoint(db, view["request_id"], view["stage"], status=reason)
        except ValueError as exc:
            return _result(view, "blocked", _text(exc))
    return _result(view, outcome, reason)
