"""Drive one claimed integration request through prepare, check, approval and apply.

``integration_queue`` owns the saved stages and ``integration`` the Git transactions.
``check(cwd, argv, timeout)`` runs the argv list in the candidate worktree and returns
``(passed, summary)`` or raises ``TimeoutError``; ``approval(view, candidate)`` returns
``True`` only for a saved owner approval. The owner command also moves exactly one
saved awaiting-approval candidate to ``ready``, and a ``ready`` request is applied only
through ``integration.apply``, which revalidates the exact base branch, checkout, ref
and candidate commit, so a stale approval refuses instead of fast-forwarding. A
returned prepare must match every captured identity field, and an apply
``WorkspaceUnavailable`` is an unknown outcome: both wait and keep the exact candidate
ref/worktree for inspection, never re-preparing or re-applying. T3.4c wires commands,
settings and the runner; T3.5 wires conflict resolution. Re-preparing a checked or
conflicted candidate is left to T3.4c, which can discard and re-queue it.
"""

import hashlib
from pathlib import Path

from ..workspaces import WorkspaceUnavailable, git
from . import integration
from . import integration_queue as queue
from .gitworkspace import _commit, _main_checkout

PREPARING = "preparing"
APPLYING = "applying"
UNKNOWN_APPLY = "apply outcome unknown"
CAPTURED_FIELDS = ("repo", "key", "kind", "base", "base_commit", "head_commit", "worktree", "ref")


class _Stop(Exception):
    def __init__(self, view, outcome, reason=None):
        super().__init__(reason or outcome)
        self.view, self.outcome, self.reason = view, outcome, reason


def _text(value):
    return str(value)[:400]


def _result(view, outcome, reason=None):
    return {
        "request_id": view["request_id"],
        "stage": view["stage"],
        "outcome": outcome,
        "candidate": view["candidate"],
        "reason": reason,
    }


def _save(store, view, stage, *, candidate=None, status=None):
    status = None if status is None else _text(status)
    try:
        with store.tx() as db:
            return queue.checkpoint(db, view["request_id"], stage, candidate=candidate, status=status)
    except ValueError as exc:
        raise _Stop(view, "blocked", _text(exc)) from None


def _config(project):
    if not isinstance(project, dict):
        raise ValueError("project config data must be a mapping")
    base = project.get("base", "main")
    command = project.get("check_command")
    timeout = project.get("check_timeout", 600)
    require = project.get("require_approval", False)
    if not isinstance(base, str) or not base or base.startswith("-") or ".." in base:
        raise ValueError("the project base must be one local branch name")
    if command is not None and not (
        isinstance(command, list) and command and all(isinstance(item, str) and item for item in command)
    ):
        raise ValueError("the project check command must be a non-empty argv list")
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or timeout <= 0:
        raise ValueError("the project check timeout must be positive seconds")
    if not isinstance(require, bool):
        raise ValueError("project require_approval must be a boolean")
    return {"base": base, "check_command": command, "check_timeout": timeout, "require_approval": require}


def _request(view, head):
    kind = view["request"].get("kind", "merge")
    if kind not in ("merge", "revert"):
        raise ValueError("the integration request kind must be merge or revert")
    value = head if kind == "merge" else view["request"].get("merge_commit", head)
    if not isinstance(value, str) or not value:
        raise ValueError("an exact branch head or merge commit is required")
    return kind, _commit(view["repo"], value)


def _intent(repo, request_id, kind, head, base):
    key = "req-" + hashlib.sha256(request_id.encode()).hexdigest()[:32]
    return {
        "repo": repo,
        "key": key,
        "kind": kind,
        "base": base,
        "base_commit": _commit(_main_checkout(repo), f"refs/heads/{base}"),
        "head_commit": head,
        "worktree": str(Path(repo) / ".worktrees" / integration.WORKTREE),
        "ref": f"{integration.CANDIDATE_PREFIX}{key}",
        "candidate_commit": None,
        "state": PREPARING,
        "conflicts": [],
    }


def _prepare(repo, candidate, prepare, prepare_revert):
    build = prepare_revert if candidate["kind"] == "revert" else prepare
    result = build(repo, candidate["key"], candidate["head_commit"], candidate["base"])
    if not isinstance(result, dict) or result.get("state") not in ("ready", "conflict"):
        raise WorkspaceUnavailable("the integration prepare returned no candidate")
    if result["state"] == "ready" and not result.get("candidate_commit"):
        raise WorkspaceUnavailable("the integration prepare returned no candidate commit")
    for field in CAPTURED_FIELDS:
        if result.get(field) != candidate.get(field):
            raise WorkspaceUnavailable(f"the integration prepare changed the captured {field}")
    return result


def _exact(repo, candidate):
    worktree = Path(candidate.get("worktree") or "")
    commit = candidate.get("candidate_commit")
    if (
        candidate.get("repo") != repo
        or candidate.get("state") != "ready"
        or not isinstance(commit, str)
        or not commit
        or not isinstance(candidate.get("ref"), str)
        or not candidate["ref"].startswith(integration.CANDIDATE_PREFIX)
        or worktree != Path(repo) / ".worktrees" / integration.WORKTREE
        or worktree.is_symlink()
    ):
        raise ValueError
    try:
        ref = _commit(repo, candidate["ref"])
    except WorkspaceUnavailable:
        raise ValueError from None
    if ref != commit:
        raise ValueError
    try:
        if git(worktree, "rev-parse", "HEAD").decode().strip() != commit:
            raise ValueError
        if git(worktree, "status", "--porcelain").decode().strip():
            raise ValueError
    except WorkspaceUnavailable:
        raise ValueError from None


def _check(repo, candidate, config, check):
    try:
        _exact(repo, candidate)
    except Exception:
        return False, "the candidate worktree is not the exact clean candidate"
    command = config["check_command"]
    if command is None:
        return True, "no project check configured"
    try:
        passed, _summary = check(Path(candidate["worktree"]), command, config["check_timeout"])
    except Exception as exc:
        timeout = isinstance(exc, TimeoutError)
        return False, "the project check timed out" if timeout else "the project check could not run"
    try:
        _exact(repo, candidate)
    except Exception:
        return False, "the candidate changed during the project check"
    if passed is not True:
        return False, "the project check failed"
    return True, "the project check passed"


def _approved(view, candidate, approval):
    try:
        return approval is not None and approval(view, candidate) is True
    except Exception:
        return False


def _apply_ready(store, view, apply, discard):
    repo, candidate = view["repo"], view["candidate"]
    if (
        not isinstance(candidate, dict)
        or candidate.get("state") != "ready"
        or not candidate.get("candidate_commit")
    ):
        raise _Stop(view, "blocked", "no exact prepared integration candidate to apply")
    status = view["status"] or ""
    if status == APPLYING:
        view = _save(store, view, "ready", status=UNKNOWN_APPLY + "; inspect the exact candidate")
        status = view["status"]
    if status.startswith(UNKNOWN_APPLY):
        return _result(view, "waiting", "the earlier apply outcome needs inspection")
    view = _save(store, view, "ready", status=APPLYING)
    try:
        result = apply(repo, candidate)
    except WorkspaceUnavailable as exc:
        # A Git timeout or post-mutation lost response leaves the exact outcome unknown.
        view = _save(store, view, "ready", status=UNKNOWN_APPLY + ": " + _text(exc))
        return _result(view, "waiting", _text(exc))
    if not isinstance(result, dict) or result.get("merged_commit") != candidate["candidate_commit"]:
        view = _save(store, view, "ready", status=UNKNOWN_APPLY + ": the merge reported another commit")
        return _result(view, "waiting", "the fast-forward did not apply the exact candidate")
    try:
        with store.tx() as db:
            view = queue.finish(db, view["request_id"], "applied")
    except ValueError as exc:
        raise _Stop(view, "blocked", _text(exc)) from None
    try:
        discard(repo, candidate)
    except WorkspaceUnavailable as exc:
        return _result(view, "applied", "candidate cleanup blocked: " + _text(exc))
    return _result(view, "applied")


def _obtain(store, view, config, head, prepare, prepare_revert):
    candidate = view["candidate"]
    if candidate is not None:
        preparing = candidate.get("state") == PREPARING
        reason = "prepare outcome unknown" if preparing else "re-preparation is left to T3.4c"
        if not (view["status"] or "").startswith(reason):
            view = _save(store, view, "claimed", status=reason + "; inspect the saved candidate")
        raise _Stop(view, "waiting", view["status"])
    kind, head_commit = _request(view, head)
    candidate = _intent(view["repo"], view["request_id"], kind, head_commit, config["base"])
    _save(store, view, "claimed", candidate=candidate, status=PREPARING)
    try:
        return _prepare(view["repo"], candidate, prepare, prepare_revert)
    except WorkspaceUnavailable as exc:
        view = _save(store, view, "claimed", status="prepare outcome unknown: " + _text(exc))
        raise _Stop(view, "waiting", view["status"]) from None


def _claimed(store, view, config, head, prepare, prepare_revert, check, approval, apply, discard):
    candidate = _obtain(store, view, config, head, prepare, prepare_revert)
    if candidate.get("state") == "conflict":
        files = ", ".join(candidate.get("conflicts") or []) or "unmerged files"
        view = _save(store, view, "conflict", candidate=candidate, status="integration conflict: " + files)
        return _result(view, "conflict")
    passed, reason = _check(view["repo"], candidate, config, check)
    if not passed:
        view = _save(store, view, "check_failed", candidate=candidate, status=reason)
        return _result(view, "check_failed", reason)
    if config["require_approval"] and not _approved(view, candidate, approval):
        view = _save(store, view, "awaiting_approval", candidate=candidate, status="awaiting owner approval")
        return _result(view, "awaiting_approval")
    view = _save(store, view, "ready", candidate=candidate, status="project check passed")
    return _apply_ready(store, view, apply, discard)


def drive(
    store,
    request_id,
    *,
    head=None,
    project,
    prepare=integration.prepare,
    prepare_revert=integration.prepare_revert,
    apply=integration.apply,
    discard=integration.discard,
    check=None,
    approval=None,
):
    """Advance one saved request; ``head`` is the merge branch head and ``project``
    supplies ``base``, the ``check_command`` argv list, ``check_timeout`` and
    ``require_approval``.
    """
    with store.tx() as db:
        view = queue.saved(db, request_id)
    if view is None:
        raise ValueError(f"unknown integration request: {request_id}")
    if view["state"] != "open":
        return _result(view, "terminal", view["stage"])
    config = _config(project)
    try:
        stage = view["stage"]
        if stage in ("queued", "conflict", "check_failed"):
            return _result(view, "waiting", "the next responsible action is not this driver's")
        if stage == "awaiting_approval":
            if not _approved(view, view["candidate"], approval):
                return _result(view, "awaiting_approval", "awaiting owner approval")
            view = _save(store, view, "ready", status="owner approved")
            return _apply_ready(store, view, apply, discard)
        if stage == "ready":
            return _apply_ready(store, view, apply, discard)
        return _claimed(store, view, config, head, prepare, prepare_revert, check, approval, apply, discard)
    except _Stop as stop:
        return _result(stop.view, stop.outcome, stop.reason)
