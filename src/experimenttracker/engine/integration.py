"""Integration candidates and clean fast-forwards (T3.4a).

Repository-local Git transactions only: prepare a ``--no-ff`` merge or a
``git revert -m 1`` candidate in the dedicated ``.worktrees/_integration`` worktree at
a captured exact base commit, keep it reachable under ``refs/et/candidates/<key>``,
and apply it to the base branch with an exact clean fast-forward. The base branch is
never moved to build a candidate. Callers own serialization, project checks, approval
and saved records (T3.4b); nothing here dispatches work, runs commands or writes the
database.
"""

import re
from pathlib import Path

from ..workspaces import WorkspaceUnavailable, git
from .gitworkspace import (
    AUTHOR,
    EMAIL,
    WORKTREES,
    _commit,
    _exclude_worktrees,
    _main_checkout,
    _ref_commit,
)

WORKTREE = "_integration"
CANDIDATE_PREFIX = "refs/et/candidates/"
LOCK_PREFIX = "experimenttracker:integration:"
_KEY = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


def _key(value):
    if not isinstance(value, str) or not _KEY.fullmatch(value):
        raise WorkspaceUnavailable("Integration candidates need one safe key")
    return value


def _base_commit(repo, base):
    if not isinstance(base, str) or not base or base.startswith("-") or ".." in base:
        raise WorkspaceUnavailable("A local base branch name is required")
    return _commit(repo, f"refs/heads/{base}")


def _identity(repo, key, kind, base, base_commit):
    return {
        "repo": str(repo),
        "key": key,
        "kind": kind,
        "base": base,
        "base_commit": base_commit,
        "worktree": str(Path(repo) / WORKTREES / WORKTREE),
        "ref": f"{CANDIDATE_PREFIX}{key}",
    }


def _conflicts(worktree):
    names = git(worktree, "diff", "--name-only", "--diff-filter=U", "-z").decode().split("\0")
    return sorted(filter(None, names))


def _entries(root):
    """Parse ``git worktree list --porcelain -z`` records, including lock reasons."""
    entries, entry = [], {}
    for field in git(root, "worktree", "list", "--porcelain", "-z").decode().split("\0"):
        if not field:
            if entry:
                entries.append(entry)
            entry = {}
            continue
        name, _, value = field.partition(" ")
        entry[name] = value
    if entry:
        entries.append(entry)
    return entries


def _entry_at(root, worktree):
    for entry in _entries(root):
        if entry["worktree"] == str(worktree):
            return entry
    return None


def _claim(root, worktree, key, head):
    """Return this candidate's exact locked detached worktree entry, or refuse."""
    if worktree.is_symlink() or (root / WORKTREES).is_symlink():
        raise WorkspaceUnavailable("The integration worktree path is unsafe; nothing was removed")
    entry = _entry_at(root, worktree)
    if entry is None:
        return None
    if (
        "branch" in entry
        or "detached" not in entry
        or entry.get("HEAD") != head
        or entry.get("locked") != f"{LOCK_PREFIX}{key}"
    ):
        raise WorkspaceUnavailable("The integration worktree does not belong to this candidate")
    return entry


def _saved(root, candidate):
    """Validate a saved candidate identity and resolve its exact commits."""
    try:
        key = _key(candidate["key"])
        kind, base = candidate["kind"], candidate["base"]
        base_commit = _commit(root, candidate["base_commit"])
        head_commit = _commit(root, candidate["head_commit"])
        worktree = Path(candidate["worktree"])
        commit = _commit(root, candidate["candidate_commit"]) if candidate["candidate_commit"] else None
    except (KeyError, TypeError):
        raise WorkspaceUnavailable("A saved integration candidate identity is required") from None
    if (
        candidate.get("repo") not in (None, str(root))
        or kind not in ("merge", "revert")
        or candidate.get("ref") != f"{CANDIDATE_PREFIX}{key}"
        or worktree.parent != root / WORKTREES
        or worktree.name != WORKTREE
    ):
        raise WorkspaceUnavailable("The saved integration candidate identity is not safe")
    return {
        "kind": kind,
        "base": base,
        "base_commit": base_commit,
        "head_commit": head_commit,
        "ref": candidate["ref"],
        "candidate_commit": commit,
    }


def _start(root, identity, head, command, parents):
    """Create the dedicated worktree at the captured base and run one merge/revert."""
    worktree = Path(identity["worktree"])
    if _entry_at(root, worktree) is not None:
        raise WorkspaceUnavailable("The integration worktree is already registered; discard it first")
    if _ref_commit(root, identity["ref"]) is not None:
        raise WorkspaceUnavailable("A saved integration candidate already uses this key; discard it first")
    parent = worktree.parent
    if worktree.exists() or worktree.is_symlink():
        raise WorkspaceUnavailable("The integration worktree path is occupied; no files were overwritten")
    if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
        raise WorkspaceUnavailable("The integration worktree parent is not a safe directory")
    _exclude_worktrees(root)
    parent.mkdir(parents=True, exist_ok=True)
    path, reason = str(worktree), f"{LOCK_PREFIX}{identity['key']}"
    git(root, "worktree", "add", "--detach", "--lock", "--reason", reason, path, identity["base_commit"])
    if _claim(root, worktree, identity["key"], identity["base_commit"]) is None:
        raise WorkspaceUnavailable("Git did not create the exact locked integration worktree")
    try:
        git(
            worktree,
            "-c",
            f"user.name={AUTHOR}",
            "-c",
            f"user.email={EMAIL}",
            "-c",
            "commit.gpgsign=false",
            *command,
        )
    except WorkspaceUnavailable:
        conflicts = _conflicts(worktree)
        if not conflicts:
            raise WorkspaceUnavailable(
                "The integration candidate could not be prepared; no base branch was changed"
            ) from None
        return {
            **identity,
            "head_commit": head,
            "candidate_commit": None,
            "state": "conflict",
            "conflicts": conflicts,
        }
    commit = _commit(worktree, "HEAD")
    actual = git(worktree, "rev-list", "--parents", "-n", "1", commit).decode().split()[1:]
    if actual != parents:
        raise WorkspaceUnavailable("The integration candidate does not merge the exact captured commits")
    git(root, "update-ref", "-m", "integration candidate", identity["ref"], commit, "")
    # A base move after this pin is not an error here: apply rejects the stale base and
    # the caller still holds the exact identity, so it can discard and prepare again.
    return {**identity, "head_commit": head, "candidate_commit": commit, "state": "ready", "conflicts": []}


def prepare(repo, key, head, base="main"):
    """Prepare a ``--no-ff`` merge candidate of exactly ``head`` onto captured ``base``.

    Returns the exact candidate identity, or the same identity with ``state``
    ``"conflict"`` and the unmerged paths preserved in the integration worktree.
    """
    root = _main_checkout(repo)
    head_commit = _commit(root, head)
    base_commit = _base_commit(root, base)
    identity = _identity(root, _key(key), "merge", base, base_commit)
    message = f"Integrate {identity['key']} ({head_commit[:12]})"
    return _start(
        root,
        identity,
        head_commit,
        ["merge", "--no-ff", "-m", message, head_commit],
        [base_commit, head_commit],
    )


def prepare_revert(repo, key, merge_commit, base="main"):
    """Prepare a ``git revert -m 1`` candidate for an applied integration merge."""
    root = _main_checkout(repo)
    merged = _commit(root, merge_commit)
    base_commit = _base_commit(root, base)
    if len(git(root, "rev-list", "--parents", "-n", "1", merged).decode().split()) != 3:
        raise WorkspaceUnavailable("Only an integration merge commit can be reverted")
    try:
        git(root, "merge-base", "--is-ancestor", merged, base_commit)
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable("The integration merge is not part of the base branch") from None
    identity = _identity(root, _key(key), "revert", base, base_commit)
    return _start(root, identity, merged, ["revert", "-m", "1", "--no-edit", merged], [base_commit])


def apply(repo, candidate):
    """Fast-forward the base branch to a prepared candidate after exact verification."""
    root = _main_checkout(repo)
    saved = _saved(root, candidate)
    if candidate.get("state") != "ready" or saved["candidate_commit"] is None:
        raise WorkspaceUnavailable("Unresolved integration conflicts must be resolved before applying")
    if _base_commit(root, saved["base"]) != saved["base_commit"]:
        raise WorkspaceUnavailable("The base branch moved; prepare a fresh integration candidate")
    try:
        branch = git(root, "symbolic-ref", "--short", "HEAD").decode().strip()
    except WorkspaceUnavailable:
        branch = None
    if branch != saved["base"]:
        raise WorkspaceUnavailable("The project checkout is not on the integration base branch")
    if git(root, "status", "--porcelain", "--untracked-files=normal").decode().strip():
        raise WorkspaceUnavailable("The project checkout has uncommitted changes; commit or stash them first")
    if _ref_commit(root, saved["ref"]) != saved["candidate_commit"]:
        raise WorkspaceUnavailable("The saved integration candidate is missing or changed")
    parents = git(root, "rev-list", "--parents", "-n", "1", saved["candidate_commit"]).decode().split()[1:]
    expected = [saved["base_commit"], saved["head_commit"]]
    if parents != (expected if saved["kind"] == "merge" else expected[:1]):
        raise WorkspaceUnavailable("The saved integration candidate no longer matches the captured commits")
    git(root, "merge", "--ff-only", saved["candidate_commit"])
    if _commit(root, "HEAD") != saved["candidate_commit"]:
        raise WorkspaceUnavailable("The fast-forward did not apply the exact integration candidate")
    return {
        **candidate,
        "previous_base_commit": saved["base_commit"],
        "merged_commit": saved["candidate_commit"],
    }


def discard(repo, candidate):
    """Remove only this candidate's exact locked worktree and private ref, or refuse."""
    root = _main_checkout(repo)
    saved = _saved(root, candidate)
    worktree = Path(candidate["worktree"])
    expected = saved["candidate_commit"] or saved["base_commit"]
    entry = _claim(root, worktree, candidate["key"], expected)
    current = _ref_commit(root, saved["ref"])
    if current is not None and current != saved["candidate_commit"]:
        raise WorkspaceUnavailable("The saved candidate ref names another commit; nothing was removed")
    if entry is not None:
        git(root, "worktree", "remove", "--force", "--force", str(worktree))
    if current is not None:
        try:
            git(root, "update-ref", "-d", saved["ref"], current)
        except WorkspaceUnavailable:
            raise WorkspaceUnavailable("The saved candidate ref changed; it was kept") from None
    return {**candidate, "state": "discarded"}
