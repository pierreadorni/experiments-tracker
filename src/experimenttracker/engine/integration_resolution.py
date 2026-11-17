"""Bounded conflict resolver seam and exact pinned finalization (T3.5a-b).

``integration.prepare`` leaves a conflicted ``--no-ff`` merge in the locked detached
``.worktrees/_integration`` worktree and returns ``state`` ``"conflict"`` with the
unmerged paths. A resolver edits only those paths there. ``finalize`` re-verifies that
exact saved identity, stages the permitted conflict paths literally, checks the result
and commits the merge with the saved parents, pinned under the saved candidate ref. It
returns the shape ``integration.apply`` accepts; it never applies the candidate, runs a
project check, removes the worktree or dispatches an agent.

``resolver_input`` is the bounded contract for that resolver turn: the exact saved
candidate, its conflicted paths, redacted and bounded logs for both sides and the saved
experiment summary, built without changing Git state. ``accept_resolution`` finalizes
the resolver's edits and moves the saved request back to ``claimed``, never to
``ready``, so the integration driver reruns the project check and any owner approval.
``integration_resolver`` dispatches the accepted integrator action through the existing
durable agent action, admission and restart machinery; its turn returns edits only
inside the saved worktree and records them through ``accept_resolution``.

Every refusal leaves the conflict directory and the base branch untouched. A merge
commit that was created but could not be verified or pinned stays unbound in the
worktree for inspection: it is never reset and the operation is never repeated. A
commit whose Git response was lost is reported as an unknown outcome for the same
reason, and a merge without Git's recorded automatic result (``AUTO_MERGE``) is
refused rather than judged from the working tree alone.
"""

import json
from pathlib import Path, PurePosixPath

from ..progress import redact_secrets
from ..workspaces import WorkspaceUnavailable, git
from . import integration_queue as queue
from .gitworkspace import AUTHOR, EMAIL, _commit, _main_checkout
from .integration import LOCK_PREFIX, _claim, _conflicts, _entry_at, _ref_commit, _saved

_MARKERS = (b"<<<<<<<", b"=======", b">>>>>>>")
_OTHER_OPERATIONS = ("REVERT_HEAD", "CHERRY_PICK_HEAD")
MAX_LOG_COMMITS = 20
MAX_LOG_CHARS = 1500
MAX_SUMMARY_CHARS = 1200
MAX_INPUT_BYTES = 8192
_OMITTED = "[older commits omitted]"
RESOLVED_STATUS = "resolved conflict; project check and approval required"


def _permitted(candidate):
    """The exact saved conflict paths, refused unless each one is a plain relative name."""
    conflicts = candidate.get("conflicts")
    if not isinstance(conflicts, list) or not conflicts:
        raise WorkspaceUnavailable("A saved integration conflict path list is required")
    for name in conflicts:
        if not isinstance(name, str) or not name or "\\" in name or any(ord(char) < 32 for char in name):
            raise WorkspaceUnavailable("The saved integration conflict paths are not safe")
        path = PurePosixPath(name)
        if str(path) != name or path.is_absolute() or ".." in path.parts:
            raise WorkspaceUnavailable("The saved integration conflict paths are not safe")
    if conflicts != sorted(set(conflicts)):
        raise WorkspaceUnavailable("The saved integration conflict paths are not exact")
    return conflicts


def _git_path(worktree, name):
    path = Path(git(worktree, "rev-parse", "--git-path", name).decode().strip())
    return path if path.is_absolute() else worktree / path


def _merge_head(worktree, head_commit):
    """Require the merge in progress to merge exactly the saved experiment commit."""
    path = _git_path(worktree, "MERGE_HEAD")
    try:
        merged = path.read_text().split()
    except OSError:
        raise WorkspaceUnavailable("The saved integration conflict has no merge in progress") from None
    if merged != [head_commit]:
        raise WorkspaceUnavailable("The saved integration conflict merges another commit")


def _only_conflict(worktree):
    """Refuse a worktree where another Git operation replaced the saved conflict."""
    for name in _OTHER_OPERATIONS:
        if _git_path(worktree, name).exists():
            raise WorkspaceUnavailable(
                "Another Git operation is in progress; only the saved conflict is available"
            )


def _auto_merge(worktree):
    """The merge's exact automatic result tree, refused when Git did not record it.

    ``git merge`` (ort) writes ``AUTO_MERGE`` for the merge in progress, including the
    conflict-marker blobs it materialised. Without it the staged index cannot be judged
    against the merge itself, so finalizing fails closed.
    """
    lookup = ("rev-parse", "--verify", "--end-of-options", "AUTO_MERGE^{tree}")
    try:
        return git(worktree, *lookup).decode().strip()
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable("Git did not record the merge's automatic result") from None


def _merged(worktree, permitted, auto_tree):
    """Refuse a staged change outside the permitted paths against the merge's ``AUTO_MERGE``."""
    try:
        staged = git(worktree, "write-tree").decode().strip()
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable("The staged resolution could not be read as a tree") from None
    rows = git(worktree, "diff-tree", "-r", "--no-renames", "--name-status", "-z", auto_tree, staged)
    entries = rows.decode().split("\0")
    for index in range(0, len(entries) - 1, 2):
        if entries[index] and entries[index + 1] not in permitted:
            raise WorkspaceUnavailable("The staged resolution changed a path outside the conflicted files")


def _edits(worktree, permitted, staged):
    """Refuse resolver edits outside the permitted paths and keep the index exact.

    The prepared merge already stages its clean result, so only working-tree changes
    against the index belong to the resolver.
    """
    untracked = git(worktree, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")
    if any(untracked):
        raise WorkspaceUnavailable("The resolved conflict has untracked files; remove them before finalizing")
    changed = git(worktree, "diff", "--name-only", "--no-renames", "-z").decode().split("\0")
    if staged and any(changed):
        raise WorkspaceUnavailable("The resolved conflict still has unstaged changes")
    if any(name not in permitted for name in changed if name):
        raise WorkspaceUnavailable("The resolved conflict changed a path outside the conflicted files")


def _markers(worktree, permitted):
    """Refuse a permitted path that still contains a conflict marker line.

    Refuse symlinks and paths resolving outside the worktree.
    """
    root = worktree.resolve()
    for name in permitted:
        path = worktree / name
        if path.is_symlink():
            raise WorkspaceUnavailable(f"The conflicted path {name} is a symlink")
        resolved = path.resolve()
        if not resolved.is_relative_to(root):
            raise WorkspaceUnavailable(f"The conflicted path {name} resolves outside the worktree")
        if not resolved.is_file():
            continue
        for line in resolved.read_bytes().splitlines():
            if line.startswith(_MARKERS):
                raise WorkspaceUnavailable(f"The resolved conflict still contains conflict markers in {name}")


def finalize(repo, candidate):
    """Commit the resolver's edits as the exact pinned merge candidate.

    ``candidate`` is the saved ``state`` ``"conflict"`` identity returned by
    ``integration.prepare``. The staged result is compared with that merge's own
    ``AUTO_MERGE`` tree outside the saved conflict paths, so only a resolution of those
    paths can be committed. The result is that identity with ``state`` ``"ready"``, an
    empty ``conflicts`` list and the new ``candidate_commit``, ready for
    ``integration.apply``. Revert conflicts are refused: their specialized path is not
    available yet, so they stay open in the worktree.
    """
    root = _main_checkout(repo)
    saved = _saved(root, candidate)
    if candidate.get("state") != "conflict" or saved["candidate_commit"] is not None:
        raise WorkspaceUnavailable("Only a saved integration conflict can be finalized")
    if saved["kind"] != "merge":
        raise WorkspaceUnavailable(
            "Revert conflicts need a specialized resolution path; the conflict was kept"
        )
    permitted = _permitted(candidate)
    if _ref_commit(root, saved["ref"]) is not None:
        raise WorkspaceUnavailable("A saved integration candidate already uses this key; discard it first")
    worktree = Path(candidate["worktree"])
    entry = _entry_at(root, worktree)
    if entry is None:
        raise WorkspaceUnavailable("The saved integration worktree is missing; nothing was finalized")
    ours = entry.get("locked") == f"{LOCK_PREFIX}{candidate['key']}"
    if ours and entry.get("HEAD") != saved["base_commit"]:
        raise WorkspaceUnavailable("The worktree is not at the saved conflict; an unpinned merge was kept")
    if _claim(root, worktree, candidate["key"], saved["base_commit"]) is None:
        raise WorkspaceUnavailable("The saved integration worktree is missing; nothing was finalized")
    _only_conflict(worktree)
    _merge_head(worktree, saved["head_commit"])
    auto_tree = _auto_merge(worktree)
    if any(name not in permitted for name in _conflicts(worktree)):
        raise WorkspaceUnavailable("The integration worktree no longer matches the saved conflict")
    _markers(worktree, permitted)
    _edits(worktree, permitted, staged=False)
    git(worktree, "--literal-pathspecs", "add", "-A", "--", *permitted)
    if _conflicts(worktree):
        raise WorkspaceUnavailable("The resolved conflict still has unmerged paths")
    _edits(worktree, permitted, staged=True)
    _merged(worktree, permitted, auto_tree)
    try:
        git(worktree, "diff", "--cached", "--check")
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable(
            "The resolved conflict has whitespace errors or leftover markers"
        ) from None
    message = f"Integrate {candidate['key']} ({saved['head_commit'][:12]})\n\nResolved integration conflicts."
    identity = ("-c", f"user.name={AUTHOR}", "-c", f"user.email={EMAIL}", "-c", "commit.gpgsign=false")
    try:
        git(worktree, *identity, "commit", "-m", message)
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable(
            "The merge commit outcome is unknown; inspect the worktree before retrying"
        ) from None
    commit = _commit(worktree, "HEAD")
    parents = git(root, "rev-list", "--parents", "-n", "1", commit).decode().split()[1:]
    if parents != [saved["base_commit"], saved["head_commit"]]:
        raise WorkspaceUnavailable("The resolved merge commit has other parents; it was kept unbound")
    try:
        git(root, "update-ref", "-m", "integration candidate", saved["ref"], commit, "")
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable("The merge commit could not be pinned; it was kept unbound") from None
    if _ref_commit(root, saved["ref"]) != commit:
        raise WorkspaceUnavailable("The candidate ref was taken; the merge commit was kept unbound")
    if _claim(root, worktree, candidate["key"], commit) is None:
        raise WorkspaceUnavailable("The resolved candidate worktree is missing; it was not recorded")
    return {**candidate, "candidate_commit": commit, "state": "ready", "conflicts": []}


def _redacted(value, limit):
    return redact_secrets(" ".join(str(value).split()))[:limit]


def _side_log(root, older, newer):
    """One side's commits since the other, bounded and redacted for the resolver prompt."""
    rows = git(
        root,
        "log",
        "--no-color",
        "--no-decorate",
        f"--max-count={MAX_LOG_COMMITS + 1}",
        "--format=%h %s",
        f"{older}..{newer}",
    )
    lines, size = [], 0
    for row in rows.decode("utf-8", "replace").splitlines():
        text = _redacted(row, MAX_LOG_CHARS)
        if not text:
            continue
        if len(lines) >= MAX_LOG_COMMITS or size + len(text) + 1 > MAX_LOG_CHARS:
            lines.append(text[:MAX_LOG_CHARS] if not lines else _OMITTED)
            break
        lines.append(text)
        size += len(text) + 1
    return lines


def _experiment_summary(experiment):
    """A bounded, redacted title and plan/brief summary of the saved experiment."""
    if not isinstance(experiment, dict):
        raise WorkspaceUnavailable("A saved experiment summary is required")
    snapshot = experiment.get("snapshot")
    plan = snapshot.get("plan") if isinstance(snapshot, dict) else None
    plan = plan if isinstance(plan, dict) else {}
    title = experiment.get("title") or plan.get("title")
    summary = plan.get("summary") or experiment.get("brief")
    parts = [value.strip() for value in (title, summary) if isinstance(value, str) and value.strip()]
    if not parts:
        raise WorkspaceUnavailable("A saved experiment summary is required")
    return _redacted("\n".join(parts), MAX_SUMMARY_CHARS)


def resolver_input(repo, candidate, experiment):
    """Build the bounded, redacted input for one saved integration conflict.

    The candidate must be the exact saved ``state`` ``"conflict"`` identity, still
    checked out in its locked worktree with the saved merge in progress and the same
    unmerged paths. ``base_log`` and ``head_log`` are each bounded and redacted;
    ``experiment_summary`` combines the saved title and plan/brief summary. No Git
    state is changed and no agent is dispatched.
    """
    root = _main_checkout(repo)
    saved = _saved(root, candidate)
    if candidate.get("state") != "conflict" or saved["candidate_commit"] is not None:
        raise WorkspaceUnavailable("Only a saved integration conflict can be resolved")
    if saved["kind"] != "merge":
        raise WorkspaceUnavailable(
            "Revert conflicts need a specialized resolution path; the conflict was kept"
        )
    permitted = _permitted(candidate)
    worktree = Path(candidate["worktree"])
    if _claim(root, worktree, candidate["key"], saved["base_commit"]) is None:
        raise WorkspaceUnavailable("The saved integration worktree is missing; nothing was resolved")
    _only_conflict(worktree)
    _merge_head(worktree, saved["head_commit"])
    _auto_merge(worktree)
    if _conflicts(worktree) != permitted:
        raise WorkspaceUnavailable("The integration worktree no longer matches the saved conflict")
    value = {
        "key": candidate["key"],
        "kind": saved["kind"],
        "base": saved["base"],
        "base_commit": saved["base_commit"],
        "head_commit": saved["head_commit"],
        "worktree": str(worktree),
        "ref": saved["ref"],
        "conflicts": permitted,
        "base_log": _side_log(root, saved["head_commit"], saved["base_commit"]),
        "head_log": _side_log(root, saved["base_commit"], saved["head_commit"]),
        "experiment_summary": _experiment_summary(experiment),
    }
    if len(json.dumps(value, ensure_ascii=False).encode("utf-8")) > MAX_INPUT_BYTES:
        raise WorkspaceUnavailable("The resolver input exceeds its bound")
    return value


def accept_resolution(store, request_id, candidate):
    """Finalize the saved conflict and return it to the gated check/approval path.

    The request must still be the open saved ``conflict`` with exactly ``candidate``.
    ``finalize`` pins the verified merge first; if Git refuses or its outcome is
    uncertain the queue stays at ``conflict`` for inspection. Only then does the
    request move to ``claimed`` with the finalized candidate, so the driver reruns the
    project check and any owner approval. This function never moves the request to
    ``ready`` and never applies it. If recording the transition fails after the pin,
    the exact merge stays pinned and the request stays at ``conflict`` for inspection;
    it is never finalized twice.
    """
    with store.tx() as db:
        view = queue.saved(db, request_id)
    if view is None:
        raise ValueError(f"unknown integration request: {request_id}")
    if view["state"] != "open" or view["stage"] != "conflict":
        raise ValueError("only an open saved integration conflict can be resolved")
    if view["candidate"] != candidate:
        raise ValueError("only the exact saved integration conflict can be resolved")
    ready = finalize(view["repo"], candidate)
    with store.tx() as db:
        current = queue.saved(db, request_id)
        if current is None or current["state"] != "open" or current["stage"] != "conflict":
            raise ValueError("the saved integration conflict changed before the resolution was recorded")
        return queue.checkpoint(
            db,
            request_id,
            "claimed",
            candidate=ready,
            status=RESOLVED_STATUS,
        )
