"""Named experiment branches, bounded commits and durable job source exports (T3.1).

The module owns repository-local Git state only. Callers save the identity returned by
``create`` in ``experiment_branches`` and decide when a commit, export or pin happens;
nothing here dispatches work or writes project registrations.
"""

import os
import re
import shutil
import stat
import tarfile
import tempfile
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from .. import clock
from ..workspaces import WorkspaceUnavailable, git, safe_name

BRANCH_PREFIX = "exp/"
WORKTREES = ".worktrees"
PIN_PREFIX = "refs/et/"
AUTHOR = "ExperimentTracker Agent"
EMAIL = "experimenttracker@localhost"
_SLUG = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_REF_COMPONENT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")


def _root(folder):
    """The exact repository working-tree root, never a subdirectory."""
    path = Path(folder)
    if path.is_symlink() or not path.is_dir():
        raise WorkspaceUnavailable("The Git repository is missing or its identity changed")
    try:
        top = Path(git(path, "rev-parse", "--show-toplevel").decode().strip()).resolve()
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable("The selected folder is not a Git repository") from None
    if top != path.resolve():
        raise WorkspaceUnavailable("Branch worktrees require the project's Git repository root")
    return top


def _main_checkout(repo):
    root = _root(repo)
    git_dir = Path(git(root, "rev-parse", "--absolute-git-dir").decode().strip()).resolve()
    common = Path(git(root, "rev-parse", "--git-common-dir").decode().strip())
    common = (root / common if not common.is_absolute() else common).resolve()
    if git_dir != common:
        raise WorkspaceUnavailable("Branch worktrees require the project's main checkout")
    return root


def _commit(repo, value):
    if not isinstance(value, str) or not value or value.startswith("-"):
        raise WorkspaceUnavailable("A commit identity is required")
    try:
        return git(repo, "rev-parse", "--verify", "--end-of-options", f"{value}^{{commit}}").decode().strip()
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable("The requested commit is not available in the repository") from None


def _ref_commit(repo, ref):
    rows = git(repo, "for-each-ref", "--format=%(refname) %(objectname)", ref).decode().splitlines()
    for row in rows:
        name, _, commit = row.partition(" ")
        if name == ref:
            return commit
    return None


def _worktrees(repo):
    result, path, branch = {}, None, None
    for field in git(repo, "worktree", "list", "--porcelain", "-z").decode().split("\0"):
        if not field:
            if path is not None:
                result[Path(os.path.realpath(path))] = branch
            path, branch = None, None
        elif field.startswith("worktree "):
            path = field[len("worktree ") :]
        elif field.startswith("branch "):
            branch = field[len("branch ") :]
    if path is not None:
        result[Path(os.path.realpath(path))] = branch
    return result


def _branch_at(repo, target):
    return _worktrees(repo).get(Path(os.path.realpath(target)))


def _today():
    return datetime.fromtimestamp(clock.now(), UTC).strftime("%Y%m%d")


def _exclude_worktrees(repo):
    path = Path(git(repo, "rev-parse", "--git-path", "info/exclude").decode().strip())
    path = path if path.is_absolute() else repo / path
    if path.is_symlink():
        raise WorkspaceUnavailable("The repository exclude file is a symlink; no Git state was changed")
    path.parent.mkdir(parents=True, exist_ok=True)
    content = path.read_text() if path.exists() else ""
    line = f"/{WORKTREES}/"
    if line in content.splitlines():
        return
    with path.open("a") as handle:
        if content and not content.endswith("\n"):
            handle.write("\n")
        handle.write(line + "\n")


def plan(repo, slug, base="main"):
    """Return the validated identity ``create`` would use, without changing Git state."""
    root = _main_checkout(repo)
    if not isinstance(slug, str) or not _SLUG.fullmatch(slug):
        raise WorkspaceUnavailable("Experiment slugs must be one safe path component")
    base_commit = _commit(root, base)
    return {
        "repo": str(root),
        "branch": f"{BRANCH_PREFIX}{_today()}-{slug}",
        "worktree": str(root / WORKTREES / slug),
        "base_commit": base_commit,
        "head_commit": base_commit,
    }


def verify(repo, identity):
    """Confirm a saved identity names the exact branch and worktree; never adopt."""
    root = _main_checkout(repo)
    try:
        branch = identity["branch"]
        worktree = Path(identity["worktree"])
        base_commit = identity["base_commit"]
    except (KeyError, TypeError):
        raise WorkspaceUnavailable("A saved experiment branch identity is required") from None
    if (
        not isinstance(branch, str)
        or not branch.startswith(BRANCH_PREFIX)
        or worktree.parent != root / WORKTREES
        or identity.get("repo") not in (None, str(root))
    ):
        raise WorkspaceUnavailable("The saved experiment branch identity is not safe")
    if _branch_at(root, worktree) != f"refs/heads/{branch}":
        raise WorkspaceUnavailable("The exact experiment branch worktree is missing")
    _commit(root, base_commit)
    head = git(worktree, "rev-parse", "HEAD").decode().strip()
    return {**identity, "head_commit": head}


def create(repo, slug, base="main"):
    """Create ``exp/<date>-<slug>`` at ``<repo>/.worktrees/<slug>`` from ``base``.

    Returns the exact identity to save in ``experiment_branches``. Existing branches or
    worktree paths are never adopted or overwritten.
    """
    identity = plan(repo, slug, base)
    root = Path(identity["repo"])
    branch, worktree, base_commit = identity["branch"], Path(identity["worktree"]), identity["base_commit"]
    if _ref_commit(root, f"refs/heads/{branch}") or _branch_at(root, worktree) is not None:
        raise WorkspaceUnavailable("The named experiment branch or worktree already exists")
    parent = root / WORKTREES
    if worktree.exists() or worktree.is_symlink():
        raise WorkspaceUnavailable("The intended experiment worktree path is already occupied")
    if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
        raise WorkspaceUnavailable("The experiment worktree parent is not a safe directory")
    _exclude_worktrees(root)
    parent.mkdir(parents=True, exist_ok=True)
    git(root, "worktree", "add", "-b", branch, str(worktree), base_commit)
    if _branch_at(root, worktree) != f"refs/heads/{branch}":
        raise WorkspaceUnavailable("Git did not register the exact experiment branch worktree")
    head = git(worktree, "rev-parse", "HEAD").decode().strip()
    if head != base_commit:
        raise WorkspaceUnavailable("The new experiment worktree does not start at the requested base")
    return {**identity, "head_commit": head}


def _allowed(folder, name, limit):
    path = folder / name
    return safe_name(name) and not path.is_symlink() and path.is_file() and path.stat().st_size <= limit


def _untracked(folder, limit):
    names = git(folder, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")
    safe, skipped = [], []
    for name in filter(None, names):
        (safe if _allowed(folder, name, limit) else skipped).append(name)
    return safe, skipped


def _add(folder, names):
    for start in range(0, len(names), 500):
        git(folder, "--literal-pathspecs", "add", "--", *names[start : start + 500])


def commit(worktree, message, max_untracked_mb=5):
    """Commit tracked edits and safe untracked files; return ``(head_sha, skipped)``.

    Untracked files are staged individually, so oversized files and excluded/sensitive
    paths stay untracked and are listed in ``skipped``. Tracked edits are always kept.
    """
    folder = _root(worktree)
    if not isinstance(message, str) or not message.strip():
        raise WorkspaceUnavailable("A commit message is required")
    if not isinstance(max_untracked_mb, (int, float)) or max_untracked_mb < 0:
        raise WorkspaceUnavailable("The untracked file limit must be a non-negative size in MiB")
    limit = max_untracked_mb * 1024 * 1024
    staged = git(folder, "diff", "--cached", "--no-renames", "--diff-filter=A", "--name-only", "-z")
    if any(not _allowed(folder, name, limit) for name in filter(None, staged.decode().split("\0"))):
        raise WorkspaceUnavailable("The index already contains a new file outside the commit boundary")
    safe, skipped = _untracked(folder, limit)
    git(folder, "--literal-pathspecs", "add", "-u")
    if safe:
        _add(folder, safe)
    if not any(git(folder, "diff", "--cached", "--name-only", "-z").decode().split("\0")):
        return git(folder, "rev-parse", "HEAD").decode().strip(), skipped
    git(
        folder,
        "-c",
        f"user.name={AUTHOR}",
        "-c",
        f"user.email={EMAIL}",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "--no-verify",
        "-m",
        message,
    )
    return git(folder, "rev-parse", "HEAD").decode().strip(), skipped


def _extract(archive, destination):
    with tarfile.open(archive) as bundle:
        for member in bundle:
            parts = PurePosixPath(member.name).parts
            if not parts or member.name.startswith("/") or any(part in ("", ".", "..") for part in parts):
                raise WorkspaceUnavailable("The Git archive contains an unsafe path")
            path = destination.joinpath(*parts)
            if member.isdir():
                path.mkdir(parents=True, exist_ok=True)
                path.chmod(stat.S_IMODE(member.mode) & 0o777)
            elif member.isreg():
                path.parent.mkdir(parents=True, exist_ok=True)
                source = bundle.extractfile(member)
                if source is None:
                    raise WorkspaceUnavailable("The Git archive contains an unreadable file")
                with source, path.open("wb") as handle:
                    shutil.copyfileobj(source, handle)
                path.chmod(stat.S_IMODE(member.mode) & 0o777)
            else:
                raise WorkspaceUnavailable("The Git archive contains a link or special file")


def export(repo, sha, target):
    """Extract exactly ``sha`` into an empty or absent local directory and return it.

    Extraction happens in a sibling staging directory and is renamed into place only
    after every archive member passed validation, so a failed export leaves no files.
    """
    root = _root(repo)
    commit_sha = _commit(root, sha)
    destination = Path(target).absolute()
    if destination.is_symlink():
        raise WorkspaceUnavailable("The export target cannot be a symlink")
    if destination.exists():
        if not destination.is_dir() or any(destination.iterdir()):
            raise WorkspaceUnavailable("The export target is already occupied; no files were overwritten")
    destination.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".export-", dir=destination.parent) as staging:
        archive = Path(staging) / "source.tar"
        tree = Path(staging) / "tree"
        git(root, "archive", "--format=tar", "--output", str(archive), commit_sha)
        tree.mkdir()
        _extract(archive, tree)
        os.replace(tree, destination)
    return destination


def pin(repo, sha, ref):
    """Keep ``sha`` reachable under a ``refs/et/`` ref and return that ref.

    Repeating a pin for the same commit is a no-op; an existing ref for another commit
    is a conflict, never a silent overwrite.
    """
    root = _root(repo)
    commit_sha = _commit(root, sha)
    if not isinstance(ref, str) or not ref.startswith(PIN_PREFIX):
        raise WorkspaceUnavailable("Pinned commits must use a refs/et/ ref")
    parts = ref.split("/")
    if (
        len(parts) < 3
        or any(not _REF_COMPONENT.fullmatch(part) for part in parts)
        or any(part.endswith(".lock") for part in parts)
        or ".." in ref
        or "@{" in ref
    ):
        raise WorkspaceUnavailable("The pin ref is not a safe refs/et/ name")
    current = _ref_commit(root, ref)
    if current == commit_sha:
        return ref
    if current:
        raise WorkspaceUnavailable("The pin ref already names a different commit")
    git(root, "update-ref", "-m", "pin experiment commit", ref, commit_sha, "")
    return ref
