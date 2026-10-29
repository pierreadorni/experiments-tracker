"""Executor-owned isolated checkouts, with an intent before Git changes state."""

import base64
import json
import os
import stat
import subprocess
import time
from pathlib import Path

from .projects import ProjectUnavailable
from .store import encode

LIMIT = 32 * 1024 * 1024
EXCLUDED = {
    ".git",
    ".experimenttracker",
    ".venv",
    "venv",
    "node_modules",
    "__pycache__",
    ".cache",
    ".pytest_cache",
    ".runtime",
    ".local",
    ".codex",
    ".aws",
    ".ssh",
    ".env",
    ".env.local",
    ".env.production",
}


class WorkspaceUnavailable(ProjectUnavailable):
    pass


def git(folder, *args):
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_TERMINAL_PROMPT="0", GIT_LFS_SKIP_SMUDGE="1")
    try:
        result = subprocess.run(
            ["git", "-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false", "-C", str(folder), *args],
            env=env,
            capture_output=True,
            timeout=60,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise WorkspaceUnavailable(
            "The local Git operation failed; inspect the saved workspace intent before retrying"
        ) from None
    if result.returncode:
        # Git stderr can contain private remote/configuration values.
        raise WorkspaceUnavailable(
            "Git could not establish the isolated checkout; inspect its saved workspace intent"
        )
    return result.stdout


def safe_name(name):
    path = Path(name)
    if path.is_absolute() or not path.parts or any(part in ("..", *EXCLUDED) for part in path.parts):
        return False
    return True


def capture(source):
    try:
        toplevel = Path(git(source, "rev-parse", "--show-toplevel").decode().strip()).resolve()
        head = git(source, "rev-parse", "HEAD").decode().strip()
    except WorkspaceUnavailable:
        raise WorkspaceUnavailable(
            "This imported project needs a Git repository with an initial commit for parallel worktree "
            "implementation. Initialize Git in the project folder and commit its source, or wait for its active runs to finish."
        ) from None
    if toplevel != source:
        raise WorkspaceUnavailable(
            "Worktree isolation requires the selected folder to be a Git repository root"
        )
    entries = git(source, "ls-files", "--stage", "-z").split(b"\0")
    for entry in filter(None, entries):
        metadata, _ = entry.split(b"\t", 1)
        if metadata.split()[0] in (b"120000", b"160000"):
            raise WorkspaceUnavailable(
                "Automatic worktrees require regular source files; resolve tracked symlinks/submodules first"
            )
    names = git(source, "diff", "--name-only", "HEAD", "-z").decode().split("\0")
    names += git(source, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")
    tracked = set(filter(None, git(source, "ls-tree", "-r", "--name-only", "-z", head).decode().split("\0")))
    if any(not safe_name(name) for name in tracked):
        raise WorkspaceUnavailable(
            "Tracked credentials/runtime folders cannot enter an isolated checkout; keep them out of Git"
        )
    # The saved commit already owns clean tracked files. Only outstanding edits
    # need durable bytes; large committed research assets need no SQLite copy.
    files, external, size = {}, [], 0
    for name in sorted(set(filter(None, names))):
        if not safe_name(name):
            continue
        path = source / name
        if any(parent.is_symlink() for parent in [path, *path.parents] if parent != source):
            raise WorkspaceUnavailable("Source symlinks cannot be copied into a writable isolated checkout")
        if not path.exists():
            files[name] = None
            continue
        info = path.stat()
        if path.is_dir() and (path / ".git").exists():
            # Git reports an untracked embedded repository as a directory. Its
            # code remains explicit read-only input; never copy Git metadata or
            # silently give the agent write access to its original checkout.
            if Path(git(path, "rev-parse", "--show-toplevel").decode().strip()).resolve() != path:
                raise WorkspaceUnavailable("An embedded repository has an invalid source identity")
            external.append(name.rstrip("/"))
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_size > LIMIT - size:
            raise WorkspaceUnavailable(
                "The source baseline exceeds the 32 MiB isolation limit or includes a special file"
            )
        content = path.read_bytes()
        size += len(content)
        if size > LIMIT:
            raise WorkspaceUnavailable("The source baseline exceeds the 32 MiB isolation limit")
        files[name] = {"bytes": base64.b64encode(content).decode(), "mode": stat.S_IMODE(info.st_mode)}
    for name, entry in files.items():
        path = source / name
        if entry is None:
            if path.exists():
                raise WorkspaceUnavailable("Project files changed while capturing their baseline")
        elif path.is_symlink() or path.read_bytes() != base64.b64decode(entry["bytes"]):
            raise WorkspaceUnavailable("Project files changed while capturing their baseline")
    current = git(source, "diff", "--name-only", "HEAD", "-z").decode().split("\0")
    current += git(source, "ls-files", "--others", "--exclude-standard", "-z").decode().split("\0")
    if {name.rstrip("/") for name in current if name and safe_name(name)} != set(files) | set(external):
        raise WorkspaceUnavailable("Project edits changed while capturing their baseline")
    # Never silently race a human edit/commit while capturing the baseline.
    if git(source, "rev-parse", "HEAD").decode().strip() != head:
        raise WorkspaceUnavailable(
            "Project HEAD changed while capturing the source baseline; try after edits settle"
        )
    return head, files, external


class Workspaces:
    def __init__(self, projects):
        self.projects, self.store = projects, projects.store

    def row(self, eid):
        return self.store.one("SELECT * FROM isolated_workspaces WHERE experiment_id=?", (eid,))

    def validate(self, row):
        target = Path(row["folder"])
        common = Path(row["common_dir"])
        if (
            common.is_symlink()
            or not common.is_dir()
            or (common.stat().st_dev, common.stat().st_ino) != (row["git_device"], row["git_inode"])
        ):
            raise WorkspaceUnavailable("The saved source Git repository identity changed")
        if target.is_symlink() or not target.is_dir():
            raise WorkspaceUnavailable("The saved isolated checkout is missing or replaced")
        if row["state"] == "ready" and (target.stat().st_dev, target.stat().st_ino) != (
            row["device"],
            row["inode"],
        ):
            raise WorkspaceUnavailable("The saved isolated checkout identity changed")
        entries = git(Path(row["source"]), "worktree", "list", "--porcelain").decode().split("\n\n")
        reason = "locked experimenttracker:" + row["experiment_id"]
        expected = "worktree " + str(target)
        if not any(expected in entry.splitlines() and reason in entry.splitlines() for entry in entries):
            raise WorkspaceUnavailable("The checkout does not match the exact saved Git worktree intent")
        actual_common = (target / git(target, "rev-parse", "--git-common-dir").decode().strip()).resolve()
        if actual_common != common:
            raise WorkspaceUnavailable("The isolated checkout belongs to a different Git repository")
        if git(target, "rev-parse", "HEAD").decode().strip() != row["head"]:
            raise WorkspaceUnavailable("The isolated checkout HEAD differs from the saved baseline")

    def checkout(self, eid):
        row = self.row(eid)
        if row:
            if row["state"] != "ready":
                raise WorkspaceUnavailable("The saved workspace creation needs reconciliation")
            self.validate(row)
            return row["folder"]
        workspace = self.projects.workspace(eid)
        return workspace["folder"] if workspace else None

    def ensure(self, eid):
        row = self.row(eid)
        if row and row["state"] == "ready":
            self.validate(row)
            return
        if not row:
            workspace = self.projects.workspace(eid)
            if not workspace:
                raise WorkspaceUnavailable("Worktree isolation requires a registered writable project")
            source = Path(workspace["folder"])
            root = Path(workspace["root"])
            head, files, external = capture(source)
            common = (source / git(source, "rev-parse", "--git-common-dir").decode().strip()).resolve()
            git_info = common.stat()
            with self.store.tx() as db:
                db.execute(
                    "INSERT INTO isolated_workspaces(experiment_id,source,folder,head,baseline,common_dir,git_device,git_inode,state,created_at) "
                    "VALUES(?,?,?,?,?,?,?,?,'pending',?)",
                    (
                        eid,
                        str(source),
                        str(root / "checkout"),
                        head,
                        encode({"files": files, "read_only_repositories": external}),
                        str(common),
                        git_info.st_dev,
                        git_info.st_ino,
                        time.time(),
                    ),
                )
                self.store.emit(
                    db, eid, "workspace_isolation_queued", {"kind": "worktree", "base_commit": head}
                )
            row = self.row(eid)
        target = Path(row["folder"])
        if row["state"] == "pending":
            if target.exists() or target.is_symlink():
                raise WorkspaceUnavailable(
                    "The intended checkout path is already occupied; no files were overwritten"
                )
            target.parent.mkdir(parents=True, exist_ok=True)
            with self.store.tx() as db:
                db.execute("UPDATE isolated_workspaces SET state='creating' WHERE experiment_id=?", (eid,))
                self.store.emit(db, eid, "workspace_creation_intent", {})
            git(
                Path(row["source"]),
                "worktree",
                "add",
                "--detach",
                "--lock",
                "--reason",
                "experimenttracker:" + eid,
                str(target),
                row["head"],
            )
        self.validate(self.row(eid))
        # A killed Git checkout may have registered its identity before writing
        # every committed file. Restore only this exact not-yet-dispatched target;
        # Git's index lock refuses concurrent checkout/reset work. Ready rows
        # return above, so agent edits are never reset.
        git(target, "reset", "--hard", row["head"])
        # Repeating an interrupted baseline overlay is deterministic. No agent
        # can dispatch until ready is committed; ready checkouts are never reset.
        baseline = json.loads(row["baseline"])
        files = baseline["files"] if "read_only_repositories" in baseline else baseline
        for name, entry in files.items():
            path = target / name
            if not safe_name(name) or any(
                part.is_symlink() for part in [path, *path.parents] if part != target
            ):
                raise WorkspaceUnavailable("The saved baseline path is unsafe")
            if entry is None:
                path.unlink(missing_ok=True)
                continue
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(base64.b64decode(entry["bytes"], validate=True))
            path.chmod(entry["mode"])
        info = target.stat()
        with self.store.tx() as db:
            db.execute(
                "UPDATE isolated_workspaces SET state='ready',device=?,inode=? WHERE experiment_id=?",
                (info.st_dev, info.st_ino, eid),
            )
            self.store.emit(db, eid, "workspace_isolated", {"kind": "worktree", "base_commit": row["head"]})


def git_available(folder):
    if not folder:
        return False
    try:
        return Path(git(folder, "rev-parse", "--show-toplevel").decode().strip()).resolve() == Path(
            folder
        ).resolve() and bool(git(folder, "rev-parse", "HEAD").strip())
    except (WorkspaceUnavailable, OSError, subprocess.TimeoutExpired):
        return False


def initialize_project(folder):
    """Only called for an exact tracker-created folder identity, never imports."""
    ignore = folder / ".gitignore"
    content = ".experimenttracker/\n.venv/\n.local/\n.env*\n__pycache__/\n.pytest_cache/\n.experimenttracker-project.json\n"
    if ignore.exists() and (ignore.is_symlink() or ignore.read_text() != content):
        raise WorkspaceUnavailable(
            "New project initialization found a different .gitignore; no files were overwritten"
        )
    ignore.write_text(content)
    if git_available(folder):
        return
    git(folder, "init", "--initial-branch=main")
    git(folder, "add", "--", ".gitignore")
    git(
        folder,
        "-c",
        "user.name=ExperimentTracker",
        "-c",
        "user.email=experimenttracker@localhost",
        "-c",
        "commit.gpgsign=false",
        "commit",
        "-m",
        "Initialize ExperimentTracker project",
    )
