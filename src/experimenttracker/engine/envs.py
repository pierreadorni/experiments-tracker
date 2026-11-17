"""One read-only Python environment per dependency lock, shared by all worktrees (T3.2).

Runtime API for the workspace integration (T3.3):

    runtime = env_for(project, worktree)   # blocks on the first uv build
    runtime.python                         # interpreter path for jobs and agents
    runtime.root                           # the environment directory, or None when fixed

`project` is a `ProjectConfig`; the cache lives at
`<project.checkout>/.experimenttracker/envs/<lock_hash>` so every worktree of that
repository reuses the same build. uv builds directly at that path, because the
console scripts it writes embed it in their shebangs, but nothing counts as ready
until a marker naming the lock hash is published last, and a failed build is removed
in full. Concurrent processes serialize on a `flock` file per lock hash; the kernel
releases it if a holder dies and the uv call it protects has its own timeout.

The cache must stay out of Git. Tracker-created projects commit the ignore rule in
their `.gitignore`; imported repositories get a `.git/info/exclude` entry.
"""

import fcntl
import hashlib
import json
import os
import shutil
import subprocess
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from .. import clock
from ..projects import ProjectUnavailable

CACHE = ".experimenttracker/envs"
# One spelling covers the tracker's committed `.gitignore` and `.git/info/exclude`.
IGNORED = ".experimenttracker"
EXCLUDE = f"/{IGNORED}/"
MARKER = ".experimenttracker-env.json"
UV_TIMEOUT_SECONDS = 1800


class EnvironmentUnavailable(ProjectUnavailable):
    pass


@dataclass(frozen=True)
class Runtime:
    """The interpreter a project worktree runs with, and the environment it came from."""

    python: Path
    root: Path | None = None
    lock_hash: str | None = None


def lock_hash(worktree):
    """Identify the dependency set of one worktree, or None when it declares none."""
    worktree = Path(worktree)
    lock = worktree / "uv.lock"
    if lock.is_file():
        return hashlib.sha256(lock.read_bytes()).hexdigest()
    # Without a uv lock, the project file and requirement lists are the whole input.
    files = [
        path
        for path in (worktree / "pyproject.toml", *sorted(worktree.glob("requirements*.txt")))
        if path.is_file()
    ]
    digest = hashlib.sha256()
    for path in files:
        digest.update(path.name.encode() + b"\0" + path.read_bytes() + b"\0")
    return digest.hexdigest() if files else None


def runtime_for(project, worktree):
    """The interpreter this project worktree would run with, without building it."""
    if project.environment.python:
        python = Path(project.environment.python)
        if not python.is_file():
            raise EnvironmentUnavailable("The configured project.environment.python interpreter is missing")
        return Runtime(python=python)
    repo = project.checkout
    if not repo:
        raise EnvironmentUnavailable("A shared environment needs a registered project folder")
    worktree = Path(worktree)
    if not worktree.is_dir():
        raise EnvironmentUnavailable("The project worktree for the shared environment is missing")
    digest = lock_hash(worktree)
    if digest is None:
        raise EnvironmentUnavailable("The project declares no uv.lock, pyproject.toml or requirements file")
    cache = Path(repo) / CACHE
    if cache.is_symlink() or cache.parent.is_symlink():
        raise EnvironmentUnavailable("The shared environment cache cannot be under a symlink")
    root = cache / digest
    return Runtime(python=root / "bin/python", root=root, lock_hash=digest)


def env_for(project, worktree):
    """Resolve the shared interpreter for one worktree.

    Blocking: a first build runs uv. Async callers must use `asyncio.to_thread`.
    """
    runtime = runtime_for(project, worktree)
    if runtime.root is None:
        return runtime
    root, digest = runtime.root, runtime.lock_hash
    worktree = Path(worktree)
    if not _ready(root, digest):
        ensure_excluded(Path(project.checkout))
        with _exclusive(Path(project.checkout) / CACHE / f"{digest}.lock"):
            if lock_hash(worktree) != digest:
                raise EnvironmentUnavailable(
                    "The project's dependency files changed while resolving its environment"
                )
            if not _ready(root, digest):
                _build(worktree, root, digest)
    return runtime


def ensure_excluded(repo):
    """Keep the environment cache out of Git for tracker-created and imported projects."""
    repo = Path(repo)
    git_dir = repo / ".git"
    if not git_dir.exists():
        return
    if not git_dir.is_dir():
        raise EnvironmentUnavailable(
            "The shared environment cache needs the project folder to be a Git repository root"
        )
    # Tracker-created projects commit the ignore rule (workspaces.initialize_project);
    # their marker file is the saved creation identity of exactly such a project.
    ignore = repo / ".gitignore"
    tracked = (repo / ".experimenttracker-project.json").is_file() and ignore.is_file()
    if tracked and IGNORED in {line.strip().strip("/") for line in ignore.read_text().splitlines()}:
        return
    with _exclusive(git_dir / "info" / "exclude") as stream:
        stream.seek(0)
        content = stream.read()
        if EXCLUDE in {line.strip() for line in content.splitlines()}:
            return
        if content and not content.endswith("\n"):
            stream.write("\n")
        stream.write(EXCLUDE + "\n")


@contextmanager
def _exclusive(path):
    """One writer at a time; the kernel drops this lock when a holder exits."""
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor = os.open(path, os.O_APPEND | os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
    with os.fdopen(descriptor, "a+", encoding="utf-8") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX)
        try:
            yield stream
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def _ready(root, digest):
    """A published environment only counts when its marker names this exact lock."""
    marker = root / MARKER
    if root.is_symlink() or not (root / "bin/python").is_file() or not marker.is_file():
        return False
    try:
        return json.loads(marker.read_text())["lock_hash"] == digest
    except (OSError, ValueError, KeyError, TypeError):
        return False


def _build(worktree, root, digest):
    """Build at the cache path and publish the ready marker last, so no partial env is ready."""
    if root.is_symlink():
        raise EnvironmentUnavailable("The shared environment cache path cannot be a symlink")
    if root.exists():
        # Not ready under the lock: a crashed or hand-damaged build of this hash.
        shutil.rmtree(root)
    try:
        if (worktree / "uv.lock").is_file():
            # The project itself stays out: the cache is shared by worktrees whose
            # checked-out source differs, and uv would pin an editable path to one.
            _uv(
                ["sync", "--frozen", "--no-install-project"],
                worktree,
                UV_PROJECT_ENVIRONMENT=str(root),
            )
        else:
            _uv(["venv", str(root)], worktree)
            for requirement in sorted(worktree.glob("requirements*.txt")):
                _uv(
                    ["pip", "install", "-r", str(requirement), "--python", str(root / "bin/python")],
                    worktree,
                )
        if not (root / "bin/python").is_file():
            raise EnvironmentUnavailable("The shared Python environment build produced no interpreter")
        if lock_hash(worktree) != digest:
            raise EnvironmentUnavailable(
                "The project's dependency files changed during its environment build"
            )
        marker = root / f"{MARKER}.partial"
        marker.write_text(json.dumps({"lock_hash": digest, "created_at": clock.now()}))
        os.replace(marker, root / MARKER)
    except BaseException:
        shutil.rmtree(root, ignore_errors=True)
        raise


def _uv(arguments, worktree, **environment):
    # uv output can echo private index URLs, so failures stay terse like Git's.
    try:
        result = subprocess.run(
            ["uv", *arguments],
            cwd=str(worktree),
            env={**os.environ, **environment},
            capture_output=True,
            timeout=UV_TIMEOUT_SECONDS,
        )
    except (OSError, subprocess.TimeoutExpired):
        raise EnvironmentUnavailable("The shared Python environment build could not run uv") from None
    if result.returncode:
        raise EnvironmentUnavailable("uv could not build the shared Python environment from this lock")
