"""The shared environment is one build per dependency lock, shared by every worktree."""

import json
import os
import subprocess
from concurrent.futures import ThreadPoolExecutor

import pytest

from experimenttracker.config import EnvironmentConfig, ProjectConfig
from experimenttracker.engine.envs import (
    CACHE,
    EXCLUDE,
    MARKER,
    EnvironmentUnavailable,
    env_for,
    lock_hash,
)

# A stand-in for uv: it records each call and publishes the interpreter the real
# tool would create, so no test needs a network install.
UV = """#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$UV_LOG"
[ -z "${UV_FAIL:-}" ] || exit 1
sleep "${UV_SLEEP:-0}"
case "$1" in
  sync) target="$UV_PROJECT_ENVIRONMENT" ;;
  venv) target="$2" ;;
  *) exit 0 ;;
esac
mkdir -p "$target/bin"
: > "$target/bin/python"
chmod +x "$target/bin/python"
"""


@pytest.fixture
def uv(tmp_path, monkeypatch):
    binary = tmp_path / "stub" / "uv"
    binary.parent.mkdir()
    binary.write_text(UV)
    binary.chmod(0o755)
    log = tmp_path / "uv.log"
    monkeypatch.setenv("UV_LOG", str(log))
    monkeypatch.setenv("PATH", f"{binary.parent}{os.pathsep}{os.environ['PATH']}")
    for name in ("UV_FAIL", "UV_SLEEP", "UV_PROJECT_ENVIRONMENT"):
        monkeypatch.delenv(name, raising=False)
    return log


def calls(log):
    return log.read_text().splitlines() if log.exists() else []


def git(folder, *args):
    result = subprocess.run(["git", "-C", str(folder), *args], capture_output=True, check=True)
    return result.stdout


def project(tmp_path, *, tracker_created=False, environment=None):
    repo = tmp_path / "research"
    repo.mkdir()
    (repo / "pyproject.toml").write_text('[project]\nname = "research"\nversion = "0.1.0"\n')
    (repo / "uv.lock").write_text("version = 1\n")
    tracked = ["pyproject.toml", "uv.lock"]
    if tracker_created:
        # Exactly what a tracker-created project folder carries: the creation
        # marker and the committed ignore rule.
        (repo / ".experimenttracker-project.json").write_text('{"request_id": "abc"}\n')
        (repo / ".gitignore").write_text(".experimenttracker/\n.experimenttracker-project.json\n")
        tracked.append(".gitignore")
    git(repo, "init", "--initial-branch=main")
    git(repo, "add", "--", *tracked)
    git(repo, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Source")
    return ProjectConfig(
        id="research",
        name="Research",
        checkout=repo,
        environment=environment or EnvironmentConfig(),
    )


def worktree(project_, name):
    target = project_.checkout.parent / name
    git(project_.checkout, "worktree", "add", "-b", f"exp/{name}", str(target), "main")
    return target


def test_worktrees_with_the_same_lock_share_one_environment(tmp_path, uv):
    research = project(tmp_path)
    first, second = worktree(research, "one"), worktree(research, "two")

    assert lock_hash(first) == lock_hash(second)
    one, two = env_for(research, first), env_for(research, second)

    assert one == two
    assert one.root == research.checkout / CACHE / lock_hash(first)
    assert one.python.is_file()
    assert calls(uv) == ["sync --frozen --no-install-project"]


def test_a_changed_lock_builds_a_second_environment(tmp_path, uv):
    research = project(tmp_path)
    first, second = worktree(research, "one"), worktree(research, "two")
    one = env_for(research, first)

    (second / "uv.lock").write_text("version = 2\n")
    two = env_for(research, second)

    assert one.root != two.root
    assert two.python.is_file()
    assert len(calls(uv)) == 2


def test_imported_project_excludes_the_cache_once(tmp_path, uv):
    research = project(tmp_path)
    checkout = worktree(research, "one")
    exclude = research.checkout / ".git" / "info" / "exclude"

    env_for(research, checkout)
    env_for(research, checkout)

    assert exclude.read_text().splitlines().count(EXCLUDE) == 1
    assert git(research.checkout, "status", "--porcelain", "--untracked-files=all") == b""
    assert len(calls(uv)) == 1


def test_tracker_created_project_relies_on_its_committed_ignore_rule(tmp_path, uv):
    research = project(tmp_path, tracker_created=True)
    checkout = worktree(research, "one")

    env_for(research, checkout)

    exclude = research.checkout / ".git" / "info" / "exclude"
    assert not exclude.exists() or EXCLUDE not in exclude.read_text()
    assert git(research.checkout, "status", "--porcelain", "--untracked-files=all") == b""


def test_fixed_project_interpreter_skips_the_shared_cache(tmp_path, uv):
    interpreter = tmp_path / "conda" / "bin" / "python"
    interpreter.parent.mkdir(parents=True)
    interpreter.write_text("")
    research = project(tmp_path, environment=EnvironmentConfig(python=interpreter))

    runtime = env_for(research, worktree(research, "one"))

    assert runtime.python == interpreter
    assert runtime.root is None and runtime.lock_hash is None
    assert not (research.checkout / CACHE).exists()
    assert calls(uv) == []


def test_a_missing_fixed_interpreter_is_refused(tmp_path, uv):
    research = project(tmp_path, environment=EnvironmentConfig(python=tmp_path / "missing" / "python"))

    with pytest.raises(EnvironmentUnavailable):
        env_for(research, worktree(research, "one"))


def test_a_project_without_dependency_files_has_no_shared_environment(tmp_path, uv):
    empty = tmp_path / "empty"
    empty.mkdir()
    research = ProjectConfig(id="research", name="Research", checkout=empty)

    assert lock_hash(empty) is None
    with pytest.raises(EnvironmentUnavailable):
        env_for(research, empty)


def test_a_failed_or_partial_build_is_never_ready(tmp_path, uv, monkeypatch):
    research = project(tmp_path)
    checkout = worktree(research, "one")
    digest = lock_hash(checkout)
    cache = research.checkout / CACHE

    monkeypatch.setenv("UV_FAIL", "1")
    with pytest.raises(EnvironmentUnavailable):
        env_for(research, checkout)
    monkeypatch.delenv("UV_FAIL")
    assert not (cache / digest).exists()

    # A directory that exists without the published marker is a damaged build.
    (cache / digest / "bin").mkdir(parents=True)
    (cache / digest / "bin" / "python").write_text("")
    runtime = env_for(research, checkout)

    assert runtime.python.is_file()
    assert len(calls(uv)) == 2
    assert json.loads((cache / digest / MARKER).read_text())["lock_hash"] == digest


def test_symlink_cache_refuses_to_write_outside_the_project(tmp_path, uv):
    research = project(tmp_path)
    checkout = worktree(research, "one")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    (research.checkout / ".experimenttracker").symlink_to(elsewhere, target_is_directory=True)

    with pytest.raises(EnvironmentUnavailable, match="symlink"):
        env_for(research, checkout)
    assert calls(uv) == [] and not list(elsewhere.iterdir())


def test_concurrent_worktrees_build_the_shared_environment_once(tmp_path, uv, monkeypatch):
    research = project(tmp_path)
    worktrees = [worktree(research, f"wt{index}") for index in range(2)]
    monkeypatch.setenv("UV_SLEEP", "0.3")

    with ThreadPoolExecutor(max_workers=2) as pool:
        runtimes = [future.result() for future in [pool.submit(env_for, research, wt) for wt in worktrees]]

    assert runtimes[0] == runtimes[1]
    assert calls(uv) == ["sync --frozen --no-install-project"]
