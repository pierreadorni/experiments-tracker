"""Named branch experiments reuse one shared environment per dependency lock (T3.3b)."""

import json
import os
import sys
from pathlib import Path

import pytest
from helpers import IterationAgent, approve_plan, until
from test_branch_workspaces import create
from test_projects import register

from experimenttracker.config import EnvironmentConfig
from experimenttracker.engine import envs, gitworkspace
from experimenttracker.store import encode
from experimenttracker.workflow import Workflow
from experimenttracker.workspaces import git, initialize_project

UV = """#!/bin/sh
set -eu
printf '%s\\n' "$*" >> "$UV_LOG"
case "$1" in
  sync) target="$UV_PROJECT_ENVIRONMENT" ;;
  venv) target="$2" ;;
  *) exit 0 ;;
esac
mkdir -p "$target/bin"
printf '#!/bin/sh\\nexec "%s" "$@"\\n' "$UV_REAL_PYTHON" > "$target/bin/python"
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
    monkeypatch.setenv("UV_REAL_PYTHON", sys.executable)
    monkeypatch.setenv("PATH", f"{binary.parent}{os.pathsep}{os.environ['PATH']}")
    return log


def dependency_project(workflow, store, tmp_path, environment=None):
    source = register(workflow.config, store, tmp_path)
    initialize_project(source)
    (source / "pyproject.toml").write_text('[project]\nname = "research"\nversion = "0.1.0"\n')
    (source / "uv.lock").write_text("version = 1\n")
    (source / "shared.py").write_text("value = 1\n")
    git(source, "add", "--", "pyproject.toml", "uv.lock", "shared.py")
    git(source, "-c", "user.name=Test", "-c", "user.email=test@localhost", "commit", "-m", "Dependencies")
    if environment is not None:
        project = workflow.config.projects[-1]
        workflow.config.projects[-1] = project.model_copy(update={"environment": environment})
    workflow.projects.reload()
    return source


async def test_dependency_branch_uses_one_shared_environment(runtime, tmp_path, monkeypatch, uv):
    store, scheduler, _, old = runtime
    dependency_project(old, store, tmp_path)
    workflow = Workflow(old.config, store, scheduler, IterationAgent())
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    try:
        await approve_plan(workflow, store, eid)
        await until(workflow, store, eid, lambda e: e["phase"] == "training")
        row = workflow.workspaces.branch(eid)
        digest = envs.lock_hash(Path(row["worktree"]))
        shared = Path(row["repo"]) / envs.CACHE / digest / "bin/python"
        assert shared.is_file()
        assert uv.read_text().splitlines() == ["sync --frozen --no-install-project"]
        exp = store.experiment(eid)
        assert exp["snapshot"]["python_executable"] == str(shared)
        assert not (workflow.working_directory(exp) / ".venv").exists()
        for job in store.rows("SELECT execution_manifest FROM jobs WHERE experiment_id=?", (eid,)):
            assert json.loads(job["execution_manifest"])["python_executable"] == str(shared)
        prompt = json.loads(workflow.prompt(store.experiment(eid), "prepare").split("\nPAYLOAD:\n", 1)[1])
        assert prompt["python"] == str(shared)
    finally:
        await workflow.close()


async def test_fixed_interpreter_replaces_the_shared_cache(runtime, tmp_path, monkeypatch, uv):
    store, scheduler, _, old = runtime
    dependency_project(old, store, tmp_path, environment=EnvironmentConfig(python=Path(sys.executable)))
    workflow = Workflow(old.config, store, scheduler, IterationAgent())
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    try:
        await approve_plan(workflow, store, eid)
        await until(workflow, store, eid, lambda e: e["phase"] == "training")
        exp = store.experiment(eid)
        assert exp["snapshot"]["python_executable"] == sys.executable
        assert not (workflow.working_directory(exp) / ".venv").exists()
        assert not uv.exists()
        row = workflow.workspaces.branch(eid)
        assert not (Path(row["repo"]) / envs.CACHE).exists()
    finally:
        await workflow.close()


async def test_remote_branch_keeps_its_executor_runtime(runtime, tmp_path, monkeypatch, uv):
    store, scheduler, _, old = runtime
    dependency_project(old, store, tmp_path)
    workflow = Workflow(old.config, store, scheduler, IterationAgent())
    monkeypatch.setattr(gitworkspace, "_today", lambda: "20260101")
    eid = create(store)
    workflow.workspaces.ensure_branch(eid)
    contract = store.experiment(eid)["contract"]
    contract["execution"]["context"] = "jean-zay"
    with store.tx() as db:
        db.execute("UPDATE experiments SET contract=? WHERE id=?", (encode(contract), eid))
    row = workflow.workspaces.branch(eid)
    assert workflow.workspaces.shared_runtime(eid) is None
    assert not (Path(row["repo"]) / envs.CACHE).exists()
    assert not uv.exists()
