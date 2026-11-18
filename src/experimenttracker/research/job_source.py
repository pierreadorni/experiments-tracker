"""Bounded local preparation of an approved research job source."""

import hashlib
import shlex
from pathlib import Path

from pydantic import Field

from ..config import StrictModel
from ..engine import gitworkspace
from ..workspaces import WorkspaceUnavailable
from .admission import preflight
from .job_intent import PreparedJob


class PreparedSource(StrictModel):
    prepared: PreparedJob
    root: Path
    source: Path
    script: Path
    commit: str = Field(pattern=r"^[0-9a-f]{40}$")


def prepare(workflow, experiment_id, action, request):
    """Commit, pin and export one approved source and create its argv-safe script."""
    request = preflight(workflow, experiment_id, action, request)
    intent = str(request.request_id)
    # Admission precedes all filesystem/Git effects, including path inspection.
    root = Path(workflow.projects.base_root(experiment_id)) / "attempts" / intent
    if root.exists() or root.is_symlink():
        raise WorkspaceUnavailable("The job preparation root is already occupied")
    branch = workflow.workspaces.branch(experiment_id)
    if not branch:
        raise WorkspaceUnavailable("The named research worktree is unavailable")
    worktree = workflow.workspaces.branch_checkout(experiment_id)
    if Path(branch["worktree"]) != Path(worktree):
        raise WorkspaceUnavailable("The named research worktree is unavailable")
    root.parent.mkdir(parents=True, exist_ok=True)
    if root.parent.resolve() != root.parent:
        raise WorkspaceUnavailable("The job preparation parent is not the exact saved path")
    root.mkdir(mode=0o700)
    committed = workflow.workspaces.commit_branch(experiment_id, "Prepare research job " + intent)
    commit = committed["commit"]
    workflow.workspaces.pin_job_commit(experiment_id, intent, commit)
    source = root / "source"
    script = root / "job.sh"
    gitworkspace.export(branch["repo"], commit, source)
    exports = {
        "EXPERIMENT_ROOT": str(root),
        "EXPERIMENT_SOURCE": str(source),
        "OMP_NUM_THREADS": str(request.resources.cpus_per_job),
        "OPENBLAS_NUM_THREADS": str(request.resources.cpus_per_job),
        "MKL_NUM_THREADS": str(request.resources.cpus_per_job),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONUNBUFFERED": "1",
    }
    body = (
        "#!/bin/sh\nset -eu\n"
        + "".join(f"export {name}={shlex.quote(value)}\n" for name, value in exports.items())
        + f"cd {shlex.quote(str(source))}\n"
        + "exec "
        + shlex.join(request.command)
        + "\n"
    ).encode()
    with script.open("xb") as stream:
        stream.write(body)
    script.chmod(0o444)
    digest = hashlib.sha256(body).hexdigest()
    return PreparedSource(
        prepared=PreparedJob(commit=commit, script_hash=digest),
        root=root,
        source=source,
        script=script,
        commit=commit,
    )
