"""Validate immutable C4 Jean Zay execution metadata against its job identity."""

from pathlib import Path, PurePosixPath

from pydantic import ValidationError

from ..execution_settings import ExecutionSettings


def validate_remote_route(experiment, intent_id, execution, local_run):
    """Reject route metadata that does not name this experiment's exact attempt."""
    if not isinstance(execution, dict):
        raise ValueError("saved remote execution route is missing, invalid, or incompatible")
    expected = experiment["contract"].get("execution", {}).get("context")
    try:
        settings = ExecutionSettings.model_validate(execution.get("settings"))
    except (ValidationError, TypeError, ValueError) as exc:
        raise ValueError("saved remote execution route is missing, invalid, or incompatible") from exc
    layout = execution.get("layout")
    if not isinstance(layout, dict):
        raise ValueError("saved remote execution route is missing, invalid, or incompatible")
    base = layout.get("remote_base")
    run = layout.get("remote_run")
    project = layout.get("remote_project")
    local_project = layout.get("local_project")
    if not isinstance(local_run, str):
        raise ValueError("saved remote execution route is missing, invalid, or incompatible")
    root = Path(local_run)
    try:
        canonical_root = root.resolve(strict=True)
    except (OSError, RuntimeError):
        canonical_root = None
    expected_prefix = (
        f"{base.rstrip('/')}/.experimenttracker/executions/{experiment['id']}/{intent_id}"
        if isinstance(base, str)
        else ""
    )
    if (
        expected != "jean-zay"
        or (execution or {}).get("context") != expected
        or settings.context != expected
        or layout.get("host") != settings.ssh_host
        or not isinstance(base, str)
        or not Path(base).is_absolute()
        or base != settings.remote_project
        or ".." in PurePosixPath(base).parts
        or not isinstance(run, str)
        or run != expected_prefix + "/run"
        or not isinstance(project, str)
        or project != expected_prefix + "/project"
        or layout.get("local_run") != local_run
        or not isinstance(local_project, str)
        or not Path(local_project).is_absolute()
        or not root.is_absolute()
        or root.is_symlink()
        or canonical_root != root
        or root.name != intent_id
        or root.parent.name != "attempts"
        or execution.get("memory_allocation")
        != "cluster-assigned; --mem is not submitted; memory_mb_per_job is a planning estimate"
    ):
        raise ValueError("saved remote execution route is missing, invalid, or incompatible")
    return settings, layout
