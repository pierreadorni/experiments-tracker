"""Validate immutable C4 Jean Zay execution metadata against its job identity."""

from pathlib import Path

from ..execution_settings import ExecutionSettings


def validate_remote_route(experiment, intent_id, execution, local_run):
    """Reject route metadata that does not name this experiment's exact attempt."""
    expected = experiment["contract"].get("execution", {}).get("context")
    settings = ExecutionSettings.model_validate((execution or {}).get("settings"))
    layout = (execution or {}).get("layout") or {}
    base = layout.get("remote_base")
    run = layout.get("remote_run")
    project = layout.get("remote_project")
    local_project = layout.get("local_project")
    root = Path(local_run)
    try:
        canonical_root = Path(local_run).resolve(strict=True)
    except (OSError, RuntimeError):
        canonical_root = None
    expected_prefix = f"{base.rstrip('/')}/.experimenttracker/executions/{experiment['id']}/{intent_id}"
    if (
        expected != "jean-zay"
        or (execution or {}).get("context") != expected
        or settings.context != expected
        or layout.get("host") != settings.ssh_host
        or not isinstance(base, str)
        or not Path(base).is_absolute()
        or not isinstance(run, str)
        or run != expected_prefix + "/run"
        or not isinstance(project, str)
        or project != expected_prefix + "/project"
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
