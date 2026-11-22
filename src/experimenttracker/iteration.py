"""Version 3 execution: bounded control records, direct producers and small results.

Scientific output paths are declarations, never byte identities. All executable
sources are copied before dispatch into an attempt-private package tree; agents
may edit the stable workspace while allocations use their own copies.
"""

import stat
from pathlib import Path

from . import artifacts, legacy_archive
from .models import ExperimentResult


def enabled(exp):
    return exp["contract"].get("execution_contract") == 3


def runnable(exp):
    """The stage engine also requires that this saved format is still active."""
    return enabled(exp) and not legacy_archive.execution_frozen(exp)


def manifest_mode(manifest):
    return manifest.get("plan", {}).get("schema_version") == 3


def metadata(root, name):
    """O(path depth) lstat only. Do not resolve a foreign-host path locally."""
    from .remote_worker import checked

    root = Path(root)
    if root.is_symlink() or root.parent.is_symlink():
        raise ValueError("output root must retain its owned attempt directory")
    try:
        path = checked(root.resolve(), name)
        info = path.lstat()
    except OSError as exc:
        raise ValueError(
            f"missing or inaccessible declared output {name}; replace it or rerun only its producer"
        ) from exc
    return {
        "name": name,
        "kind": "directory" if stat.S_ISDIR(info.st_mode) else "file",
        "size": info.st_size if stat.S_ISREG(info.st_mode) else None,
        "mtime_ns": info.st_mtime_ns,
    }


def check_stage(root, stage, manifest):
    spec = next(s for s in manifest["plan"]["stages"] if s["id"] == stage)
    for name in spec["outputs"]:
        artifacts.allowed_path(root / name, root)
    if not manifest.get("remote_execution"):
        for name in spec.get("local_outputs", []):
            metadata(root, name)
    if spec.get("result_file"):
        result = ExperimentResult.model_validate(artifacts.read_json(root / spec["result_file"], root))
        if result.protocol != manifest["plan"]["protocol"]:
            raise ValueError("evaluation protocol differs from the requested protocol")
        keys = [(m.name, m.variant) for m in result.metrics]
        if len(keys) != len(set(keys)):
            raise ValueError("evaluation contains duplicate metrics")
        if set(spec.get("expected_metrics", [])) - {m.name for m in result.metrics}:
            raise ValueError("evaluation is missing expected metrics")
        if spec.get("sample_count") is not None and result.sample_count != spec["sample_count"]:
            raise ValueError("evaluation sample count differs from the requested count")
    return spec["outputs"]
