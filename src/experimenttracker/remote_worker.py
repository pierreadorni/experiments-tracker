"""Standalone Python 3.9+ job helper. Copied to the allocation; no service or dependencies."""

import hashlib
import json
import os
import shutil
import stat
import sys
from pathlib import Path


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def digest(path):
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            result.update(block)
    return result.hexdigest()


def checked(root, name):
    path = root / name
    if not path.is_relative_to(root) or ".." in path.parts:
        raise ValueError("output outside execution directory")
    for parent in (path, *path.parents):
        if parent == root:
            break
        if parent.is_symlink():
            raise ValueError("symlink in execution evidence")
    mode = path.lstat().st_mode
    if not (stat.S_ISREG(mode) or stat.S_ISDIR(mode)):
        raise ValueError("evidence must be a regular file or directory")
    return path


def identity(root, name):
    path = checked(root, name)

    def record(file):
        checked(root, file.relative_to(root).as_posix())
        return {
            "name": file.relative_to(root).as_posix(),
            "size": file.stat().st_size,
            "sha256": digest(file),
        }

    if path.is_file():
        return record(path)
    files, directories, pending = [], [], [path]
    while pending:
        directory = pending.pop()
        directories.append(directory.relative_to(root).as_posix())
        for child in sorted(directory.iterdir()):
            checked(root, child.relative_to(root).as_posix())
            if child.is_dir():
                pending.append(child)
            else:
                files.append(record(child))
    contents = {"directories": sorted(directories), "files": sorted(files, key=lambda r: r["name"])}
    return {
        "name": name,
        "type": "directory",
        "size": sum(f["size"] for f in files),
        "sha256": hashlib.sha256(encode(contents).encode()).hexdigest(),
        **contents,
    }


def cleanup_temporary(root, manifest):
    """Delete only declared scratch paths; missing entries make replay harmless.

    No recursive content reads or hashes. rmtree does not follow child symlinks;
    declared targets and their parents must never themselves be symlinks.
    """
    names = [name for stage in manifest["plan"]["stages"] for name in stage.get("temporary_outputs", [])]
    targets = []
    for name in names:
        if not name.startswith("temporary/") or str(Path(name)) != name or ".." in Path(name).parts:
            raise ValueError("temporary outputs must be under temporary/")
        path = root / name
        for parent in (path, *path.parents):
            if parent == root:
                break
            if parent.is_symlink():
                raise ValueError("symlink in temporary cleanup path")
        if path.exists():
            checked(root, name)
            targets.append(path)
    for path in targets:
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
    return names


def iteration_operation(operation, root, stage, intent, manifest):
    """No dataset/checkpoint byte reads or directory enumeration."""
    if intent != manifest.get("intent_id"):
        raise ValueError("attempt identity mismatch")
    if operation == "verify":
        for ref in manifest.get("inputs", []):
            if ref["context"] != "jean-zay" or ref["host"] != os.environ.get("EXPERIMENT_HOST"):
                raise ValueError("input belongs to another execution host; arrange an explicit transfer")
            if not Path(ref["path"]).exists():
                raise ValueError(
                    "missing selected output " + ref["id"] + "; replace or rerun only its producer"
                )
        return
    spec = next(s for s in manifest["plan"]["stages"] if s["id"] == stage)

    def metadata(name, published=False):
        path = checked(root, name)
        info = path.lstat()
        record = {"name": name, "kind": "directory" if stat.S_ISDIR(info.st_mode) else "file"}
        if published:
            if record["kind"] != "file" or info.st_size > 10 * 1024 * 1024:
                raise ValueError("published outputs must be regular files at most 10 MiB")
            record.update(size=info.st_size, sha256=digest(path))
        return record

    receipt = {
        "schema_version": 3,
        "intent_id": intent,
        "stage": stage,
        "snapshot_hash": manifest["snapshot_hash"],
        "outputs": [metadata(n, True) for n in spec["outputs"]],
        "local_outputs": [metadata(n) for n in spec.get("local_outputs", [])],
        "provenance_bytes_read": 0,
    }
    target = root / ("remote-receipt-" + stage + "-" + intent + ".json")
    pending = target.with_suffix(".pending")
    pending.write_text(encode(receipt))
    pending.replace(target)


def main():
    operation, directory, stage, intent = sys.argv[1:]
    root = Path(directory).resolve(strict=True)
    manifest = json.loads((root / "snapshot/manifest.json").read_text())
    core = {key: value for key, value in manifest.items() if key != "snapshot_hash"}
    if hashlib.sha256(encode(core).encode()).hexdigest() != manifest["snapshot_hash"]:
        raise ValueError("remote manifest identity mismatch")
    if os.environ.get("EXPERIMENT_SNAPSHOT_HASH", manifest["snapshot_hash"]) != manifest["snapshot_hash"]:
        raise ValueError("remote manifest differs from the submitted identity")
    if operation == "cleanup":
        if intent != manifest["snapshot_hash"]:
            raise ValueError("remote cleanup snapshot identity mismatch")
        cleanup_temporary(root, manifest)
        return
    if manifest.get("plan", {}).get("schema_version") == 3:
        iteration_operation(operation, root, stage, intent, manifest)
        return
    for name, expected in manifest["files"].items():
        if digest(checked(root / "snapshot", name)) != expected:
            raise ValueError("remote source hash mismatch: " + name)
    if operation == "verify":
        # Validate retained dependency bytes on allocated compute resources, then
        # expose them without copying multi-gigabyte checkpoints between revisions.
        for dependency, reused in manifest.get("reused_stages", {}).items():
            previous = Path(reused["remote_root"]).resolve(strict=True)
            receipt = json.loads(
                (
                    previous / ("remote-receipt-" + dependency + "-" + reused["intent_id"] + ".json")
                ).read_text()
            )
            if (
                receipt["snapshot_hash"] != reused["snapshot_hash"]
                or receipt["intent_id"] != reused["intent_id"]
            ):
                raise ValueError("reused remote execution identity mismatch")
            for record in receipt["outputs"] + receipt["local_outputs"]:
                if identity(previous, record["name"]) != record:
                    raise ValueError("reused remote evidence changed")
                destination = root / record["name"]
                destination.parent.mkdir(parents=True, exist_ok=True)
                if destination.is_symlink():
                    if destination.resolve() != (previous / record["name"]).resolve():
                        raise ValueError("reused dependency link changed")
                elif destination.exists():
                    if identity(root, record["name"]) != record:
                        raise ValueError("reused dependency copy changed")
                else:
                    try:
                        destination.symlink_to(
                            previous / record["name"], target_is_directory=record.get("type") == "directory"
                        )
                    except FileExistsError:
                        if (
                            not destination.is_symlink()
                            or destination.resolve() != (previous / record["name"]).resolve()
                        ):
                            raise
        # Project files were copied from the frozen snapshot into this run's checkout.
        project = Path(os.environ["EXPERIMENT_PROJECT"])
        for name, frozen in manifest.get("project_sources", {}).items():
            if digest(checked(project, name)) != manifest["files"][frozen]:
                raise ValueError("remote project source changed: " + name)
        return
    plan = next(s for s in manifest["plan"]["stages"] if s["id"] == stage)
    outputs = [identity(root, name) for name in plan["outputs"]]
    if any(record.get("type") or record["size"] > 10 * 1024 * 1024 for record in outputs):
        raise ValueError("published evidence must be a regular file at most 10 MiB")
    local_outputs = [identity(root, name) for name in plan.get("local_outputs", [])]
    receipt = {
        "schema_version": 1,
        "stage": stage,
        "intent_id": intent,
        "snapshot_hash": manifest["snapshot_hash"],
        "outputs": outputs,
        "local_outputs": local_outputs,
    }
    value = encode(receipt)
    if len(value.encode()) > 10 * 1024 * 1024:
        raise ValueError("remote evidence receipt exceeds 10 MiB")
    target = root / ("remote-receipt-" + stage + "-" + intent + ".json")
    pending = target.with_suffix(".pending")
    pending.write_text(value)
    pending.chmod(0o444)
    pending.replace(target)


if __name__ == "__main__":
    main()
