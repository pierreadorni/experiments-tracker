"""Adopt exact script outputs without rerunning a discussion or changing its input evidence."""

import hashlib
import os
import tempfile
from uuid import UUID

from . import artifacts, reporting
from .store import encode


class UndeclaredOutput(ValueError):
    """A known completed reply needs an output-manifest correction."""


def workspace(root, identity):
    path = root / "discussion-work" / str(UUID(identity))
    if path.parent.is_symlink() or path.is_symlink() or not path.resolve().is_relative_to(root.resolve()):
        raise ValueError("discussion workspace must be inside its experiment")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def write_once(path, data, root):
    """An interrupted copy may leave a complete file, never partially replace saved bytes."""
    if path.exists() or path.is_symlink():
        if artifacts.allowed_path(path, root).read_bytes() != data:
            raise ValueError("discussion output already exists with different bytes")
        return
    fd, pending = tempfile.mkstemp(prefix=".discussion-", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(pending, 0o444)
        os.replace(pending, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(pending):
            os.unlink(pending)


def prepare(exp, root, identity, action, reply, payload):
    """Validate declared hashes, map local references and construct reproducible provenance."""
    folder = workspace(root, identity)
    names = [item.name for item in reply.generated_artifacts]
    if len(names) != len(set(names)) or "provenance.json" in names:
        raise ValueError("discussion output names must be unique; provenance.json is reserved")
    references_used = list(reply.evidence_ids)
    if reply.presentation:
        for block in reply.presentation.blocks:
            references_used.extend(block.evidence_ids)
            if block.artifact_id:
                references_used.append(block.artifact_id)
    missing = sorted({i[10:] for i in references_used if i.startswith("generated:")} - set(names))
    if missing:
        raise UndeclaredOutput(
            "report refers to undeclared generated artifacts: "
            + ", ".join(missing)
            + ". Return generated_artifacts entries with basename, exact SHA-256 and saved source "
            "evidence_ids for every output, including the scripts and derived data. Reuse existing files; "
            "do not rerun scientific compute."
        )
    saved_ids = {a["id"] for a in payload["artifacts"]}
    files, records, references = {}, [], {}

    def record(name, data, source):
        row = {
            "id": reporting.artifact_id(exp, name),
            "name": name,
            "path": str(source),
            "sha256": hashlib.sha256(data).hexdigest(),
            "size": len(data),
            "media_type": artifacts.media_type(name),
        }
        files[name] = data
        records.append(row)
        return row

    for item in reply.generated_artifacts:
        if not set(item.evidence_ids) <= saved_ids:
            raise ValueError("generated output cites evidence outside its saved context")
        name = f"discussion-{identity}-{item.name}"
        # Once copied, use the sealed output even if temporary work files are later removed.
        target = root / name
        source = target if target.exists() or target.is_symlink() else folder / item.name
        data = artifacts.allowed_path(source, root if source == target else folder).read_bytes()
        if hashlib.sha256(data).hexdigest() != item.sha256:
            raise ValueError("discussion output differs from the saved turn's hash")
        row = record(name, data, source)
        references["generated:" + item.name] = row["id"]

    if records:
        provenance = {
            "schema_version": 1,
            "discussion_id": identity,
            "action_id": action["id"],
            "thread_id": action["thread_id"],
            "turn_id": action["turn_id"],
            "experiment_id": exp["id"],
            "evidence_revision": exp["evidence_revision"],
            "source_snapshot_sha256": exp["snapshot"]["snapshot_hash"],
            "source_artifacts": [
                {k: a[k] for k in ("id", "name", "sha256", "size")}
                for a in payload["artifacts"]
                if a["id"] in {i for item in reply.generated_artifacts for i in item.evidence_ids}
            ],
            "outputs": [{k: row[k] for k in ("id", "name", "sha256", "size")} for row in records],
        }
        name = f"discussion-{identity}-provenance.json"
        data = (encode(provenance) + "\n").encode()
        target = root / name
        source = target if target.exists() or target.is_symlink() else folder / "provenance.json"
        write_once(source, data, root)
        record(name, data, source)

    def resolve(value):
        if value and value.startswith("generated:"):
            if value not in references:
                raise ValueError("report refers to an undeclared generated artifact")
            return references[value]
        return value

    reply.evidence_ids = [resolve(i) for i in reply.evidence_ids]
    if reply.presentation:
        for block in reply.presentation.blocks:
            block.artifact_id = resolve(block.artifact_id)
            block.evidence_ids = [resolve(i) for i in block.evidence_ids]
    return records, files
