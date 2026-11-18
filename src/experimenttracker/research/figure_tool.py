"""Inert C7 ``register_figure`` persistence for one exact live research turn.

The tool seals the image as an immutable copy under the experiment root and
registers it through ``artifacts.register``, so the gateway, reports and later
readers never point at the mutable source. A stable ``request_id`` replays the
saved receipt without reading the source; a different source, caption or turn is
refused. Live and maintenance gates run before writes and again in the
registering transaction.
"""

import json
from pathlib import Path
from uuid import UUID

from pydantic import Field, field_validator

from .. import artifacts
from ..config import StrictModel
from ..core import action_obligations
from ..store import Conflict
from . import tools

EVENT_KIND = "research_figure_registered"
STAGE = "research"
MAX_PATH_CHARS = 1024
MAX_CAPTION_CHARS = 1500


class RegisterFigure(StrictModel):
    """One existing image to register; the UUID makes a replay the same figure."""

    request_id: UUID
    path: str = Field(min_length=1, max_length=MAX_PATH_CHARS)
    caption: str = Field(min_length=1, max_length=MAX_CAPTION_CHARS)

    @field_validator("path")
    @classmethod
    def bounded_path(cls, value):
        if not value.strip():
            raise ValueError("figure path must not be blank")
        if ".." in Path(value).parts:
            raise ValueError("figure path must not contain traversal")
        return value

    @field_validator("caption")
    @classmethod
    def bounded_caption(cls, value):
        if not value.strip():
            raise ValueError("figure caption must not be blank")
        return value


def _saved_experiment(db, eid):
    row = db.execute("SELECT * FROM experiments WHERE id=?", (eid,)).fetchone()
    if row is None:
        return None
    experiment = dict(row)
    experiment["contract"] = json.loads(experiment["contract"]) if experiment["contract"] else None
    experiment["pause_agent"] = bool(experiment["pause_agent"])
    return experiment


def _live_turn(workflow, db, eid, action):
    """Re-read and gate the exact live research turn inside the caller's transaction."""
    saved_action = action_obligations.expected_saved(db, action)
    saved = _saved_experiment(db, eid)
    if saved is None:
        raise ValueError("register_figure requires a saved research experiment")
    tools.require_research_write(workflow, saved, saved_action)
    if not saved_action.get("thread_id") or saved_action["thread_id"] != saved.get("thread_id"):
        raise ValueError("register_figure requires the exact saved research thread")
    return saved_action, saved


def _source_reference(worktree, output_root, value):
    """Store a relative source identity without opening the source file."""
    requested = Path(value)
    if not requested.is_absolute():
        return "worktree:" + requested.as_posix()
    for kind, root in (("worktree", worktree), ("output", output_root)):
        try:
            return f"{kind}:{requested.relative_to(root).as_posix()}"
        except ValueError:
            continue
    raise ValueError("figure must be inside the saved worktree or output root")


def _locate(worktree, output_root, value):
    """Resolve one existing image inside its saved root, applying the shared checks."""
    requested = Path(value)
    roots = (worktree, output_root) if requested.is_absolute() else (worktree,)
    path = requested if requested.is_absolute() else worktree / requested
    try:
        resolved = path.resolve(strict=True)
    except OSError as exc:
        raise ValueError(f"missing or inaccessible figure: {path.name}") from exc
    root = next((candidate for candidate in roots if resolved.is_relative_to(candidate.resolve())), None)
    if root is None:
        raise ValueError("figure must be inside the saved worktree or output root")
    path = artifacts.allowed_path(path, root)
    if not artifacts.media_type(path.relative_to(root.resolve()).as_posix()).startswith("image/"):
        raise ValueError("figure must be a registered image")
    return path


def _publish(root, source, request_id):
    """Seal the image once at its stable name; never overwrite the destination."""
    if root.is_symlink():
        raise ValueError("figure output root must not be a symlink")
    root.mkdir(parents=True, exist_ok=True)
    name = f"figure-{request_id}{source.suffix.lower()}"
    target = root / name
    with source.open("rb") as stream:
        data = stream.read(artifacts.MAX_ARTIFACT_BYTES + 1)
    if len(data) > artifacts.MAX_ARTIFACT_BYTES:
        raise ValueError("artifact exceeds 10 MiB limit")
    try:
        with target.open("xb") as stream:
            stream.write(data)
    except FileExistsError:
        if artifacts.allowed_path(target, root).read_bytes() != data:
            raise ValueError("published figure copy cannot change; choose a new request") from None
        return name
    target.chmod(0o444)
    return name


def _replay(db, request_id, eid, saved_action, source, caption):
    """Return the saved receipt only for the identical request metadata."""
    row = db.execute(
        "SELECT experiment_id,payload FROM events WHERE kind=? AND "
        "json_extract(payload,'$.request_id')=? LIMIT 1",
        (EVENT_KIND, str(request_id)),
    ).fetchone()
    if row is None:
        return None
    prior = json.loads(row["payload"])
    if (
        row["experiment_id"] != eid
        or prior.get("action_id") != saved_action["id"]
        or prior.get("source") != source
        or prior.get("caption") != caption
    ):
        raise Conflict("figure request identity already used with different metadata or scope")
    return prior


def register_figure(workflow, experiment, action, request):
    """Seal one bounded image and register its stable copy for the live turn."""
    request = RegisterFigure.model_validate(request)
    eid = experiment["id"]
    store = workflow.store
    worktree = Path(workflow.workspaces.branch_checkout(eid))
    output_root = workflow.root(eid)
    source_reference = _source_reference(worktree, output_root, request.path)
    caption = request.caption
    with store.tx() as db:
        saved_action, _ = _live_turn(workflow, db, eid, action)
        replay = _replay(db, request.request_id, eid, saved_action, source_reference, caption)
        if replay is not None:
            return replay

    source = _locate(worktree, output_root, request.path)
    name = _publish(output_root, source, request.request_id)
    with store.tx() as db:
        saved_action, saved = _live_turn(workflow, db, eid, action)
        replay = _replay(db, request.request_id, eid, saved_action, source_reference, caption)
        if replay is not None:
            return replay
        artifacts.register(store, eid, STAGE, output_root, [name], _db=db)
        artifact = db.execute(
            "SELECT id,sha256,size,media_type FROM artifacts "
            "WHERE experiment_id=? AND name=? AND evidence_revision=?",
            (eid, name, saved["evidence_revision"]),
        ).fetchone()
        payload = {
            "request_id": str(request.request_id),
            "experiment_id": eid,
            "action_id": saved_action["id"],
            "artifact_id": artifact["id"],
            "name": name,
            "caption": caption,
            "source": source_reference,
            "sha256": artifact["sha256"],
            "size": artifact["size"],
            "media_type": artifact["media_type"],
        }
        store.emit(db, eid, EVENT_KIND, payload)
        return payload
