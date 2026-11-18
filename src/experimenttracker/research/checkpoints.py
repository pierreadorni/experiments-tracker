"""C7 research checkpoint: one private, versioned notebook saved per turn.

The checkpoint is the agent-authored Markdown that seeds the next turn when the
session rotates. It is saved only in the event payload of the live contract 4
research turn that owns the action, inside the same transaction that verifies that
action's exact saved identity, thread and not-paused experiment. The controller
calls the tool from the pool slot that owns the saved action; these checks repeat
that ownership before a replay or a write. ``Store.public_projection`` drops the
Markdown from every published view, so the bytes stay on the executor's local disk
while the version and request identity remain visible. No table, migration,
configuration knob or hold reason is added.

A stable ``request_id`` makes the tool idempotent: replaying the identical request
returns the saved checkpoint without a second event, while reusing the identity for
different bytes or a different action is refused.
"""

import json
from uuid import UUID

from pydantic import field_validator

from ..codex_client import NeedsAttention
from ..config import StrictModel
from ..core import action_obligations
from ..store import Conflict
from .prompt import MAX_CHECKPOINT_BYTES

EVENT_KIND = "research_checkpoint_saved"
PHASE = "research"
_OWNED_STATES = frozenset({"dispatching", "running"})
_TERMINAL_PHASES = frozenset({"complete", "failed", "cancelled"})


class SaveCheckpoint(StrictModel):
    """One ``save_checkpoint`` tool request: stable identity plus bounded Markdown."""

    request_id: UUID
    markdown: str

    @field_validator("markdown")
    @classmethod
    def bounded(cls, value):
        if not value.strip():
            raise ValueError("checkpoint Markdown must not be blank")
        if len(value.encode("utf-8")) > MAX_CHECKPOINT_BYTES:
            raise ValueError(f"checkpoint Markdown must be at most {MAX_CHECKPOINT_BYTES} UTF-8 bytes")
        return value


def _owned_experiment(db, action):
    """Re-verify the live contract 4 turn that owns this checkpoint write.

    The outer pool gate offers a research tool only to the slot holding the exact
    saved action; this transaction repeats the live, unpaused and exact-thread
    checks so a later replay or write can never outlive that ownership.
    """
    row = db.execute("SELECT * FROM experiments WHERE id=?", (action["experiment_id"],)).fetchone()
    if row is None:
        raise NeedsAttention("a checkpoint needs a saved experiment")
    experiment = dict(row)
    if experiment["deleted_at"] is not None or experiment["phase"] in _TERMINAL_PHASES:
        raise NeedsAttention("a checkpoint needs a live experiment")
    if json.loads(experiment["contract"] or "{}").get("execution_contract") != 4:
        raise NeedsAttention("a checkpoint needs a contract 4 research experiment")
    if experiment["pause_agent"]:
        raise NeedsAttention("a paused experiment cannot save a checkpoint")
    if not action["thread_id"] or action["thread_id"] != experiment["thread_id"]:
        raise NeedsAttention("a checkpoint needs the exact saved research thread")
    return experiment


def _scope(action):
    return action["experiment_id"], action["id"]


def save_checkpoint(store, action, request):
    """Save one versioned checkpoint for an active contract 4 research action.

    ``action`` is the saved action whose pool slot is serving the turn. It is
    re-read inside the transaction and must still match its snapshot, remain a
    ``dispatching``/``running`` ``research`` row, and belong to a live unpaused
    contract 4 experiment with the same saved thread; otherwise nothing is written
    and a replay is refused too. Replaying the same ``request_id`` with identical
    Markdown and scope returns the saved payload, while reusing it for different
    bytes or scope raises ``Conflict``.
    """
    if not isinstance(request, SaveCheckpoint):
        raise TypeError("a save request must be a SaveCheckpoint")
    if not isinstance(action, dict) and not hasattr(action, "keys"):
        raise TypeError("a checkpoint needs the saved action")
    markdown = request.markdown
    request_id = str(request.request_id)
    with store.tx() as db:
        saved = action_obligations.expected_saved(db, action)
        if saved["phase"] != PHASE or saved["state"] not in _OWNED_STATES:
            raise NeedsAttention("a checkpoint needs the research turn that owns the action")
        _owned_experiment(db, saved)
        experiment_id, action_id = _scope(saved)
        prior_row = db.execute(
            "SELECT experiment_id,payload FROM events WHERE kind=? AND "
            "json_extract(payload,'$.request_id')=? LIMIT 1",
            (EVENT_KIND, request_id),
        ).fetchone()
        if prior_row is not None:
            prior = json.loads(prior_row["payload"])
            if (
                prior_row["experiment_id"] != experiment_id
                or prior.get("action_id") != action_id
                or prior.get("markdown") != markdown
            ):
                raise Conflict("checkpoint request identity already used with different content or scope")
            return prior
        version = (
            db.execute(
                "SELECT MAX(CAST(json_extract(payload,'$.version') AS INTEGER)) "
                "FROM events WHERE experiment_id=? AND kind=?",
                (experiment_id, EVENT_KIND),
            ).fetchone()[0]
            or 0
        ) + 1
        payload = {
            "request_id": request_id,
            "experiment_id": experiment_id,
            "action_id": action_id,
            "version": version,
            "bytes": len(markdown.encode("utf-8")),
            "markdown": markdown,
        }
        store.emit(db, experiment_id, EVENT_KIND, payload)
        return payload


def latest_checkpoint(store, experiment_id):
    """Return the newest saved checkpoint payload for an experiment, or ``None``."""
    row = store.one(
        "SELECT payload FROM events WHERE experiment_id=? AND kind=? ORDER BY seq DESC LIMIT 1",
        (str(experiment_id), EVENT_KIND),
    )
    return json.loads(row["payload"]) if row is not None else None
