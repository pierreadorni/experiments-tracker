"""C7 research turn: the bounded turn prompt and the enforced turn-end result.

The prompt is pure: it renders only the brief, owner constraints, the latest saved
checkpoint, recent events and open obligations. Cluster, resource, job and project
knowledge stay behind the turn tools, so nothing here reads state or dispatches work.
A section that does not fit is cut at a character boundary and marked, and dropped
events, decisions or obligations are named, so a bounded prompt never passes off a
partial view as the whole history.
"""

from collections.abc import Sequence
from typing import Literal
from uuid import UUID

from pydantic import Field, field_validator, model_validator

from ..config import StrictModel

# Design C7 targets one turn under 8 KB; keep every prompt strictly below that.
MAX_PROMPT_BYTES = 8000
MAX_CHECKPOINT_BYTES = 4000
# The remaining per-section caps keep room for the checkpoint and the instructions.
MAX_BRIEF_BYTES = 1600
MAX_CONSTRAINTS_BYTES = 800
MAX_EVENTS_BYTES = 1600
MAX_OBLIGATIONS_BYTES = 800

_TRUNCATED = " [truncated]"
_OLDER_EVENTS = "[older events omitted]"
_MORE_CONSTRAINTS = "[more owner constraints omitted]"
_MORE_OBLIGATIONS = "[more obligations omitted]"

_INSTRUCTIONS = (
    "Work in this experiment's worktree. Use the turn tools for cluster, job,\n"
    "checkpoint, report, owner and integration actions; this prompt carries no\n"
    "cluster context. End every turn with one JSON object:\n"
    '{"status": "continue" | "wait" | "needs_owner" | "done",\n'
    ' "summary": "<nonblank>", "next": "<nonblank>",\n'
    ' "integrate": {"value": true | false, "reason": "<nonblank>"}}\n'
    "integrate is optional, and required only when status is done.\n"
    "continue starts the next turn; wait needs an active job or pending answer;\n"
    "needs_owner needs an open ask_owner; done needs a published report.\n"
)

_FRAME = (
    "RESEARCH TURN\n"
    "\nBRIEF\n\n"
    "\nOWNER CONSTRAINTS\n\n"
    "\nLATEST CHECKPOINT\n\n"
    "\nRECENT EVENTS\n\n"
    "\nOPEN OBLIGATIONS\n\n"
    "\nINSTRUCTIONS\n"
)
# The frame, the instructions and the newline that ends the joined prompt.
_FRAME_BYTES = len(_FRAME.encode()) + len(_INSTRUCTIONS.encode()) + 1


def _nonblank(value):
    if not value.strip():
        raise ValueError("must not be blank")
    return value


def _action_marker(action_id):
    """Return the saved action's marker line, or nothing when none was named."""
    if action_id is None:
        return ""
    if not isinstance(action_id, str):
        raise TypeError("action_id must be text")
    try:
        canonical = str(UUID(action_id))
    except ValueError:
        raise ValueError("action_id must be a UUID") from None
    return f"ACTION {canonical}\n"


class IntegrationDecision(StrictModel):
    """C6's required pre-integration answer, kept inside the turn-end result."""

    value: bool = Field(strict=True)
    reason: str = Field(min_length=1, max_length=2000)

    @field_validator("reason")
    @classmethod
    def reason_present(cls, value):
        return _nonblank(value)


class TurnEnd(StrictModel):
    """The exact turn-end object C7 enforces; no other result shape is accepted."""

    status: Literal["continue", "wait", "needs_owner", "done"]
    summary: str = Field(min_length=1, max_length=4000)
    next: str = Field(min_length=1, max_length=2000)
    integrate: IntegrationDecision | None = None

    @field_validator("summary", "next")
    @classmethod
    def text_present(cls, value):
        return _nonblank(value)

    @model_validator(mode="after")
    def done_integrates(self):
        if self.status == "done" and self.integrate is None:
            raise ValueError("done requires an integration decision")
        return self


class TurnEvidence(StrictModel):
    """Facts the caller observed; this module never reads state to guess them."""

    active_jobs: int = Field(default=0, ge=0, strict=True)
    pending_answers: int = Field(default=0, ge=0, strict=True)
    validated_timer: bool = False
    open_ask_owner: bool = False
    published_report: bool = False


def validate_turn_end(turn: TurnEnd, evidence: TurnEvidence | None = None) -> TurnEnd:
    """Enforce C7's turn-end evidence rules and return the accepted result.

    A timer is evidence only once the caller has validated it against ``next``; this
    function parses no timer text. ``continue`` needs no evidence because it queues
    the next turn immediately.
    """
    evidence = evidence or TurnEvidence()
    if turn.status == "wait" and not (
        evidence.active_jobs or evidence.pending_answers or evidence.validated_timer
    ):
        raise ValueError("wait needs an active job, a pending owner answer, or a validated timer")
    if turn.status == "needs_owner" and not evidence.open_ask_owner:
        raise ValueError("needs_owner needs an open ask_owner")
    if turn.status == "done" and not evidence.published_report:
        raise ValueError("done needs a published report")
    return turn


def _text(value, name):
    if not isinstance(value, str):
        raise TypeError(f"{name} must be text")
    return value


def _items(values, name):
    if isinstance(values, str):
        raise TypeError(f"{name} must be a sequence of text")
    items = list(values)
    if any(not isinstance(item, str) for item in items):
        raise TypeError(f"every {name} must be text")
    return items


def _printable(text):
    """Keep line structure but drop control characters from agent-authored text."""
    return "".join(char for char in text if char == "\n" or char.isprintable())


def _one_line(text):
    return " ".join(_printable(text).split())


def _fit(text, limit):
    """Cut text to at most limit UTF-8 bytes on a character boundary, marked."""
    encoded = text.encode()
    if len(encoded) <= limit:
        return text
    keep = limit - len(_TRUNCATED.encode())
    if keep <= 0:
        return ""
    return encoded[:keep].decode("utf-8", "ignore") + _TRUNCATED


def _bounded_lines(items, limit, *, none_text, omitted_marker, recent):
    """Render bullet lines within limit bytes, marking every omission."""
    if not items:
        return none_text
    lines = [f"- {_one_line(item)}" for item in items]
    if recent:
        lines.reverse()
    reserve = len(omitted_marker.encode()) + 1
    kept = []
    used = 0
    for position, line in enumerate(lines):
        cost = len(line.encode()) + 1
        reserved = reserve if position < len(lines) - 1 else 0
        if used + cost + reserved > limit:
            break
        kept.append(line)
        used += cost
    if not kept:
        kept = [_fit(lines[0], max(0, limit - reserve - 1))]
    omitted = len(lines) > len(kept)
    if recent:
        kept.reverse()
        if omitted:
            kept.insert(0, omitted_marker)
    elif omitted:
        kept.append(omitted_marker)
    return "\n".join(kept)


def turn_prompt(
    *,
    brief: str,
    owner_constraints: Sequence[str] = (),
    checkpoint: str | None = None,
    events: Sequence[str] = (),
    obligations: Sequence[str] = (),
    action_id: str | None = None,
) -> str:
    """Build one deterministic, bounded turn prompt from the saved turn inputs.

    Sections keep their fixed order and headers whatever the input. Recent events are
    kept newest-first before the rest are dropped, the checkpoint takes up to about
    4 KB of the space left over, and cuts are marked instead of silently shortening.
    A contract 4 action's UUID is rendered inside the same byte budget so the saved
    prompt names the exact action it belongs to.
    """
    brief = _printable(_text(brief, "brief"))
    constraints = _items(owner_constraints, "owner constraint")
    events = _items(events, "event")
    obligations = _items(obligations, "obligation")
    if checkpoint is not None:
        checkpoint = _printable(_text(checkpoint, "checkpoint"))

    marker = _action_marker(action_id)
    budget = MAX_PROMPT_BYTES - 1 - _FRAME_BYTES - len(marker.encode())
    brief_text = _fit(brief, min(budget, MAX_BRIEF_BYTES))
    budget -= len(brief_text.encode())
    constraints_text = _bounded_lines(
        constraints,
        min(budget, MAX_CONSTRAINTS_BYTES),
        none_text="none recorded",
        omitted_marker=_MORE_CONSTRAINTS,
        recent=False,
    )
    budget -= len(constraints_text.encode())
    events_text = _bounded_lines(
        events,
        min(budget, MAX_EVENTS_BYTES),
        none_text="no events since the last turn",
        omitted_marker=_OLDER_EVENTS,
        recent=True,
    )
    budget -= len(events_text.encode())
    obligations_text = _bounded_lines(
        obligations,
        min(budget, MAX_OBLIGATIONS_BYTES),
        none_text="none open",
        omitted_marker=_MORE_OBLIGATIONS,
        recent=False,
    )
    budget -= len(obligations_text.encode())
    checkpoint_text = (
        _fit(checkpoint, min(budget, MAX_CHECKPOINT_BYTES)) if checkpoint else "no checkpoint saved yet"
    )

    return (
        "RESEARCH TURN\n"
        f"{marker}"
        f"\nBRIEF\n{brief_text}\n"
        f"\nOWNER CONSTRAINTS\n{constraints_text}\n"
        f"\nLATEST CHECKPOINT\n{checkpoint_text}\n"
        f"\nRECENT EVENTS\n{events_text}\n"
        f"\nOPEN OBLIGATIONS\n{obligations_text}\n"
        f"\nINSTRUCTIONS\n{_INSTRUCTIONS}\n"
    )
