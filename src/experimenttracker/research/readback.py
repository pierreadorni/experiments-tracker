"""C7 readback: one exact saved research turn through the shared settlement path.

Automatic turn resolution and offline operator reconciliation both enter here. The
saved action must still own its exact thread and turn; a missing turn needs a
non-empty read-back identity that no other action owns and the exact saved dispatch
prompt hash, so a thread's older turn with identical scientific input is never
accepted. The read goes through the pool's read slot and the result goes only to
``research.settlement.complete``: this module never dispatches, retries or continues
a turn and never validates a contract 3 stage result.
"""

from ..codex_client import NeedsAttention
from ..core import action_obligations
from . import settlement

PHASE = "research"
_LIVE_STATES = frozenset({"dispatching", "running", "uncertain"})
_ADOPTING_STATES = frozenset({"dispatching", "uncertain"})


def _saved(store, expected):
    current = store.one("SELECT * FROM agent_actions WHERE id=?", (expected["id"],))
    if current is None:
        raise NeedsAttention("saved research action disappeared during readback")
    action_obligations.check_expected(current, expected)
    return current


def _receipt(action):
    return {
        "action_id": action["id"],
        "thread_id": action["thread_id"],
        "turn_id": action["turn_id"],
        "state": action["state"],
    }


def _identity(workflow, action, turn_id):
    """Return the exact turn to read and the saved prompt hash only for a missing turn."""
    if action["phase"] != PHASE:
        raise ValueError("not a research action")
    if action["state"] not in _LIVE_STATES:
        raise ValueError("action must be a saved uncertain/running research dispatch")
    if not action["thread_id"]:
        raise NeedsAttention("saved research dispatch has no exact thread identity")
    if turn_id is not None and (not isinstance(turn_id, str) or not turn_id.strip()):
        raise NeedsAttention("a read-back turn identity must be a non-empty string")
    saved = action["turn_id"]
    if saved is not None:
        if turn_id is not None and turn_id != saved:
            raise NeedsAttention("supplied turn differs from the saved research turn")
        exact = saved
    else:
        if action["state"] not in _ADOPTING_STATES:
            raise NeedsAttention("a running research action cannot adopt a different turn")
        if turn_id is None:
            raise NeedsAttention(
                "a research turn without a saved identity requires the exact read-back turn"
            )
        exact = turn_id
    owned = workflow.store.one(
        "SELECT id FROM agent_actions WHERE thread_id=? AND turn_id=? AND id<>?",
        (action["thread_id"], exact, action["id"]),
    )
    if owned is not None:
        raise NeedsAttention("read-back turn already belongs to a saved action")
    # The saved dispatch hash includes the action marker, so an older turn with the
    # same scientific input cannot match a later action.
    prompt_hash = None if saved is not None else workflow.dispatch_prompt_hash(action)
    return exact, prompt_hash


async def reconcile(workflow, action, *, turn_id=None):
    """Read and settle one exact saved research turn; a completed replay is a no-op.

    The saved row, thread, turn and (for a missing turn) dispatch hash are checked
    before the read. ``settlement.complete`` re-checks the row after every await and
    owns the commit, the terminal state and the obligation.
    """
    store = workflow.store
    expected = _saved(store, action)
    if (
        expected["state"] == "completed"
        and turn_id is not None
        and expected["turn_id"] == turn_id
    ):
        return _receipt(expected)
    exact, prompt_hash = _identity(workflow, expected, turn_id)
    reservation = workflow.agent_pool.lookup("agent_actions", expected["id"])
    slot = workflow.agent_pool.read_slot(reservation)
    if slot is None:
        raise NeedsAttention("no configured connection can read the saved research thread")
    if prompt_hash is None:
        result = await slot.agent.read_turn(expected["thread_id"], exact)
    else:
        result = await slot.agent.read_turn(
            expected["thread_id"], exact, expected_prompt_hash=prompt_hash
        )
    settled = await settlement.complete(
        workflow,
        expected,
        result,
        recovered_turn_id=exact if expected["turn_id"] is None else None,
    )
    return _receipt(settled)
