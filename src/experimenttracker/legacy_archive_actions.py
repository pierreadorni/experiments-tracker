"""Settlement-only helpers for saved legacy agent work."""

import asyncio

from .codex_client import TerminalTurnError
from .legacy_archive import execution_frozen


def _live_action(workflow, action_id):
    owner = workflow.pool_owner
    return any(
        lane.active_action == action_id and lane.agent_task and not lane.agent_task.done()
        for lane in owner.lanes
    )


async def settle_actions(workflow):
    """Settle only exact saved legacy turns and drain completed archived checks."""
    store = workflow.store
    rows = store.rows(
        "SELECT a.* FROM agent_actions a JOIN experiments e ON e.id=a.experiment_id "
        "WHERE a.state IN ('dispatching','running','uncertain') AND a.phase NOT LIKE 'discuss:%' "
        "AND e.deleted_at IS NULL AND e.phase!='cancelled' ORDER BY a.created_at"
    )
    for action in rows:
        exp = store.experiment(action["experiment_id"])
        if not execution_frozen(exp) or _live_action(workflow, action["id"]):
            continue
        if not action["thread_id"] or not action["turn_id"]:
            continue
        lane = workflow.pool_owner.agent_assignments.get(action["experiment_id"])
        lane = lane or workflow
        if not lane.agent_ready:
            continue
        try:
            result = await lane.agent.read_turn(action["thread_id"], action["turn_id"])
        except TerminalTurnError as exc:
            workflow._action_state(action["id"], "failed", error=str(exc), expected=action)
        except Exception as exc:
            workflow._action_state(action["id"], "uncertain", error=str(exc), expected=action)
        else:
            workflow._action_state(action["id"], "completed", result=result, expected=action)

    for checks in (workflow.output_checks, workflow.evidence_checks):
        for identity, value in list(checks.items()):
            if isinstance(identity, tuple):
                eid = identity[0]
            else:
                job = store.one("SELECT experiment_id FROM jobs WHERE intent_id=?", (identity,))
                if not job:
                    continue
                eid = job["experiment_id"]
            exp = store.experiment(eid)
            if (
                not execution_frozen(exp)
                or exp["deleted_at"] is not None
                or exp["phase"] in ("complete", "failed", "cancelled")
                or exp["hold_reason"] != "legacy_contract_archived"
            ):
                continue
            task = value[0] if isinstance(value, tuple) else value
            if not task.done():
                continue
            try:
                task.result()
            except (Exception, asyncio.CancelledError):
                pass
            checks.pop(identity, None)
