"""C2 liveness watchdog: report engines that cannot progress; never dispatch."""

import logging

from .core import liveness

log = logging.getLogger(__name__)

# Holds whose next step belongs to the owner, so silence is expected until they answer.
OWNER_HOLDS = (
    "plan_review",
    "agent_paused",
    "new_compute_paused",
    "needs_attention",
    "quota_hold",
    "agent_busy",
    "cancellation_pending",
    "legacy_contract_archived",
)
NOTICE_KIND = "engine_stuck"
HINT = "Inspect the saved obligation and resolve it; reconcile uncertain outcomes before retrying."
NOTICED_SQL = "SELECT text FROM inbox WHERE experiment_id=? AND kind=?"


def engine_watchdog(store, now, minutes, service):
    """Report each exact liveness defect once per (experiment, cause, obligation).

    One read transaction judges every open obligation and every saved contract-3
    lifetime against its exact source. Each defect then gets one ``engine_stuck``
    notice whose deterministic text starts with its cause and names the obligation,
    so repeated passes and owner restarts add no second notice; a defect with no
    experiment to notify is only logged. The watchdog never dispatches, repairs,
    retries or asserts external absence.
    """
    with store.tx() as db:
        report = liveness.inspect(db, now, interval=minutes * 60, service=service, owner_holds=OWNER_HOLDS)
    notices = 0
    for verdict in report.defects:
        text = f"{verdict.cause}: {verdict.obligation_id or verdict.family}. {HINT}"
        if verdict.experiment_id is None:
            log.warning("engine watchdog: %s", text)
            continue
        if text in _noticed_texts(store, verdict.experiment_id):
            continue
        log.warning("engine watchdog: experiment %s %s", verdict.experiment_id, text)
        store.notify(verdict.experiment_id, NOTICE_KIND, text)
        notices += 1
    return notices


def _noticed_texts(store, eid):
    """Notice texts already saved for the experiment; the text carries cause and identity."""
    return {row["text"] for row in store.rows(NOTICED_SQL, (eid, NOTICE_KIND))}
