"""Map saved controller wakeups to backend timer responsibility episodes.

Call ``sync_wakeup`` inside the transaction that mutates the wakeup. It records
only the persisted timer; it never dispatches a turn or changes dispatch intent.
"""

import json

from . import obligation_sources


def sync_wakeup(db, wakeup_id):
    """Synchronize one saved wakeup's timer obligation in its caller's transaction.

    A pending wakeup waits on a backend timer at its saved due time. Queueing its
    saved action (whose own obligation then covers the work) or completing it
    satisfies the timer; supersession or failure abandons it.
    """
    if not db.in_transaction:
        raise RuntimeError("wakeup obligation sync requires the caller's active transaction")
    wakeup = db.execute("SELECT * FROM agent_wakeups WHERE id=?", (wakeup_id,)).fetchone()
    if wakeup is None:
        raise ValueError(f"saved wakeup does not exist: {wakeup_id}")
    experiment = db.execute(
        "SELECT contract FROM experiments WHERE id=?", (wakeup["experiment_id"],)
    ).fetchone()
    if experiment is None:
        raise ValueError(f"saved wakeup experiment does not exist: {wakeup['experiment_id']}")
    return obligation_sources.sync_episode(
        db,
        "wakeup",
        wakeup_id,
        state=wakeup["state"],
        live_states={"pending"},
        satisfied_states={"queued", "complete"},
        abandoned_states={"superseded", "failed"},
        experiment_id=wakeup["experiment_id"],
        project_id=(json.loads(experiment["contract"]).get("project") or {}).get("id"),
        kind="timer",
        owner="backend",
        wait=f"wakeup:{wakeup_id}",
        detail=f"reason={wakeup['reason']}",
        due_at=wakeup["due_at"],
    )
