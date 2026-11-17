"""Derived agent reservations: which saved source row owns which agent slot.

The registry is shadow bookkeeping for T1.6a-1. Existing lane allocation, busy
sets and global role guards keep deciding admission unchanged; nothing reads a
reservation to route or schedule work yet. A reservation is reconstructed from
the authoritative source row, so it never becomes a second execution database.
"""

from dataclasses import dataclass

# A saved row in one of these states may still be an outstanding remote turn, so
# its owner stays exclusive while no live task holds a physical slot.
UNSETTLED_STATES = frozenset({"dispatching", "running", "uncertain", "received"})

# Turns whose identity can outlive the process that sent them.
SAVED_OWNED_STATES = "('dispatching','running','uncertain')"

TITLED_PHASES = {"title": "title", "summarize_title": "title"}


def experiment_owner(eid):
    return ("experiment", eid)


def project_owner(pid):
    return ("project", pid)


def role_for_phase(phase):
    if phase.startswith("discuss:"):
        return "conversation"
    return TITLED_PHASES.get(phase, "stage")


@dataclass
class Reservation:
    source_table: str
    action_id: str
    role: str
    owner: tuple
    slot: int | None = None
    thread_id: str | None = None
    turn_id: str | None = None

    @property
    def key(self):
        return (self.source_table, self.action_id)


class AgentPool:
    def __init__(self, workflow):
        self.workflow = workflow
        self.store = workflow.store
        self.reservations = {}

    def slot(self, workflow):
        """Physical lane index for a dispatch, or None for an unlisted workflow."""
        for index, lane in enumerate(self.workflow.lanes):
            if lane is workflow:
                return index
        return None

    def claim(self, *, role, source_table, row, owner, workflow):
        """Reserve an owner's agent before its dispatch task exists."""
        reservation = Reservation(
            source_table=source_table,
            action_id=row["id"],
            role=role,
            owner=owner,
            slot=self.slot(workflow),
            thread_id=row.get("thread_id"),
            turn_id=row.get("turn_id"),
        )
        self.reservations[reservation.key] = reservation
        return reservation

    def settle(self, reservation):
        """Release a terminal or never-sent turn; unresolved work keeps its owner.

        A returned task proves nothing about the remote turn, so the source row
        decides: an unsettled state keeps logical ownership without a live slot.
        """
        saved = self.store.one(
            f"SELECT state,thread_id,turn_id FROM {reservation.source_table} WHERE id=?",
            (reservation.action_id,),
        )
        if saved and saved["state"] in UNSETTLED_STATES:
            reservation.slot = None
            reservation.thread_id = saved["thread_id"]
            reservation.turn_id = saved["turn_id"]
            return
        self.reservations.pop(reservation.key, None)

    def lookup(self, source_table, action_id):
        return self.reservations.get((source_table, action_id))

    def occupied(self):
        """Every reservation; a slot holds a live task, None is saved ownership."""
        return list(self.reservations.values())

    def reconstruct(self):
        """Restore saved ownership from the source tables before any admission."""
        for row in self.store.rows(
            "SELECT id,experiment_id,phase,thread_id,turn_id FROM agent_actions "
            f"WHERE state IN {SAVED_OWNED_STATES}"
        ):
            self._saved(
                "agent_actions", row, role_for_phase(row["phase"]), experiment_owner(row["experiment_id"])
            )
        for row in self.store.rows(
            f"SELECT id,project_id,thread_id,turn_id FROM project_turns WHERE state IN {SAVED_OWNED_STATES}"
        ):
            self._saved("project_turns", row, "supervisor", project_owner(row["project_id"]))
        for row in self.store.rows(
            "SELECT a.id,r.project_id,a.thread_id,a.turn_id FROM autopilot_actions a "
            f"JOIN autopilot_runs r ON r.id=a.run_id WHERE a.state IN {SAVED_OWNED_STATES}"
        ):
            self._saved("autopilot_actions", row, "research", project_owner(row["project_id"]))

    def _saved(self, source_table, row, role, owner):
        self.reservations[(source_table, row["id"])] = Reservation(
            source_table=source_table,
            action_id=row["id"],
            role=role,
            owner=owner,
            thread_id=row["thread_id"],
            turn_id=row["turn_id"],
        )
