"""Derived agent reservations: which saved source row owns which agent process.

Lane allocation, busy sets and global role guards keep deciding admission. A
callback (request, activity, pending answer) is routed through a reservation, so
it reaches only the owner its saved turn proves; a reservation is reconstructed
from the authoritative source row and never becomes a second execution database.
"""

from dataclasses import dataclass

# A saved row in one of these states may still be an outstanding remote turn, so
# its owner stays exclusive while no live task holds a physical slot.
UNSETTLED_STATES = frozenset({"dispatching", "running", "uncertain", "received"})

# Turns whose identity can outlive the process that sent them.
SAVED_OWNED_STATES = "('dispatching','running','uncertain')"

TITLED_PHASES = {"title": "title", "summarize_title": "title"}

# Tables whose saved thread/turn identity can be matched to a callback.
ROUTED_SOURCES = ("agent_actions", "autopilot_actions", "project_turns")


def experiment_owner(eid):
    return ("experiment", eid)


def project_owner(pid):
    return ("project", pid)


def role_for_phase(phase):
    if phase.startswith("discuss:"):
        return "conversation"
    return TITLED_PHASES.get(phase, "stage")


@dataclass(frozen=True)
class Connection:
    """One App Server process: the slot that runs it and that slot's start count."""

    slot: int | None
    start: int


@dataclass
class Reservation:
    source_table: str
    action_id: str
    role: str
    owner: tuple
    slot: int | None = None
    thread_id: str | None = None
    turn_id: str | None = None
    connection: Connection | None = None

    @property
    def key(self):
        return (self.source_table, self.action_id)


class AgentPool:
    def __init__(self, workflow):
        self.workflow = workflow
        self.store = workflow.store
        self.reservations = {}
        self.answers = {}

    def slot(self, workflow):
        """Physical lane index for a dispatch, or None for an unlisted workflow."""
        for index, lane in enumerate(self.workflow.lanes):
            if lane is workflow:
                return index
        return None

    def lane(self, index):
        """The workflow that owns a physical slot, or None outside the pool."""
        if index is None or index >= len(self.workflow.lanes):
            return None
        return self.workflow.lanes[index]

    def connection(self, workflow, start_count):
        """The App Server process a callback arrived on, read at receipt."""
        return Connection(slot=self.slot(workflow), start=start_count)

    def live_owner(self, connection, thread_id, turn_id):
        """The reservation whose saved turn matches a callback on this exact process.

        The saved row carries the exact thread/turn; the reservation must still
        hold that same process, so a settled or restarted owner is never credited.
        """
        if connection.slot is None or not thread_id or not turn_id:
            return None
        # Only the few reservations on this process are candidates; each is checked
        # against its saved row by primary key, never by scanning a source table.
        for reservation in list(self.reservations.values()):
            if reservation.connection != connection or reservation.source_table not in ROUTED_SOURCES:
                continue
            row = self.store.one(
                f"SELECT thread_id,turn_id FROM {reservation.source_table} WHERE id=?",
                (reservation.action_id,),
            )
            if row and (row["thread_id"], row["turn_id"]) == (thread_id, turn_id):
                return reservation
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
            connection=self.connection(workflow, workflow.agent.start_count),
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
        self.forget_answers(reservation)
        if saved and saved["state"] in UNSETTLED_STATES:
            reservation.slot = None
            reservation.connection = None
            reservation.thread_id = saved["thread_id"]
            reservation.turn_id = saved["turn_id"]
            return
        self.reservations.pop(reservation.key, None)

    def remember_answer(self, pending_id, connection, reservation):
        """Bind a saved answer to the process and reservation that asked for it."""
        self.answers[pending_id] = (connection, reservation)

    def forget_answers(self, reservation):
        for pending_id, (_, owner) in list(self.answers.items()):
            if owner is reservation:
                del self.answers[pending_id]

    async def reply(self, pending):
        """Send one recorded answer through its own process, at most once.

        An expired, settled or restarted owner cannot be replayed: its answer is
        dropped instead of answering a live successor's turn.
        """
        saved = self.answers.pop(pending["id"], None)
        if saved is None:
            return False
        connection, reservation = saved
        lane = self.lane(connection.slot)
        if (
            lane is None
            or reservation.connection != connection
            or self.connection(lane, lane.agent.start_count) != connection
        ):
            return False
        await lane.agent.reply(pending)
        return True

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
