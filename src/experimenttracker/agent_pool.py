"""One agent selector over derived reservations: which saved row owns which process.

Every admission site keeps its own authority checks and deterministic preparation,
then offers a prepared proposal instead of creating a dispatch task. One selector
sorts those proposals by priority class and each role's own scan order, re-checks
eligibility and claims the owner and a free connection in a single synchronous
section before the task exists. A callback (request, activity, pending answer) is
routed through the reservation, so it reaches only the owner its saved turn proves;
a reservation is reconstructed from the authoritative source row and never becomes a
second execution database. Interrupts, readback and shutdown also leave through the
reservation's own live connection; a reservation without one keeps its uncertainty.
The pool also owns the one quota-policy decision that selection filters and the
binding check before dispatch intent share.
"""

import asyncio
from dataclasses import dataclass

from .quota import Decision, plenty

# Priority classes: the next free connection serves the lowest class first.
# Conversation, experiment (stage and monitor), research, supervisor, title. A
# non-weekly owner-requested project turn ranks as conversation, weekly synthesis
# as supervision; the owner key does not change with the priority class.
CONVERSATION_PRIORITY = 0
EXPERIMENT_PRIORITY = 1
RESEARCH_PRIORITY = 2
SUPERVISOR_PRIORITY = 3
TITLE_PRIORITY = 4

# A saved row in one of these states may still be an outstanding remote turn, so
# its owner stays exclusive while no live task holds a physical slot.
UNSETTLED_STATES = frozenset({"dispatching", "running", "uncertain", "received"})
UNSETTLED_STATES_SQL = "('dispatching','running','uncertain','received')"

# Turns whose identity can outlive the process that sent them.
SAVED_OWNED_STATES = "('dispatching','running','uncertain')"

# Weekly synthesis needs spare quota, not merely the ordinary admission threshold.
WEEKLY_PLENTY_REASON = "weekly synthesis requires plentiful subscription quota"

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
class Proposal:
    """One prepared turn offered to the next selection; nothing is claimed yet.

    ``order`` is the whole selection key: priority class, the proposal's rank in
    its role's current scan, then creation time, source table and row ID. A role's
    proposals therefore keep the scan order they always had, and proposals of two
    roles sharing a priority class fall back to their saved identity.
    """

    role: str
    source_table: str
    row: dict
    owner: tuple
    priority: int
    rank: int
    connections: tuple
    dispatch: object

    @property
    def key(self):
        return (self.source_table, self.row["id"])

    @property
    def order(self):
        return (self.priority, self.rank, self.row.get("created_at") or 0, *self.key)


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
        # Prepared turns of the current selection round; never a persistent queue.
        self.proposals = []
        # Shutdown closes the transports and later the store; a task that ends then
        # must not read or rewrite the rows its withdrawal already left uncertain.
        self.closing = False

    def quota(self, *, eid=None, run=None, origin=None):
        """The one quota decision for a saved turn, at selection and before dispatch.

        An experiment turn of an Autopilot member takes that run's stricter reserve
        policy, a research turn its own run's, and a weekly synthesis also needs
        plenty of quota left; every other turn keeps the ordinary threshold. The
        gate keeps its own hysteresis and reserve arithmetic. A selection decision is
        a filter only, so each dispatch path asks again immediately before it saves
        dispatch intent; a denial there claims nothing and launches nothing.
        """
        gate, service = self.workflow.gate, self.workflow.research.service
        if run is not None:
            return service.quota(gate, run=run)
        if eid is not None:
            return service.quota(gate, eid=eid)
        decision = gate.decide()
        if origin == "weekly" and decision.admitted and not plenty(gate, decision):
            return Decision(False, WEEKLY_PLENTY_REASON, decision.remaining, decision.observed_at)
        return decision

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

    def live_lane(self, reservation):
        """The lane whose current process still is this reservation's connection.

        A reconstructed or settled reservation names no process, and a restarted
        lane holds a later start count; neither can speak for the saved turn.
        """
        connection = getattr(reservation, "connection", None)
        if connection is None:
            return None
        lane = self.lane(connection.slot)
        if lane is None or self.connection(lane, lane.agent.start_count) != connection:
            return None
        return lane

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

    def propose(self, *, role, source_table, row, owner, priority, connections, dispatch):
        """Offer one prepared turn; nothing is claimed or created until ``admit``."""
        rank = sum(1 for proposal in self.proposals if proposal.role == role)
        proposal = Proposal(
            role=role,
            source_table=source_table,
            row=row,
            owner=owner,
            priority=priority,
            rank=rank,
            connections=tuple(connections),
            dispatch=dispatch,
        )
        self.proposals = [offered for offered in self.proposals if offered.key != proposal.key]
        self.proposals.append(proposal)

    def admit(self):
        """Claim and start the best eligible proposals; one synchronous section.

        The sorted proposals are tried in order, so a busy owner or an occupied
        connection skips only the work it cannot host and unrelated eligible work
        still starts. A proposal no free connection can host is dropped with its
        source row still pending; a turn whose outcome is unknown never returns
        here as a replacement.
        """
        pending = sorted(self.proposals, key=lambda proposal: proposal.order)
        self.proposals = []
        for proposal in pending:
            if self.closing:
                return
            workflow = self.free_connection(proposal)
            if workflow is None or not self.owner_free(proposal.owner):
                continue
            self.start(proposal, workflow)

    def free_connection(self, proposal):
        """The proposal's first connection that no live claim or task occupies."""
        for workflow in proposal.connections:
            slot = self.slot(workflow)
            if not workflow.agent_ready or workflow.agent_task is not None:
                continue
            claimed = any(
                reservation.slot == slot and reservation.connection is not None
                for reservation in self.reservations.values()
            )
            if not claimed:
                return workflow
        return None

    def owner_free(self, owner):
        """A live claim or a saved unsettled row keeps its owner's next turn out.

        The claim covers the moment before a dispatch saves its intent; the source
        rows cover an owner whose task ended while its turn stayed unsettled.
        """
        if any(
            reservation.owner == owner and reservation.connection is not None
            for reservation in self.reservations.values()
        ):
            return False
        kind, identity = owner
        if kind == "experiment":
            return not self.store.one(
                f"SELECT 1 FROM agent_actions WHERE experiment_id=? AND state IN {UNSETTLED_STATES_SQL}",
                (identity,),
            )
        return not (
            self.store.one(
                f"SELECT 1 FROM project_turns WHERE project_id=? AND state IN {UNSETTLED_STATES_SQL}",
                (identity,),
            )
            or self.store.one(
                "SELECT 1 FROM autopilot_actions a JOIN autopilot_runs r ON r.id=a.run_id "
                f"WHERE r.project_id=? AND a.state IN {UNSETTLED_STATES_SQL}",
                (identity,),
            )
        )

    def start(self, proposal, workflow):
        """Claim owner and connection, then create the dispatch task for that claim."""
        if workflow is not self.workflow:
            # A lane prepares with the primary's current cluster snapshot, as before.
            for name in ("cluster_snapshot", "cluster_error", "last_cluster_refresh"):
                setattr(workflow, name, getattr(self.workflow, name))
        reservation = self.claim(
            role=proposal.role,
            source_table=proposal.source_table,
            row=proposal.row,
            owner=proposal.owner,
            workflow=workflow,
        )
        task = asyncio.create_task(proposal.dispatch(workflow))
        task.add_done_callback(lambda finished: self.settle(reservation, finished))
        self.occupy(proposal, workflow, task)
        return reservation

    def occupy(self, proposal, workflow, task):
        """Publish the claimed task in the busy flag its own role still reads."""
        if proposal.source_table == "autopilot_actions":
            self.workflow.research.task, self.workflow.research.active_id = task, proposal.row["id"]
        elif proposal.source_table == "project_turns":
            self.workflow.supervisor.task, self.workflow.supervisor.active_id = task, proposal.row["id"]
        else:
            workflow.agent_task, workflow.active_action = task, proposal.row["id"]

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

    def settle(self, reservation, task=None):
        """Release a terminal or never-sent turn; unresolved work keeps its owner.

        A returned task proves nothing about the remote turn, so the source row
        decides: an unsettled state keeps logical ownership without a live slot.
        """
        if self.closing:
            return
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
        # A row without a saved turn was never sent, so the busy flag its claim
        # published is cleared with it instead of waiting for the role's own drain.
        if task is not None and (saved is None or saved["turn_id"] is None):
            self.release_flag(reservation, task)

    def release_flag(self, reservation, task):
        """Clear the busy flag of a released never-sent claim, where its role reads it."""
        if reservation.source_table in ("autopilot_actions", "project_turns"):
            owner = (
                self.workflow.research
                if reservation.source_table == "autopilot_actions"
                else self.workflow.supervisor
            )
            if owner.task is task:
                owner.task, owner.active_id = None, None
            return
        lane = self.lane(reservation.connection.slot) if reservation.connection else None
        if lane is not None and lane.agent_task is task:
            lane.agent_task, lane.active_action = None, None

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

    def settle_row(self, source_table, action_id):
        """Re-settle a row that a controller settled outside its dispatch task."""
        reservation = self.lookup(source_table, action_id)
        if reservation is not None:
            self.settle(reservation)

    async def interrupt(self, reservation):
        """Interrupt the exact saved turn through the process that owns it, if live.

        Returns whether a request was sent. A reservation without a live process
        keeps its saved uncertainty: another process is never asked to stop it.
        """
        lane = self.live_lane(reservation)
        if lane is None:
            return False
        saved = self.store.one(
            f"SELECT thread_id,turn_id FROM {reservation.source_table} WHERE id=?",
            (reservation.action_id,),
        )
        if not saved or not saved["thread_id"] or not saved["turn_id"]:
            return False
        await lane.agent.interrupt(saved["thread_id"], saved["turn_id"])
        return True

    def hold(self, action, reason):
        """Record a hold on one exact saved agent action and its owner."""
        with self.store.tx() as db:
            db.execute("UPDATE agent_actions SET interrupt_reason=? WHERE id=?", (reason, action["id"]))
        if action["phase"].startswith("discuss:"):
            self.workflow.conversation.hold(action["phase"].removeprefix("discuss:"), reason)
        else:
            self.store.update(action["experiment_id"], hold_reason=reason)

    async def interrupt_occupied(self, reason):
        """Hold and interrupt every occupied agent turn through its own live process."""
        for reservation in self.live():
            if reservation.source_table != "agent_actions":
                continue
            action = self.store.one("SELECT * FROM agent_actions WHERE id=?", (reservation.action_id,))
            if not action or action["state"] not in ("dispatching", "running") or not action["turn_id"]:
                continue
            self.hold(action, reason)
            await self.interrupt(reservation)

    def live(self):
        """Occupied reservations whose own process is still the one that holds them."""
        return [reservation for reservation in self.occupied() if self.live_lane(reservation) is not None]

    def read_lane(self, reservation):
        """A configured connection that may read a saved thread without a new turn.

        Readback prefers the reservation's own live process and falls back to any
        configured connection; it never starts a turn and never proves absence.
        """
        lane = self.live_lane(reservation)
        if lane is not None and lane.agent_ready:
            return lane
        return next((lane for lane in self.workflow.lanes if lane.agent_ready), None)

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
