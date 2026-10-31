import asyncio
import hashlib
import inspect
import json
import logging
import time
from pathlib import Path
from uuid import UUID

import httpx

from . import artifacts
from .knowledge import Knowledge
from .models import (
    ActionCommand,
    AnswerCommand,
    CreateExperiment,
    DeleteCommand,
    DiscussCommand,
    NoteCommand,
    PlanReviewCommand,
    PresentCommand,
    ReserveCommand,
    SteerCommand,
)
from .projects import Projects
from .store import Conflict, encode, public_projection

log = logging.getLogger(__name__)


class GatewayClient:
    """Outbound only. Commands are committed before ACK; events remain durable until accepted."""

    def __init__(self, config, store, *, status_provider=None, transport=None):
        self.config = config
        self.store = store
        self.status_provider = status_provider
        self.projects = Projects(config, store)
        token = config.gateway.credential_file.read_text().strip()
        if len(token) < 32:
            raise ValueError("executor credential must contain at least 32 characters")
        headers = {"Authorization": f"Bearer {token}"}
        if config.gateway.sites_credential_file:
            headers["OAI-Sites-Authorization"] = (
                "Bearer " + config.gateway.sites_credential_file.read_text().strip()
            )
        self.client = httpx.AsyncClient(
            base_url=config.gateway.url.rstrip("/"),
            headers=headers,
            timeout=15,
            transport=transport,
            follow_redirects=False,
        )
        self.last_heartbeat = 0
        self.operation_errors = {}
        self.artifact_retry_at = {}

    async def request(self, method, path, **kwargs):
        response = await self.client.request(method, path, **kwargs)
        response.raise_for_status()
        result = response.json()
        if result.get("schema_version") != 1:
            raise ValueError("unsupported gateway response schema")
        return result

    async def commands(self):
        received = await self.request("GET", "/api/executor/commands")
        commands = received.get("commands")
        if not isinstance(commands, list) or len(commands) > 20:
            raise ValueError("invalid gateway command batch")
        acknowledged = []
        rejected = []
        models = {
            "create": CreateExperiment,
            "delete": DeleteCommand,
            "present": PresentCommand,
            "discuss": DiscussCommand,
            "note": NoteCommand,
            "action": ActionCommand,
            "answer": AnswerCommand,
            "plan_review": PlanReviewCommand,
            "steer": SteerCommand,
            "reserve": ReserveCommand,
        }
        for command in commands:
            try:
                if command["executor_id"] != self.config.executor_id:
                    raise ValueError("command targets another executor")
                kind = command["kind"]
                eid = None if kind == "reserve" else str(UUID(command["experiment_id"]))
                if kind == "reserve" and command.get("experiment_id") is not None:
                    raise ValueError("reserve command must not target an experiment")
                receipt = self.store.one(
                    "SELECT error FROM rejected_gateway_commands WHERE request_id=?", (command["request_id"],)
                )
                if receipt:
                    rejected.append({"request_id": command["request_id"], "reason": receipt["error"]})
                    continue
                payload = (
                    models[kind].model_validate(command["payload"]).model_dump(mode="json", exclude_none=True)
                )
                if payload["request_id"] != command["request_id"]:
                    raise ValueError("command request identity mismatch")
                self.store.command(
                    payload["request_id"],
                    kind,
                    payload,
                    origin="gateway",
                    eid=None if kind in ("create", "reserve") else eid,
                    experiment_id=eid if kind == "create" else None,
                )
                # Local commit has completed. Redelivery after a lost ACK returns the prior result.
                acknowledged.append(payload["request_id"])
            except (ValueError, KeyError) as exc:
                # Persist refusal before acknowledgement, just like accepted commands.
                # An expired answer must not poison the oldest command batch indefinitely.
                if command.get("executor_id") != self.config.executor_id:
                    raise ValueError("gateway delivered a command for another executor") from exc
                request_id = str(UUID(command["request_id"]))
                eid = str(UUID(command["experiment_id"])) if command.get("experiment_id") is not None else ""
                reason = (
                    str(exc)[:1000]
                    if isinstance(exc, Conflict)
                    else "Command does not match the supported schema or experiment"
                )
                with self.store.tx() as db:
                    db.execute(
                        "INSERT OR IGNORE INTO rejected_gateway_commands VALUES(?,?,?,?,?,?)",
                        (
                            request_id,
                            eid,
                            command.get("kind", "unknown"),
                            encode(command.get("payload", {})),
                            reason,
                            time.time(),
                        ),
                    )
                rejected.append({"request_id": request_id, "reason": reason})
                log.warning("gateway command %s rejected: %s", request_id, reason)
        if acknowledged:
            await self.request(
                "POST", "/api/executor/ack", json={"schema_version": 1, "request_ids": acknowledged}
            )
        if rejected:
            await self.request(
                "POST", "/api/executor/reject", json={"schema_version": 1, "rejections": rejected}
            )

    async def publish_outbox(self):
        # A current view must never wait behind thousands of obsolete projections.
        # Its original event identity stays unchanged; older identities replay separately.
        rows = self.store.rows(
            "SELECT o.* FROM experiments x JOIN events e ON e.experiment_id=x.id "
            "AND e.revision=x.revision JOIN outbox o ON o.id=e.id "
            "WHERE o.delivered_at IS NULL AND o.next_attempt_at<=? ORDER BY e.seq LIMIT 20",
            (time.time(),),
        )
        for row in rows:
            message = json.loads(row["payload"])
            message["experiment"] = public_projection(message["experiment"])
            eid = message["experiment"]["id"]
            message["inbox"] = self.store.rows("SELECT * FROM inbox WHERE experiment_id=?", (eid,))
            try:
                response = await self.request(
                    "POST",
                    "/api/executor/publish",
                    json={"schema_version": 1, "executor_id": self.config.executor_id, "messages": [message]},
                )
                if response.get("accepted") != [row["id"]]:
                    raise ValueError("gateway did not acknowledge the exact event identity")
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE outbox SET delivered_at=?,error=NULL WHERE id=?", (time.time(), row["id"])
                    )
                self.operation_errors.pop("projection:" + eid, None)
            except Exception as exc:
                attempts = row["attempts"] + 1
                with self.store.tx() as db:
                    db.execute(
                        "UPDATE outbox SET attempts=?,next_attempt_at=?,error=? WHERE id=?",
                        (
                            attempts,
                            time.time() + min(120, 2 ** min(attempts, 7)),
                            self.error_text(exc),
                            row["id"],
                        ),
                    )
                self.operation_errors["projection:" + eid] = self.error_text(exc)
                log.warning("gateway projection %s retained for retry: %s", eid, self.error_text(exc))
                # Failure of this experiment must not suppress other current views.
        await self.replay_events()

    async def replay_events(self):
        # D1 event records contain identity/revision only. Once the current projection
        # is acknowledged, sending obsolete full scientific views adds no information.
        rows = self.store.rows(
            "SELECT o.id,o.attempts,e.experiment_id,e.revision FROM outbox o JOIN events e ON e.id=o.id "
            "JOIN experiments x ON x.id=e.experiment_id "
            "JOIN events latest ON latest.experiment_id=x.id AND latest.revision=x.revision "
            "JOIN outbox current ON current.id=latest.id "
            "WHERE o.delivered_at IS NULL AND o.next_attempt_at<=? AND current.delivered_at IS NOT NULL "
            "AND e.revision<x.revision ORDER BY e.seq LIMIT 500",
            (time.time(),),
        )
        if not rows:
            return
        try:
            response = await self.request(
                "POST",
                "/api/executor/events",
                json={
                    "schema_version": 1,
                    "executor_id": self.config.executor_id,
                    "events": [
                        {"event_id": r["id"], "experiment_id": r["experiment_id"], "revision": r["revision"]}
                        for r in rows
                    ],
                },
            )
            if response.get("accepted") != [r["id"] for r in rows]:
                raise ValueError("gateway did not acknowledge the exact replayed event identities")
            with self.store.tx() as db:
                db.executemany(
                    "UPDATE outbox SET delivered_at=?,error=NULL WHERE id=?",
                    [(time.time(), r["id"]) for r in rows],
                )
        except Exception as exc:
            with self.store.tx() as db:
                db.executemany(
                    "UPDATE outbox SET attempts=?,next_attempt_at=?,error=? WHERE id=?",
                    [
                        (
                            r["attempts"] + 1,
                            time.time() + min(120, 2 ** min(r["attempts"] + 1, 7)),
                            self.error_text(exc),
                            r["id"],
                        )
                        for r in rows
                    ],
                )
            raise

    async def upload_artifacts(self):
        # The newest acknowledged full projection establishes metadata registration.
        # Replay only acknowledges revisions below a delivered full projection, so
        # the maximum delivered revision always contains acknowledged metadata.
        rows = self.store.rows(
            "SELECT a.* FROM artifacts a JOIN experiments x ON x.id=a.experiment_id "
            "JOIN events e ON e.experiment_id=x.id AND e.revision=("
            "SELECT MAX(e2.revision) FROM events e2 JOIN outbox o2 ON o2.id=e2.id "
            "WHERE e2.experiment_id=x.id AND o2.delivered_at IS NOT NULL) "
            "JOIN outbox o ON o.id=e.id "
            "WHERE a.local_valid=1 AND a.remote_published=0 "
            "AND x.deleted_at IS NULL AND x.phase!='cancelled' "
            "AND (json_extract(x.contract,'$.execution_contract')=3 OR a.evidence_revision=x.evidence_revision) "
            "AND EXISTS (SELECT 1 FROM json_each(o.payload,'$.experiment.artifacts') j "
            "WHERE json_extract(j.value,'$.id')=a.id "
            "AND json_extract(j.value,'$.sha256')=a.sha256 "
            "AND json_extract(j.value,'$.size')=a.size) ORDER BY a.rowid",
        )
        attempts = 0
        for row in rows:
            if self.artifact_retry_at.get(row["id"], 0) > time.time():
                continue
            if attempts >= 3:
                break
            attempts += 1
            try:
                root = self.projects.base_root(row["experiment_id"])

                content = await asyncio.to_thread(self.artifact_bytes, row, root)
                response = await self.request(
                    "PUT",
                    f"/api/executor/artifacts/{row['id']}",
                    content=content,
                    headers={"Content-Type": row["media_type"]},
                )
                if (
                    response.get("artifact_id") != row["id"]
                    or response.get("sha256") != row["sha256"]
                    or response.get("size") != row["size"]
                    or response.get("available") is not True
                ):
                    raise ValueError("gateway upload verification response mismatch")
                with self.store.tx() as db:
                    db.execute("UPDATE artifacts SET remote_published=1 WHERE id=?", (row["id"],))
                    self.store.emit(
                        db, row["experiment_id"], "artifact_published", {"artifact_id": row["id"]}
                    )
                self.artifact_retry_at.pop(row["id"], None)
                self.operation_errors.pop("artifact:" + row["id"], None)
            except Exception as exc:
                self.artifact_retry_at[row["id"]] = time.time() + 30
                self.operation_errors["artifact:" + row["id"]] = self.error_text(exc)
                log.warning("gateway artifact %s retained for retry: %s", row["id"], self.error_text(exc))

    @staticmethod
    def artifact_bytes(row, root):
        path = artifacts.allowed_path(Path(row["path"]), root)
        content = path.read_bytes()
        if len(content) != row["size"] or hashlib.sha256(content).hexdigest() != row["sha256"]:
            raise ValueError("immutable artifact changed before upload")
        return content

    @staticmethod
    def error_text(exc):
        return f"{type(exc).__name__}: {exc}"[:2000]

    async def isolated(self, name, operation):
        try:
            await operation()
            self.operation_errors.pop(name, None)
        except Exception as exc:
            self.operation_errors[name] = self.error_text(exc)
            log.warning("gateway %s retained for retry: %s", name, self.error_text(exc))

    async def heartbeat(self):
        if time.time() - self.last_heartbeat < 30:
            return
        status = (
            self.status_provider()
            if self.status_provider
            else {
                "last_tick": None,
                "agent_ready": False,
                "agent_error": None,
                "quota": {
                    "admitted": False,
                    "reason": "No workflow status provider",
                    "remaining": {},
                    "observed_at": None,
                },
            }
        )
        if inspect.isawaitable(status):
            status = await status
        status = {
            **status,
            "publication": {
                "pending_events": self.store.one(
                    "SELECT COUNT(*) AS n FROM outbox WHERE delivered_at IS NULL"
                )["n"],
                "failed_operations": sorted(self.operation_errors),
            },
        }
        await self.request(
            "POST",
            "/api/executor/publish",
            json={
                "schema_version": 1,
                "executor_id": self.config.executor_id,
                "messages": [],
                "heartbeat": status,
            },
        )
        self.last_heartbeat = time.time()

    async def project_commands(self):
        response = await self.request("GET", "/api/executor/project-commands")
        commands = response.get("commands")
        if not isinstance(commands, list) or len(commands) > 20:
            raise ValueError("invalid project command batch")
        for row in commands:
            identity = str(UUID(row["request_id"]))
            if (
                row["project_id"] != "project-" + UUID(identity).hex
                or row["payload"].get("request_id") != identity
            ):
                raise ValueError("project request identity mismatch")
            self.projects.enqueue(row["payload"], origin="gateway")
        # Save registration/filesystem outcomes before publishing their receipts.
        self.projects.tick()
        rows = self.store.rows(
            "SELECT request_id FROM project_requests WHERE state IN ('applied','rejected') AND origin='gateway' AND delivered_at IS NULL ORDER BY created_at LIMIT 20"
        )
        if rows:
            results = [self.projects.receipt(row["request_id"]) for row in rows]
            response = await self.request(
                "POST",
                "/api/executor/project-results",
                json={
                    "schema_version": 1,
                    "results": [
                        {
                            key: result[key]
                            for key in ("request_id", "project_id", "state", "error", "project")
                        }
                        for result in results
                    ],
                },
            )
            if response.get("accepted") != [row["request_id"] for row in rows]:
                raise ValueError("project acknowledgement identity mismatch")
            with self.store.tx() as db:
                db.executemany(
                    "UPDATE project_requests SET delivered_at=? WHERE request_id=?",
                    [(time.time(), row["request_id"]) for row in rows],
                )
            self.last_heartbeat = 0

    async def project_settings_commands(self):
        response = await self.request("GET", "/api/executor/project-settings-commands")
        commands = response.get("commands")
        if not isinstance(commands, list) or len(commands) > 20:
            raise ValueError("invalid project settings command batch")
        for row in commands:
            identity = str(UUID(row["request_id"]))
            if row["payload"].get("request_id") != identity:
                raise ValueError("project settings request identity mismatch")
            self.projects.enqueue_settings(row["project_id"], row["payload"], origin="gateway")
        self.projects.settings_tick()
        rows = self.store.rows(
            "SELECT request_id FROM project_settings_requests WHERE state IN ('applied','rejected') "
            "AND origin='gateway' AND delivered_at IS NULL ORDER BY created_at LIMIT 20"
        )
        if rows:
            results = [self.projects.settings_receipt(row["request_id"]) for row in rows]
            response = await self.request(
                "POST",
                "/api/executor/project-settings-results",
                json={
                    "schema_version": 1,
                    "results": [
                        {
                            key: result[key]
                            for key in ("request_id", "project_id", "state", "error", "settings")
                        }
                        for result in results
                    ],
                },
            )
            if response.get("accepted") != [row["request_id"] for row in rows]:
                raise ValueError("project settings acknowledgement identity mismatch")
            with self.store.tx() as db:
                db.executemany(
                    "UPDATE project_settings_requests SET delivered_at=? WHERE request_id=?",
                    [(time.time(), row["request_id"]) for row in rows],
                )
            self.last_heartbeat = 0

    async def project_memory(self):
        memory = Knowledge(self.store)
        batch = await self.request("GET", "/api/executor/project-memory-commands")
        commands = batch.get("commands")
        if not isinstance(commands, list) or len(commands) > 20:
            raise ValueError("invalid project memory command batch")
        outcomes = []
        for command in commands:
            if command.get("request_id") != command.get("payload", {}).get("request_id"):
                raise ValueError("project memory command identity mismatch")
            prior = self.store.one(
                "SELECT * FROM project_memory_rejections WHERE request_id=?", (command["request_id"],)
            )
            if prior:
                if prior["project_id"] != command["project_id"] or prior["payload"] != encode(
                    command["payload"]
                ):
                    raise ValueError("rejected project memory command identity changed")
                outcomes.append(
                    {"request_id": command["request_id"], "state": "rejected", "error": prior["error"]}
                )
                continue
            try:
                memory.enqueue(command["project_id"], command["payload"], origin="gateway")
                outcomes.append({"request_id": command["request_id"], "state": "accepted", "error": None})
            except (ValueError, KeyError) as exc:
                with self.store.tx() as db:
                    db.execute(
                        "INSERT INTO project_memory_rejections VALUES(?,?,?,?)",
                        (
                            command["request_id"],
                            command["project_id"],
                            encode(command["payload"]),
                            str(exc)[:1000],
                        ),
                    )
                outcomes.append(
                    {"request_id": command["request_id"], "state": "rejected", "error": str(exc)[:1000]}
                )
        if outcomes:
            response = await self.request(
                "POST",
                "/api/executor/project-memory-commands",
                json={"schema_version": 1, "results": outcomes},
            )
            if response.get("accepted") != [r["request_id"] for r in outcomes]:
                raise ValueError("project memory command acknowledgement identity mismatch")
        # Immutable versions travel before their ready projection. Every event remains
        # in the local outbox until its exact project/revision has been acknowledged.
        for row in self.store.rows(
            "SELECT * FROM project_memory_events WHERE delivered_at IS NULL ORDER BY created_at,revision LIMIT 20"
        ):
            value = json.loads(row["payload"])
            response = await self.request("POST", "/api/executor/project-memory", json=value)
            if response.get("project_id") != row["project_id"] or response.get("revision") != row["revision"]:
                raise ValueError("project memory acknowledgement identity mismatch")
            with self.store.tx() as db:
                db.execute(
                    "UPDATE project_memory_events SET delivered_at=? WHERE project_id=? AND revision=?",
                    (time.time(), row["project_id"], row["revision"]),
                )

    async def maintenance(self):
        from .maintenance import InstallCommand

        manager = self.maintenance_provider() if getattr(self, "maintenance_provider", None) else None
        if manager is None:
            return
        response = await self.request("GET", "/api/executor/maintenance-commands")
        commands = response.get("commands", [])
        if not isinstance(commands, list) or len(commands) > 20:
            raise ValueError("invalid maintenance command batch")
        for row in commands:
            command = InstallCommand.model_validate(row)
            wire = encode(command.model_dump(mode="json"))
            prior = self.store.one(
                "SELECT * FROM rejected_gateway_commands WHERE request_id=?", (str(command.request_id),)
            )
            try:
                if prior:
                    if prior["kind"] != "maintenance" or prior["payload"] != wire:
                        raise ValueError("maintenance request identity conflicts with a saved rejection")
                    raise Conflict(prior["error"])
                manager.install(command)
                state, error = "accepted", None
            except Conflict as exc:
                state, error = "rejected", str(exc)[:1000]
                with self.store.tx() as db:
                    db.execute(
                        "INSERT OR IGNORE INTO rejected_gateway_commands VALUES(?,?,?,?,?,?)",
                        (str(command.request_id), "", "maintenance", wire, error, time.time()),
                    )
            receipt = await self.request(
                "POST",
                "/api/executor/maintenance-commands",
                json={
                    "schema_version": 1,
                    "request_id": str(command.request_id),
                    "release_id": command.release_id,
                    "state": state,
                    "error": error,
                },
            )
            if receipt.get("accepted") != str(command.request_id):
                raise ValueError("maintenance acknowledgement identity mismatch")
        await self.request(
            "POST",
            "/api/executor/maintenance",
            json=manager.status(getattr(self, "maintenance_workflow", None)),
        )

    async def once(self):
        async def publication():
            await self.isolated("projections", self.publish_outbox)
            await self.isolated("artifacts", self.upload_artifacts)

        # Command polling, heartbeat and publication can wait on independent RPCs.
        # SQLite writes still run transactionally under the one executor owner.
        await asyncio.gather(
            self.isolated("project_commands", self.project_commands),
            self.isolated("project_memory", self.project_memory),
            self.isolated("project_settings", self.project_settings_commands),
            self.isolated("commands", self.commands),
            self.isolated("maintenance", self.maintenance),
            self.isolated("heartbeat", self.heartbeat),
            publication(),
        )

    async def run(self):
        while True:
            try:
                await self.once()
            except Exception as exc:
                log.warning("gateway unavailable; local state retained: %s", exc)
            await asyncio.sleep(self.config.gateway.poll_seconds)

    async def close(self):
        await self.client.aclose()
