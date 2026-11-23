import asyncio
import hashlib
import inspect
import logging
from pathlib import Path
from urllib.parse import quote
from uuid import UUID

import httpx

from . import artifacts, clock
from .knowledge import Knowledge
from .legacy_archive import ALL_REVISIONS_EVIDENCE_CONTRACTS
from .projects import Projects
from .publish.commands import Services, dispatch
from .publish.projections import (
    AGGREGATES,
    BLOB_MAX,
    TOMBSTONE_DIGEST,
    Projections,
    envelope,
    experiment_key,
    local_roots,
    parse_key,
    scrub_local,
    wire,
    wire_digest,
)

log = logging.getLogger(__name__)

_ALL_REVISIONS_CONTRACT_SQL = ",".join(str(version) for version in ALL_REVISIONS_EVIDENCE_CONTRACTS)

# Which artifact bytes may leave the host.
_ARTIFACT_ELIGIBLE_SQL = (
    "a.local_valid=1 AND x.deleted_at IS NULL AND x.phase!='cancelled' "
    "AND (COALESCE(json_extract(x.contract,'$.execution_contract'),0) "
    f"IN ({_ALL_REVISIONS_CONTRACT_SQL}) OR a.evidence_revision=x.evidence_revision)"
)

RELAY_BATCH = 20  # documents per tick, so a backfill cannot starve other work
RELAY_BLOB_MAX = BLOB_MAX
RELAY_AGGREGATE_SECONDS = 30  # status carries clocks, so an unchanged view still changes
RELAY_RETRY_SECONDS = 30
RELAY_UNSUPPORTED_SECONDS = 300


UNSUPPORTED_ERROR = "Worker lacks relay routes; publish the current Worker"


class RelayUnsupported(Exception):
    """The gateway answered 404 on a relay route: it is an older Worker."""


class GatewayClient:
    """Outbound only. Commands are committed before their receipt; documents follow the Worker manifest."""

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
        self.operation_errors = {}
        # Relay publication keeps no SQLite state: the Worker's revisions manifest is the
        # ledger, and these are only throttles and a cache of blobs already accepted.
        self.relay_retry_at = 0
        self.relay_key_retry_at = {}
        self.relay_aggregates_at = 0
        self.relay_blobs = set()

    async def relay_request(self, method, path, **kwargs):
        headers = {"x-executor-id": self.config.executor_id, **kwargs.pop("headers", {})}
        response = await self.client.request(method, path, headers=headers, **kwargs)
        if response.status_code == 404:
            raise RelayUnsupported(path)
        response.raise_for_status()
        result = response.json()
        if result.get("schema_version") != 1:
            raise ValueError("unsupported gateway response schema")
        return result

    async def publish_relay(self):
        """Publish relay projections, diffed against the Worker's manifest."""
        if clock.now() < self.relay_retry_at:
            return
        try:
            await self.relay_cycle()
            self.operation_errors.pop("relay:unsupported", None)
        except RelayUnsupported:
            self.relay_unsupported()

    def relay_unsupported(self):
        # An older Worker cannot be served at all: back off quietly, but say why.
        self.relay_retry_at = clock.now() + RELAY_UNSUPPORTED_SECONDS
        if "relay:unsupported" not in self.operation_errors:
            log.warning(UNSUPPORTED_ERROR)
        self.operation_errors["relay:unsupported"] = UNSUPPORTED_ERROR

    def command_services(self):
        def autopilot():
            from .autopilot import Autopilot

            return Autopilot(self.store, self.config)

        return Services(
            self.store,
            projects=lambda: self.projects,
            memory=lambda: Knowledge(self.store),
            autopilot=autopilot,
            maintenance=lambda: getattr(self, "maintenance_provider", lambda: None)(),
        )

    async def relay_commands(self):
        """Dispatch pending relay commands; each receipt is posted only after its outcome is saved."""
        if clock.now() < self.relay_retry_at:
            return
        try:
            batch = (await self.relay_request("GET", "/api/executor/relay/commands")).get("commands")
            self.operation_errors.pop("relay:unsupported", None)
            if not isinstance(batch, list) or len(batch) > RELAY_BATCH:
                raise ValueError("invalid relay command batch")
            services = self.command_services()
            for command in batch:
                try:
                    request_id = str(UUID(command["request_id"]))
                    outcome = dispatch(
                        services,
                        request_id,
                        command["kind"],
                        command.get("target"),
                        command.get("payload"),
                        origin="gateway",
                    )
                except (KeyError, TypeError, ValueError) as exc:
                    log.warning("relay command ignored, it has no usable identity: %s", exc)
                    continue
                if outcome["state"] == "pending":
                    continue  # redelivered until its saved outcome is terminal
                try:
                    await self.relay_request(
                        "POST",
                        "/api/executor/relay/receipts",
                        json={
                            "schema_version": 1,
                            "request_id": request_id,
                            "state": outcome["state"],
                            "result": outcome.get("result"),
                            "error": outcome.get("error"),
                        },
                    )
                except httpx.HTTPStatusError as exc:
                    if exc.response.status_code != 409:
                        raise
                    log.warning("relay receipt for %s differs from its terminal receipt", request_id)
                    continue
        except RelayUnsupported:
            self.relay_unsupported()

    async def relay_cycle(self):
        manifest = (await self.relay_request("GET", "/api/executor/relay/revisions")).get("revisions")
        if not isinstance(manifest, dict):
            raise ValueError("invalid relay revisions manifest")
        now = clock.now()
        live = self.store.rows(
            "SELECT id,revision FROM experiments WHERE deleted_at IS NULL ORDER BY created_at"
        )
        keys = []
        projections = self.relay_projections()
        if now - self.relay_aggregates_at >= RELAY_AGGREGATE_SECONDS:
            try:
                status = await self.relay_status()
                projections = self.relay_projections(status)
                keys += ["status", "projects", "inbox", *projections.autopilot_keys()]
                if projections.maintenance and projections.maintenance():
                    keys.append("maintenance")
                self.relay_aggregates_at = now
                self.operation_errors.pop("relay:status-provider", None)
            except Exception as exc:
                self.operation_errors["relay:status-provider"] = self.error_text(exc)
        keys += [
            experiment_key(r["id"])
            for r in live
            if experiment_key(r["id"]) not in manifest or manifest[experiment_key(r["id"])][0] < r["revision"]
        ]
        revisions = projections.revisions()
        keys += [k for k, rev in revisions.items() if k not in manifest or manifest[k][0] < rev]
        present = {experiment_key(r["id"]) for r in live} | set(revisions)
        keys += [
            k
            for k, (_, digest) in manifest.items()
            if k.startswith(("experiment:", "conversation:"))
            and k not in present
            and digest != TOMBSTONE_DIGEST
        ]
        for key in keys[:RELAY_BATCH]:
            if self.relay_key_retry_at.get(key, 0) > now:
                continue
            try:
                await self.relay_put(key, projections, manifest.get(key))
                self.relay_key_retry_at.pop(key, None)
                self.operation_errors.pop("relay:" + key, None)
            except RelayUnsupported:
                raise
            except Exception as exc:
                # A stale 409 or a lost response is retried later from the manifest, not in a tight loop.
                self.relay_key_retry_at[key] = now + RELAY_RETRY_SECONDS
                self.operation_errors["relay:" + key] = self.error_text(exc)
                log.warning("gateway relay %s retained for retry: %s", key, self.error_text(exc))

    def relay_projections(self, status=None):
        def autopilot():
            from .autopilot import Autopilot

            return Autopilot(self.store, self.config)

        def maintenance():
            manager = getattr(self, "maintenance_provider", lambda: None)()
            return manager.status(getattr(self, "maintenance_workflow", None)) if manager else None

        return Projections(
            self.store,
            status=status,
            catalog=(status or {}).get("projects", []),
            autopilot=autopilot,
            maintenance=maintenance,
        )

    async def relay_status(self):
        status = self.status_provider() if self.status_provider else {"projects": []}
        if inspect.isawaitable(status):
            status = await status
        return {
            **status,
            "schema_version": 1,
            "executor_id": self.config.executor_id,
            "publication": {"failed_operations": sorted(self.operation_errors)},
        }

    async def relay_put(self, key, projections, prior):
        built = projections.build(key, last_acked=(prior or [0])[0])
        if not built:
            if (
                not prior
                or prior[1] == TOMBSTONE_DIGEST
                or not key.startswith(("experiment:", "conversation:"))
            ):
                return
            body = {"schema_version": 1, "key": key, "revision": prior[0] + 1}  # tombstone
        else:
            roots = local_roots(self.config)
            # Only the status block can carry free-text paths from the host.
            built["document"] = scrub_local(built["document"], roots, paths=key == "status")
            if parse_key(key) in AGGREGATES and prior and prior[1] == wire_digest(built["document"]):
                return
            if key.startswith("experiment:"):
                await self.relay_upload_blobs(key, built["document"])
            elif key.startswith("autopilot:"):
                await self.relay_upload_autopilot_blobs(key, projections, built["document"])
            body = envelope(built)
        await self.relay_request(
            "PUT",
            "/api/executor/relay/projections/" + quote(key, safe=":"),
            content=wire(body).encode(),
            headers={"Content-Type": "application/json"},
        )
        if key.startswith("experiment:") and built:
            # The Worker accepted this revision: its events are delivered (retention reads this).
            with self.store.tx() as db:
                db.execute(
                    "UPDATE outbox SET delivered_at=?,error=NULL WHERE delivered_at IS NULL AND id IN "
                    "(SELECT id FROM events WHERE experiment_id=? AND revision<=?)",
                    (clock.now(), key.partition(":")[2], built["revision"]),
                )

    async def relay_upload_blobs(self, key, document):
        # Blobs are accepted before the document revision that references them.
        for artifact in document.get("artifacts", []):
            if (artifact["id"], artifact["sha256"]) in self.relay_blobs:
                continue
            row = self.store.one(
                "SELECT a.* FROM artifacts a JOIN experiments x ON x.id=a.experiment_id "
                f"WHERE a.id=? AND {_ARTIFACT_ELIGIBLE_SQL}",
                (artifact["id"],),
            )
            if not row:
                continue  # withheld; the document still lists it
            if row["size"] > RELAY_BLOB_MAX:
                # Recorded once, never retried: the Worker would answer 413 every time.
                self.operation_errors["relay:blob:" + row["id"]] = (
                    f"artifact exceeds the {RELAY_BLOB_MAX}-byte relay blob limit"
                )
                continue
            root = self.projects.base_root(row["experiment_id"])
            content = await asyncio.to_thread(self.artifact_bytes, row, root)
            response = await self.relay_request(
                "PUT",
                "/api/executor/relay/blobs/" + quote(row["id"], safe=":"),
                content=content,
                headers={
                    "Content-Type": row["media_type"],
                    "x-sha256": row["sha256"],
                    "x-size": str(row["size"]),
                    "x-owner-key": key,
                    "x-name": quote(row["name"]),
                },
            )
            if response.get("sha256") != row["sha256"] or response.get("size") != row["size"]:
                raise ValueError("relay blob verification response mismatch")
            self.relay_blobs.add((row["id"], row["sha256"]))
            if not row["remote_published"]:
                # Report building waits for this; the event republishes the document.
                with self.store.tx() as db:
                    db.execute("UPDATE artifacts SET remote_published=1 WHERE id=?", (row["id"],))
                    self.store.emit(
                        db, row["experiment_id"], "artifact_published", {"artifact_id": row["id"]}
                    )

    async def relay_upload_autopilot_blobs(self, key, projections, document):
        items = projections.autopilot_items(key.partition(":")[2])
        for entry in document.get("downloads", []):
            item = items.get(entry["blob_id"])
            if not item or not item["available"]:
                if entry["size"] > RELAY_BLOB_MAX:  # recorded, never retried
                    self.operation_errors["relay:blob:" + entry["blob_id"]] = (
                        f"download exceeds the {RELAY_BLOB_MAX}-byte relay blob limit"
                    )
                continue
            if (item["blob_id"], item["sha256"]) in self.relay_blobs:
                continue
            content = await asyncio.to_thread(item["read"])
            if len(content) != item["size"] or hashlib.sha256(content).hexdigest() != item["sha256"]:
                raise ValueError("autopilot download changed before upload")
            response = await self.relay_request(
                "PUT",
                "/api/executor/relay/blobs/" + quote(item["blob_id"], safe=":"),
                content=content,
                headers={
                    "Content-Type": item["media_type"],
                    "x-sha256": item["sha256"],
                    "x-size": str(item["size"]),
                    "x-owner-key": key,
                    "x-name": quote(item["name"]),
                },
            )
            if response.get("sha256") != item["sha256"] or response.get("size") != item["size"]:
                raise ValueError("relay blob verification response mismatch")
            self.relay_blobs.add((item["blob_id"], item["sha256"]))

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

    async def once(self):
        # Polling and publication wait on independent RPCs; SQLite writes stay under the one owner.
        await asyncio.gather(
            self.isolated("relay_commands", self.relay_commands),
            self.isolated("relay", self.publish_relay),
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
