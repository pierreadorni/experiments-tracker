"""Local acceptance of the relay: executor and browser against an isolated Miniflare Worker.

Fake D1/R2 on loopback, fresh generated tokens in a temporary directory, the demo scheduler and
agent, and no real Site. It never reads `.local/` and touches no live state.

Flow: relay manifest, browser command POST, executor command fetch, local dispatch commit,
receipt, browser receipt read, projection PUT and browser read, blob PUT and browser read,
plan approval, completion, readability with the executor stopped, push follow by key.
"""

import asyncio
import base64
import hashlib
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import httpx

from experimenttracker import clock
from experimenttracker.cli import OwnershipLock
from experimenttracker.codex_client import DemoAgent
from experimenttracker.config import Config, GatewayConfig
from experimenttracker.gateway_client import GatewayClient
from experimenttracker.research import demo
from experimenttracker.store import Store
from experimenttracker.workflow import Workflow

ROOT = Path(__file__).resolve().parents[1]
GATEWAY = ROOT / "gateway"
ORIGIN = "https://gateway.test"


class LoopbackTestTransport(httpx.AsyncBaseTransport):
    """Translate the test HTTPS origin to the isolated loopback Miniflare process only."""

    def __init__(self, port):
        self.port = port
        self.transport = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request):
        if request.url.host != "gateway.test":
            raise ValueError("test transport cannot access another host")
        request.url = request.url.copy_with(scheme="http", host="127.0.0.1", port=self.port)
        return await self.transport.handle_async_request(request)

    async def aclose(self):
        await self.transport.aclose()


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def b64url(data):
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def subscription():
    """A syntactically valid synthetic subscription; nothing is ever delivered to it."""
    script = "const k=require('node:crypto').createECDH('prime256v1');k.generateKeys();process.stdout.write(k.getPublicKey())"
    point = subprocess.run(["node", "-e", script], check=True, capture_output=True).stdout
    return {
        "endpoint": "https://fcm.googleapis.com/fcm/send/synthetic-acceptance",
        "expirationTime": None,
        "keys": {"p256dh": b64url(point), "auth": b64url(os.urandom(16))},
    }


def start_worker(directory, port):
    local = directory / "local"
    local.mkdir()
    (local / "api-token").write_text(uuid4().hex + uuid4().hex)
    (local / "executor-token").write_text(uuid4().hex + uuid4().hex)
    subprocess.run(["npm", "run", "build", "--prefix", str(GATEWAY)], check=True, capture_output=True)
    keys = directory / "vapid.json"
    subprocess.run(
        ["node", str(GATEWAY / "scripts/generate-push-keys.mjs"), str(keys)], check=True, capture_output=True
    )
    environment = {
        **os.environ,
        "ET_GATEWAY_LOCAL": str(local),
        "ET_GATEWAY_PORT": str(port),
        "ET_GATEWAY_STATE": str(directory / "gateway-state"),
        "ET_GATEWAY_VAPID_FILE": str(keys),
    }
    process = subprocess.Popen(
        ["node", str(GATEWAY / "scripts/dev.mjs")],
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    line = process.stdout.readline()  # blocks until the Worker is ready or the process exits
    if "ready" not in line:
        process.kill()
        raise RuntimeError("gateway did not start: " + line)
    return process, local


async def main():
    directory = Path(tempfile.mkdtemp(prefix="et-relay-acceptance-"))
    port = free_port()
    worker, local = await asyncio.to_thread(start_worker, directory, port)
    base = Config(
        state_dir=directory / "unused",
        job_output_root=directory / "unused",
        backup_dir=directory / "unused",
        web_dir=ROOT / "web/dist",
        api_token_file=local / "api-token",
        gateway=GatewayConfig(url=ORIGIN, credential_file=local / "executor-token"),
    )
    # The isolated synthetic contract-4 project and scripted agent the CLI demo uses.
    config = demo._temporary_config(base, directory).model_copy(
        update={
            "gateway": base.gateway,
            "api_token_file": base.api_token_file,
            "scheduler_seconds": 0.1,
        }
    )
    lock = OwnershipLock(config.state_dir)
    store = Store(config.state_dir / "workflow.sqlite")
    workflow = Workflow(
        config,
        store,
        demo._ResearchDemoScheduler(config.state_dir / "demo-scheduler"),
        DemoAgent(),
    )
    agent = demo._ScriptedDemoAgent(workflow)
    primary = workflow.agent_pool.primary
    primary.agent = agent
    workflow._attach_handlers(primary)
    primary.agent_ready = True
    primary.resolved_model = agent.resolved_model
    primary.models = agent.models
    workflow.gate.update(await agent.quota())
    closed = False
    wall_now = clock.now
    bridge = GatewayClient(
        config,
        store,
        transport=LoopbackTestTransport(port),
        status_provider=lambda: workflow.status(time.time()),
    )
    executor = bridge.client  # the executor's own authenticated client
    user_token = (local / "api-token").read_text().strip()
    log = []
    try:
        async with httpx.AsyncClient(
            base_url=f"http://127.0.0.1:{port}", headers={"Authorization": "Bearer " + user_token}
        ) as user:

            async def post(kind, target, payload):
                request_id = str(uuid4())
                response = await user.post(
                    "/api/commands",
                    json={
                        "request_id": request_id,
                        "kind": kind,
                        "target": target,
                        "payload": {"schema_version": 1, "request_id": request_id, **payload},
                    },
                )
                response.raise_for_status()
                return response.json()

            async def read(path):
                response = await user.get(path)
                response.raise_for_status()
                return response

            async def projection(key):
                return (await read("/api/projections/" + key)).json()

            # 1. Relay manifest: a fresh Worker has nothing, and the executor sees an empty ledger.
            manifest = await bridge.relay_request("GET", "/api/executor/relay/revisions")
            assert manifest["revisions"] == {}
            log.append("manifest empty")

            # 2. Browser command POST, executor fetch, local dispatch commit, receipt, browser read.
            posted = await post(
                "create",
                None,
                {
                    "brief": demo.BRIEF,
                    "execution_contract": 4,
                    "project_id": "synthetic-demo",
                },
            )
            assert posted["state"] == "pending" and posted["target"] is None
            pending = (await read("/api/commands?state=pending")).json()["commands"]
            assert [c["request_id"] for c in pending] == [posted["request_id"]]
            fetched = (await bridge.relay_request("GET", "/api/executor/relay/commands"))["commands"]
            assert [c["request_id"] for c in fetched] == [posted["request_id"]]
            assert store.rows("SELECT 1 FROM experiments") == []  # nothing before dispatch
            await bridge.relay_commands()  # dispatch commits locally, then posts the receipt
            receipt = (await read("/api/commands/" + posted["request_id"])).json()
            assert receipt["state"] == "applied", receipt
            eid = receipt["result"]["experiment_id"]
            target = f"experiment:{eid}"
            assert store.experiment(eid)["id"] == eid
            assert (await read("/api/commands?state=pending")).json()["commands"] == []
            log.append("create command dispatched and receipt applied")

            # 3. Drive the scripted workflow, publishing through the relay, until the report is ready.
            agent.eid = eid
            scripted_finish = agent.finish

            async def finish(thread_id, turn_id):
                # A hosted report is ready only after its bytes are in the relay, and the closing
                # turn needs it ready: publish through the relay before that turn ends.
                if agent.turn == 4:
                    for _ in range(100):
                        if store.one(
                            "SELECT 1 FROM report_versions WHERE experiment_id=? AND state='ready'", (eid,)
                        ):
                            break
                        await workflow.tick()
                        await bridge.once()
                        await asyncio.sleep(0.1)
                return await scripted_finish(thread_id, turn_id)

            agent.finish = finish
            workflow.workspaces.ensure_branch(eid)
            elapsed = [0, 0]
            clock.now = lambda: wall_now() + elapsed[0]  # skip debounce windows deterministically

            async def cycle():
                bridge.relay_aggregates_at = 0  # status, projects and inbox are throttled; force them
                await workflow.tick()
                await bridge.once()
                if agent.turn > elapsed[1]:
                    elapsed[1] = agent.turn
                    elapsed[0] += 61

            answered = None
            job_window_advanced = answer_settled = False
            settling = idle = last_revision = 0
            for _ in range(600):
                await cycle()
                await asyncio.sleep(0.1)
                experiment = store.experiment(eid)
                if not job_window_advanced and store.one(
                    "SELECT 1 FROM jobs WHERE experiment_id=? AND state='complete'", (eid,)
                ):
                    elapsed[0] += 61
                    job_window_advanced = True
                pending_request = experiment.get("pending_request")
                if (
                    not answered
                    and pending_request
                    and pending_request.get("id") == agent.approval_id
                    and not pending_request.get("answer")
                ):
                    answered = (
                        await post(
                            "answer",
                            target,
                            {"pending_request_id": pending_request["id"], "answer": "accept"},
                        )
                    )["request_id"]
                if (
                    answered
                    and not answer_settled
                    and (await read("/api/commands/" + answered)).json()["state"] == "applied"
                ):
                    # The saved answer starts the owner debounce window; step past it.
                    answer_settled = True
                    elapsed[0] += 61
                report = store.one(
                    "SELECT state FROM report_versions WHERE experiment_id=? ORDER BY version DESC LIMIT 1",
                    (eid,),
                )
                if experiment["phase"] == "complete" and report and report["state"] == "ready":
                    break
                # Network awaits can land an event just after a clock step; a bounded few extra
                # steps past the next debounce window replace the CLI demo's exact tick timing.
                idle = idle + 1 if experiment["revision"] == last_revision else 0
                last_revision = experiment["revision"]
                if idle >= 15 and settling < 8:
                    settling += 1
                    idle = 0
                    elapsed[0] += 61
            else:
                raise RuntimeError(f"relay round trip did not complete: {store.experiment(eid)}")
            assert answered
            for _ in range(5):  # the last receipt and revision settle on later polls
                await cycle()
            assert (await read("/api/commands/" + answered)).json()["state"] == "applied"
            log.append("approval answered through a relay command; experiment complete")

            # 4. Projection PUT and browser read: the stored document is the executor's, by revision.
            stored = await projection(target)
            assert stored["revision"] == store.experiment(eid)["revision"]
            revisions = (await bridge.relay_request("GET", "/api/executor/relay/revisions"))["revisions"]
            assert revisions[target][0] == stored["revision"]
            assert {"status", "projects", "inbox"} <= set(revisions)
            assert (await projection("status"))["document"]["executor_id"] == config.executor_id
            entries = (await projection("inbox"))["document"]["entries"]
            local_ids = {r["id"] for r in store.rows("SELECT id FROM inbox WHERE experiment_id=?", (eid,))}
            assert local_ids and local_ids <= {n["id"] for n in entries}
            assert (await user.get("/api/projections/experiment:" + str(uuid4()))).status_code == 404
            # A stale revision is refused and an executor credential does not open browser routes.
            stale = await executor.put(
                f"/api/executor/relay/projections/{target}",
                headers={"x-executor-id": config.executor_id},
                json={"schema_version": 1, "key": target, "revision": 1, "document": {}},
            )
            assert stale.status_code == 409
            assert (await executor.get("/api/projections/" + target)).status_code == 401
            log.append("projections match the executor; stale revision refused")

            # 5. Blob PUT and browser read: every published artifact has the saved size and digest.
            document = stored["document"]
            assert document["artifacts"], "the experiment published no artifacts"
            fetched_blobs = {}
            for artifact in document["artifacts"]:
                content = (await read("/api/blobs/" + artifact["id"])).content
                assert len(content) == artifact["size"]
                assert hashlib.sha256(content).hexdigest() == artifact["sha256"]
                fetched_blobs[artifact["name"]] = len(content)
            assert any(name.startswith("report") for name in fetched_blobs), fetched_blobs
            log.append(f"{len(fetched_blobs)} blobs read back with matching digests")

            # 6. A note command settles through the same path and reaches the projection.
            note = await post("note", target, {"text": "A relay-authored researcher note."})
            for _ in range(3):
                await cycle()
            assert (await read("/api/commands/" + note["request_id"])).json()["state"] == "applied"
            notes = (await projection(target))["document"]["notes"]
            assert len(notes) == 1

            # 7. The same bytes return the same receipt; different bytes under that id are refused.
            replay = await user.post(
                "/api/commands",
                json={
                    "request_id": note["request_id"],
                    "kind": "note",
                    "target": target,
                    "payload": {"schema_version": 1, "request_id": note["request_id"], "text": "changed"},
                },
            )
            assert replay.status_code == 409

            # 8. Push follow by key (after the last publication, so nothing is delivered).
            device = subscription()
            follow_id = str(uuid4())
            followed = await user.post(
                "/api/push/follows/" + target,
                json={
                    "schema_version": 1,
                    "request_id": follow_id,
                    "subscription": device,
                    "following": True,
                },
            )
            assert followed.status_code == 200 and followed.json() == {"following": True}
            status = await user.post("/api/push/follows/status", json={"endpoint": device["endpoint"]})
            assert status.json()["keys"] == [target]
            log.append("push follow by key listed")

            # 9. Stop the executor: the Worker alone still serves the projection and the bytes.
            await workflow.close()
            store.close()
            lock.close()
            closed = True
            after = await projection(target)
            assert after["document"]["phase"] == "complete"
            first = document["artifacts"][0]
            assert len((await read("/api/blobs/" + first["id"])).content) == first["size"]
            record = {
                "passed": True,
                "mode": "local Miniflare relay with demo scheduler and model",
                "experiment_id": eid,
                "steps": log,
                "projection_revision": stored["revision"],
                "blobs": fetched_blobs,
                "readable_after_executor_stop": True,
                "physical_phone_tested": False,
            }
            print(json.dumps(record, indent=2))
    finally:
        clock.now = wall_now
        await bridge.close()
        if not closed:
            try:
                await workflow.close()
            finally:
                store.close()
                lock.close()
        worker.terminate()
        worker.wait(timeout=15)
        shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
