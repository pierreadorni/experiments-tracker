"""Actual HTTP/D1/R2 round trip with explicitly simulated scheduler/model; no live research work."""

import asyncio
import json
import tempfile
import time
from pathlib import Path
from uuid import uuid4

import httpx

from experimenttracker.cli import runtime
from experimenttracker.config import Config, GatewayConfig
from experimenttracker.gateway_client import GatewayClient


class LoopbackTestTransport(httpx.AsyncBaseTransport):
    """Translate the test HTTPS origin to the isolated loopback Miniflare process only."""

    def __init__(self):
        self.transport = httpx.AsyncHTTPTransport()

    async def handle_async_request(self, request):
        if request.url.host != "gateway.test":
            raise ValueError("test transport cannot access another host")
        request.url = request.url.copy_with(scheme="http", host="127.0.0.1", port=8788)
        return await self.transport.handle_async_request(request)

    async def aclose(self):
        await self.transport.aclose()


ROOT = Path(__file__).resolve().parents[1]


async def main():
    root = ROOT
    temporary = tempfile.TemporaryDirectory(prefix="et-roundtrip-")
    directory = Path(temporary.name)
    config = Config(
        state_dir=directory / "state",
        job_output_root=directory / "outputs",
        backup_dir=directory / "backups",
        recipe_dir=root / "recipes/toy_compare",
        web_dir=root / "web/dist",
        api_token_file=root / ".local/api-token",
        scheduler_seconds=0.1,
        gateway=GatewayConfig(url="https://gateway.test", credential_file=root / ".local/executor-token"),
    )
    lock, store, workflow = runtime(config)
    executor_closed = False
    bridge = GatewayClient(
        config,
        store,
        transport=LoopbackTestTransport(),
        status_provider=lambda: workflow.status(time.time()),
    )
    user_token = config.api_token_file.read_text().strip()
    async with httpx.AsyncClient(
        base_url="http://127.0.0.1:8788", headers={"Authorization": "Bearer " + user_token}
    ) as user:

        async def command(path, payload):
            response = await user.post(
                path, json={"schema_version": 1, "request_id": str(uuid4()), **payload}
            )
            response.raise_for_status()
            return response.json()

        try:
            received = await command(
                "/api/experiments",
                {
                    "brief": "Compare the fixed synthetic models and publish their validated evidence.",
                    "recipe_id": "toy_compare_v1",
                },
            )
            eid = received["experiment_id"]
            assert received["state"] == "waiting_for_executor"
            for _ in range(150):
                await bridge.commands()
                await workflow.tick(force_observe=True)
                await bridge.heartbeat()
                await bridge.publish_outbox()
                await bridge.upload_artifacts()
                await asyncio.sleep(0.025)
                remote = (await user.get(f"/api/experiments/{eid}")).json()
                if remote["phase"] == "complete":
                    break
            else:
                raise RuntimeError(f"round trip did not complete: {store.experiment(eid)}")
            assert store.experiment(eid)["id"] == eid
            assert workflow.agent.calls == 2 and workflow.scheduler.submissions == 3
            await command(f"/api/experiments/{eid}/notes", {"text": "A gateway-authored researcher note."})
            await bridge.commands()
            await bridge.publish_outbox()
            remote = (await user.get(f"/api/experiments/{eid}")).json()
            assert len(remote["notes"]) == 1
            assert workflow.agent.calls == 2 and workflow.scheduler.submissions == 3
            inbox = (await user.get("/api/inbox")).json()["entries"]
            assert len([n for n in inbox if n["experiment_id"] == eid and n["kind"] == "result_ready"]) == 1
            await workflow.close()
            store.close()
            lock.close()
            executor_closed = True
            # Executor is now stopped; the gateway alone serves its persisted projection and artifact bytes.
            after_stop = (await user.get(f"/api/experiments/{eid}")).json()
            assert after_stop["phase"] == "complete"
            fetched = {}
            for artifact in after_stop["artifacts"]:
                response = await user.get(f"/api/artifacts/{artifact['id']}")
                response.raise_for_status()
                assert len(response.content) == artifact["size"]
                fetched[artifact["name"]] = len(response.content)
            record = {
                "passed": True,
                "mode": "local HTTP/D1/R2 with demo scheduler and model",
                "experiment_id": eid,
                "jobs": 3,
                "agent_actions": 2,
                "notes": 1,
                "result_inbox_entries": 1,
                "readable_after_executor_stop": True,
                "fetched_artifacts": fetched,
                "physical_phone_tested": False,
            }
            await asyncio.to_thread(
                (root / ".local/gateway-roundtrip.json").write_text, json.dumps(record, indent=2)
            )
            print(json.dumps(record))
        finally:
            await bridge.close()
            if not executor_closed:
                try:
                    await workflow.close()
                finally:
                    store.close()
                    lock.close()
            temporary.cleanup()


asyncio.run(main())
