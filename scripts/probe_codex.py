"""Read-only protocol/auth/quota probe; never prints credentials or starts inference."""

import asyncio
import json
from pathlib import Path

from experimenttracker.codex_client import CodexAgent
from experimenttracker.config import CodexConfig, QuotaConfig
from experimenttracker.quota import QuotaGate


async def main():
    agent = CodexAgent(CodexConfig(), Path(".local/probe"))
    try:
        await agent.start()
        payload = await agent.quota()
        gate = QuotaGate(QuotaConfig())
        gate.update(payload)
        decision = gate.decide()
        print(
            json.dumps(
                {
                    "authenticated": True,
                    "quota_fields": sorted(payload),
                    "bucket_ids": list((payload.get("rateLimitsByLimitId") or {}).keys()),
                    "decision": decision.__dict__,
                },
                indent=2,
            )
        )
        await asyncio.to_thread(
            Path(".local/probe/quota-shape.json").write_text, json.dumps(payload, indent=2)
        )
    finally:
        await agent.close()


asyncio.run(main())
