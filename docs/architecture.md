# Architecture and scope

ExperimentTracker has one execution authority: a Python process with a local SQLite database and an ownership lock. Its FastAPI server binds to loopback and serves the built React client. A managed Codex App Server child handles agent conversations; Slurm runs scientific stages.

A user brief becomes a persisted experiment contract. Planning/review precedes implementation. For execution contract 3, the backend copies bounded declared sources into each attempt, records the selected producer/output bindings and dispatch intent transactionally, validates resource requests and submits through Slurm. Metrics can publish before independent figures. Working reports have versions; plotting failure need not invalidate already available metrics. Reuse names successful stages with a rationale and retains original output identity. Targeted repairs create new attempts instead of replaying successful compute.

There is no scientific data/checkpoint hashing, recursive inventory, dependency freeze, universal final validation barrier or automatic completion cleanup on contract 3. Small published objects still have transport integrity checks. Historical/demo contracts keep their saved semantics. Convenience exports are not reproducibility bundles for new work.

Submission and agent dispatch can fail after the external system accepted work but before the response was saved. The backend records intent first and reconciles exact job/action/thread/turn identities. Uncertainty causes a hold; automatic duplicate dispatch is not a recovery strategy. User commands, events, outbox writes and publication readiness are transactional and idempotent.

## Optional gateway

The TypeScript Worker holds commands and published projections in D1 and small deliverables in R2. It never decides whether a stage succeeded. The executor polls outbound, commits received commands locally before acknowledgment, and retries publication with stable message IDs and monotonic revisions. Previously published views remain available while the executor is offline.

Browser identity and executor credentials are separate. An executor token cannot sign in a browser user. Hosted production is owner-private; this is not a shared multi-tenant service. The service worker caches only installation assets, not authenticated research content.

## Source map

| Directory | Purpose |
| --- | --- |
| `src/experimenttracker` | CLI, API, workflow/controller, SQLite store, scheduler, runtime, reporting |
| `src/experimenttracker/machine_contexts` | Agent guidance for local and site-specific remote execution |
| `recipes/toy_compare` | Synthetic deterministic legacy demo |
| `web` | Shared responsive interface, frontend/export checks, synthetic browser smoke scripts |
| `gateway` | Worker API, build pipeline, append-only D1 migrations and local D1/R2 tests |
| `tools/codex` | Locked project-local managed runtime |
| `tests` | Workflow, recovery, resources, publication and API tests |

The app retains historical local/remote identifiers `acacia` and `jean-zay`. The latter implements site-specific scheduler policy. See [agent installation](agent-install.md) for the adaptation map. Runtime compatibility is pinned; other clusters and accounts need fresh acceptance, not just a configuration copied from the original host.
