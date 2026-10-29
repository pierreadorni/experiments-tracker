# Agent installation and system adaptation

Treat installation as a short discovery-and-acceptance task. Produce a working local demo, a concrete local configuration, a record of adaptations and separate acceptance results. Ask only for missing owner decisions; discover facts read-only where possible.

## Discovery before live execution

1. Read AGENTS.md, install.md, architecture.md, operations.md and gateway.md if hosting is requested. Inspect configuration validation, CLI help, locked manifests and source before assuming commands exist.
2. Record OS, Python/uv/Node/npm/Git versions and noninteractive PATH. Use Node 24. Install from locked manifests in this checkout, without modifying research environments.
3. Inspect filesystems, mount type and retention for state/output/backups. State must be local disk; outputs must be compute-visible. Establish service policy with the owner/admin where it cannot be inferred. Never treat a writable login node as permission for a permanent daemon.
4. Run the labeled local demo first. Inspect report/figure and read-only `doctor`. Keep its state and port separate from a live owner. Never build over an active owner's served `web/dist`.
5. Discover Slurm executables, partition/node/GPU details, account/QoS authorization, accounting lag and exact job-name/comment visibility. Use read-only scheduler queries and authorized `sbatch --test-only`; neither substitutes for a real accepted compute test.
6. Check the project-local Codex version and login status. Let the human complete login. Never read or extract credentials, install API fallback or invent quota permission. Do not make inference calls merely to prove authentication without authorization.
7. Generate config with `init`, then set concrete absolute paths, stable executor identity, pinned executable/version, scheduler defaults, optional owner limits and an unused port. Keep it ignored and private.
8. Summarize the intended live test and resource budget to the owner before executing it. Keep synthetic, managed-runtime, real Slurm, restart and phone evidence distinct.

## Portability map

| Area | Configuration or source to inspect |
| --- | --- |
| Local paths and limits | `src/experimenttracker/config.py`, generated `.local/live.yaml` |
| Initialization and Linux assumptions | `src/experimenttracker/cli.py`, `doctor.py`; POSIX flock and GNU `stat -f -c %T` |
| Local Slurm policy | `src/experimenttracker/slurm.py`; fresh capabilities, authorization, wrapper flags, exact reconciliation |
| Saved context identifiers | `execution_settings.py`, `controller.py`, `iteration.py`, `workflow.py`, `web/src/types.ts`, gateway schemas |
| Agent machine instructions | `src/experimenttracker/machine_contexts/*.md`, workflow prompt builders |
| Remote scheduler/transfer policy | `remote.py`, `remote_worker.py`, execution settings, tests/test_remote_execution.py |
| Hosting/auth | `gateway/src/index.ts`, `push.ts`, gateway bindings, build and migration scripts |

The `acacia` context means the owner's local Slurm route in saved records. It does not require that DNS hostname. Keep the identifier for compatibility; adjust inaccurate machine prose and prompt instructions for your installation. `executor_id` is the actual configurable executor identity and must match the gateway. Do not blindly replace `acacia` across saved state, schemas and tests.

The `jean-zay` context is a specific remote Slurm adapter with SSH/rsync, minimum 600-second polling and memory-flag translation for Jean Zay. Setting a different SSH alias does not make that policy generic. For another remote cluster, compare required Slurm flags, memory semantics, paths, modules, authorization and accounting. Implement and test a suitable adapter/schema change or stay on the local route. Do not rename a saved context or silently reuse incompatible policy. The machine guide contains historical account/hardware observations; discover current facts instead of using them as authorization.

A pure Python wheel is not the supported install path: frontend assets, recipe scripts and machine guides are loaded from the source layout. Containers, alternate schedulers, multi-user hosting and native Windows/macOS require additional work. Document them as adaptations, not existing support.

## Preserve execution semantics during adaptation

SQLite owns execution. D1/R2 hold commands and published views. Persist job/action/transfer intents before dispatch, commit events/outbox/readiness transactionally, deduplicate commands and reconcile exact saved identities after uncertainty. A missing queue entry is never permission to retry a submission. An unresolved turn is never permission to start another turn.

Future brief-driven work uses execution contract 3 / plan schema 3: at most 16 MiB of declared small sources per attempt, direct producing-output references, explicit reuse and rationale, independently published metrics and figures, and versioned working reports. Preserve historical contracts. Do not add scientific hashing, recursive output inventories, dependency freezes, a final validation barrier or completion cleanup. Retain lightweight scientific checks of shapes, units, splits and requested repetitions.

Agents may edit selected project folders. Worktree isolation protects concurrent source edits; it is not OS containment. The backend owns scheduling, but trusted agent shell access is not a security boundary against arbitrary unmanaged behavior. Keep this a single trusted owner.

## Installation acceptance receipt

Save a short ignored `.local/install-receipt.md` with tool versions, verified paths/storage policy, config path (no contents containing secrets), source adaptations, commands/checks and remaining limits. Report these independently:

- **Demo:** deterministic synthetic lifecycle, report, figure, loopback login.
- **Source checks:** `make check` in an isolated build checkout, with test counts and failures.
- **Managed runtime:** supported login status; a separately authorized real turn and quota admission if tested.
- **Slurm:** separately authorized real job IDs/outcomes, output access and exact-intent recovery if tested.
- **Operations:** approved service placement, retained disk, successful online backup and restart adoption if tested.
- **Hosting/phone:** private browser access, executor authentication, D1/R2 publication, actual phone sign-in/install if tested.

If something is unavailable, complete independent work and identify the specific remaining prerequisite. Do not relabel mocked boundaries as live acceptance.
