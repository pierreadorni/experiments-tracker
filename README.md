# ExperimentTracker

Turn a research brief into a reviewed plan, Slurm jobs, and a working report. ExperimentTracker keeps track of agent conversations, compute attempts, metrics, figures, and follow-up notes so work can continue across executor restarts.

This is an early, self-hosted tool for a single researcher with a trusted coding agent and access to a Linux Slurm environment. It was developed on one installation; adapting it to another cluster is part of setup. A local synthetic demo works without Slurm, a ChatGPT login, or hosting credentials.

## Try it locally

Prerequisites: Linux, Git, Python 3.11+, [uv](https://docs.astral.sh/uv/getting-started/installation/), Node.js **24** and npm. Source tests use Node's native TypeScript support. Run from the repository root:

```sh
git clone https://github.com/pierreadorni/experiments-tracker.git
cd experiments-tracker
make setup-demo
uv run --locked experimenttracker --config .local/demo.yaml init
uv run --locked experimenttracker --config .local/demo.yaml doctor
uv run --locked experimenttracker --config .local/demo.yaml demo --max-seconds 60
uv run --locked experimenttracker --config .local/demo.yaml serve
```

Open <http://127.0.0.1:8765> and enter the token from the protected local file `.local/api-token`. Read it privately; do not paste it into an agent conversation. `init` creates absolute paths and refuses to overwrite a configuration. Demo state lives in `/tmp` and can be removed by the host; choose durable storage for real work.

The demo compares linear and quadratic regression on synthetic data. Its model interpretation is deterministic. It demonstrates infrastructure, not scientific results or live scheduler/model integration.

## Install with your agent

Give your agent this instruction:

> Install ExperimentTracker on my system. Read AGENTS.md, docs/install.md, docs/agent-install.md, docs/architecture.md and docs/operations.md. First get the synthetic demo working in an isolated checkout. Discover my actual storage, Slurm, runtime and host-policy constraints. Prepare a local configuration and explain any source adaptations. Reuse my supported managed ChatGPT login without reading credentials. Ask me before the first live experiment or any permanent service installation. Report demo, live compute, hosting and phone checks separately. Preserve existing jobs and research files.

[Installation](docs/install.md) covers the path from demo to live use. [Agent installation](docs/agent-install.md) gives discovery steps, adaptation points and acceptance criteria. [Operations](docs/operations.md) covers access, recovery, backups and upgrades.

## How it works

The Python executor owns a SQLite database on local disk and one ownership lock. It connects to a pinned project-local Codex App Server using managed ChatGPT authentication. An agent prepares the experiment; the backend validates resources against current Slurm capabilities, records dispatch intents and submits jobs. Optional owner limits constrain resources and concurrency.

New brief-driven experiments use execution contract 3: bounded declared source copies per attempt, direct references to producing outputs, explicit reuse with rationale, metrics before independent figures, and versioned working reports. Large scientific data stays in the research environment. Exports collect small published objects; they do not promise reproducibility. Historical/demo contracts retain their original behavior.

The optional TypeScript gateway holds commands and published views in D1 and small artifacts in R2. The executor polls outbound; the browser never controls scheduler state directly. The local interface works over an SSH tunnel without deploying a gateway. See [architecture and portability](docs/architecture.md).

## Current limits

- Linux/POSIX executor; native Windows and macOS execution are not tested.
- Saved execution identifiers and some UI/prompt wording still say `acacia` (local Slurm) and `jean-zay` (a site-specific SSH adapter). Read the adaptation guide before using another cluster.
- Managed ChatGPT authentication, explicit included-usage permission and fresh quota windows are required for live agent work. There is no API-billed fallback.
- This is trusted single-owner software. Agent and scheduler restrictions are workflow controls, not an OS security boundary or tenant isolation.
- Synthetic integration checks do not establish live Slurm, managed account, private hosting or physical phone acceptance on your installation.

## Develop

```sh
make setup             # locked Python, frontend, gateway and Codex dependencies
make check             # Python, frontend, Worker build/types, real local D1/R2 tests
make format
make audit
```

Run checks in a temporary source checkout if an owner serves the current checkout's `web/dist`; the checks rebuild served assets. No live credentials or Slurm jobs are required for the test suite. Contribution guidance is in [CONTRIBUTING.md](CONTRIBUTING.md). Licensed under [MIT](LICENSE).
