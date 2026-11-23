# Installation

Run the synthetic demo, discover the live environment, configure live execution,
then add hosting if wanted. The application is installed from source: keep the
checkout, because frontend assets, machine guides and the locked Codex runtime are
loaded from it. Treat installation as discovery and acceptance: find facts
read-only, ask the owner only for missing decisions, and record adaptations.

## 1. Isolated checkout and demo

Use Linux with Python 3.11+, Git, uv, GNU make and Node 24/npm, installed through
the host's approved mechanism; no root is needed. Keep the checkout separate from
research environments and running installations. Native Windows/macOS are not
supported (POSIX locking, Linux filesystem checks).

```sh
git clone https://github.com/pierreadorni/experiments-tracker.git
cd experiments-tracker
make setup-demo        # uv sync --locked, npm ci and a frontend build
uv run --locked experimenttracker --config .local/demo.yaml init
uv run --locked experimenttracker --config .local/demo.yaml doctor
uv run --locked experimenttracker --config .local/demo.yaml demo --max-seconds 60
uv run --locked experimenttracker --config .local/demo.yaml serve
```

`demo` runs one contract-4 experiment in a temporary directory with a scripted
agent and a demo scheduler: an approval request, one tiny CPU job, a registered
result and a ready report. It prints a JSON summary and exits 0, or 2 if the time
limit ends first. It needs no Slurm, model or hosting.

`serve` opens the UI at <http://127.0.0.1:8765>; enter `.local/api-token` privately
(the CLI prints paths, never the token). Run `init` at the checkout root; it refuses
to overwrite configuration, and `config.example.yaml` paths are only illustrative.
For remote access, forward the loopback port: `ssh -N -L 8765:127.0.0.1:8765 HOST`.
If 8765 is taken, change `port` in the generated configuration and the tunnel.

## 2. Discover the live environment

Record OS, tool versions and the noninteractive PATH, then establish:

- **Placement.** An approved host and process placement. A writable login node is
  not permission for a permanent daemon.
- **Storage.** Retained local disk for SQLite (NFS/CIFS are refused; `/var/tmp` is
  only a candidate), a job output path visible to compute nodes, and a separate
  backup destination with a retention policy.
- **Slurm.** Working `sbatch`, `squeue`, `sacct`, `sinfo`, `scontrol`, `sacctmgr`;
  account/QoS/partition authorization (a visible partition grants nothing),
  accounting lag, job-name/comment visibility and Python on compute nodes. Use
  read-only queries and authorized `sbatch --test-only`; neither replaces a real job.
- **Agent runtime.** Outbound HTTPS, the managed login on the owning host and any
  institutional limits on agents or background services.

A machine without Slurm runs the demo but needs source adaptation for real work.

### Portability map

| Area | Where to look |
| --- | --- |
| Paths and limits | `src/experimenttracker/config.py`, generated `.local/live.yaml` |
| Linux assumptions | `cli.py`, `doctor.py`; POSIX flock, GNU `stat -f -c %T` |
| Local Slurm policy | `slurm.py`: capabilities, authorization, wrapper flags, exact reconciliation |
| Saved context identifiers | `execution_settings.py`, `research/`, `web/src/types.ts` |
| Agent machine guides | `src/experimenttracker/machine_contexts/*.md` |
| Remote scheduler and transfer | `remote.py`, `remote_worker.py`, `research/remote_*.py` |
| Hosting | `gateway/src/index.ts`, `push.ts`, `gateway/drizzle` |

In saved records, `acacia` names the owner's local Slurm route; it does not need
that hostname. Keep the identifier and adjust the machine prose for your site.
`executor_id` is the real configurable identity and must match the gateway.
`jean-zay` is a specific remote adapter (SSH/rsync, 600-second minimum polling,
memory-flag translation); a different SSH alias does not make it generic. For
another remote cluster, write and test an adapter or stay on the local route. The
machine guides hold historical observations; discover current facts instead.
Containers, other schedulers and multi-user hosting are adaptations, not support.

## 3. Managed runtime

```sh
make setup
./tools/codex/node_modules/.bin/codex --version     # pinned 0.160.1
./tools/codex/node_modules/.bin/codex login status
```

Use this project-local executable. If login is needed, the owner runs `codex login`
(or `login --device-auth`); never read or copy authentication files. Codex runs on
the managed ChatGPT login under fresh quota windows. The adapter checks an exact
version; review generated protocol schemas and run the tests before changing it.
No API-billed backend exists yet; one may be added only when the owner configures
it explicitly with a spend budget.

## 4. Live configuration

```sh
uv run --locked experimenttracker --config .local/live.yaml init --mode live \
  --state-dir /RETAINED/LOCAL/DISK/experimenttracker
```

Edit the generated file privately; every path is absolute.

| Setting | What to establish |
| --- | --- |
| `executor_id` | Stable installation identity; equals the gateway's `EXECUTOR_ID`. |
| `state_dir` | Retained local disk, one owning process. |
| `job_output_root` | Visible on compute nodes. |
| `backup_dir` | Retained storage independent of the state disk. |
| `projects`, `projects_root` | Authorized research checkouts (Git, with an initial commit). |
| `codex.executable`, `codex.expected_version` | This checkout's `tools/codex/.../codex` and `0.160.1` (the generated default still says `0.156.1`). |
| `slurm.partition`, `account`, `qos` | Verified authorized defaults. |
| `slurm.limits`, `slurm.max_concurrent_jobs` | Optional owner ceilings; the cap defaults to 1, `null` removes it. |
| `port` | An unused loopback port. |

Project options: `check_command` (argv for the integration check),
`integration.require_approval` (default false) and `environment.python` (a fixed
interpreter instead of the shared lock cache). The retired `recipe_dir` key is
ignored. See [config.example.yaml](../config.example.yaml) and
[config.py](../src/experimenttracker/config.py).

The agent picks resources from fresh cluster capabilities; the backend validates
each request with `sbatch --test-only`. Contract-4 jobs are CPU-only until GPU
budget accounting is wired.

## 5. Preflight and first live experiment

`doctor` is read-only: it checks files, filesystems, executables and versions, not
permission, quota, scheduler authorization, host policy or phone access. Keep uv,
Node 24 and Slurm tools on the daemon's PATH.

After the owner authorizes the first live brief and its resource budget, run
`serve` with the live configuration, create a small experiment in an authorized
project, approve its objective and watch its first jobs and report. Test restart
only at an authorized safe boundary: saved jobs and actions must be adopted, not
resubmitted. Do not install a permanent service until host policy and storage
retention are confirmed; setup installs none.

## 6. Optional private gateway

The loopback UI with an SSH tunnel is a complete installation. The Worker (see
[architecture](architecture.md#hosted-relay)) is a Sites-compatible build with D1
(`DB`) and R2 (`BUCKET`) bindings.

Local checks use real D1/R2 emulation with synthetic identities:

```sh
npm run build --prefix gateway && npm run typecheck --prefix gateway && npm test --prefix gateway
```

`node gateway/scripts/dev.mjs` runs a local Worker on port 8788 (needs protected
`.local/api-token` and `.local/executor-token`; state under
`/tmp/experimenttracker-gateway-local` or `ET_GATEWAY_STATE`). It is not a
production endpoint.

For production, register your own owner-private Site with the Sites workflow and
keep its identity in `.local/installation/site.json`. `gateway/.openai/hosting.json`
holds binding metadata only. Keep the deployment checkout separate from research
checkouts; `scripts/sync_site.py` refreshes `.local/phone-site` once it has its own
registered manifest. Apply `gateway/drizzle` migrations in order through the
hosting workflow; never rewrite an applied one.

| Binding | Purpose |
| --- | --- |
| `AUTH_MODE=sites` | Platform browser identity; restrict the Site to the owner. `token` mode with `USER_TOKEN` is for controlled tests. |
| `EXECUTOR_ID` | Equals the local `executor_id`. |
| `EXECUTOR_TOKEN` | Independent machine secret, not a browser login. |
| `DB`, `BUCKET` | D1 commands/projections, R2 blobs. |
| `VAPID_PUBLIC_KEY`, `VAPID_PRIVATE_KEY`, `VAPID_SUBJECT` | Optional Web Push. |

Save secrets with a hidden-input tool such as `scripts/save_secret.py` and
configure only paths:

```yaml
gateway:
  url: https://YOUR-PRIVATE-SITE-ORIGIN   # HTTPS origin, no path/query/credentials
  credential_file: /ABSOLUTE/PROTECTED/executor-token   # sent as Authorization
  sites_credential_file: /ABSOLUTE/PROTECTED/sites-token # sent as OAI-Sites-Authorization
  poll_seconds: 5
```

## 7. Acceptance record

Keep a short record in `.local/installation/`: tool versions, verified paths and
storage policy, the config path, source adaptations, checks run and remaining
limits. Report each area separately, and name the missing prerequisite for any area
not tested:

- **Demo:** synthetic lifecycle and loopback login.
- **Source checks:** `make check` in an isolated checkout.
- **Managed runtime:** login status; an authorized real turn under quota admission.
- **Slurm:** authorized real job IDs, outputs and exact-intent recovery.
- **Operations:** service placement, retained disk, online backup, restart adoption.
- **Hosting:** signed-out and non-owner requests cannot read results; the executor
  credential cannot reach browser routes; the executor fetches a command, commits it
  locally, posts its receipt and publishes a blob; replay deduplicates; published
  views stay readable while the executor is stopped.
- **Phone:** sign-in and Add to Home Screen on the actual device.
