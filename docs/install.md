# Installation

Start with the local demo, then configure live execution. Hosting is optional. This release is source-installed; there is no published PyPI package or container image. Keep the repository: recipes, machine guidance, frontend assets and the locked Codex runtime are used alongside the Python package.

## 1. Prepare an isolated checkout

Use a Linux host with Python 3.11+, Git, uv, GNU make and Node 24/npm. Install tools through your system's approved mechanism. No root installation is needed for the application. Keep this checkout separate from research environments and existing deployments. Native Windows/macOS are not supported by the Linux filesystem checks and POSIX locking.

```sh
git clone https://github.com/pierreadorni/experiments-tracker.git
cd experiments-tracker
make setup-demo
```

`setup-demo` runs `uv sync --locked`, `npm ci --prefix web` and a production frontend build. It does not install Codex or the hosted gateway. An offline install needs prepopulated uv/npm caches; the demo needs no inference or scheduler once dependencies are installed.

## 2. Complete the synthetic demo

```sh
uv run --locked experimenttracker --config .local/demo.yaml init
uv run --locked experimenttracker --config .local/demo.yaml doctor
uv run --locked experimenttracker --config .local/demo.yaml demo --max-seconds 60
uv run --locked experimenttracker --config .local/demo.yaml serve
```

Open <http://127.0.0.1:8765>. Enter `.local/api-token` privately. The CLI prints paths, not the token. A completed demo should show a report and figure. `demo` returns 2 if its time limit ends before completion; state is preserved. Stop `serve` with Ctrl+C. Do not run two owners of the same state directory.

`init` takes paths from the current directory. Always run it at the checkout root. Do not copy `config.example.yaml` unchanged: its absolute paths are illustrative. Initialization refuses to replace existing configuration. Demo and live configurations in the same checkout share the local browser token, but have distinct state/output directories.

For remote access, forward the loopback port from your own computer, using your host alias:

```sh
ssh -N -L 8765:127.0.0.1:8765 YOUR_HOST
```

The service binds only to loopback. If another service uses 8765, change `port` in your generated configuration and both tunnel ports.

## 3. Discover your live environment

Work with [the agent checklist](agent-install.md). Establish:

- An approved host/process placement and a retained local disk path for SQLite. NFS/CIFS are rejected. `/var/tmp` is only a generated candidate, not a retention guarantee.
- A job output path visible to compute nodes, a separate online-backup destination and a retention policy for source attempts, results and scientific data.
- Working `sbatch`, `squeue`, `sacct`, `sinfo`, `scontrol`, `sacctmgr`, account/QoS/partition authorization, accounting visibility, and Python/runtime access on compute nodes. A visible partition alone does not grant permission.
- Outbound HTTPS for the managed account, supported login on the owning host, and any institutional restrictions on agents or background services.

The local Slurm route is the default. A machine without Slurm can run the demo, but needs source adaptation to run real experiments outside Slurm.

## 4. Install the pinned managed runtime

```sh
make setup
./tools/codex/node_modules/.bin/codex --version
./tools/codex/node_modules/.bin/codex login status
```

`tools/codex/package-lock.json` pins Codex **0.160.1**. Use this project-local executable rather than replacing another project's global runtime. If login is needed, have the owner complete supported `codex login` (or `login --device-auth` where enabled). Reuse the existing supported managed login; do not read/copy authentication files or put tokens in chat. See [official authentication guidance](https://learn.chatgpt.com/docs/auth).

The adapter checks an exact version. Review generated protocol schemas and run the tests before changing versions; see [official App Server documentation](https://learn.chatgpt.com/docs/app-server). Do not configure API keys or a paid fallback. Account/model availability must be established on this installation.

## 5. Generate and adapt live configuration

Choose a verified durable local directory and pass its absolute path:

```sh
uv run --locked experimenttracker --config .local/live.yaml init --mode live \
  --state-dir /YOUR/RETAINED/LOCAL/DISK/experimenttracker
```

Edit the generated file privately before starting it:

| Setting | What to establish |
| --- | --- |
| `executor_id` | A stable installation identity; use the same value in a gateway's `EXECUTOR_ID`. The historical default is `acacia`. |
| `state_dir` | Retained local disk, writable by the owner; only one owning process. |
| `job_output_root` | Absolute path visible on the compute nodes. |
| `backup_dir` | Retained storage independent of the state disk. |
| `projects_root`, `projects` | Authorized research directories; selected folders may be edited. Keep `test-project` for legacy/demo records. |
| `recipe_dir`, `web_dir` | Existing recipe directory and built frontend in this checkout. |
| `api_token_file` | Generated protected token, absolute path, mode 600. |
| `codex.executable` | Absolute path to this checkout's `tools/codex/node_modules/.bin/codex`. |
| `codex.expected_version` | `0.160.1` for the pinned runtime. Generated legacy defaults otherwise expect `0.156.1`; explicitly override both fields. |
| `slurm.partition`, `account`, `qos` | Verified authorized local defaults. `shortrun` is a historical default, not portable discovery. |
| `slurm.limits` | Optional owner ceilings/allowlists; omitted fields defer to Slurm authorization. |
| `slurm.max_concurrent_jobs` | Defaults to 1; explicit `null` removes this executor-wide cap. |
| `port` | An unused loopback port, especially when retaining the demo owner. |

All paths must be absolute. See [config.example.yaml](../config.example.yaml) and the validated fields in [config.py](../src/experimenttracker/config.py). Example live overrides, with placeholders you must replace:

```yaml
codex:
  executable: /YOUR/CHECKOUT/tools/codex/node_modules/.bin/codex
  expected_version: 0.160.1
slurm:
  partition: YOUR_AUTHORIZED_PARTITION
  account: null
  qos: null
  max_concurrent_jobs: 1
  limits: {}  # add owner limits after discussing the desired budget
```

The small CPU defaults are fallbacks, not ceilings for live runs. The agent selects resources from fresh capabilities; the backend validates each request with `sbatch --test-only`. The three short CPU jobs/no-GPU envelope belongs to the synthetic legacy recipe.

## 6. Preflight, then accept live execution

```sh
uv run --locked experimenttracker --config .local/live.yaml doctor
```

`doctor` is read-only. It checks files, filesystem, executable availability and version. It does not prove account permission/quota, scheduler authorization, host policy or phone access. Resolve failures instead of disabling checks. Keep uv, Node 24 and Slurm tools on the daemon's noninteractive PATH; preparation invokes uv independently of the Python executable.

After the owner's explicit authorization for the first live brief and resource budget:

```sh
uv run --locked experimenttracker --config .local/live.yaml serve
```

Select/create an authorized research project in the UI, submit a small brief, review its plan and observe its first real jobs and working results. Use a Git checkout with an initial commit for parallel worktree isolation. Fresh included-usage permission and quota windows gate each agent dispatch. Do not bypass holds or assume a successful demo proves this step.

Test restart only at an authorized safe boundary; the same saved jobs/actions must be adopted rather than submitted again. [Operations](operations.md) describes backup and reconciliation. A permanent service needs confirmed host policy and storage retention first; no systemd unit is installed by setup.

## 7. Optional private gateway and phone access

The loopback UI plus SSH tunnel is a complete local installation. To host published views, follow [gateway setup](gateway.md). Use your own private deployment and storage; no existing installation identity is included in the public snapshot. Validate private browser sign-in and executor access separately. A phone installation must be checked on the actual device.
