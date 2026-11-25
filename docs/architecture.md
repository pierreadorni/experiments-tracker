# Architecture

ExperimentTracker has one execution authority: a Python process with a SQLite
database on local disk and one ownership lock. Its FastAPI server binds to loopback
and serves the built React client. A pinned Codex App Server child runs the agent
sessions; Slurm runs the jobs. An optional Worker relays commands and published
views for hosted and phone access. This is single-owner software with a trusted
agent: worktree isolation and tool checks are workflow controls, not OS
containment or tenant isolation.

## Research loop (contract 4)

New experiments use execution contract 4. Each experiment has one agent session
whose working directory is the experiment's Git worktree on branch
`exp/<date>-<slug>`. The session is resumed while under a context threshold and
otherwise rotates to a new thread seeded from the agent's saved checkpoint. Each
turn's input is small: the brief, the latest checkpoint, events since the last
turn (job outcomes with a log tail, owner messages and answers, integration
results), open obligations and a short instruction block. Everything else is behind
tools, each validated when called:

| Tool | Purpose |
| --- | --- |
| `cluster_info` | Partitions, limits and owner caps |
| `submit_job` | Commit the worktree, export that commit, validate resources, record the intent, submit |
| `job_status`, `job_logs`, `cancel_job` | Bounded reads; exact cancellation |
| `register_result`, `register_figure` | Validated metrics JSON; a small published image |
| `update_report`, `save_checkpoint` | New working report version; versioned notebook |
| `ask_owner` | A question, or an objective approval |
| `request_integration` | Queue the branch for merging |

A turn ends with `continue`, `wait` (needs an active job, a pending answer or a
timer), `needs_owner` (needs an open `ask_owner`) or `done` (needs a published
report and an integrate decision). An experiment must have its objective approved,
by the owner or by Autopilot within its envelope, before `submit_job` succeeds.
Jobs run from an exported commit with per-attempt output directories outside the
repository. Contract-4 jobs may request GPUs within the optional owner resource
limits; no GPU-hour budget is applied.

Agents wake only on events: a job's terminal state, an answer, an owner message,
an integration result or their own timer, debounced by 60 seconds. Healthy jobs
cost no inference. The backend commits, waits, polls and publishes; the agent
researches.

**Integration.** The backend merges an experiment branch into the project base
branch through a serialized queue: a `--no-ff` merge in a dedicated worktree, an
integrator turn on conflict, the project's optional `check_command`, optional owner
approval (`integration.require_approval`), then a fast-forward of the main checkout.
A failed check never merges. **Revert integration** reverts the merge commit.

**Obligations.** Every unfinished experiment has an open obligation owned by the
backend, the agent, the owner or an external system. A watchdog reports experiments
without one. Unexpected errors hold one experiment with backoff; only explicit
scientific or owner decisions fail it.

**Uncertainty.** Submissions and turns can fail after the external system accepted
them. The backend records each intent first and reconciles exact job, action,
thread and turn identities. Uncertainty holds; it never causes a duplicate
dispatch. Commands, events, outbox writes and readiness changes are transactional
and idempotent.

**Admission.** Agent turns run only under fresh subscription quota windows
(defaults reserve 25% of the five-hour and 10% of the weekly window, resuming at
30%/15%). Stale, unknown or denied quota holds turns; job observation and
publication continue. A future API-billed backend would run only when the owner
configures it, under an owner spend budget.

**Archived contracts.** Contract 3 and older records are read-only: their saved
evidence stays readable, exact in-flight identities could settle, but their stages
never advance.

## Autopilot

Autopilot is opt-in per project. Activating a run creates a lead experiment, a
contract-4 experiment with a deterministic id and the marker
`contract.autopilot_lead.run_id`. The lead has project tools instead of compute
tools: `project_status`, `create_experiment`, `message_experiment`,
`control_experiment`, `request_integration`, `update_document` and
`approve_experiment_objective`, plus `cluster_info`, `save_checkpoint`,
`update_report` and `ask_owner`. It wakes once per burst of project updates
(member approval requests, completions and failures, owner messages, policy
revisions, calendar boundaries, deadlines).

The policy (calendar, open-experiment limit, approval envelope, subscription
reserves) is data checked where work is admitted, not a second engine. Pausing or
revising a run holds the lead's turns. The GPU-week budget is not enforced on
contract 4 (T5.1g deferred), so member GPU jobs are bounded only by Slurm and the
owner resource limits. Project documents are versioned and published; missing
results stay TODO. Handoff readiness is the lead's judgment for researcher review,
never scientific certification or submission.

## Hosted relay

The Worker is a relay with no research semantics. The executor publishes
revisioned projection documents (experiments, projects, status, inbox,
conversation, knowledge, Autopilot, maintenance) and blobs, polls the command queue
outbound, commits each command locally and settles it with a receipt. The browser
reads projections and blobs and posts generic commands; refusals come back in
receipts. The local FastAPI server serves the same projection and command endpoints,
so the UI has one data layer. The Worker also holds the browser session, PWA assets
and Web Push follows keyed by projection. Pre-relay D1 tables remain, unused, so
migrations stay append-only. Published views stay available while the executor is
offline.

Browser identity and the executor credential are separate. Hosting is
owner-private. The service worker caches installation assets only, not research
content.

## Testing boundary

The Python suite, the gateway tests (real local D1/R2) and the browser scripts in
`web/scripts/` use synthetic state, fake clocks and demo schedulers and agents. They
are not live acceptance of Slurm, the managed runtime, private hosting or a phone.

## Source map

| Directory | Purpose |
| --- | --- |
| `src/experimenttracker` | CLI, API, workflow tick, SQLite store, scheduler adapters, agent pool |
| `.../research` | Contract-4 session, tools, dispatch, settlement, wakeups, demo |
| `.../engine` | Git worktrees, environments, job runner, integration queue |
| `.../core` | Obligations and liveness |
| `.../publish` | Projections, notifications, command dispatch |
| `.../machine_contexts` | Agent guidance for the local and remote routes |
| `web` | Responsive UI; `web/scripts/` holds synthetic browser checks |
| `gateway` | Worker, append-only D1 migrations, local D1/R2 tests |
| `tools/codex` | Locked project-local Codex runtime |
| `tests` | Workflow, recovery, resources, publication and API tests |
