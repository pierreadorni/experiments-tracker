# Refactor design

This document describes the target components. [audit.md](audit.md) explains why;
[todo.md](todo.md) lists the work. Component IDs (C1…C13) are referenced from both.

## 0. Principles

1. **The agent researches; the backend keeps records.** The agent decides what to
   implement, run and analyse. The backend owns submission identity, quota admission,
   job observation, cheap waiting and publication.
2. **Validate at the moment of action.** Tools validate their input when called and
   return errors to the same turn. Nothing important is validated only after a turn.
3. **Every unfinished thing has an owner.** An unfinished experiment always has an
   open obligation owned by the backend, the agent, the owner or an external system,
   with a named wait condition. If none exists, that is an engine bug, and the
   watchdog reports it.
4. **Bugs hold; they never fail.** Unexpected exceptions put one experiment on hold
   with backoff and a log entry. Only explicit scientific or owner decisions fail an
   experiment.
5. **Resolve uncertainty automatically when the facts allow it.** Escalate to a
   human only on real conflicts.
6. **Code lives in Git.** Each experiment has a branch, jobs run from commits, and
   useful work is merged back.
7. **One engine.** Legacy contracts become read-only archives. New behaviour gets a
   new contract number; it never adds branches to an old one.
8. **Spend inference on research.** Waiting, polling, retries and delivery are
   deterministic backend work.

## C1. Error policy in the tick

**Now:** `tick()` maps `ValueError`/`KeyError`/`OSError` from `advance()` to
`fail(eid, "validation_failed")` (`workflow.py:573`).

**Change:**
- Only explicitly signalled scientific or validation failures fail an experiment.
  Introduce `class ExperimentFailure(Exception)` with fields `reason` and `message`.
  Raise it where a real terminal failure is intended (the existing `self.fail(...)`
  call sites stay).
- Any other exception from `advance(eid)` leads to:
  - `hold_reason="executor_error"` with the error text;
  - `log.exception`;
  - a per-experiment backoff: in-memory `self.error_backoff[eid] = (count, next_at)`,
    with delay `min(30 * 2**count, 1800)` seconds; skip `advance(eid)` until
    `next_at`; reset on the next successful `advance`;
  - one inbox notice per distinct error text.
- `validation_failed` stays for genuine output validation
  (`workflow.py:2027`, the output-check path). Those sites call `self.fail` directly
  and do not rely on the tick-level catch.

## C2. Obligations and liveness

### Interim (Tier 0): resume paths and a watchdog over existing tables

**Resume blocked work from steering and answers.** When an owner steering request
(any mode) or an answered question arrives for an experiment whose `hold_reason` is
`agent_stage_blocked`, `agent_stage_failed` or `implementation_blocked`:
- Queue a continuation of the settled blocked action (`store.continue_action`) with
  the steering text as feedback.
- Mark that steering row `applied`, clear the hold, and emit
  `steering_resumed_blocked_work`.
- Manual retry must accept outstanding `later` steering and consume it the same way,
  instead of rejecting.

**Watchdog** (`engine_watchdog()` runs every tick and is cheap). An experiment is
*stuck* when all of these hold:
- its phase is not terminal;
- no agent action is `pending`, `dispatching` or `running`;
- no job is `submitting` or `submitted`, and no transfer is pending;
- `pending_request` is null;
- its `hold_reason` is not owner-owned (`plan_review`, `agent_paused`,
  `new_compute_paused`, `needs_attention`, `quota_hold`, `agent_busy`,
  `cancellation_pending`);
- `hold_reason` and `revision` have not changed for 30 minutes (configurable).

For a stuck experiment, emit one inbox notice `engine_stuck` per
(experiment, hold_reason) with the hold, the error and a hint, and log a warning. The
watchdog never dispatches.

### Target (Tier 1): an obligations table

```sql
CREATE TABLE obligations(
  id TEXT PRIMARY KEY,
  experiment_id TEXT REFERENCES experiments(id),
  project_id TEXT,
  kind TEXT NOT NULL,          -- agent_turn | job | transfer | owner_answer | owner_approval
                               -- | publication | integration | reconcile | timer
  target TEXT,                 -- action id, job intent id, question id, ...
  owner TEXT NOT NULL CHECK(owner IN ('backend','agent','owner','external')),
  wait TEXT,                   -- quota | admission | job:<intent> | answer:<id> | time:<ts> | none
  state TEXT NOT NULL CHECK(state IN ('open','satisfied','abandoned')),
  detail TEXT,
  created_at REAL NOT NULL, updated_at REAL NOT NULL, due_at REAL);
CREATE INDEX obligations_open ON obligations(experiment_id) WHERE state='open';
```

- Open and close obligations in the **same transaction** as the state change that
  causes them (action queued, job submitted, question asked, and so on).
- The displayed status is derived: the highest-priority open obligation, ranked
  owner > agent > backend > external. `hold_reason` becomes a read-only computed
  field kept for the UI and gateway.
- **Liveness invariant:** every non-terminal experiment has at least one open
  obligation, and each one is live:
  - backend obligations are handled within N ticks;
  - agent obligations have a queued or running action;
  - owner obligations have an inbox notice;
  - external obligations point at a non-terminal job or transfer;
  - timer obligations have a future `due_at`.
- The watchdog checks this invariant instead of applying the heuristic above.

## C3. Outbox by reference

**Now:** `Store.emit()` serializes `self.projection(eid)` into `outbox.payload` for
every event. Publishing only ever sends the payload of the latest revision; replay
of older events sends identity and revision only.

**Change (no gateway change needed):**
- `emit()` inserts `outbox(id, payload='{}')`; the projection is no longer computed
  inside each transaction.
- `GatewayClient.publish_outbox()` builds the message at send time. For each latest
  undelivered event of an experiment, it reads `store.projection(eid)` and the
  current `experiments.revision` in one transaction. If the revision equals the
  event's revision, it sends `{schema_version, event_id, experiment: projection}`.
  If not, it skips; the newer event will be sent.
- Retention (`retention.py`) only matters for legacy rows; leave the code in place
  for old databases. Reference-only `{}` rows have no snapshot to validate or prune.
- The local API already reads projections directly; check `api.py` for any reader
  of `outbox.payload`.

Artifact upload also used the latest acknowledged snapshot as proof of metadata
registration. SQLite schema 23 adds compact `artifact_registrations` receipts
(artifact ID, event ID, transport SHA-256 and size), committed with the exact
publication acknowledgement. Upload checks these receipts or a legacy acknowledged
snapshot, so progress during an in-flight publication and publisher restarts do not
allow unregistered artifacts to upload. No gateway schema or protocol change is
needed.

Effects: less write volume, faster transactions, and no database growth from
projections.

## C4. Agent pool

**Now:** lanes are cloned `Workflow` objects. Global blocks: `agent_task` on the main
lane, `research.busy`, `supervisor.blocks_dispatch`.

**Target:**
```python
class AgentPool:
    def __init__(self, config, slots: int): ...      # one CodexAgent per slot
    async def submit(self, job: AgentJob) -> None    # role, owner key, priority, coroutine
    def busy(self, owner_key) -> bool                # at most one active job per experiment/project
```
- Roles: `experiment`, `research` (Autopilot), `supervisor`, `conversation`, `title`,
  `monitor`. They share the slots and differ in priority:
  conversation > experiment > research > supervisor > title.
- The quota gate is checked once per submission.
- No role blocks another globally. `pool.busy(key)` replaces `research.busy` and
  `blocks_dispatch`.
- Interim (Tier 0): keep the lanes, but make Autopilot, supervisor and conversation
  turns occupy a lane instead of blocking every lane. This is only safe if each of
  those uses its own `CodexAgent`. Verify first; if it is not trivial, move the work
  to Tier 1.

## C5. Uncertainty resolver

One module, `resolver.py`, runs each tick for rows in uncertain states.

**Slurm submission** (`jobs.state='submitting'` with no `job_id`):
1. Query `squeue` and `sacct` by the intent comment (existing reconcile code).
2. Exactly one match: adopt it.
3. Several matches: quarantine (`reconcile` obligation owned by the owner) and notify.
4. No match, and `now - submitted_at > accounting_lag_seconds` (default 600, per
   execution context), and the `sacct` query itself succeeded: mark the intent
   `not_submitted` with a receipt. The next admission may submit a **new** intent.
5. Scheduler unreachable: keep waiting with backoff; never conclude anything.

**Codex turn** (`agent_actions.state='uncertain'`):
1. Read the thread. If a turn exists whose input matches the saved prompt hash,
   adopt it (as in the existing `reconcile_action`).
2. The thread is readable, no such turn exists, and more than 5 minutes have passed
   since dispatch: mark the action failed with `turn_not_started`, then continue it
   with a new action (no duplicate risk, because the thread proves absence).
3. The thread is unreadable: retry with backoff. After 6 failures, open an owner
   obligation for this experiment only; other experiments continue.

Every decision writes an event with the evidence used.

## C6. Git-native workspaces

### Model

| Item | Design |
|---|---|
| Branch | `exp/<YYYYMMDD>-<slug>` from the project base branch (default `main`). |
| Worktree | `<repo>/.worktrees/<slug>`, excluded via `.git/info/exclude`, or under a configured path outside the repo. |
| Record | Table `experiment_branches(experiment_id PK, repo, branch, worktree, base_commit, head_commit, state, merged_commit, created_at, updated_at)`; state is one of `active`, `integrating`, `merged`, `abandoned`. |
| Who commits | The **backend**, never the agent. The agent edits files. Commits happen before each job submission and at the end of each agent turn: `git add -A` respecting `.gitignore`, skipping untracked files larger than 5 MB (reported back to the agent), with author `ExperimentTracker Agent`. |
| Job source | `git archive <sha> \| tar -x -C <attempt>/src`; the job runs with `EXPERIMENT_SOURCE=<attempt>/src`. The job records `commit`. A ref `refs/et/jobs/<intent>` keeps the commit reachable. |
| Remote jobs | The exported `src/` is rsynced as today, without project-wide syncs. |
| Outputs | Unchanged: per-attempt output directories outside the repo, with direct output references. |
| Environment | Shared cache inside the project folder, `<repo>/.experimenttracker/envs/<lockhash>/` (excluded via `.git/info/exclude`; tracker-created projects already ignore `.experimenttracker/`), built by `uv sync --frozen` with `UV_PROJECT_ENVIRONMENT`. `lockhash` = sha256 of `uv.lock`, or of `pyproject.toml` plus `requirements*.txt` when there is no lock. Projects can instead declare a fixed interpreter (`project.environment.python`, e.g. conda). A branch that changes dependencies gets its own environment, built once. Environments are read-only for jobs. |
| Data and weights | Declared read-only `data_roots` in project settings, passed to the agent and jobs as paths or env vars. Never copied into the repo. |
| Main checkout | The tracker never writes to the user's main working tree, except the fast-forward at the end of integration. |

### Integration (merge queue, serialized per repository)

Integration is **automatic** (owner decision, 2026-11-12): there is no approval gate.
Work that passes conflict resolution and the project check merges by itself.

Triggers:
- the agent calls `request_integration(reason)` when its code is worth keeping;
- at experiment completion, the agent's final status says whether to integrate
  (`integrate: true|false`, with a reason);
- Autopilot policy requests it;
- the owner clicks **Integrate**.

The owner can mark an experiment **Don't merge**. Every integration is a `--no-ff`
merge commit, so it can be undone with `git revert -m 1 <merge>`; the UI offers
**Revert integration**, which the backend applies through the same queue.

Steps:
1. Commit the experiment worktree. Record an `integration` obligation.
2. In a dedicated worktree `<repo>/.worktrees/_integration`, check out the base
   branch and `git merge --no-ff exp/<slug>`. Use merges, not rebases, so job commits
   are never rewritten.
3. On conflict, run an integrator agent turn in that worktree. Input: the conflicted
   files, both branches' commit logs and the experiment summary. The backend then
   checks for leftover conflict markers and `git diff --check`.
4. Run the project's check command (`project.check_command`, optional, e.g.
   `pytest -q tests`) with a bounded timeout, locally or as a small Slurm job.
5. Optional approval: only if the project sets `integration.require_approval: true`
   (default **false**), open an owner approval obligation with the diff stat and
   summary. Otherwise continue directly. A failed check never merges; it returns to
   the experiment's agent as an event (fix and re-request) and is visible in the UI.
6. Apply: `git -C <main checkout> merge --ff-only <integration commit>`. This refuses
   safely if the user's uncommitted edits would be overwritten; in that case, open
   an owner obligation ("commit or stash your edits to X, then retry"). If the base
   branch moved meanwhile, go back to step 2.
7. Mark the branch `merged`, and remove the experiment worktree after the experiment
   is complete. The branch keeps the code.

**Long-running branches** may merge the base branch into themselves when idle
(`sync_with_base`). On conflict, abort and leave it for integration.

**Negative or abandoned experiments** are not merged. The branch is marked
`abandoned` and its worktree removed when the experiment ends.

### Conventions supplied to agents (project AGENTS.md template)

- Put experiment-specific code in `experiments/<slug>/`.
- Changes to shared code stay backward compatible: add flags or config, keep defaults.
- No data, checkpoints or outputs in Git; read data through the declared data roots.
- Add or adjust tests when shared code changes.

### Sandbox

The agent's writable roots are the experiment worktree plus scratch space. Commits
are made by the backend, so the agent needs no write access to `.git`. Read-only Git
commands must work, because the worktree's git directory under `<repo>/.git` is
readable. Verify this with the pinned Codex runtime.

### Migrating existing detached worktrees

The CLI command `experimenttracker workspaces adopt-legacy [--apply]`:
- for each locked detached worktree, creates branch `exp/legacy-<short-id>` at its
  HEAD and commits its pending changes there;
- records the branch and never touches the main checkout;
- defaults to a dry run.

## C7. Research loop (execution contract 4)

**Session.** One Codex thread per experiment, with `cwd` set to the experiment
worktree. It is resumed while under a context threshold. Otherwise it rotates: a new
thread is seeded from the checkpoint.

**Turn input** (target under 8 KB):
- brief and owner constraints;
- latest checkpoint (Markdown, at most about 4 KB, written by the agent);
- events since the last turn: job terminal states with a short log tail, owner
  messages and answers, integration results;
- open obligations;
- one short static instruction block.

Cluster, resource and project-knowledge data move behind tools.

**Tools** (extend the controller's MCP socket; each validates its input immediately):

| Tool | Purpose |
|---|---|
| `cluster_info()` | Partitions, GPUs, limits, owner caps (cached). |
| `submit_job(request_id, name, command, resources, inputs?, depends_on?, tensorboard?, result_file?)` | Backend commits the worktree, exports it, validates resources (`sbatch --test-only`, cached), records the intent, then submits. Returns the intent and commit. Refused until the plan is approved. |
| `job_status(intent_ids?)`, `job_logs(intent_id, tail?)` | Bounded reads. |
| `cancel_job(request_id, intent_id, reason)` | Existing semantics. |
| `register_result(intent_id, file)` | Metrics JSON: finite numbers, metric/variant/unit. |
| `register_figure(path, caption)` | Small image copied to stable published storage. |
| `update_report(request_id, document)` | New working report version. |
| `save_checkpoint(markdown)` | Versioned research notebook. |
| `ask_owner(request_id, question, kind=question\|approval, options?, blocks=[...])` | Owner obligation. Plan approval is `kind=approval`. |
| `request_integration(reason)` | Queues C6 integration. |

**Turn end schema** (small):
`{status: "continue"|"wait"|"needs_owner"|"done", summary: str, next: str, integrate?: {value: bool, reason: str}}`
(`integrate` is required with `done`; see C6).
The backend enforces it:
- `wait` needs a live condition (active job, pending answer, or a timer from `next`);
- `needs_owner` needs an open `ask_owner`;
- `done` needs a published report;
- `continue` queues the next turn right away, subject to quota.

**Review.** Manual experiments must get `ask_owner(kind=approval)` approved before
`submit_job` succeeds. Activated Autopilot approves automatically within its envelope.
A material objective change needs another approval.

**Wakeups** come only from events: job terminal state, actionable measured stall,
answer, steering, or the agent's own timer. They are coalesced with a 60-second
debounce. Healthy jobs cost no inference.

**Steering** becomes an owner message added to the next turn's input; if the session
is idle, it wakes it. The `later`/`restart` modes go away.

**Removed for contract 4:** the `ResearchPlan`/`Implementation`/`Analysis` manifests,
the separate plan/prepare/interpret phases, the separate title turn (the title comes
from the first summary), the stage graph as execution authority, and repair-candidate
folders.

## C8. Autopilot as a policy layer

- A project-level lead session uses the same loop, plus project tools:
  `create_experiment(brief)`, `message_experiment(id, text)`,
  `request_integration(id)`, `update_document(name, text)`.
- The policy (calendar, GPU-week budget, approval envelope, reserves) is data
  enforced at admission. It is not a second engine.
- Foundation documents become optional working documents, not a gate.

## C9. Gateway as a thin relay

- **D1 tables:** `commands` (queue), `projections(key, revision, json)`,
  `push_subscriptions`, and auth. R2 holds blobs.
- **Executor:** pushes typed projection documents (experiment, project, executor
  status, inbox).
- **Gateway:** checks authentication, size and monotonic revision only. Domain
  validation stays in Python.
- **Local FastAPI:** serves the same projection and command endpoints, so the UI has
  one data layer.
- Existing migrations stay append-only. A new migration adds the generic tables; old
  tables are no longer used.

## C10. Freezing legacy contracts

- Contracts other than 3 (and later 4) become archived:
  - reports, artifacts and events stay viewable;
  - `advance()` skips them;
  - at startup, a non-terminal legacy experiment gets hold `legacy_contract_archived`
    and an inbox notice; the owner may cancel it.
- Then delete the legacy branches (`toy_compare_v1` execution, plan schemas 1/2,
  `strict_reproducibility`, contract ≤2 repair and validation, `Preparation`).
- The new demo runs the current contract with a fake agent and a fake scheduler.
- After contract 4 is default and contract-3 experiments have drained, archive
  contract 3 the same way.

## C11. Module layout of the new core

```
experimenttracker/
  core/      store.py  obligations.py  outbox.py  errors.py
  engine/    runner.py      # independent periodic tasks, each isolated (C1)
             jobs.py        # admission, submission, observation
             resolver.py    # C5
             agents.py      # C4 pool, dispatch, turn reconciliation
             workspaces.py  envs.py  integration.py   # C6
             watchdog.py    # C2
  research/  session.py  prompt.py  tools.py          # C7
  autopilot/ policy.py  lead.py                        # C8
  publish/   projections.py  gateway.py               # C3, C9
  legacy/    readers for archived contracts           # C10
```

`runner.py` replaces the single sequential tick with separate asyncio tasks, each on
its own cadence: observe jobs, dispatch agents, resolver, publish, backup, watchdog.
Each catches and logs its own errors, and slow I/O always runs in threads.

## C12. Scenario test harness

- **Fakes:** `FakeClock` (all engine time goes through it); `FakeSlurm` (programmable
  outcomes, lost `sbatch` responses, accounting lag, multiple matches); `FakeCodex`
  (scripted tool calls and turn results, lost turn-start responses, unreadable
  threads). Git runs for real in temporary directories.
- **Execution:** `run_until(predicate, max_steps)` advances the clock and runs one
  engine step at a time. No real sleeps, so no flaky timing.
- **Scenarios:**
  - blocked stage resumed by steering;
  - job failure with a reused checkpoint;
  - quota pause, then restart, then resume;
  - lost submission resolved after the accounting lag;
  - lost turn start;
  - plot failure keeping metrics;
  - a question in one branch while another continues;
  - healthy long job with zero turns;
  - two experiments editing the same file, with conflict resolution at integration;
  - watchdog detection of an orphaned experiment.

## C13. Docs and process

- `AGENTS.md` shrinks to about 40 lines of plain rules.
- Docs become: install, operate, architecture, and this refactor folder.
- One sentence states that synthetic tests are not live acceptance; it is not
  repeated everywhere.
- Installation evidence stays in `.local/installation/` without ceremony.

## Migration strategy

- Use a strangler approach. Each todo phase ships independently through the normal
  owner-authorized update.
- Tier 0 and Tier 1 change the current engine in place.
- Contract 4 (C7) is built beside contract 3 behind a feature flag
  (`research_loop: true` per project). Once proven, it becomes the default for new
  experiments. Contract-3 experiments finish on the old engine, then that engine is
  archived (C10) and deleted.
- Every schema change is additive. Saved job, action and turn identities keep their
  meaning.
