# Refactor working todo list

Status: `[ ]` todo · `[~]` in progress · `[x]` done · `[-]` dropped.
Model: **F** = a DeepSeek Flash subagent can do it alone · **F+R** = Flash
implements, a stronger model reviews the diff · **S** = needs a stronger model
(design judgment or a large cross-cutting change).
Size: S < 150 changed lines · M < 500 · L > 500 (split L tasks before delegating).

Each task is one commit. Tasks that edit `workflow.py`, `store.py` or `gateway/src/index.ts`
must run one at a time, or in separate worktrees merged by the coordinator.
Briefing rules and the brief template are in [README.md](README.md).

---

## Phase 1 — Tier 0: fewer stalls now

### [ ] T0.1 Internal errors hold an experiment instead of failing it (C1) · F · S
- **Files:** `src/experimenttracker/workflow.py` (`tick`, around lines 560–576; `fail`, around line 1054).
- **Do:**
  1. Add `class ExperimentFailure(Exception)` (fields `reason`, `message`) to a new
     `src/experimenttracker/errors.py`.
  2. In `tick()`, replace `except (ValueError, KeyError, OSError): self.fail(eid,
     "validation_failed", ...)` with `except ExperimentFailure as exc:
     self.fail(eid, exc.reason, exc.message)`. Leave `ProjectUnavailable` unchanged.
     Any other `Exception`: set hold `executor_error` with the error text, call
     `log.exception`, and back off.
  3. Backoff: `self.error_backoff[eid] = (count, next_at)`, delay
     `min(30 * 2**count, 1800)` seconds. Skip `advance(eid)` while `now < next_at`.
     Clear the entry after a successful `advance(eid)`.
  4. Search `advance()` and the methods it calls for any `raise ValueError` that was
     *meant* to fail the experiment through the tick catch (for example
     `"prepared source does not compile"` and the job-budget check in
     `agent_stage`). Decide case by case:
     - agent-output problems are already caught inside `agent_stage` and turned into
       continuations; leave those alone;
     - convert only true terminal conditions to `ExperimentFailure`;
     - list each decision in the commit message.
- **Acceptance:** a `KeyError` or `OSError` raised from `advance` leaves `phase`
  unchanged and sets `hold_reason='executor_error'`; the next tick inside the backoff
  window does not call `advance` for that experiment; a later successful `advance`
  clears the backoff. All existing tests pass.
- **Tests:** new `tests/test_tick_error_policy.py` (monkeypatch `Workflow.advance`);
  `uv run --locked pytest -q tests/test_tick_error_policy.py tests/test_workflow.py tests/test_generic.py`.

### [ ] T0.2 Steering and answers resume blocked work (C2 interim) · F+R · M
- **Files:** `store.py` (`steer` command, around lines 1373–1405; `retry_agent` and
  implementation-repair action handling, around lines 1040–1290, the
  `SELECT 1 FROM steering ... state IN ('pending','cancelling')` guards);
  `workflow.py` (`advance`, lines 1127–1131; `apply_planning_steering`);
  `iteration.py` (around line 311); `steering.py`; `autopilot.py` (around line 515).
- **Do:**
  1. When a `steer` command (any mode) or an answer arrives for an experiment with
     `hold_reason` in (`agent_stage_blocked`, `agent_stage_failed`,
     `implementation_blocked`), the same transaction must:
     - find the latest settled action for the blocked phase;
     - call the existing continuation mechanism (`continue_action` semantics, with
       feedback = steering text);
     - mark the steering row `applied`, clear `hold_reason`, and emit
       `steering_resumed_blocked_work`.
  2. Manual `retry_agent` and implementation repair must no longer reject pending
     `later` steering. They consume it as feedback and mark it `applied`.
     `restart` steering keeps its current behaviour.
  3. `continue_action` may need a variant usable inside an existing transaction
     (`_db=`). Keep the public behaviour identical.
- **Acceptance:** fixture: an agent stage returns `outcome="blocked"` → hold
  `agent_stage_blocked` → a `later` steering command → exactly one new action is
  queued with the steering text in its feedback; the steering is `applied`; the hold
  is cleared. The same holds for `implementation_blocked`. A duplicate `request_id`
  creates no second action.
- **Tests:** extend `tests/test_iteration_agent_stages.py` and `tests/test_steering.py`.

### [ ] T0.3 Liveness watchdog over existing tables (C2 interim) · F · S
- **Files:** new `src/experimenttracker/watchdog.py`; call it from `Workflow.tick`
  (after `advance`); `config.py` (`watchdog_minutes: int = 30`).
- **Do:**
  1. Implement `stuck_experiments(store, now, minutes)` following the rule in
     design.md C2 (interim).
  2. For each stuck experiment, call `store.notify(eid, "engine_stuck", text)` once
     per (eid, hold_reason). The `inbox` UNIQUE constraint already deduplicates
     identical text, so put the hold reason in the text.
  3. Log a warning. Never dispatch.
- **Acceptance:** an experiment with `hold_reason='agent_stage_blocked'`, no active
  actions or jobs, and 31 minutes unchanged gets exactly one notice. Experiments in
  `plan_review`, `quota_hold` or `agent_paused` get none.
- **Tests:** new `tests/test_watchdog.py` (direct store setup; monkeypatch time).

### [ ] T0.4 Outbox by reference (C3) · F+R · M
- **Files:** `store.py` (`emit`, around line 657); `gateway_client.py`
  (`publish_outbox`, around line 151); check readers of `outbox.payload` with
  `rg "outbox" src tests`; `retention.py` stays, legacy only.
- **Do:**
  1. In `emit`, store `payload='{}'` and stop calling `self.projection(eid)` there.
  2. In `publish_outbox`, inside one `store.tx()`, read the event revision, the
     current `experiments.revision` and `store.projection(eid)`. If the revisions
     differ, skip the row. Otherwise build
     `{"schema_version":1,"event_id":id,"experiment":projection}`. Keep the existing
     `public_projection` and `inbox` additions.
  3. Fix tests that read the outbox payload.
- **Acceptance:** emitting 100 events stores 100 outbox rows of `{}`. Publishing
  sends one message carrying the latest projection. Replay of older events works
  unchanged. Gateway tests (`npm test --prefix gateway`) need no change.
- **Tests:** `tests/test_gateway_client.py`, `tests/test_publication*.py` (whichever exist),
  plus a new size assertion.

### [ ] T0.5 Investigate global agent serialization (C4 interim) · F (investigation) · S
- **Files:** `workflow.py` (`agent_stage` end, around lines 2470–2500; `agent_lane`;
  `start_agent_pool`); `autopilot_controller.py:30`; `knowledge.py:576`;
  `conversation.py`.
- **Do:** produce `docs/refactor/notes/t0.5-serialization.md`. For each blocker
  (`self.agent_task`, `research.busy`, `supervisor.blocks_dispatch`, the
  `active_sql` single-slot check), record:
  - which `CodexAgent` it uses;
  - why it blocks;
  - whether removing the block is safe when `codex.max_sessions > 1`.

  Propose the smallest safe change. Do **not** change code in this task.
- **Acceptance:** the note exists, with file and line evidence. The follow-up becomes
  T0.5b, or moves into T1.6.

### [ ] T0.6 Deterministic timing in tests (C12 prerequisite) · F · M
- **Do:**
  1. `rg -n "range\((10|20|30|50|100)\)|asyncio.sleep|time.sleep" tests`.
  2. Replace fixed tick-count loops with a shared helper `tests/helpers.py:
     wait_until(predicate, *, ticks, timeout=30)` that ticks until the predicate
     holds or the wall timeout expires.
  3. Restore a deterministic version of the deleted test
     `test_known_quota_interruption_resumes_automatically_after_owner_restart`
     (see `git log -S` for its body).
- **Acceptance:** `uv run --locked pytest -q` passes 3 times in a row, plus once
  under load (for example while 3 other copies of the suite run in parallel).

### [x] T0.7 Link the refactor docs · F · S
- Add one line to `AGENTS.md` and `docs/development.md` pointing at `docs/refactor/`.

---

## Phase 2 — Tier 1: consolidation

### [ ] T1.1a Inventory legacy branches (C10) · F · S
- Produce `docs/refactor/notes/t1.1-legacy-inventory.md`. List every branch
  conditioned on:
  - `recipe_id == "toy_compare_v1"`;
  - `planning_version` absent;
  - plan `schema_version` < 3;
  - `iteration.enabled(exp)` false;
  - `strict_reproducibility`;
  - contract ≤2 `auto_repair` (`repairs/` roots);
  - `Preparation`, `freeze_recipe`, `verify_snapshot`, the `validate` stage,
    `dependencies()`.

  For each: file:line, what it does, its tests, and whether display or export code
  still needs it to read archived experiments.

### [ ] T1.1b Archive legacy contracts at runtime (C10) · F · S
- At startup (after migrations) and in `tick`: a non-terminal experiment that is not
  contract 3 gets hold `legacy_contract_archived` plus an inbox notice. `advance`
  returns early for it. Cancel still works.
- **Tests:** one archived fixture (legacy contract created directly in the store).

### [ ] T1.1c New demo on contract 3 (C10) · S · M
- Make `experimenttracker demo` create an `agent_experiment_v1`, contract 3
  experiment. Extend the demo agent (`codex_client.py`, around line 506) so it scripts
  plan → implementation → result for a tiny synthetic regression. `make demo` must
  complete within 60 s, and the browser smoke scripts must use it.

### [ ] T1.1d–h Delete legacy execution code (C10) · F+R · M each
One commit per area, removing only branches that T1.1a marked not needed for reading:
- d: `advance()`;
- e: `agent_stage` legacy `prepare` / `Preparation`;
- f: the validate stage and `strict_reproducibility`;
- g: contract ≤2 repair (`repair_failed_experiments` legacy parts, `repairs/` roots);
- h: `artifacts.py` freeze and verify helpers, `recipes/toy_compare`.

Delete or replace the matching tests. Acceptance for each: the remaining suite passes,
and archived fixtures still render their report via the API.

### [ ] T1.2a Clock indirection (C12) · F · M
- Add `src/experimenttracker/clock.py` with `now()` and `monotonic()`. Replace
  `time.time()` in `src/` mechanically, except in log formatting. Tests can patch
  `clock.now`.

### [ ] T1.2b Scenario harness (C12) · S · M
- Add `tests/scenario/` with `FakeClock`, a programmable `FakeSlurm` (lost responses,
  accounting lag, duplicate matches) and `FakeCodex` (scripted turns, lost turn
  start, unreadable thread), plus a `run_until` helper.
- Port 3 existing scenarios: blocked stage plus steering (T0.2), healthy long job with
  zero turns, and a lost submission.

### [ ] T1.3a–d Obligations table (C2 target) · S (a) · F+R (b–d)
- a: schema migration, `core/obligations.py` API (`open`, `satisfy`, `abandon`,
  `open_for`, `derived_status`), and a priority table.
- b: open and close obligations at their sources: agent actions, jobs, transfers,
  pending requests, plan review, publication.
- c: compute `hold_reason` from obligations in `Store.projection`; stop writing it
  directly (one call site group per commit).
- d: the watchdog checks the liveness invariant.

### [ ] T1.4a Slurm submission resolver (C5) · F+R · M
- New `resolver.py`. Apply the rules in design.md C5 (Slurm) to
  `jobs.state='submitting' AND job_id IS NULL`. Add config
  `accounting_lag_seconds` per execution context. Write receipt events.
- **Tests:** scenario harness (lost `sbatch` response → found / absent after lag /
  duplicate).

### [ ] T1.4b Codex turn resolver (C5) · F+R · M
- Same, for `agent_actions.state='uncertain'`, using `agent.read_turn` and the thread
  read. **Tests:** lost turn start → adopt / absent → continuation / unreadable →
  owner obligation after 6 tries.

### [ ] T1.5 Independent periodic tasks (C11 runner) · S · M
- Split `tick()` into isolated periodic coroutines: observe, dispatch, resolver,
  publish, backup and retention, watchdog, research and supervisor. Each has its own
  interval and its own `try/except` with logging. Keep the order constraints that
  matter, and document them in code comments.

### [ ] T1.6 AgentPool replaces lanes (C4) · S · L → split after T0.5
### [ ] T1.7a–f Split `workflow.py` mechanically (C11) · F · M each
Move code without changing behaviour; each commit keeps the tests green.
- a: prompts → `research/prompt.py`;
- b: `observe_jobs` and submission → `engine/jobs.py`;
- c: `dispatch`, `reconcile_action` → `engine/agents.py`;
- d: publication and reporting helpers → `publish/`;
- e: workspace helpers → `engine/workspaces.py`;
- f: remote helpers.

---

## Phase 3 — Git-native workspaces (C6)

### [ ] T3.1 Branch/worktree module · F · M
- New `engine/gitworkspace.py` (or extend `workspaces.py`): `create(repo, slug, base)`,
  `commit(worktree, message, max_untracked_mb=5)` returning `(sha, skipped)`,
  `export(repo, sha, target)` via `git archive`, and `pin(repo, sha, ref)`. Add the
  `experiment_branches` table.
- **Tests:** real Git in `tmp_path`: create, commit with a large file skipped, export,
  and verify that pinned commits survive `git gc --prune=now`.

### [ ] T3.2 Shared environment cache · F · M
- New `engine/envs.py`: `env_for(project, worktree) -> python path`. Compute the
  lockhash and build once under `<repo>/.experimenttracker/envs/<hash>` using
  `uv sync --frozen` (or `uv venv` plus `uv pip install -r` when there is no lock),
  protected by a file lock. Make sure `.experimenttracker/` is excluded from Git
  (`.gitignore` for tracker-created projects, `.git/info/exclude` for imported ones).
  Support a fixed `project.environment.python`.
- **Tests:** two worktrees with the same lock share one env; a changed lock builds a
  second.

### [ ] T3.3 Contract 3 uses named branches for new experiments · F+R · M
- For projects with Git, new experiments get `exp/<date>-<slug>` worktrees instead
  of shared-mode editing or detached worktrees. This removes the `project_busy` hold
  for Git projects.
- The backend commits after an accepted implementation and before each submission,
  and records `commit` on the job's execution manifest.
- Per-attempt source copies stay, because they are contract 3 semantics.
- Use the shared env (T3.2) instead of a per-experiment `work/.venv` when the project
  has a lock.

### [ ] T3.4 Integration queue (backend) · S · M
- `engine/integration.py` implementing design.md C6 steps 1–7. Merging is automatic:
  no approval unless the project sets `integration.require_approval: true`. Conflicts
  → owner obligation for now (agent resolution in T3.5). A failed check never merges.
- Triggers: the API/CLI command, and a completion hook reading the agent's
  `integrate` decision. Add a `dont_merge` experiment flag and a `revert_integration`
  command (`git revert -m 1` through the same queue).
- Expose it as an API command `integrate`, through the gateway command and the CLI
  `experimenttracker integrate EXPERIMENT`.
- Add a `project.check_command` setting.
- **Tests:** clean merge; conflict; dirty main checkout refusing the fast-forward;
  base branch moved meanwhile.

### [ ] T3.5 Agent conflict resolution · F+R · M
- Add an integrator agent turn in `_integration` with the conflicted files and both
  logs; the backend checks markers and the diff before continuing.

### [ ] T3.6 UI: branch, commits, Integrate · F · M
- The experiment view shows the branch, head commit, the commit for each job, and
  integration state. Add an **Integrate…** confirmation and a gateway command
  passthrough (append-only migration if needed).

### [ ] T3.7 `workspaces adopt-legacy` CLI · F · S
- Dry run by default. Per locked detached worktree: create `exp/legacy-<short>`,
  commit pending changes, record it. Never touch the main checkout.

### [ ] T3.8 Project AGENTS.md template and prompt conventions · F · S
- `src/experimenttracker/templates/project-AGENTS.md`, offered when a project is
  created or imported. Add the conventions from design.md C6 to the prompts.

---

## Phase 4 — Research loop, execution contract 4 (C7)

### [ ] T4.1 Contract 4 skeleton · S · L
- Add a project flag `research_loop`. The create path sets `execution_contract: 4`.
- One session per experiment, with `cwd` = branch worktree. Add the turn-input
  builder (under 8 KB) and the turn-end schema with the enforcement rules.
  Contract 4 skips `advance()` entirely.

### [ ] T4.2a–j Tools, one per task · F (+R for submit_job) · S–M each
`cluster_info`, `submit_job`, `job_status`/`job_logs`, `cancel_job` (reuse),
`register_result`, `register_figure`, `update_report` (reuse), `save_checkpoint`,
`ask_owner`, `request_integration`. Each: a Pydantic input schema, immediate
validation, an idempotent `request_id`, unit tests, and a scenario test.

### [ ] T4.3 Event wakeups with debounce · F+R · M
### [ ] T4.4 Approval gate (manual review and Autopilot envelope) · F+R · S
### [ ] T4.5 UI for contract 4 (turns, checkpoint, jobs, approvals, report) · F · M
### [ ] T4.6 Contract 4 scenario suite (design.md C12 list) · F · M
### [ ] T4.7 Live pilot — owner-authorized, small brief, measure turns, tokens and interventions · owner + S
### [ ] T4.8 Make contract 4 the default; archive contract 3 after it drains; delete the old engine · S · L

---

## Phase 5 — Simplification

### [ ] T5.1 Autopilot as a policy layer on contract 4 (C8) · S · L
### [ ] T5.2 Gateway as a thin relay (C9) · S · L
### [ ] T5.3 Rewrite AGENTS.md and docs (C13) · F+R · M
### [ ] T5.4 Split `web/src/main.tsx` by view; remove tracked design mocks · F · M
### [ ] T5.5 Reassess the self-update subsystem once restarts are routine · S · note

---

## Log

Record completed tasks here: `date · task · commit · notes`.

2026-11-12 · T0.7 · this commit · Linked the refactor workspace from agent and development instructions; link targets, Ruff and all 452 Python tests pass.
