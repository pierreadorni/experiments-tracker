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

### [x] T0.1 Internal errors hold an experiment instead of failing it (C1) · F · S
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

### [x] T0.2 Steering and answers resume blocked work (C2 interim) · F+R · M
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

### [x] T0.3 Liveness watchdog over existing tables (C2 interim) · F · S
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

### [x] T0.4 Outbox by reference (C3) · F+R · M
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

### [x] T0.5 Investigate global agent serialization (C4 interim) · F (investigation) · S
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
- **Result:** [serialization investigation](notes/t0.5-serialization.md). Experiment
  lanes already run concurrently; other roles share the primary agent. Guard-only
  removal is unsafe. Implementation moves into T1.6a–c; no T0.5b is needed.

### [x] T0.6 Deterministic timing in tests (C12 prerequisite) · F · M
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

The owner approved [rollout decisions 1–6](notes/t1-owner-decisions.md) on
2026-11-13. Ship T1.1c and bounded fixture-migration cards before T1.1b activation;
then delete legacy execution by area. Independent clock, harness and obligations
work may proceed once its own dependencies are ready. Measure production-source
net reduction across the phase, with source/test/doc counts per commit. Reviewed
subcards remain individually within their size bands; no live activation follows.

### [x] T1.1a Inventory legacy branches (C10) · F · S
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
- **Result:** [legacy inventory](notes/t1.1-legacy-inventory.md), including all 51
  `iteration.enabled` sites, 21 plan-schema branches, archive readers, tests and
  background-dispatch hazards. Shared contract-3 resource repair remains protected.

### [x] T1.1c New demo on contract 3 (C10) · S · M per subcard
- Make `experimenttracker demo` create an `agent_experiment_v1`, contract 3
  experiment. Extend the demo agent so it scripts plan → implementation → result
  for tiny synthetic regression. `make demo` completes within 60 s; browser smoke
  scripts use the same contract. [Reviewed design](notes/t1.1c-design.md).
- [x] **T1.1c-1:** (b6c1297) Python agent/CLI, current-contract demo tests and gateway
  acceptance script; retain existing toy paths until their fixtures migrate.
- [x] **T1.1c-2:** browser create/review/report-version/export assertions, with
  Node 24 checks and synthetic browser smoke. Reject toy API creation only in h.

### [~] T1.1m Migrate execution fixtures before archival (C10) · F+R · M per subcard
- Expand into ordered, file-bounded cards from the T1.1a test map before delegation.
  Use current contract-3 fixtures for execution/recovery coverage, and explicit
  saved legacy fixtures for API/report/export readers. Preserve safety assertions.
- Keep each group green; do not bypass archival in test-only execution code. Remove
  the global historical-contract downgrade once its remaining callers migrate.
- **Acceptance:** no remaining test depends on advancing a legacy contract; archived
  fixtures still render via the API. Activation of T1.1b waits for this gate.
- **Plan (Opus-approved):** [disposition and final decision](notes/t1.1m-disposition.md) §7–§8. Order: M0, M0b (widened probe), M1 (frozen legacy fixture), P1 (follow-up producer), M2–M8d (conversions), then M9-r1..4 (retire legacy-only tests at the gate), M10 (remove the downgrade), gate probe, T1.1b-1 in one push.
- Status: [ ] M0 · [ ] M0b · [ ] M1 · [ ] P1 · [ ] M2 · [ ] M3 · [ ] M4 · [ ] M5 · [ ] M6 · [ ] M7a/b · [ ] M8a–d · [ ] M9-r1–4 · [ ] M10

### [ ] T1.1b Archive legacy contracts at runtime (C10) · F · S per bounded subcard
- Depends on T1.1c and T1.1m. At startup (after migrations) and in `tick`, a
  non-terminal non-contract-3 experiment gets hold `legacy_contract_archived` and
  an existing-kind inbox notice. `advance` returns early; cancel still works.
- Settle exact in-flight identities/accounting without advancing legacy stages or
  failing the archived experiment. Finish queued reports from saved evidence.
- **Tests:** explicit archived fixture, idempotent startup/ticks, no new dispatch,
  late outcome settlement and cancellation. Split before exceeding the S budget.

### [ ] T1.1d–h Delete legacy execution code (C10) · F+R · M each
One commit per area, removing only branches that T1.1a marked not needed for reading:
- d: `advance()`;
- e: `agent_stage` legacy `prepare` / `Preparation`;
- f: the validate stage and `strict_reproducibility`;
- g: contract ≤2 repair (`repair_failed_experiments` legacy parts, `repairs/` roots);
- h: `artifacts.py` freeze and verify helpers, `recipes/toy_compare`.

Delete or replace the matching tests. Acceptance for each: the remaining suite passes,
and archived fixtures still render their report via the API.

### [x] T1.2a Clock indirection (C12) · F · M
- Add `src/experimenttracker/clock.py` with `now()` and `monotonic()`. Replace
  `time.time()` in `src/` mechanically, except in log formatting. Tests can patch
  `clock.now`.
- **Result:** shared patchable clock exports; all executable wall-clock calls and
  18 behavioral monotonic sites migrated. Eight diagnostic duration sites and a
  standalone prompt snippet keep stdlib time. Independent card fits current rules:
  497 changed source/test lines, source net −2; no accounting exception needed.

### [x] T1.2b Scenario harness (C12) · S · M per subcard
- Add `tests/scenario/` with `FakeClock`, a programmable `FakeSlurm` (lost responses,
  accounting lag, duplicate matches) and `FakeCodex` (scripted turns, lost turn
  start, unreadable thread), plus a `run_until` helper.
- Port 3 existing scenarios: blocked stage plus steering (T0.2), healthy long job with
  zero turns, and a lost submission.
- [Reviewed design and test-only rollout](notes/t1.2b-scenario-design.md).
- [x] **T1.2b-1a:** backend-split fake primitives/gates and control tests, zero
  production-source change; landed 9b7bc8f (see log).
- [x] **T1.2b-1b:** bounded driver, fixture, failure diagnostics and lost-submission
  port; committed-intent proof and new adapters across restart. Landed 9bbef58.
- [x] **T1.2b-2:** blocked-steering and healthy-long-job ports; landed (see log).

### [x] T1.3a Obligations schema and API (C2) · S · M
- Add the named schema migration and `core/obligations.py` API (`open`, `satisfy`,
  `abandon`, `open_for`, `derived_status`) and priority table.
- [Approved design](notes/t1.3a-obligations-design.md); no source integration,
  backfill, derived holds or watchdog behavior yet.

### [ ] T1.3b Obligations at their sources (C2) · F+R · M per source group
- Open/close obligations transactionally for actions, jobs, transfers, pending
  requests, plan review and publication; split by source before delegation.

### [ ] T1.3c Derived holds (C2) · F+R · M per call-site group
- Compute `hold_reason` from obligations in `Store.projection`; remove direct
  writes one call-site group per commit, retaining compatibility readers.

### [ ] T1.3d Obligation liveness watchdog (C2) · F+R · M
- Replace the interim heuristic with the liveness invariant.

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
- [Approved design and subcards](notes/t1.5-runner-design.md): a mechanical
  extraction (F), b submission-race groundwork (S), c activation (S). Safety
  groundwork lands before parallel activation; each card stays below 500 lines.
- Split `tick()` into isolated periodic coroutines: observe, dispatch, resolver,
  publish, backup and retention, watchdog, research and supervisor. Each has its own
  interval and its own `try/except` with logging. Keep the order constraints that
  matter, and document them in code comments.

### [ ] T1.6a AgentPool reservations and ownership routing (C4) · S · M
- [Approved design](notes/t1.6a-reservations-design.md): a-1 derived registry,
  a-2 all-role process-generation routing, a-3 interruption/readback/shutdown;
  each subcard below 500 lines, no scheduling changes until b.
- Follow [T0.5 evidence](notes/t0.5-serialization.md). Introduce role/owner/action
  reservations around the existing agents while preserving scheduling behavior.
- Route callbacks, input replies, activity, interruption, reconciliation and
  shutdown through the owning slot. Preserve action tables and saved thread/turn
  identities; reconstruct reservations before admitting work after restart.
- **Acceptance/tests:** existing lane concurrency and role recovery tests pass;
  reservations retain uncertain work, including after session-count changes.

### [ ] T1.6b Admit mixed roles through AgentPool (C4) · S · M
- [Approved design](notes/t1.6b-admission-design.md): b-1 proposal/selector,
  b-2 binding quota, b-3 capacity/guard replacement; each below 500 lines. The
  owner accepted conservative possibly-live-turn capacity accounting.
- Depends on T1.6a. Admit experiment, research, supervisor, conversation, title and
  monitor work through common slots with C4 priority, owner exclusion and fresh
  quota admission. Reserve before asynchronous dispatch.
- Replace global research/supervisor/conversation guards only once their ownership
  paths use reservations; separate deterministic advancement from turn admission.
- **Acceptance/tests:** unrelated roles overlap on distinct agents with two slots;
  one-slot and same-owner work serialize; uncertain outcomes prevent duplicate
  turns while independent owners can progress.

### [ ] T1.6c Mixed-role recovery scenarios (C4, C12) · S · M
- [Approved matrix](notes/t1.6c-recovery-scenarios-design.md): c-1 overlap/owner
  exclusion, c-2 restart/debt/routing, c-3 binding quota/shutdown; bounded cards.
- Depends on T1.6b and the T1.2b harness. Cover experiment plus research, experiment
  plus supervisor, discussion plus unrelated experiment, single-slot and same-owner
  exclusion, lost outcome/restart, callback/reply routing, quota interrupts and
  shutdown across all occupied slots.
- **Acceptance:** exact saved identities survive restart and unknown outcomes never
  cause duplicate dispatch; all mixed-role scenarios and the full suite pass.

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

2026-11-13 · T1.1c-1 · b6c1297 · Flash implementation; Opus review blocked the first candidate (live-mode auto-approval, held-out points overlapping training points, weak report assertion); all fixed. Contract-3 demo: schema-3 plan, two stdlib CPU stages, backend-written results.json, final report. `make demo` completes in ~3 s with report and figure; live config refuses. Isolated Node 24 locked checkout: Ruff clean, 541 Python tests pass, local Miniflare gateway acceptance passes (3 agent actions, 2 jobs, active final report fetched after executor stop). Source +307/−3, tests +129, docs/scripts separate. Legacy toy branches retained until T1.1e/h. Browser half (T1.1c-2) not started.

2026-11-13 · T1.3a · this commit · Flash implementation with coordinator review and strengthened non-finite-time, new-obligation rollback and migration-DDL rollback proofs. SQLite schema 24 adds only obligations; saved job/action/thread/turn identities, projections and holds remain unchanged, with no backfill. Source +202/−2 (net +200), tests +268/−2; 474 changed source/test lines. Exact staged-index isolated Node 24 locked setup, Ruff, 26 targeted and all 545 Python tests pass. Additive intermediate source is owner-approved; no live migration/activation.

2026-11-13 · T1 rollout · 006a39f · Owner explicitly approved proposals 1–6. Recorded phase-wide production-source accounting, bounded implementation splits, demo/fixture-before-archive order, toy API/export compatibility, conservative possibly-live-turn capacity and in-flight archival settlement. Coordinator-authored S plans received final Opus 5.5 decisions; implementation status remains separate. Existing Node 24 source verification remains applicable: production/test files are unchanged by this documentation commit.

2026-11-13 · T1.2a · this commit · Flash implementation with coordinator AST/import and local-name-shadowing audits; normalized source ASTs are unchanged beyond time/clock references and imports. Source +232/−234 (net −2), tests +27/−4; 497 source/test changed lines. Isolated locked Node 24 validation matches the final worktree: Ruff clean, 90 targeted and all 537 Python tests pass; synthetic demo completes with labelled report/figure. No live activation. This independent card obeys existing size and net-source rules; archival and S-card implementation decisions remain pending.

2026-11-13 · T1.1a · 42829f4 · Flash investigation with coordinator corrections and source/citation audit; no runtime changes. All 51 iteration predicate sites and 21 plan-schema branches are indexed, with reader/deletion boundaries and test coverage. Isolated Node 24 locked setup/check passes (535 Python, 29 web, 41 gateway); baseline synthetic demo report and figure inspected. T1.1b activation and T1.1c implementation await owner decisions on ordering, fixture-migration splits and net-negative accounting; Opus 5.5 reviewed the coordinator's T1.1c plan and blocked implementation pending those decisions. Source/test delta: 0; documentation only.

2026-11-12 · T0.7 · dcf22d4 · Linked the refactor workspace from agent and development instructions; link targets, Ruff and all 452 Python tests pass.

2026-11-12 · T0.3 · eb61db9 · Notice-only watchdog uses persisted revision timestamps and deduplicates by hold; 29 targeted and all 481 Python tests pass, with Ruff checks.

2026-11-12 · T0.1 · bd826be · Explicit terminal failures, per-experiment internal-error backoff and deduplicated notices; 76 targeted (including watchdog integration) and all 491 Python tests pass, with Ruff checks. The commit message records the ValueError audit.

2026-11-12 · T0.6 · 99c80ec · Shared wall-bounded callback waits replace fixed polling budgets; quota-interruption restart regression restored with continuation and saved-identity assertions. All 499 Python tests pass three consecutive times and with three other full suites running concurrently (separate temporary roots). Node 24 make check passes, including 29 web and 41 gateway tests. First batch complete; synthetic demo and doctor pass, with no live activation.

2026-11-12 · T0.2 · this commit · Steering in either mode and validated answers transactionally continue settled blocked work; manual retries and implementation repairs consume pending later guidance. Saved turns/attempts, active and uncertain work, owner pauses, and normal restart behavior are preserved. Rollback, duplicate commands, expired settled questions and nested Autopilot commands covered; 126 targeted and all 530 Python tests pass, with Ruff checks on Node 24. No live activation.

2026-11-12 · T0.4 · this commit · Implemented and reviewed by the coordinator at the owner's request. Events store `{}` references; publication builds a consistent current projection and skips stale revisions. SQLite schema 23 records compact artifact metadata acknowledgements transactionally, preserving upload registration during revision churn and publisher restarts; legacy snapshots remain readable and retention ignores reference-only payloads. Size, stale-selection, migration and legacy compatibility coverage passes: 41 targeted tests and isolated Node 24 `make setup`/`make check` (535 Python, 29 web, 41 unchanged gateway tests). Synthetic lifecycle coverage only; no live activation.

2026-11-13 · T1.2b-1a · 9b7bc8f · Flash candidate (second attempt after the combined candidate was rejected) reviewed by the coordinator against the Opus-approved split: adapter signatures and raised exception types match `slurm.py`/`codex_client.py` (exact-cancel identity, `rsplit` payload parsing, catalog metadata, unique thread counters, unknown-observation failure, accepted/failed/interrupted/unreadable turns, backend sharing across fresh adapters, gate task capture). Production source 0; tests +488 (2 new files, 7 tests; under the 500-line budget). Isolated worktree: Ruff clean, all 556 Python tests pass. No driver, fixture or workflow port yet. No live activation.

2026-11-13 · T1.2b-1b · 9bbef58 · Flash implementation; coordinator review of driver, fixture and port against the approved split (bounded tick+settle, dynamic gate observation, separate-connection committed-intent probe with `not in_transaction`, restart on shared backends, exact adoption with one `job_reconciled` and one submission). Added a release/settle of the second gated task to avoid a leaked pending task. Production source 0; tests +469 (4 files, 12 tests incl. 7 from 1a; under 500). Isolated worktree: Ruff clean, scenario tests stable over 3 runs, all 561 Python tests pass. Hung-tick/gated-tick driver tests use a workflow-shaped stub; the real engine path is exercised by the port. Probe assumes the store's journal_mode=DELETE. No live activation.

2026-11-13 · T1.2b-2 · this commit · Flash implementation; coordinator review of both ports (no row insertion or dispatch mocking; engine-driven blocked agent stage, steering continuation on the saved thread with duplicate-request idempotence; engine-submitted RUNNING job with refreshed fake-clock telemetry, controller sentinel declared and reset, wakeups proven to have run, zero additional begin/monitor/submission, parametrized instrumented/uninstrumented). Worker sanity-checked that dropping telemetry refresh, the sentinel or the duplicate request id makes the tests fail. Production source 0; tests +274 (2 new files, 3 tests). Ruff clean, 15 scenario tests stable, all 564 Python tests pass. Harness T1.2b complete; existing tests retained. No live activation.

2026-11-13 · T1.1c-2 · this commit · Coordinator implementation (small, serialized hot-file card). The UI always creates `agent_experiment_v1`; smoke scripts approve the plan, wait for working and final reports, select versions by saved number, assert the contract-3 results ZIP (`evidence/<id>/<name>`, no frozen recipe) and no longer reproduce a recipe. `smoke.mjs` also needed its staged brief to be supported by the demo and a click on the Next ideas section, which an earlier UI change had left stale. Verified in an isolated worktree on Node 24 with a Playwright browser-path workaround (installed chromium-1248 symlinked as the expected 1243 under `PLAYWRIGHT_BROWSERS_PATH`): `smoke.mjs` (twice), `progress-smoke.mjs`, `maintenance-smoke.mjs` (ready release seeded via the test helper), `cancellation-smoke.mjs` (already contract 3) and `autopilot-smoke.mjs` (dedicated `tests/iteration_browser_server.py` fixture) all pass; web format check, 29 web tests and typecheck pass. Python unchanged. Production source +1/−1 (`web/src/main.tsx`); scripts +63/−48. `iteration-smoke.mjs` untouched. Toy creation still accepted by the API until T1.1h. No live activation.
