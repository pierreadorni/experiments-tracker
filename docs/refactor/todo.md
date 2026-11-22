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

**Complete under the owner-approved scope cut.** Final Node 24 `make check`:
866 Python, 29 web and 45 gateway tests pass; see
[acceptance and remaining boundaries](notes/t1-acceptance.md). Deferred T1.3c
writer migration and T1.7 engine splits remain deferred, not implemented.

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

### [x] T1.1m Migrate execution fixtures before archival (C10) · F+R · M per subcard
- Expand into ordered, file-bounded cards from the T1.1a test map before delegation.
  Use current contract-3 fixtures for execution/recovery coverage, and explicit
  saved legacy fixtures for API/report/export readers. Preserve safety assertions.
- Keep each group green; do not bypass archival in test-only execution code. Remove
  the global historical-contract downgrade once its remaining callers migrate.
- **Acceptance:** no remaining test depends on advancing a legacy contract; archived
  fixtures still render via the API. Activation of T1.1b waits for this gate.
- **Plan (Opus-approved):** [disposition](notes/t1.1m-disposition.md) §8 plus **§9 (M0b final amendments, 7254d2e)**. After M5, land M4b, M11 and P-a before the gate; continue M6, M7a/b, M8a–d, M8e and explicit static legacy fixture rewrites before M10. Remeasured retirement cards are M9-r1, r2, r3a (local outputs), r3b (resources), r4, r5 (M-sized temporary-output prompt/validation cleanup, not its live cleanup tests). Push M9-r*, M10, gate and T1.1b-1 together. P-b precedes toy-create removal in h.
- **Temporary opt-in (M0c, S, test-only):** register `current_contract` in `tests/conftest.py` and skip the historical downgrade only for marked tests; no uses in this prerequisite card. CONVERT cards mark individual functions and prove contract 3 via `prepared()` or an explicit assertion. M10 removes the registration, downgrade and every marker use (`rg current_contract tests pyproject.toml` must find nothing).
- **Completed investigation:** pinned `34f29ec` baseline: 564 passing, 156 probe nodeids (132 entrypoint / 24 non-gate); no-downgrade dry run: 76 failed / 488 passed in 1797.49 s. Opus `94dbc071`/`d5467d95` approved corrected §9 with coordinator amendments. Before retirement, map every baseline nodeid and rerun the widened probe on current main. Gate zero applies to §8 execution entrypoints, with explicit static-origin reporting for retained reader/observation/reconciliation and cleanup tests. Keep live legacy recovery guards/rollback until their branch is deleted; M8d adds separate contract-3 counterparts. Keep cleanup tests until h.5. Keep sealed restart `[True]` through the gate for T1.1b-1's explicit late-settlement disposition. Do not retire the ten schema-1/2 model-validation tests.
- Status: [x] M0 · [x] M0b · [x] M0c · [x] M1 · [x] P1 · [x] M2 · [x] M3 · [x] M4 · [x] M5 · [x] M4b · [x] M11 · [x] P-a · [x] M6 · [x] M7a · [x] M7b · [x] M8a/b · [x] M8c · [x] P-budget · [x] M8d · [x] M8e · [x] static legacy fixture rewrites · [x] M9-r1/r2/r3a/r3b/r4/r5 · [x] M10 · [x] gate · [x] P-b (three bounded producer cards)

### [x] T1.1b Archive legacy contracts at runtime (C10) · F · S/M per bounded subcard
- **Implementation plan:** [archive boundary and bounded prerequisites](notes/t1.1b-archive-design.md).
  Stage inert Store/job/action helpers, then activate all execution guards together.
  The reviewed settlement/activation cards use M budgets; no partial activation.
- Status: [x] P1 Store primitive · [x] P2 exact job settlement · [x] P3 raw turn
  settlement/check drainage · [x] P4 coherent activation · [x] final acceptance.
- Depends on T1.1c and T1.1m. At startup (after migrations) and in `tick`, a
  non-terminal non-contract-3 experiment gets hold `legacy_contract_archived` and
  an existing-kind inbox notice. `advance` returns early; cancel still works.
- Settle exact in-flight identities/accounting without advancing legacy stages or
  failing the archived experiment. Finish queued reports from saved evidence.
- **Tests:** explicit archived fixture, idempotent startup/ticks, no new dispatch,
  late outcome settlement and cancellation. Split before exceeding the S budget.

### [x] T1.1d–h Delete legacy execution code (C10) · F+R · M each
One commit per area, removing only branches that T1.1a marked not needed for reading:
- [x] d: `advance()`;
- [x] e: `agent_stage` legacy `prepare` / `Preparation`;
- [x] f: the validate stage and `strict_reproducibility`;
- [x] g: contract ≤2 repair (`repair_failed_experiments` legacy parts, `repairs/` roots);
- [x] h: `artifacts.py` execution writers and shipped recipe; archived readers stay.
  Status: [x] h.1ab · [x] h.4 · [x] h.5 · [x] h.1c · [x] h.2 · [x] h.3 · [x] h.6 · [x] h.7a execution tail · [x] h.7b command/reply tail · [x] h.7c final execution fallbacks.
  T1.8 closes the remaining dominated predicates/rechecks found after acceptance;
  reachable archived readers and exact settlement remain intentionally supported.
  See [the final site table](notes/t1.8-site-inventory.md#10-final-tree-site-table-and-acceptance).

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

### [x] T1.3b Obligations at their sources (C2) · F+R · M per source group
- Open/close obligations transactionally for actions, jobs, transfers, pending
  requests, plan review and publication; split by source before delegation.
- [Rollout decisions and bounded cards](notes/t1.3b-rollout-design.md): [x] b1 inert
  episode/lookup helpers; [x] b2a inert action mapping; [x] b5a inert job mapping; [x] b2b Workflow actions; [x] b3 Store/Conversation actions; [x] b4 Controller/cancellation/archived actions; [x] b5b Workflow job intent/ID/adoption/recovery; [x] b6 job observation/cancellation/legacy/steering; [x] b7a transfer admission/outcomes; [x] b8+b9 report-version publication; [x] b7b/c preview identity and cancellation; [x] b12 wakeup timers; [x] b10 pending requests; [x] b11 plan reviews; [x] b13 advancement lifetime and cancellation responsibility (with c1b); [x] b14 bounded backfill.

### [-] T1.3c Derived holds (C2) · DEFERRED by owner 2026-11-17
- [x] c1a inert source-less admission decisions. c1b (lifetime/cancellation) is
  done inside b13. **c1c, c2a/b, c3a–f, c4a/b are deferred**: they move the
  contract-3 stage engine's `hold_reason` writers onto decision records, and that
  engine is deleted by T4.8. Contract 4 derives holds natively from obligations.
  Until then `hold_reason` stays the operational authority beside the obligations;
  its writers retire with the old engine in T4.8. See
  [t1-owner-decisions.md §8](notes/t1-owner-decisions.md#8-scope-cut-and-faster-working-method-2026-11-17).
- Compute `hold_reason` from obligations in `Store.projection`; remove direct
  writes one call-site group per commit, retaining compatibility readers.

### [x] T1.3d Obligation liveness watchdog (C2) · F+R · M
- Replace the interim heuristic with the liveness invariant (d1a/d1b in
  [t1.3c-derived-design.md](notes/t1.3c-derived-design.md)). With c deferred, it
  checks exact source obligations, owner notices, timers and the advancement
  lifetime's open dependency; it does not need derived holds. Still required:
  contract 4 relies on the same invariant.

### [x] T1.4a Slurm submission resolver (C5) · F+R · M
- New `resolver.py`. Apply the rules in design.md C5 (Slurm) to
  `jobs.state='submitting' AND job_id IS NULL`. Add config
  `accounting_lag_seconds` per execution context. Write receipt events.
- **Tests:** scenario harness (lost `sbatch` response → found / absent after lag /
  duplicate).

### [x] T1.4b Codex turn resolver (C5) · F+R · M
- Same, for `agent_actions.state='uncertain'`, using `agent.read_turn` and the thread
  read. **Tests:** lost turn start → adopt / absent → continuation / unreadable →
  owner obligation after 6 tries.

### [x] T1.5 Independent periodic tasks (C11 runner) · S · M
- [Approved design and subcards](notes/t1.5-runner-design.md): a mechanical
  extraction (F), b submission-race groundwork (S), c activation (S). Safety
  groundwork lands before parallel activation; each card stays below 500 lines.
- Split `tick()` into isolated periodic coroutines: observe, dispatch, resolver,
  publish, backup and retention, watchdog, research and supervisor. Each has its own
  interval and its own `try/except` with logging. Keep the order constraints that
  matter, and document them in code comments.

### [x] T1.6a AgentPool reservations and ownership routing (C4) · S · M
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

### [x] T1.6b Admit mixed roles through AgentPool (C4) · S · M
- [Approved design](notes/t1.6b-admission-design.md): b-1 proposal/selector,
  b-2 binding quota, b-3 capacity/guard replacement; each below 500 lines. **b-1
  and b-2 are complete; b-3 and its reconnect/ordering safety fix are complete.** The
  owner accepted conservative possibly-live-turn capacity accounting.
- Depends on T1.6a. Admit experiment, research, supervisor, conversation, title and
  monitor work through common slots with C4 priority, owner exclusion and fresh
  quota admission. Reserve before asynchronous dispatch.
- Replace global research/supervisor/conversation guards only once their ownership
  paths use reservations; separate deterministic advancement from turn admission.
- **Acceptance/tests:** unrelated roles overlap on distinct agents with two slots;
  one-slot and same-owner work serialize; uncertain outcomes prevent duplicate
  turns while independent owners can progress.

### [x] T1.6a-4 Retire cloned Workflow lanes (C4) · S · M per bounded subcard
- Final acceptance audit found the named retirement in
  [owner decision §7](notes/t1-owner-decisions.md#7-net-negative-waiver-2026-11-17)
  still pending: `Workflow.start_agent_pool` constructs another `Workflow` per
  connection and manually shares selected fields. The a/b cards removed global
  guards but did not remove this duplicate owner scaffolding.
- Replace clones with explicit per-connection transport/task state owned by the
  pool; preserve one controller/research/supervisor/conversation owner, slot/process
  generations, saved identities, binding admission and exact reconciliation.
  Delete the clone construction and manual field-sharing path in the same rollout.
  No new schema/config/reasons or changed scientific execution semantics.
- Completed as a-4a vocabulary/owner-alias consolidation and a-4b explicit
  `AgentSlot` ownership with clone deletion. All role callbacks, dispatch and
  lifecycle operations use explicit slots; primary compatibility properties
  delegate to canonical slot-zero state. Both acceptance repairs are complete;
  see the [final acceptance record](notes/t1-acceptance.md).

### [x] T1.6c Mixed-role recovery scenarios (C4, C12) · S · M
- [Approved matrix](notes/t1.6c-recovery-scenarios-design.md): c-1 overlap/owner
  exclusion, c-2 restart/debt/routing, c-3 binding quota/shutdown; bounded cards.
- Status: [x] c-1 shared-backend overlap, one-slot and typed-owner scenarios;
  [x] c-2 restart/debt/routing; [x] c-3 binding quota/interruption/shutdown.
  Final acceptance passed after a-4 retirement and research binding-denial repair.
- Depends on T1.6b and the T1.2b harness. Cover experiment plus research, experiment
  plus supervisor, discussion plus unrelated experiment, single-slot and same-owner
  exclusion, lost outcome/restart, callback/reply routing, quota interrupts and
  shutdown across all occupied slots.
- **Acceptance:** exact saved identities survive restart and unknown outcomes never
  cause duplicate dispatch; all mixed-role scenarios and the full suite pass.

### [-] T1.7a–f Split `workflow.py` mechanically (C11) · DEFERRED by owner 2026-11-17
Skipped: it reorganises the contract-3 engine that T4.8 deletes. New contract-4
code goes straight into its own modules (C11 layout) instead.
Move code without changing behaviour; each commit keeps the tests green.
- a: prompts → `research/prompt.py`;
- b: `observe_jobs` and submission → `engine/jobs.py`;
- c: `dispatch`, `reconcile_action` → `engine/agents.py`;
- d: publication and reporting helpers → `publish/`;
- e: workspace helpers → `engine/workspaces.py`;
- f: remote helpers.

### [x] T1.8 Close-out: finish the legacy deletion that T1.1d–h claims (C10, F1) · F+R · M per subcard
Post-acceptance review (2026-11-18, `7046b07`, 866 Python tests green) found T1.1d–h
marked done while its branches remain. This card deletes them before Phase 3 edits
the same functions (T3.3 touches `prepare_workspace`, `_submit_prepared_stage` and
`agent_stage`). It adds no behaviour, schema, config key, hold reason or event.

**Measured baseline (reproduced at `7046b07`):**
- `rg -c 'iteration\.enabled\(' src` → 38 (workflow 30, conversation 6,
  controller 1, early_repair 1; audit F1
  counted about 50/41). By function in `workflow.py`: `observe_jobs` 6,
  `_submit_prepared_stage` 4, `advance` 4, `advance_reports` 4,
  `validate_resource_plan` 3, `agent_stage` 2, `publish` 2,
  `repair_failed_experiments` 2, `dispatch` 1, `execution_instruction` 1,
  `scheduler_for` 1; plus `controller.call` 1, `early_repair.cancel` 1,
  `conversation.prompt` 3, `conversation.validate` 3.
- `rg -c 'execution_frozen\(' src` → 52 (51 calls + definition; workflow 42): `dispatch` 10, `advance` 6,
  `agent_stage` 5, `dispatch_title` 5, the rest 1–2 per function. Many repeat after
  each `await` in one function (e.g. `workflow.py:975–1027`).
- `execution_frozen(exp)` reads only `contract.execution_contract`. The review found
  no writer that changes that key after creation (`store.py:885,1272,1372,1412`
  rewrite `contract` for other keys). If that holds, a re-check after an entry
  guard can never change its answer.

**Subcards (one worker, one commit each; a before b–d; b–d touch different
functions and may run in parallel lanes):**
- [x] **a · inventory and proof (F, S, docs + one test).** Write
  `notes/t1.8-site-inventory.md`: every site above as `file:function:line` with one
  disposition — DEAD (behind a frozen entry guard, always true/false), READER
  (reachable for a completed or archived legacy record), SETTLE (legacy exact
  job/turn settlement), ENTRY (the one guard a public entry point keeps). Name the
  caller that makes each READER/SETTLE site reachable. Prove immutability: list
  every `UPDATE experiments SET contract` and every in-memory contract rewrite, and
  add one regression test that each Store path preserves `execution_contract`.
  Stop and ask if any path can change it.
- [x] **b · collapse always-true `iteration.enabled` (F+R).** For DEAD sites, keep
  the contract-3 arm and delete the conditional and the legacy arm, including the
  `evidence_revision` SQL variants in `advance_reports` if a shows them dead.
  Expected survivors: the six `conversation.py` readers, `scheduler_for` (legacy
  settlement key), and whatever a marks READER/SETTLE. Delete helpers that lose
  their last caller.
- [x] **c · agent paths (F+R).** `dispatch`, `dispatch_title`, `advance_title`,
  `title_applicable`, `adopt_turn`, `recover_terminal_actions`,
  `reconcile_agent_output`: one ENTRY guard per entry point; delete re-checks and
  the unreachable settle-as-frozen arms behind them.
- [x] **d · advancement paths (F+R).** `advance`, `agent_stage`,
  `_submit_prepared_stage`, `publish`, `reconcile_outputs`,
  `repair_failed_experiments`, `step_control_after_observe`, `steering`,
  `controller.call`: same rule.

**Rules:**
- Deletion only. Blocked/cancelled/quota re-checks that sit next to a frozen
  re-check stay; they guard states that do change across awaits.
- Keep untouched: archived readers (`audit.py`, `Preparation`, schema-1/2 plan
  models, `artifacts.verify_snapshot`, `Store.projection`, report/API delivery),
  `legacy_archive.settle_job` and saved-turn settlement, the startup/tick archive
  sweep, the Store/gateway command refusals for archived records, and all
  `hold_reason` writers (deferred to T4.8).
- A test may be removed only if it constructs a state a proves unreachable
  (contract flipped mid-flight). List each one in the log with the surviving test
  that covers the same safety property from a saved fixture. Never delete a
  late-settlement, cancellation or no-duplicate-dispatch assertion.
- Reviewer (coordinator) reads each diff against a, concentrating on settlement
  and identity paths, not on line counts.
- Stop and ask if a site is reachable in a way a did not record, or if removing a
  re-check changes any test that starts from a saved legacy fixture.

**Acceptance:**
- `rg 'iteration\.enabled\(' src` and `rg 'execution_frozen\(' src` return only the
  sites a lists as READER, SETTLE or ENTRY; the note's table matches the tree.
- Every deletion subcard (b–d) is net-negative in production source; the explicitly
  docs/test-only a has zero production delta. b–d add no production lines
  beyond moved guards. Report `git show --numstat` per commit.
- `tests/test_legacy_archive*.py`, `tests/test_legacy_fixture.py`,
  `tests/test_conversation*.py`, `tests/test_audit.py` and `tests/scenario` pass
  per card; Ruff and the full suite pass once on the combined tree. `make check`
  is not needed unless web or gateway files change (they should not).
- Update the T1.1d–h entry, `notes/t1-acceptance.md` (counts, new phase delta) and
  the log. No live activation.

**Out of scope, recorded so nobody reopens them here:** the `workflow.py` split
(T1.7), derived holds (T1.3c), `AgentPool` taking the whole `Workflow`, and the
role busy flags it still writes. All retire with the contract-3 engine in T4.8.

---

## Phase 3 — Git-native workspaces (C6)

### [x] T3.1 Branch/worktree module · F · M
- New `engine/gitworkspace.py` (or extend `workspaces.py`): `create(repo, slug, base)`,
  `commit(worktree, message, max_untracked_mb=5)` returning `(sha, skipped)`,
  `export(repo, sha, target)` via `git archive`, and `pin(repo, sha, ref)`. Add the
  `experiment_branches` table.
- **Tests:** real Git in `tmp_path`: create, commit with a large file skipped, export,
  and verify that pinned commits survive `git gc --prune=now`.

### [x] T3.2 Shared environment cache · F · M
- New `engine/envs.py`: `env_for(project, worktree) -> python path`. Compute the
  lockhash and build once under `<repo>/.experimenttracker/envs/<hash>` using
  `uv sync --frozen` (or `uv venv` plus `uv pip install -r` when there is no lock),
  protected by a file lock. Make sure `.experimenttracker/` is excluded from Git
  (`.gitignore` for tracker-created projects, `.git/info/exclude` for imported ones).
  Support a fixed `project.environment.python`.
- **Tests:** two worktrees with the same lock share one env; a changed lock builds a
  second.

### [x] T3.3 Contract 3 uses named branches for new experiments · F+R · M per subcard
- For projects with Git, new experiments get `exp/<date>-<slug>` worktrees instead
  of shared-mode editing or detached worktrees. This removes the `project_busy` hold
  for Git projects.
- The backend commits after an accepted implementation and before each submission,
  and records `commit` on the job's execution manifest.
- Per-attempt source copies stay, because they are contract 3 semantics.
- Use the shared env (T3.2) instead of a per-experiment `work/.venv` when the project
  has a lock.
- T3.3a saves the named-worktree mode and exact checkout identity. T3.3b adds
  backend commit intents, pinned job commits, and shared runtime selection; an
  unproven external Git outcome requires inspection rather than replay.

### [x] T3.4 Integration queue (backend) · S · M per subcard
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
- **T3.4a–c landed:** exact Git transactions, durable runner, project checks, local owner
  approval, and Store/API/CLI commands. T3.4d queues a saved explicit agent opt-in
  without blocking completion when the integration queue is full.
  A failed check or uncertain Git outcome holds the candidate for inspection.

### [x] T3.5 Agent conflict resolution · F+R · M per subcard
- Dispatch one durable integrator turn per exact saved merge conflict, subject to
  the existing pause and quota gates. The backend verifies and pins the result,
  then sends it through project check and any required owner approval. Failed or
  unresolved outcomes remain held for inspection rather than replayed.

### [x] T3.6 UI: branch, commits, Integrate · F · M per subcard
- Show branch, head, job commits and integration state. Queue confirmed integration,
  revert and exact candidate approval through local or capability-gated gateway
  commands. No gateway migration was needed.

### [x] T3.7 `workspaces adopt-legacy` CLI · F · S per subcard
- Dry run by default. Per locked detached worktree: create `exp/legacy-<short>`,
  commit pending changes, record it. Never touch the main checkout.
- T3.7a is read-only. T3.7b takes the offline ownership lock for `--apply`,
  preserves the historical row, and refuses to replay a Git outcome that needs inspection.

### [x] T3.8 Project AGENTS.md template and prompt conventions · F · S
- `src/experimenttracker/templates/project-AGENTS.md`, offered when a project is
  created or imported. Add the conventions from design.md C6 to the prompts.
- **Done:** Tracker-created folders commit a starter `AGENTS.md`; an existing owner
  file is never overwritten and imported folders are never written. The same text is
  offered read-only at `GET /api/projects/agents-template` and in the new-project
  dialog. The saved T3.3 workspace mode scopes the C6 conventions to plan/prepare
  and agent-stage prompts for named branch runs; shared and detached runs are unchanged.

---

## Phase 4 — Research loop, execution contract 4 (C7)

### [x] T4.1 Contract 4 skeleton · S · M per subcard
- Ordered source rollout: [bounded cards and identity boundaries](notes/t4.1-rollout.md).
- [x] T4.1a–d: turn protocol, stage isolation, archive boundary and inert first-turn intent.
- [x] T4.1e–g: exact dispatch, settlement and readback; opt-in creation and one first turn.
- The skeleton is not a pilot: tools, event wakeups, approvals and UI are still separate cards.
- Add a project flag `research_loop`. The create path sets `execution_contract: 4`.
- One session per experiment, with `cwd` = branch worktree. Add the turn-input
  builder (under 8 KB) and the turn-end schema with the enforcement rules.
  Contract 4 skips `advance()` entirely.

### [x] T4.2a–j Tools, one per task · F (+R for submit_job) · S–M each
See [tool rollout and effect boundaries](notes/t4.2-rollout.md).
- [x] T4.2a `cluster_info` and scoped MCP bridge; T4.2b `job_status`/`job_logs`.
- [x] `save_checkpoint`, `update_report` and `register_figure` through MCP.
- [x] `cancel_job` through MCP, reusing exact selective cancellation.
- [x] `register_result`: bounded exact-attempt validation, immutable registration, MCP and report evidence.
- [x] `ask_owner` and `request_integration` through MCP. The manual approval gate is T4.4; answer wakeup is T4.3.
- [x] `submit_job` for manually approved local CPU jobs. `register_result` advertises a usable JSON example without relaxing exact producer or metric validation. GPU admission is not enabled.
- [x] A submission can declare a bounded `result_file`. The exact saved intent binds registration to that file; older undeclared submissions and replays retain their existing behavior.
- [x] `depends_on` requires exact completed jobs in the same experiment before admission; it does not queue native Slurm dependencies.
- [x] Local and Jean Zay CPU jobs can select bounded TensorBoard previews without waking research or blocking terminal delivery.
- [x] Local CPU jobs can stage bounded `inputs` from exact registered results and read their staged paths through `EXPERIMENT_INPUTS`.
- [x] Jean Zay CPU jobs stage verified registered inputs with the pinned source and expose their exact remote paths through `EXPERIMENT_INPUTS`.
- Tool acceptance is complete for the CPU routes. GPU admission remains disabled pending reservation/accounting; live remote acceptance is separate.
- [x] Manually approved C4 CPU jobs can use the saved Jean Zay route. Scripted controller acceptance covers pinned staging, lost submission reply, exact adoption, delayed accounting, interrupted delivery and one result-ready wake. No live remote acceptance.
`cluster_info`, `submit_job`, `job_status`/`job_logs`, `cancel_job` (reuse),
`register_result`, `register_figure`, `update_report` (reuse), `save_checkpoint`,
`ask_owner`, `request_integration`. Each: a Pydantic input schema, immediate
validation, an idempotent `request_id`, unit tests, and a scenario test.

### [x] T4.3 Event wakeups with debounce · F+R · M per subcard
- [x] T4.3a: exact wake queue and fixed 60-second window, not yet wired to dispatch.
- [x] T4.3b: the exact durable owner answer queues one debounced follow-up after settlement.
- [x] Settled `continue` queues once on the exact saved thread.
- [x] Known local scheduler jobs route separately from contract 3. Terminal job wakes can share one follow-up with a due owner answer.
- [x] Missing local scheduler IDs use the existing exact-intent resolver before observation.
- [x] Contract-4 steering is saved for the next eligible turn on the exact saved thread; it never restarts the stage engine.
- [x] Explicit `timer:<seconds>` waits settle atomically and survive restart. Terminal job evidence can supersede a timer before its deadline.
- [x] Follow-up dispatch loads the latest private checkpoint without copying its text into the saved action. Lost starts still reconcile by the exact dispatched prompt hash.
- [x] Rotate at 80% of the reported context window, using only the last request of the exact settled turn and only when a private checkpoint can seed the new thread. The new thread and exact rendered prompt hash are saved before `turn/start`; prior identities stay unchanged.
- [x] Remote observation saves exact accounting, retries output delivery at the saved cadence, and queues one wake only after delivery. T4.2 covers the full lost-submit lifecycle through the controller. See the [remote job source gates](notes/t4.3-remote-rollout.md).
### [x] T4.4 Approval gate (manual review and Autopilot envelope) · F+R · S per subcard
- [x] Durable manual approval requests through `ask_owner` and exact owner decision verification.
- [x] Inert CPU-only admission validation checks the latest exact owner approval, saved action, pause/maintenance and resource limits.
- [x] Persist exact local submission intents and reconciliation obligations; replay never authorizes another submit.
- [x] Pinned source preparation and local dispatch helper; synthetic lost response recovers by exact intent without a second submit.
- [x] Manual local submission is exposed through MCP and covered by the approval-to-report scenario.
- [x] Current Autopilot compute authority further constrains human-approved C4 member submissions, including inside intent persistence. Paused/deferred members and out-of-scope writing-phase compute are refused.
- [x] The authorized project orchestrator approves exact C4 objectives within the active Autopilot run and policy. A scripted run reaches a ready report without a human answer; stale approvals cannot authorize dispatch, and compute pause prevents submission. C4 remains CPU-only.
### [x] T4.5 UI for contract 4 (turns, checkpoint, jobs, approvals, report) · F · M
- [x] Local detail shows research turns, checkpoint metadata and job status without the contract-3 stage timeline. A scripted FastAPI/SQLite loopback browser run accepts the exact objective and verifies identical replay.
- [x] Report status and convenience exports use working-report semantics for C4. Local Worker/D1 tests cover durable answers, identical replay after clearance, and C4 pause/resume and next-turn steering.
- Source acceptance is complete. The loopback Chromium smoke also checks rendered turn, checkpoint and job metadata. Private hosted and physical-device acceptance are separate rollout checks, not completed here.
### [x] T4.6 Contract 4 scenario suite (design.md C12 list) · F · M per subcard
- [x] Approval → one submit with lost reply → exact adoption → debounced terminal wake → registered result → ready report → completed experiment. Model turns are scripted; no live scheduler.
- [x] Owner pause/reopen/resume, healthy job observation without an inference turn, and concurrent branch edits that do not change the base checkout.
- [x] Quota denial → owner restart → admission dispatches the same pending action once. One branch waiting on an owner question does not block a second branch; the debounced answer only reaches the question's thread.
- [x] A failed figure call preserves metrics and still allows a metrics-only report and completion. The watchdog reports a C4 experiment with no open obligation after the grace interval without dispatching or changing work.
- [x] A valid `done` opt-in queues integration through C6 in the completion transaction; opt-out, Don't merge and an already-queued request do not add a merge.
- [x] Steering wakes an idle C4 thread once with the saved guidance; a lost turn start is reconciled only to the exact saved turn, without redispatch.
- [x] A failed job wakes a real C4 dispatcher follow-up with the latest private checkpoint; the earlier successful job stays complete and no compute is replayed.
- [x] Two actual C4 completion requests edit the same tracked file. The C6 queue merges the first, resolves the second with one scripted integrator turn, checks and applies that candidate, and performs no extra turn or merge on repeat.
### [x] T4.7 Live pilot — owner-authorized, small brief, measure turns, tokens and interventions · owner + S
- The isolated, owner-authorized live pilot completed one CPU/no-GPU Slurm job, registered its results, published a ready report, and reached `complete`. It required a metadata-only normalization of a copy of the original metric file; it was not a zero-intervention run. See the [acceptance summary](notes/t4.7-pilot.md). This does not authorize a rollout or contract-3 retirement.
### [ ] T4.8 Make contract 4 the default; archive contract 3 after it drains; delete the old engine · S · L
- [x] Switch creation only. `ae5a899` selects C4 for new experiments and the CLI demo. The owner-authorized update and deployment are recorded in the private installation receipt. Late unbound hosted creates are refused, while already-applied requests retain their exact receipts.
- [x] Add [read-only drain inventory](notes/t4.8-drain-evidence.md#run-the-offline-inventory) for saved SQLite and optional D1 backups.
- [x] Run the owner-authorized production drain check and review the registered hosted queues. The window and per-ID evidence are recorded privately.
- [x] Separate retained C3 evidence from stage execution. A simulated archive preserves projections, artifact uploads and exact scheduler settlement while preventing stage advancement and dispatch.
- [x] Align the executor, Worker and browser on the archive decision advertised by the executor. Keep exact replay and bound discussion replies, and preserve historical views.
- [x] Run the archive candidate against saved C3 startup, reads and settlement, with the C4 synthetic flow still active.
- [x] Resolve the 25 scenario failures in the archive-on probe. Shared turn, scheduler, quota and mixed-role recovery use C4. The blocked-stage continuation and progress/monitor fixtures explicitly enable the C3 engine.
- [ ] Run the full suite with the archive boundary switched in an isolated checkout. Classify any remaining stage-specific tests before switching the default archive boundary.
- [ ] Refresh drain evidence at activation. Then archive C3 and remove the old engine in reviewed slices. See the [retirement sequence](notes/t4.8-rollout.md#3-verify-drain-on-the-actual-installation-retire-separately).

---

## Phase 5 — Simplification

### [ ] T5.1 Autopilot as a policy layer on contract 4 (C8) · S · L
### [ ] T5.2 Gateway as a thin relay (C9) · S · L
### [ ] T5.3 Rewrite AGENTS.md and docs (C13) · F+R · M
### [ ] T5.4 Split `web/src/main.tsx` by view; remove tracked design mocks · F · M
### [ ] T5.5 Reassess the self-update subsystem once restarts are routine · S · note

---

## Phase 6 — Selectable agent backends: Codex or Claude Agent SDK (C14)

Plan, owner decisions and design: [notes/t6-agent-backends.md](notes/t6-agent-backends.md).
Starts after the T4.8 engine deletion; T6.0 blocks T6.5 onward. No live use without
owner authorization (T6.9).

### [ ] T6.0 Owner decisions: API-billed Claude credential, spend limits, per-project data opt-in, AGENTS.md rule amendment · owner · note
### [ ] T6.1 Capability probe: scripted fixtures plus one owner-authorized, budget-capped live probe recording transcripts for completed, interrupted and killed turns, structured output, MCP stdio and the Bash sandbox on the executor host · S · note
### [ ] T6.2 `AgentBackend` protocol and neutral `agent_errors.py`; `CodexAgent` and `DemoAgent` conform; no behaviour change · F+R · M
### [ ] T6.3 Backend binding: optional `ClaudeConfig`, project default, `contract.agent` saved at create (absent means Codex), refuse resume on a different backend, neutral MCP server spec · F+R · M
### [ ] T6.4 Per-backend admission: `AdmissionSource` (Codex windows unchanged; Claude spend ledger, per-turn cap, `RateLimitEvent`); `AgentPool` capacity per backend · S · M
### [ ] T6.5 `ClaudeAgent` backend: session, begin, finish, interrupt, structured output, MCP, permissions, sandbox, isolated `CLAUDE_CONFIG_DIR`, protected credential file · S · M
### [ ] T6.6 Exact Claude turn readback and restart scenarios (C12); uncertain turns never retried · F+R · M
### [ ] T6.7 Neutral activity events; UI and Worker show backend, model and spend; backend choice in project settings and creation · F · M
### [ ] T6.8 Lock the SDK in `uv.lock`; `doctor` version and credential checks; install and operations docs · F · S
### [ ] T6.9 Live pilot on Claude: owner-authorized, small brief, measured turns, cost and interventions (mirrors T4.7) · owner + S

---

## Log

- **T4.8 recovery scenarios:** migrates shared recovery to C4, and isolates three C3-specific stage and monitor cases. C4 now reads back saved turns after restart and wakes on resolver-proven submission absence. Ruff and the normal full suite (1,481 Python tests in isolated shards) passed. The archive-on scenario, historical compatibility and C4 synthetic group passed all 88 tests. Source +21/−7 and tests/docs +458/−494 across the three migration cards.
- **T4.8 archive startup:** the sweep reads the same supported-contract set as dispatch. With C3 archived in a disposable checkout, all 49 focused startup, historical-read, settlement and C4 demo tests passed. The earlier broader probe found 25 scenario failures in addition to four archive assertions corrected by this card. The final isolated suite passed 1,481 Python tests. Source +4/−4 and tests/docs +32/−12.
- **T4.8 archive controls:** the executor publishes the archive decision, the Worker preserves exact replay and bound discussion answers, and the browser gates stage controls without hiding evidence. Source +49/−22 and tests/docs +101/−2. Isolated Node 24 `make check` passed 1,482 Python, 42 web, and 52 gateway tests.
- **T4.8 archive preparation:** separates evidence format from stage authority and preserves late scheduler accounting without certifying failed historical results. The inventory now includes saved selective cancellations and monitor wakeups. Isolated Ruff, 125 focused tests and all 1,481 Python tests passed. Source +70/−28 and tests/docs +107/−13. The archive predicate is still opt-in in the synthetic tests.
- **T4.8 drain inventory:** adds a read-only report for saved C3 work and the exact executor's D1 queues (source +230/−0, tests/docs +205/−37). `status` and `inspect` omit global queues, so the new command queries copies of both databases. Isolated Node 24 checks passed with 1,477 Python, 42 web, and 51 gateway tests. The final staged Python suite also passed in six shards. The synthetic C4 demo reached a ready report, and the inventory CLI passed against synthetic SQLite and D1 copies. Fresh production drain evidence remains pending.
- **T4.2 tool closeout:** `8a8fd09` adds bounded Jean Zay previews, reusing C4 projection publication without agent wakeups (source +91/−12; tests +88/−3). Ruff, 59 focused tests and all **1,458 Python tests** passed in isolated shards. No live remote acceptance or GPU activation.
- **T4.8 opt-in C4 CLI demo:** `4fc9c59` and its fixture prerequisites run the real C4 dispatcher through exact approval, one synthetic CPU job and a ready report, using private temporary state with the gateway disabled. The C3 CLI default is unchanged. Ruff, five focused tests and all **1,454 Python tests** passed in isolated shards. No live activation.
- **T4.2 remote registered inputs:** `52298f8` reuses saved receipts and bounded file reads to stage exact bytes with the pinned remote source (source +118/−47; tests +75/−3). Ruff and 41 focused tests passed, including scripted transport. No live activation.
- **T4.2 registered-result inputs:** `d345db4`, `0059442` bind local input receipts to exact completed producers and verify the bytes the wrapper consumes (source +104/−3; tests +132/−1). Ruff, 38 focused tests and all **1,451 Python tests** passed in isolated shards. Remote input requests refuse before staging.
- **T4.8 browser create binding:** `a219e67` submits explicit C4 only with current project and executor support, without a C3 fallback (source +97/−2, tests +76/−0). Isolated Node 24 `make check` passed **1,440 Python, 41 web, and 51 gateway tests**; loopback owner acceptance also passed. The late unbound hosted-create refusal in `a8a8d4f` remains disabled until cutover.
- **T4.2 optional job controls:** `0328afe` adds local TensorBoard previews (source +49/−4, tests +99/−3); isolated `make check` passed **1,440 Python, 37 web, and 51 gateway tests**. `7892455` adds exact completed-job dependencies (source +26/−1, tests +140/−1); 23 coordinator-selected tests passed. No live acceptance.
- **T4.5 UI closeout:** `51c0763` extends local browser assertions for turn history, checkpoint metadata and the jobs panel (source 0; smoke script +10/−0). The isolated loopback browser acceptance passed with cached Chromium. The Worker/D1 tests retain hosted relay evidence; no private hosted or physical-device validation.
- **T4.6 scenario closeout:** `cb841e8` covers idle C4 steering on the saved thread (source 0; tests +39/−0). Ruff and 44 coordinator-selected tests passed. The scripted remote lifecycle and lost-turn readback tests cover the remaining C12 cases. No live activation.
- **T4.8 explicit create binding:** `ddfdbab` binds an explicit C4 create in the executor; `ae6b920` adds Worker validation, exact replay, and a 90-second capability gate for new explicit requests. In an isolated Node 24 checkout, all **1,436 Python, 37 web and 51 gateway tests** passed. Gateway formatting was corrected after the first `make check`; the remaining frontend and gateway checks then passed, including build and typecheck. No client or default switch; no deployment.
- **T4.4 autonomous CPU approval:** `d4c195f` and preceding T4.4 commits add exact attributed project approval, wakeup, stale-proof recovery and intent admission, reusing the existing project tool, pool, and compute authority. Combined source +445/−23; tests +461/−13. The new source verifies approval from a distinct project authority at settlement and dispatch; manual proof cannot carry this attribution. Ruff, 11 reviewer-selected tests and all **1,433 Python tests** passed in isolated shards. No live activation.
- **T4.6 failed-job checkpoint:** `4911f63` proves the latest saved private checkpoint seeds the debounced follow-up, without touching a prior successful job (source 0; tests +90/−2). Ruff and 40 focused tests passed. No live activation.
- **T4.2 remote submission:** `c391fa0`, `55fc319`, `f7fac62`, `ee77448`, `1d4e5f9`, `aa0bd36` wire bounded preparation, saved staging evidence, one fresh send and exact replay/recovery through the controller. Combined source +209/−24; tests +495/−29. Ruff, 36 focused tests and all **1,428 Python tests** passed in isolation. No live remote dispatch or activation.
- **T4.2 remote staging:** `be5beb8`, `b00bd9a`, `2792e81`, `5aed2cc`, `fe6f806` prepare, save and stage a CPU intent against its exact Jean Zay route. The upload is exported from the saved Git pin and retries use fresh pending destinations. Ruff, 48 focused tests and all **1,418 Python tests** passed in isolated shards. No send or live activation.
- **T4.8 synthetic dispatcher gate:** `1762d7a` exercises exact owner approval, one fake CPU job, debounced research follow-up, result registration and a ready report through completion (source 0; tests +213/−0). Ruff passed. The isolated suite passed 1,414 tests with one maintenance fixture error; that test and both new targets then passed after restoring the expected `recipes/` directory in the validation checkout. The C3 CLI demo and default are unchanged.
- **T4.3 remote delivery and observation:** `7d06123` delivers only the declared file using the job’s saved route, without replacing conflicting bytes (source +137/−2; tests +96/−0). `4068561` reuses exact observation and terminal wake settlement, keeping successful jobs unresolved until transfer completes (source +177/−42; tests +275/−0). Ruff, 68 focused tests, and all **1,413 Python tests** passed in isolated shards. Remote submission remains disabled; no live activation.
- **T4.4 policy guard:** `cc51dbb` reuses the existing Autopilot compute authority for C4 submissions before effects and at intent save (source +3/−0; tests +87/−0). It does not replace exact human approval. Ruff, 21 focused tests, and all **1,402 Python tests** passed in isolated shards. No live activation.
- **T4.2 remote result prerequisite and T4.6 conflict scenario:** `892a692` binds the declared result to its saved job without dropping historical resource defaults (source +43/−8; tests +53/−3); `9b571c2` exercises two C4 completion requests through real Git and one scripted C6 conflict resolution (source 0; tests +139/−0). Isolated Ruff and format passed. The 26 submit/result and 56 integration targets passed; all **1,401** Python tests passed across file shards, with the scenario directory rerun as a group to load its fixtures. Remote submission remains closed.
- **T4.2/T4.3/T4.5/T4.6 research loop follow-through:** `fd5f7ed` documents the result JSON (source +1/−1; tests +26/−0); `d8109fc` rotates with exact context evidence (+49/−11; +63/−0); `a39e704` handles C4 report status and export (+37/−15; +34/−0); `8741bba` relays C4 decisions and controls (+66/−12; +130/−0); `1f91475` detects orphaned C4 runs (+22/−3; +30/−0); `12808ac` and `6346a63` add quota, plot and owner-wait scenarios (source 0; tests +255/−0). Isolated Node 24 checks: Ruff, the full Python suite in eight file shards (**1,396 passed**), web formatting/build (**37 tests**), and gateway build/types (**50 tests**) passed. The C3 synthetic demo and C4 loopback browser acceptance passed separately. No installation change.
- **T4.7 approval repair and live pilot:** `c5216b3`. The follow-up carries lossless approval values, and the backend verifies the versioned loopback answer against its exact saved command and objective. Source +55/−9; scenario tests +20/−4. Isolated Node 24 `make check`: **1,385 Python, 36 web and 48 gateway tests passed** (one existing Python warning). Separately, the live pilot reached `complete` after one CPU job and a metadata-only operator intervention; detailed evidence stays in the private installation receipt. No rollout.

- **T4.5 loopback approval:** `4ab6dee`. Turn history is bounded and omits failed raw results. `node web/scripts/research-owner-acceptance.mjs` exercises the real local API with disposable synthetic state and headless Chromium; approval, replay and checkpoint privacy passed. Source +42/−18; tests/fixtures +343/−1. Isolated Node 24 `make check`: **1,385 Python, 36 web and 48 gateway tests passed** (one existing Python warning). No hosted or live activation.

- **T4.3/T4.5/T4.6 local research loop:** `0350874` enqueues a validated `done` integration decision through the existing C6 queue (source +35/−19; tests +35/−0). `bb9eabe` uses the private checkpoint at dispatch, not in the saved action input (source +36/−8; tests +36/−1). `e6d5acb` expands synthetic scenarios (source 0; tests +100/−1). `1d719f5` adds the local research detail and objective approval controls (source +238/−11; tests +29/−0). Isolated Node 24 `make check`: **1,385 Python, 36 web and 48 gateway tests passed** (one existing Python warning). The isolated contract-3 demo also completed. No live pilot.

- **T4.3 explicit research timers:** `a80d518`. Production +101/−17; tests +78/−0. Reuses saved wakeup obligations; scheduler and owner events keep their own debounce window. Fake-clock checks cover reopen, exact single binding, supersession and transaction rollback. Isolated Node 24 `make check`: **1,376 Python, 36 web and 48 gateway tests passed** (one existing Python warning). No live pilot.

- **T4.2 manual submission and T4.6 first scenario:** `5e88041`, `9fd44e4`, `cdf2ed9`. Production; tests: +24/−0; +96/−0, +19/−4; +14/−0, 0; +138/−0. Manual local submission is bound to a live owned turn. `done` settles atomically with experiment completion only after a ready report and resolution of pending work. Isolated Node 24 locked `make check`: **1,363 Python, 36 web, 48 gateway tests passed** (one existing Python warning). No live pilot.

- **T4.2 source preparation and dispatch:** `dbc3506`, `f1bdb64`. Production; tests respectively: +75/−0; +90/−0 and +77/−0; +87/−0. Preparation pins and exports the exact commit, preserves argv, and refuses occupied attempt directories. Fake scheduler tests prove fresh intent before send, pause refusal, and exact adoption after a lost response without a second submit. Locked Ruff and **1,356 Python tests passed** (one existing dependency warning). The isolated contract-3 synthetic demo completed; contract-4 MCP submission and the authorized live pilot have not started.

- **T4.2 saved submission intent:** `51a1a4b`. Production +142/−57; tests +55/−0. Admission and intent persistence share a transaction. The existing jobs foreign key uses one compatibility identity per job, not a stage graph. Duplicate receipts do not grant send permission. Locked Ruff and **1,352 Python tests passed** (one existing dependency warning). No scheduler dispatch or live pilot.

- **T4.3 steering and T4.4 manual admission:** `607e3c7`, `eedf0f5`. Production; tests respectively: +28/−2; +26/−0 and +91/−0; +54/−0. Steering consumes one bounded saved message in the same transaction as the follow-up. Admission has no external effects and is not exposed as a submission tool. Locked Ruff and **1,351 Python tests passed** (one existing dependency warning). No live activation.

- **T4.3 local job reconciliation:** `bfd7bfb`. Production +10/−2; tests +40/−3. Contract-4 local observation adopts exact missing scheduler IDs through the existing resolver and honors in-flight ownership. Locked Ruff and **1,346 Python tests passed** (one existing dependency warning). No live activation.

- **T4.4 manual approval requests:** `857d7f6`. Production +51/−12; tests +26/−3. Requests bind the saved brief, project, and proposed objective. Only the owner may decide; decline and cancel never grant approval. The decision survives consumption of the pending request. Locked Ruff and **1,345 Python tests passed** (one existing dependency warning). No submission tool or live activation.

- **T4.3 job wakes and T4.4 approval evidence:** `5f29487`, `b236c54`, `deed1f0`. Production; tests: +68/−0; +84/−0, +7/−0; +29/−0, +43/−21; +32/−1. Terminal local jobs wake by saved intent without entering the stage engine; owner and job evidence can share a turn. Approval verification checks the exact request, owner decision and satisfied obligation; it grants no dispatch authority yet. Locked Ruff and **1,342 Python tests passed** (one existing dependency warning). No live activation.

- **T4.2 report evidence and T4.3 job observation groundwork:** `febf9e5`, `a014dcb`, `b1bef0e`. Production; tests respectively: +61/−10; +136/−10, +4/−4; 0, +82/−0; +95/−0. Contract-4 reports now build from registered job results and verify against the exact saved metric sources; contract-3 report invocation is unchanged. The observer is not wired into the runner yet. Locked Ruff and **1,335 Python tests passed** (one existing dependency warning). No live activation.

- **T4.2 result tool bridge:** `825809b`. Production +18/−3; tests +50/−0. Separate contract-3 and contract-4 `register_result` schemas remain available; contract 4 returns a stable result receipt. Locked Ruff and **1,329 Python tests passed** (one existing dependency warning). No live activation.

- **T4.3 continue and T4.2 result persistence:** `956c3fe`, `7ccdd5a`. Production; tests respectively: +36/−12; +54/−3, +106/−17; +14/−5. A settled continue result appends a single action on the verified saved thread. Result registration keeps a stable bounded copy under the existing publication root; replay does not reopen the source. Locked Ruff and **1,327 Python tests passed** (one existing dependency warning). No live activation.

- **T4.3b and T4.2 cancellation/results:** `5ef2abc`, `ec83859`, `bbdd4d5`, `f7707eb`. Production; tests respectively: +183/−41; +259/−4, 0; +197/−0, +10/−2; +1/−0, +85/−0; +114/−0. Owner answers and wake intents commit together and only an exact settled predecessor can lead to a follow-up. Cancellation is available through the existing selective reconciler. Result validation reads only the exact successful job attempt; publishing it is a separate card. Locked Ruff and **1,325 Python tests passed** (one existing dependency warning). No live activation.

- **T4.2 figures and cancellation; T4.3a:** `37d68a9`, `62bf8e4`, `07c65d2`, `adfc366`. Production; tests respectively: +189/−0; +304/−0, +12/−0; +118/−0, +107/−10; +220/−0, +217/−0; +277/−0. Figures register an immutable copy under the existing publication root. Cancellation reuses the selective-cancellation writer and observer; its v4 adapter is not advertised yet. The wake queue reuses saved timer obligations and keeps distinct evidence. Locked Ruff and **1,299 Python tests passed** (one existing dependency warning). No live activation.

- **T4.2 owner and integration tool wiring:** `f75f44a`, `6a1c7be`. Production; tests: +10/−0; +179/−0 and +52/−20; +140/−21. Exact durable questions survive restart and reuse the owner-answer authority. Integration requests reuse the command queue rather than perform Git effects. Locked Ruff and **1,258 Python tests passed** (one existing dependency warning). No live activation.

- **T4.2 write tools and durable request foundations:** `fbd65eb`, `ace95d9`, `782d785`, `88e8da8`, `2d91ff2`, `f242a0a`. Respectively, production; tests: +141/−0; +248/−0, +91/−0; +231/−0, +44/−0; +213/−0, +198/−0; +300/−0, +64/−0; +186/−0, +31/−1; +161/−0. Reuses report versions, event redaction, owner obligations and command-based integration. Only checkpoint and report are exposed by this batch. Locked Ruff and **1,244 Python tests passed** (one existing dependency warning). Scripted turns and temporary Git only; no live activation.

- **T4.2a–b:** Scoped contract-4 MCP and cached/bounded cluster and job reads. Per-card production; tests: a `0adbccd` +115/−10; +169/−0; b `8607601` +156/−21; +235/−2. Reads reuse the existing controller socket and job log reader without scheduler or transfer effects. Isolated locked Ruff and **1,170 Python tests passed** (one existing dependency warning). No live activation.

- **T4.1e–g, 2026-11-18:** Isolated Node 24 locked Ruff and **1,150 Python tests passed** (one existing dependency warning) on byte-identical source/tests/scripts. Scripted managed turns and temporary real Git prove opt-in workspace setup, exact dispatch and restart readback without replay or stage advancement. The old engine remains until T4.8. Per-card production; test additions/deletions: e `2e92808` +200/−0; +299/−0, f1 `62a7be2` +196/−0; +303/−0, hardening `6ffcceb` +124/−28; +118/−3, f2 `2abca2c` +131/−2; +294/−0, format `4e3d9c1` +3/−11; +21/−19, g1 `7a8a518` +34/−8; +176/−0, g2 `5ff6007` +42/−0; +161/−0. Dispatch and settlement add a persistent-thread boundary while reusing the existing pool, intent ledger, resolver and Git commit primitives. Default remains 3; no live activation.

- **T4.1a–d, 2026-11-18:** `378e4ba`, `17b0f62`, `e1ba734`, `6196f95`. The bounded protocol and saved first-turn intent are inert; archive eligibility is separate from entry to the contract-3 engine. The queue and its admission obligation commit together. Numstat (production; tests): a +267/−0; +203/−0, b +65/−21; +234/−0, c +43/−13; +255/−4, d +96/−1; +167/−0. The prompt/schema add capability not present in the stage manifests; T4.8 retires those old execution paths. Isolated Node 24 locked Ruff and **1,092 Python tests passed** (one existing dependency warning), including 94 focused tests before the full run. No contract-4 create or live activation.

- **T3 closeout, 2026-11-18:** T3.5c durable integrator and T3.6c exact approval landed. Final delta from `080a3e1`: production +597/−27, tests +609/−3. Exact-source locked Node 24 `make check` at `f003147`: **1021 Python, 36 web, 48 gateway tests passed**, with one existing dependency warning. Isolated contract-3 demo completed and produced a report and figure. Conflict dispatch and browser approval were tested against scripted boundaries, not a managed account or a hosted installation. Failed and uncertain outcomes still require operator inspection. No live activation. Phase 4 is next.

- **T3.4d, T3.5b and T3.6 UI:** Completion opt-in, resolved-candidate check handoff, and branch/commit display with confirmed integration controls. Isolated `make check` passed **1013 Python, 34 web, 47 gateway**, with one Python dependency warning. Source publication only.

- **T3.1, 2026-11-17:** Named Git worktrees and exact exports/pins are available as primitives; T3.3a handles intent persistence and begins replacing existing checkout creation only for new eligible runs. Additive SQLite schema 25 preserves existing rows. Real Git tests cover bounded staging, exact archives and pins surviving pruning. Production +315/−2; tests +178/−4. Ruff check and format, 42 targeted tests, and **872 isolated Python tests passed** (one dependency warning). No live activation.
- **T3.2, 2026-11-17:** Shared environments and the fixed-interpreter option landed without changing runtime selection yet. T3.3b replaces per-experiment venv creation for eligible named worktrees; old and uncovered cases retain their existing runtime. The builder excludes its cache from Git and publishes readiness only after the exact dependencies finish building. Production +231/−3; tests +252/−0; example config +6/−0. Isolated Node 24 locked setup, Ruff check and format, 39 targeted tests, **878 Python tests passed** (one dependency warning), and contract-3 synthetic demo completed. No live activation.

Record completed tasks here: `date · task · commit · notes`.

- **T3.3a, 2026-11-17:** Production +212/−37, tests +284/−1. Named worktrees for new eligible runs leave historical shared and detached workspaces untouched. A once-ready worktree is never recreated on a missing ref or path.
- **T3.3b, 2026-11-17:** Named-branch implementation and job commits preserve bounded attempt copies, verify project source bytes against the exact commit, and pin new job identities before dispatch. A lost backend commit is adopted only against the saved intent; later edits are not swept into it. Eligible local runs replace per-experiment venv creation with the shared runtime; remote and historical runtimes are unchanged. Ruff and 21 coordinator targeted tests pass; combined `make check` passed **972 Python, 29 web, 45 gateway tests** (one Python dependency warning). No live activation.
- **T3.4a/b-1, 2026-11-17:** Candidate merge/revert/ref lifecycle (+266 production; +230 tests) and durable per-repo queue (+337 production; +157 tests). These are not wired to execution. Both intentionally add required capability rather than replace existing paths.
- **T3.4b-2/c-2/c-3/c-4, T3.5a, and T3.6a/b:** Reviewed runner, exact owner approval, local and gateway command paths, branch/commit projections, and conflict finalization. Isolated Node 24 `make check`: **1000 Python, 29 web, 47 gateway tests passed** (one Python dependency warning). Runner uses T3.3's saved commit intent and refuses stale checkouts. Agent dispatch and browser controls are separate pending cards. No installation activation.
- **T3.4c-1 and T3.7a, 2026-11-17:** Owner configuration supplies fixed check argv and an opt-in approval flag without dispatching integration. The legacy dry run verifies saved worktree identities and reports blocked checkouts without writing. Production +99/−3, tests +99/−2, example config +4/−0 across both. Combined locked Ruff and **917 Python tests passed** (one dependency warning). No live activation.
- **T3.7b, 2026-11-17:** Offline adoption commits only exact settled tracker-locked detached worktrees, preserving their existing paths and not editing main. Production +119/−23; tests +84/−1. Combined isolated suite: **944 Python tests passed**. An interrupted adoption requires inspection; no automatic replay. No live adoption performed.
- **T3.8, 2026-11-17:** Production +91/−7, tests +51/−5. Tracker-created folders get a committed starter `AGENTS.md`; imported folders are never written and can preview the template in the local new-project dialog. Real temporary Git coverage verifies prompts use the saved named workspace mode. The wheel contains the template; isolated loopback browser smoke rendered it at 1420 px and 390 px. Combined with T3.3a and T3.4a/b-1: locked Node 24 `make check` passed (914 Python, 29 web, 45 gateway tests; one Python dependency warning). No live activation.

2026-11-17 · T1.8a–d · 7ff0a11, e02aa91, a57ab65, this close-out commit · Flash workers, coordinator source/identity review and isolated combined acceptance. Corrected the initial inventory's invalid inference from today's create path to saved historical row shapes. Preserved terminal archived title settlement, archived recovery/repair entry guards, all six historical observation readers and four queued-report SQL variants. Deleted 21 dominated iteration predicates, 24 frozen rechecks and the now-orphaned `Store.initial_report` writer; no archive reader, exact settlement, mutable-state recheck or hold authority removed. Final predicate counts 17 / 28 (including the frozen definition), matching the [site table](notes/t1.8-site-inventory.md#10-final-tree-site-table-and-acceptance). Per-commit `git show --numstat`: a source 0, tests +85/−0, docs +533/−0 before final site-table additions; b source +78/−114, tests +7/−5; c source +0/−27, tests +65/−57; d source +5/−18, tests 0, close-out docs separately. Net production −76; phase delta +5,203/−2,596 = +2,607, still over the previously reported +2,500 threshold. a's one new Store regression test and b–d individually fit S/M budgets. Removed only the unreachable `test_dispatch_archived_during_turn_preserves_returned_identity_and_raw_result[begin/finish]` cross-await contract-flip cases: `test_saved_archived_turn_settles_identity_and_raw_result_without_advancing` retains exact thread/turn/result, stage/job/hold invariance, zero dispatch and replay assertions; unchanged `test_settles_exact_raw_legacy_turn_outcomes_and_replays`, `test_readback_uses_a_configured_connection_without_starting_a_turn` and `test_archived_operator_reconcile_saves_exact_raw_result` retain saved-fixture safety coverage. The initial-report obligation test now uses `working_report` with an explicit stable identity; no safety property deleted. Per-card archive/fixture/conversation/audit/scenario targets pass (a 115, b widened 183, c widened 120, d 115 plus 112 workflow/iteration and 5 publication tests). Combined Node 24.18.0 Ruff check/format and all 866 Python tests pass (453.80 s), byte-identical source/tests/scripts in the isolated validation checkout with the retained untracked `recipes/` fixture; fresh lanes' known maintenance copy failure was not counted as a pass. Locked `make setup` and a separate contract-3 synthetic demo passed; report and prediction figure produced. Web/gateway unchanged, so no `make check` required for this card. No live activation.

2026-11-14 · T1.1m-P-b3 · 3faf68b · Luna implementation and coordinator review finish Python toy-create producers. Saved historical knowledge fixtures retain selected project/execution identity and synthetic scope; backup now compares exact saved stage rows instead of a legacy-only count. Source 0; tests +42/−11 (53 changed). Isolated Ruff, 44 focused and full Python pass (580 tests). No live activation.

2026-11-14 · T1.1b-P1/P2/P3 · 5da0df5, 57cd5bc, 76c1a66 · Luna implementation, coordinator review against one Astra archival design. Inert prerequisites only: atomic/idempotent archive hold+notice; exact scheduler identity/accounting settlement without scientific reads/stage updates; raw saved-turn settlement and completed-check drainage. Review corrected invalid fixture JSON, rollback baselines, a future import cycle, and NULL-vs-zero deletion semantics. Per-card source/tests: P1 +24/+90 (114 changed); P2 +128/+195 (323); P3 +71/+175 (246). No deletions or runtime callers yet. Combined final isolated Ruff and full Python pass (580 tests); 22 archive targets pass. Locked Node 24 setup and four synthetic demo tests pass. P4 activation remains pending; no live activation.

2026-11-14 · T1.1m-P-b1/P-b2 · 365548a, 6d4bd82 · Luna fixture migrations with coordinator review. Simple producers now create contract 3; graph-backed progress/TensorBoard fixtures resolve validated schema-3 manifests with their original stage IDs. Maintenance history is explicitly static legacy state. The upload test covers both legacy revision filtering and current cross-revision artifact retention, preserving backoff and sibling progress. Source 0; b1 tests +45/−12 (57 changed), b2 +90/−11 (101). Final combined isolated full Python passes (580); P-b3 remaining producers precede h.6. No live activation.

2026-11-14 · T1.1m-timing · e7c0ff3 · Luna fixes for two observed full-suite races, reviewed by coordinator: exact task completion/tick consumption before lost-start operator recovery; explicit interrupted-turn event gate and durable failure before owner shutdown. Existing turn identity, one continuation, no duplicate dispatch and restart assertions remain; no production guard was weakened. Source 0; tests +23/−2 (25 changed). Focused recovery tests and final combined 580-test suite pass outside NSS isolation.

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

2026-11-14 · T1.1m-M9-r1…r5/M10 · 555d839…69aedf7 · Remeasured on current main (34 failures with the downgrade off, all previously classified). Retired legacy repair, toy-chain workflow, sealed-output writer, shell freeze, graph/recovery and temporary-prompt tests, each citing its current-contract counterpart (disposition §19); kept the five sealed-record verification tests (literal record) and the retained legacy recovery/cleanup rows on static state; removed the global downgrade, `current_contract` marker and the fixture generator. Test-only: +58/−1671, source 0. Gate probe: zero execution-entrypoint hits; only reader-helper, retained static rows and the cleanup allowlist remain. `make check`: 557 Python / 29 web / 41 gateway. T1.1b-1 is next and still decides `restart_rechecks[True]`; P-b precedes h.6. No live activation.

2026-11-13 · T1.1m-M8d/M8e · 17e52d0, b297a30 · M8d: measured gap fixed in production. `reconcile_outputs` gains one contract-3 branch (+45/−0) after the shared guards that validates the saved attempt's own root/manifest and commits `iteration.complete`, job/stage updates, pause and working report in one transaction; legacy body untouched. New `tests/test_iteration_output_recovery.py` (8 nodes; 4 fail on old source), legacy `failed_directory_run` fixture made explicitly static and the old observation `[False]` parameter removed. Isolated Ruff, targets and `make check`: 597 Python / 29 web / 41 gateway. M8e: test-only `tests/test_iteration_declared_outputs.py` (14 nodes, source 0) pins existing published/local declared-output guards; full Python 611. Remote contract-3 recovery and a Live run after repair-ceiling exhaustion stay unmeasured (disposition §17.1). Next: static legacy fixture rewrites, M9-r1…r5, M10, gate. No live activation.

2026-11-13 · T1.1m-P-budget · 8312f92 · Separate real mixed-graph probe proved the preparation cap incorrectly counted agent stages/rows. Final Opus S-design (`d7f2292e`), Flash implementation, fresh code approval (`2eaae260`) and coordinator acceptance. Current preparation now counts Slurm work only; historical counts and early-repair kept/live accounting remain exact. Initial cap-three and resource-repair cap-four positives fail on old source and pass fixed; a true cap-three denial preserves exact saved jobs/refs and dispatches nothing. Isolated Node 24 Ruff, 49 targeted and complete make check pass: 590 Python / 29 web / 41 gateway. Production +14/−7; tests +93/−0; 114 total changed lines (S <150). Error attribution/correction policy unchanged. M8d probes confirm wrong-root/incomplete current recovery; source design pending. M8e's corrected 14-case existing-guard S-plan is approved (`d22b29f4`) for parallel preparation, ordered landing after M8d. No live activation.

2026-11-13 · T1.1m-M8c · 4e67dd5 · Flash implementation plus coordinator review strengthenings, rebased after M8b before fresh Opus approval (`cd79c9a6`). Fourteen new current-contract nodes cover independent shared resource-revision gates with a real mixed graph, exact fresh scheduler identities and an agent-row budget positive; the configured three-repair ceiling across restart plus one duplicate-receipt manual fourth repair; and a 1024→2048 preflight resource amendment with one human approval and no successful-compute replay. No fallback was needed. Existing legacy guards remain. Isolated Ruff, 21 targeted and all 587 Python tests pass. Production source 0; tests +360/−1 (361 changed lines). Separate measured P-budget is approved (`d7f2292e`) as an S-card after M8c, preserving legacy/early accounting and excluding agent stages/rows from current preparation's Slurm cap; see disposition §16. No live activation.

2026-11-13 · T1.1m-M8a/M8b · 659a136, 3217d00 · Flash implementations, ordered final rebases and fresh Opus approvals (`25014dab`, `2554789c`). M8a's five controller and five exact-output recovery nodeids prove contract 3; the new uncertain second-budget-hold parameter preserves saved compute and refuses grants/receipts/actions/dispatch. Both positive grants replay identical receipts; saved quota-interrupted turns survive restart unchanged; schema-3 exact revalidation/refusals preserve turns and event idempotence. M8b converts compact job status, two session-retry variants and repaired readiness. Real fake-live repair completes with four calls/four submissions (failed initial train, three revision-2 producers), empty reuse and retained unused revision-1 preparation.json; no successful compute is replayed or old record upload awaited. Session retry resumes the same failed-before-dispatch action. Isolated Ruff, 24/26 targeted and all 573 Python tests pass after each rebase. Production source 0; tests M8a +174/−66 (240), M8b +214/−35 (249). M8c preparation is approved with disposition §15 corrections; a separate probe proves non-early C3 preparation incorrectly counts agent stages/rows against Slurm budgets, with a bounded S-card decision pending. No live activation.

2026-11-13 · T1.1m-M7b · 98ece19 · Flash implementation, rebased after M7a before fresh Opus approval (`dead8b5f`) and coordinator checks. Four current-contract resource/invalid-source functions retain implicit permission guards. GPU requests persist before uncertain dispatch and restart adopts the exact intent despite changed fallback configuration; actual repeated unavailable admissions leave zero jobs/intents, one notice and valid two-stage repair plans with explicit empty reuse. Live-policy five-stage metadata validation rejects a 24 GPU-hour owner cap and accepts default limits without dispatch. Broken source still fails specifically with does not compile, four bounded calls, no snapshot or jobs. Isolated Ruff, 30 targeted and all 572 Python tests pass. Production source 0; tests +137/−46 (183 changed lines). Opus `0ecc03e6` approved parallel M8a/b preparation with ordered final landings and mandatory uncertainty/retired-record amendments in disposition §14. No live activation.

2026-11-13 · T1.1m-M7a · fbbc289 · Flash implementation, rebased onto M6 before fresh Opus code approval and coordinator checks. Six watchdog/backup/project/workspace nodeids prove contract 3; current project prompt/catalog/privacy guards and explicit static historical-project fallback remain. Selected-folder writes now prove bounded per-attempt method.py copies and transport-only audit; a neutral discussion explicitly proves writable project routing with no new jobs or changed published objects. Read-only plan/writable prepare and all Git baseline/restart/exclusion guards remain. Isolated Ruff, 65 targeted and all 572 Python tests pass; optional comment wording fixed with identical AST and project tests rechecked. Production source 0; tests +116/−25 (141 changed lines). No live activation.

2026-11-13 · T1.1m-M6 · dca9d2d · Flash implementation, rebased after M4b/M11/P-a; fresh Opus code approval plus coordinator additions re-approved in `648d56bc`. Nine admission/model/reference nodeids now prove contract 3. Independent read-only project folders isolate global job caps; same-project tests retain read-only plan/writable prepare guards and prove a forged failed DB job cannot release the editor until scheduler confirmation. Single-turn serialization, uncertainty exclusion, paused-oldest progress, selected model/effort and atomic incomplete/deleted-reference refusal remain covered. Isolated Ruff, 16 targeted and all 572 Python tests pass on the final diff. Production source 0; tests +117/−48 (165 changed lines). M7a/M7b parallel preparation is Opus-approved (`fe1b0532`) with ordered final landings. No live activation.

2026-11-13 · T1.1m-M11 · abb980c · Flash implementation plus fresh Opus code approval after the explicit §12 legacy-tamper decision. Seven remote transport scenarios and the transfer-tool fixture now prove contract 3, preserving intent/job identity, lost-response adoption, polling/admission/transfer holds, saved routing and bounded tfevents retrieval. Reuse maps `test_remote_successful_checkpoint_reuse_stays_remote_and_rechecks_bytes` → `test_remote_successful_checkpoint_reuse_stays_remote`; only the deliberately absent scientific byte re-check is dropped. The old source-tamper test uses explicit static schema-2 state until h; a new contract-3 test proves manifest/attempt identity refusal and no receipt. Importer mkdir is idempotent. Coordinator isolated Ruff, 32 targeted and full Node 24 make check pass (572 Python / 29 web / 41 gateway). Production source 0; tests +232/−84 (316 changed lines). No live activation.

2026-11-13 · T1.1m-M4b · 74e7cf0 · Flash implementation of Opus `f8db76ba`'s final test-only S design, with coordinator final diff review. The renamed compact-reference test uses validated schema-3 parent/review and one original output row; actual wire length is 13139 characters under the unchanged 25 KB bound. Full plan/protocol/stages, analysis/limitations/presentation, brief/output identity, pointer integrity, stable replay and corruption refusal remain asserted. Retired as legacy-only: full-contract repair_history / manifest-wrapper assertions and the reference verify_snapshot monkeypatch/assertion; no audit history was added to current agent context and no production fix was opened. Isolated Ruff, 2 targeted and all 571 Python tests pass. Production source 0; tests +49/−20 (69 changed lines). No live activation.

2026-11-13 · T1.1m-P-a · 5fc3004 · Flash implementation of the Opus-approved S plan, coordinator diff review and final-main checks. Both ticking API producers now prove contract 3 and zero submissions; every authentication/CSRF/idempotency/conflict/stale-answer/path/no-store assertion remains. Maintenance API creates no experiment and is unchanged. Isolated Ruff, 15 targeted and all 571 Python tests pass. Production source 0; tests +12/−4. This closes the timing-dependent toy-create gate hazard, not the separate P-b stage/history fixtures. No live activation.

2026-11-13 · T1.1m next-card designs · 025f6df · Fresh Opus `f8db76ba` approved M4b as test-only: a genuine schema-3 measurement is 12902 characters, so no prompt-bound production bug is shown. Explicitly replace legacy full-contract/manifest and reference snapshot-rehash assertions with current direct plan/analysis/output-pointer guards; keep 25 KB, integrity, stability and corruption refusal. P-a's two ticking API producers are approved current-contract conversions. Opus `bc456c7a` permits M6 preparation in parallel but landing after M4b/M11/P-a, with real independent read-only project folders, IterationAgent fixtures and atomic incomplete-reference refusal. Details in disposition §10–11. Source/tests 0; docs only. No live activation.

2026-11-13 · T1.1m-M5 · 86b7e59 · Flash implementation, rebased onto M4 and the completed M0b note before fresh Opus review and coordinator checks. Three branching execution scenarios now assert contract 3 and use direct producing-output paths; parallel branches, join readiness, restart no-resubmission, exact branch cancellation, one replacement/result notification and autonomous graph amendment remain covered. Old schema-2 constants and invalid-graph unit tests are byte-identical for archived readers/importers; no global helper monkeypatching. Isolated Ruff, 15 targeted and all 571 Python tests pass. Production source 0; tests +97/−29 (126 changed lines). No live activation.

2026-11-13 · T1.1m-M0b · 7254d2e · Flash investigation, corrected after fresh Opus review and final approval `d5467d95` with coordinator wording/mapping amendments. Original pinned full runs retained: baseline 564 pass, 1126 records / 156 nodeids; downgrade-disabled 76 fail / 488 pass, 568 records / 92 nodeids, 58 toy-chain gate hits. Completed classification adds remote transport, declared-output safety, compact-reference and producer cards; preserves live legacy recovery/cleanup and schema-1/2 readers until their actual deletion boundary. M1/M2/M3 have moved 37 of those 58 toy-chain nodeids off execution; 21 remain at this point. §1–8 unchanged, conftest restored, probe uncommitted. Source/tests 0; docs +604. No live activation.

2026-11-13 · T1.1m-M4 · d440fb8 · Flash implementation, rebased onto M3 before fresh Opus review and coordinator checks. Nine execution tests now explicitly prove contract 3: seven steering/approval/clarification paths, fresh implementation-thread handoff and exact saved-title recovery. The 50k steering assertion now checks the exact revision-2 dispatch payload plus durable row; the legacy review field is replaced by contract-3 plan completion. All prior safety assertions remain; the compact-reference function is byte-identical and deferred to M4b. Isolated Ruff, 35 targeted and all 571 Python tests pass. Production source 0; tests +49/−28 (77 changed lines). No live activation.

2026-11-13 · T1.1m-M3 · 566ea41 · Flash implementation with fresh Opus code approval; coordinator final two assertions additionally pin unchanged attempt count. Fourteen observation/pause/lost-start nodeids now prove contract 3, preserving all nine exact-recovery outcomes, matched dispatch-event identity, historical prompt reconstruction, claimed-turn refusal and rollback. Agent-stage uncertainty now covers both lost start and lost completion across restart, missing-id refusal, one original producer and no dependent dispatch. Legacy analogue tests remain. Isolated Ruff, 52 targeted and all 571 Python tests pass on the final diff. Production source 0; tests +80/−28 (108 changed lines). Opus `1dd77343` permits parallel M4/M5 preparation with ordered final rebase/review/checks/landings. The added compact-reference conversion is a separate M4b card under the M0b review, not part of M4. No live activation.

2026-11-13 · T1.1m-M2 · 09c4d9b · Flash implementation, rebased onto landed P1 before fresh Opus review and final checks. All 17 discussion READER nodeids load the frozen fixture; saved evidence, replay/corrections, uncertainty, quota/pause, API auth, provenance, project inheritance and two saved parent actions remain asserted. Incomplete/deleted discussion refusal uses explicit saved phases without advancing. Coordinator isolated Ruff, 18 targeted and all 570 Python tests pass. Production source 0; tests +34/−42. Review notes that the original writable-project discussion path was incidental, not asserted; add a bounded explicit current-contract check with M7a if its budget permits. The pinned M0b dry run has finished (76 failed / 488 passed); its classification/addendum and all retirement gates remain pending. No live activation.

2026-11-13 · T1.1m-P1 · b387af4 · Flash implementation and fresh Opus F+R code approval. Discussion follow-ups always create `agent_experiment_v1`; the unchanged transaction and saved UUID preserve replay semantics. The marked test uses the frozen legacy parent with a non-default read-only project and proves contract 3, both reference fields, project/model/effort inheritance, one create receipt, repeated-completion idempotence, no compute and unchanged parent evidence. Coordinator isolated Ruff checks, 18 targeted and all 570 Python tests pass. Production source +1/−1 (net 0); tests +87. M2 is implementing in parallel in its own worktree under Opus `d90bf676`, but its final review/checks/commit follow P1. No live activation.

2026-11-13 · T1.1m-M1 · 96e0812 · Flash implementation with fresh Opus code review and coordinator corrections (valid fixed UUID, histories rejection, accurate loader docstring). Frozen legacy tree/rows generated from 2f311f3 after confirming no legacy-source changes since the pinned probe; retained historical hashes are unchanged. Six audit/reporting/knowledge readers now load saved evidence; five loader checks cover no dispatch, unknown columns, missing artifacts, histories and misplaced root tokens. Isolated Node 24 locked validation after final regeneration: Ruff clean, 31 targeted, 569 Python, 29 web and 41 gateway tests pass. Production source 0; tests/tooling +453/−19 (472 changed lines), docs +27, generated data +1921 separately. Opus `09a27616` permits landing before M0b's addendum; addendum approval and a current-main probe still gate all retirement/archival. No live activation.

2026-11-13 · T1.1m-M0c · f3fca77 · Flash implementation against the fresh Opus-approved marker plan; coordinator reviewed the 14-line diff. Registered `current_contract` in conftest only, with no uses yet; unmarked tests keep their historical contracts. Worker throwaway opt-in/unmarked probes pass without unknown-marker warnings. Coordinator isolated Ruff checks and all 564 Python tests pass (same baseline count). Production source 0; tests +12/−2. Remove marker plumbing and every use at M10. No live activation.

2026-11-13 · T1.1m sequencing · 58a2ad2 · Fresh Opus 5.5 decisions `59e3c2f9` and `09a27616`: add the temporary M0c marker prerequisite; allow retained-safety-test conversions/readers to land while M0b's pinned downgrade-disabled suite finishes. Its corrected baseline is 564 passing tests / 153 widened-probe nodeids at 34f29ec (31 extras beyond the original 122-nodeid disposition; advance itself records 121). Investigation and classification are not complete. Freeze the disposition note until its addendum; no retirement, downgrade removal, archive or legacy deletion before addendum approval and a current-main probe. Source/tests 0; docs only. M1's generator must record the actual landed-on source ref; no legacy source changed since the pinned baseline.

2026-11-13 · T1.1c-2 follow-up · 18b5b33 · Flash implementation after fresh Opus decision; coordinator reproduced and fixed the seeded-state reference-picker smoke failure. The script now mirrors the UI title fallback, chooses a completed reference and asserts its saved identity. Isolated Node 24 locked validation: empty-state and CLI-demo-seeded browser smoke pass (including offline report/figure export), Ruff clean, 564 Python, 29 web and 41 gateway tests pass; web typecheck passes. Production source 0; scripts +15/−6. No live activation.

2026-11-13 · T1.1m plan + M0 · cbb7c16, f716898, 9426245 · Flash disposition investigation of the 122 legacy-driving tests (56 CONVERT / 23 READER / 35 ANALOGUE / 7 RETIRE-ONLY / 1 ambiguous). Opus 5.5 decisions on sequencing (retire at the gate), 31 sampled rows, the §6 flags and the card order (REVISE; amendments recorded in §8). M0 moved `until`, `approve_plan` and `IterationAgent`/`PLAN`/`SOURCE`/`prepared` into `tests/helpers.py` with re-exports: tests +168/−156, production 0; Ruff clean, 564 tests collected and passing before and after. M0b was started but stopped for a handoff; its partial probe is saved in `notes/t1.1m-m0b-partial-probe.patch`. No live activation.

2026-11-15 · T1.1b-P4 · Luna implementation, coordinator review. Startup/tick archival, execution/continuation guards and raw exact job/turn settlement activated together; terminal legacy automatic job observation also avoids scientific output validation. Preserved manual terminal recovery, cancellation and readers. Replaced obsolete sealed restart check with damaged-output settlement for training/failed/complete records. Source +164/−19; tests +264/−30; 477 changed lines. Isolated Node 24 Ruff, 98 targeted tests and all 590 Python tests pass. No live activation. Queued report acceptance is separate b-A.

2026-11-15 · T1.1b-A · Luna tests, coordinator review and outside-NSS validation. Queued reports on a nonterminal archived static legacy record render from saved evidence; an actual missing metric fails only the report. Both preserve archived phase/hold and exact job/action rows with zero submissions/inference. Source 0; tests +58/−0. Ruff and 59 targeted reporting/cancellation/watchdog/synthetic-demo tests pass; P4 full suite passed 590 immediately before adding these two focused cases. T1.1b complete; ordered d–h deletion next. No live activation.

2026-11-15 · T1.1d · Luna inherited deletion, coordinator review and scope correction. Removed automatic legacy freeze/reference comparison and serial-chain/schema branches; retained current requested/dependency and uncertain-submission guards, positional pause phases and manual legacy compute lookup. Source +5/−36 (net −31), tests 0; 41 changed lines. Isolated Node 24 Ruff, 78 focused current/recovery/archive/API/report tests and all 592 Python tests pass. Reader/writer deletion boundaries recorded in the split note. No live activation.

2026-11-15 · T1.1e · Luna implementation and coordinator review. Removed unreachable Preparation execution/revalidation, toy retry and DemoAgent toy preparation response. Preserved saved models/schema readers, current Implementation and explicit terminal operator recovery. Source +1/−60 (net −59), tests 0; 61 changed lines. Isolated Node 24 Ruff and 54 focused tests pass; full tree with the independent inert b1 helper passes 599 Python tests. No live activation.

2026-11-15 · T1.1f · Luna implementation, coordinator review corrected removal of the still-consumed evidence_checks registry. Deleted automatic final-validation control, stage writes/seeding and strict execution checks; preserved saved-record readers, manual recovery and archive/cancel drainage. Source +0/−68 (workflow −47, Store −21), tests 0. Isolated Node 24 Ruff, 91 focused tests with independent inert b1 helpers and complete 602-test Python suite pass. No live activation.

2026-11-15 · T1.3b-b1 · Luna implementation and coordinator review with immutable scope/target, colon-ID isolation, caught-error savepoint rollback and bounded gap diagnostics corrections. Added inert stable responsibility episodes plus read-only latest lookup; explicit source-transition proof is a caller obligation, and terminal IDs never reopen. Existing core API/schema unchanged, zero runtime callers/backfill/projection changes. Source +171/−0; tests +276/−0; 447 changed lines (M). Ruff and 18 focused new/core obligation cases pass in aggregate (10 new, 8 existing); full tree with independent f deletion passes 602 Python tests. No live activation.

2026-11-15 · T1.1g · Luna implementation and coordinator review. Removed legacy reusable-stage hashing/copying and repairs-path guesses; preserved current explicit iteration reuse, early repair/resource revisions and saved journal/runtime fields. Source +14/−125 (net −111), tests 0; 139 changed lines. Isolated Node 24 Ruff, 103 targeted reuse/resource/remote/recovery/report tests and all 602 Python tests pass. h.1a/b remeasured at 230 total changed lines and combined into one M card by the same child; h.1c follows retirement of remote/cleanup fixture callers. No live activation.

2026-11-15 · T1.1h.1ab · Same Luna child implemented remeasured combined M card, coordinator review. Removed toy/source registration and sealing writers, their no-op current callers and exact legacy manual output-recovery body; legacy adoption now refuses before scheduler reads with no state change. Retired its five legacy refusal params and rollback test with body, preserving current3 rollback/identity recovery and archived local-manifest/audit/report/conversation readers. Source +9/−153 (net −144); tests +24/−44; 230 changed lines. Isolated Node 24 Ruff, 69 targets and complete 597 Python tests pass. No live activation.

2026-11-15 · T1.1h.4 · Luna implementation and coordinator review. Removed legacy remote transfers, recursive scientific identity/receipt worker and isolated tampering fixture/test; unsupported old worker manifests refuse execution. Preserved current3 declared recipe.py scripts, exact routing/polling/receipts/direct output references and cleanup until h5. Source +8/−155 (net −147); tests +1/−102; 266 changed lines after formatting. Isolated Node 24 Ruff, 55 focused remote/output/archive/recovery tests and all 596 Python tests pass. No live activation.

2026-11-15 · T1.1h.5 · Luna implementation plus coordinator review. Removed automatic completion cleanup and obsolete task registry together with maintenance/scenario consumers; retained temporary declarations and historical sealed-checkpoint verification. Retired matching cleanup/helper tests. Coordinator added explicit verify/seal operation allowlist so retired cleanup cannot fall through to receipt generation, with two refusal cases. Source +5/−136; tests +9/−107; 257 changed lines. Isolated Node24 Ruff, 79 targets and all592 Python tests pass in365.37s. No live activation.

2026-11-15 · T1.1h.1c · Luna implementation and coordinator review narrowed scope to preserve original validated-plan/dependency submission gate. Removed schema1/2 implementation/dependency freeze writer and empty current marker; old schemas refuse before filesystem effects, reader fixtures use static legacy records, current3 manual recovery stays and legacy adoption refuses after completed replay. Source +3/−101; tests +89/−10; 203 changed lines. Initial full run found one obsolete dependencies monkeypatch; removed it with method, keeping reader guards, then all14 iteration cases pass. Ruff,127 targets plus30 controller/resource checks and combined full618 tests with inert b2a pass. No live activation.

2026-11-15 · T1.3b-b2a · Luna implementation/review correction and coordinator acceptance. Added inert SQLite-only action responsibility mapping with before/current source proof, exact running identity, backend reconciliation, immutable source scope/phase/evidence and allowed owner/kind pairs. Terminal missing history stays missing, unproven active/terminal conflicts diagnose, frozen/deleted/cancelled pending work abandons stale obligations. No source mutation, dispatch, runtime hooks or projection changes. Source +122/−0; tests +290/−0; 412 changed lines. Ruff,23 new/41 core-helper cases and full618 combined with independent h1c pass. Source writer audit is read-only; wiring awaits final T1.1 tree.

2026-11-15 · T1.1h.2 · Luna implementation and coordinator review. Removed shipped toy recipe and recipe_dir configuration/doctor/init wiring; old YAML ignores only the retired key. Synthetic CPU bound and current fresh resource authorization remain. Source/scripts/recipe +13/−180; tests +35/−7 (235 changed lines); example −1. Ruff,47 focused cases and all620 Python tests pass (400.31s; durable XML retained in validation checkout). Updated installation and architecture guidance. No live activation.

2026-11-15 · T1.1h.3 · Luna prompt retirement, coordinator scoped application preserving adjacent submission/dependency changes. Removed generic/toy prompts and old execution guides; current iteration guides unchanged. Historical execution metadata remains readable with an explicit archival guide and frozen scientific prompts refuse. Source +26/−385; old runtime guides −28; tests +29 (468 changed lines). Ruff,45 focused checks and all621 Python tests pass in418.68s. No live activation.

2026-11-15 · T1.1h.6 · Luna implementation, coordinator review and scoped historical fixture correction. New local/gateway creates accept agent_experiment_v1 only and seed plan/prepare/interpret/publish; saved command receipt replay remains first and old recipe projection enum/models stay readable. Source +21/−42; tests +82/−4 (149 changed lines). Ruff,11 targets,623 Python,29 web,gateway build/types pass. Initial gateway run found two old publication fixtures inheriting contract3 plus one cascading reference failure; marked their saved contract2/toy identity explicitly, preserving every assertion, then all42 gateway tests and final format pass. No live activation.

2026-11-15 · T1.1h.7a · Luna implementation rebased from current root, coordinator scoped method application/review. Removed dead legacy budget/submit/checkpoint observer arms and unused check_files/semaphore, retaining validated dependency/review/resource/intent gates, script-less current3 recipe.py and all check registries/drainage. Source +22/−107 (net−85), tests+42;171 changed lines. Ruff,70 focused checks with inert job/admission helpers and full645 Python tests pass (417.90s). No live activation.

2026-11-15 · T1.3b-b5a · Luna implementation and coordinator historical-ID correction/review. Inert SQLite-only job responsibility mapper: unknown ID backend reconciliation, exact saved ID external job, known scheduler terminal settlement closes only existing episodes; failed rows alone remain unresolved and agent pseudo-jobs never become external work. All historical episodes preserve exact scope/stage/attempt/scheduler identity. Source+154/tests+271 (425 changed lines); Ruff,12 focused cases and combined full645 Python pass. No runtime hooks/projection/backfill or live activation.

2026-11-15 · T1.3c-c1a · Luna implementation and coordinator narrowing/review. Inert validated source-less admission decisions on backend advancement episode0; no source-reference parameter until typed source families land. Exact generation/scope/ownership, closed reason/wait pairs, payload types and caller transaction validated; deleted/terminal/later-lifetime use rejected pending c1b. Source+193/tests+147 (340 changed lines); Ruff,8 focused cases and combined full645 Python pass. No runtime callers, hold projection, backfill, source mutation or live activation.

2026-11-15 · T1.1-timing · Luna test-only change, coordinator review. Missing-manifest correction fixture runs up to three real Matplotlib plotting subprocesses; its default30-second total bound timed out once (657 other cases passed). Both variants passed isolated in44.32s combined. Give just this multi-turn wait an explicit60-second bound; preserve every result/error/action-count/distinct-turn/no-submission assertion. Tests+2/source0. Ruff,2 targeted variants and the full658 Python suite pass. No production or live-state change.

2026-11-17 · T1.1h.7b · Luna implementation and coordinator policy, identity and size review. Frozen retry/title/steering/review mutations reject consistently; active archives stay cancel-only across gateway/UI. Completed legacy replies require their saved live discussion action and owning ready task. UI uses minimal disabled props to preserve markup and stay within budget. Source+101/−8; tests+296/−1 (406 changed lines). Ruff,63 focused shared checks and full658 Python pass; final Node24 format/build/types,29 web and45 gateway tests pass. No live activation.

2026-11-17 · T1.1h.7c · Luna call-graph audit and implementation, coordinator review. Removed the dead Controller legacy registration arm, schema1 sequential recovery fallback and toy publication/renderer paths. Current graph validation, metrics/figure escaping and archived audit/conversation/queued-report readers remain. Source+62/−92 (net−30); renderer test+40;194 changed lines. Ruff,63 shared focused cases,658 full Python,29 web and45 gateway tests with Node24 build/types/format pass. T1.1 d–h complete; runtime obligation source integration follows. No live activation.

2026-11-17 · T1.3b-b2b · Luna implementation and coordinator scoped integration/review. Workflow action creation, thread/turn saves, state changes, reconciliation and report correction sync obligations in their source/event transaction. Post-await callbacks verify exact action/phase/evidence/thread/turn; identical terminal callback replay is a no-op and changed identity refuses mutation. Source+139/−41; tests+259 (439 changed lines). Ruff,94 targeted checks and full665 Python tests pass (488.63s). Store/conversation/controller and other source groups remain pending; projection/backfill unchanged. No live activation.

2026-11-17 · T1.3b-b3 · Frozen Luna candidate, independent Luna review and coordinator scoped integration. Store continuation/session retry/title admission and Conversation queue/correction/dispatch/readback synchronize obligations with existing source/event transactions. Async callbacks check saved action identities; completed-parent discussions retain their reader path. Added correction rollback/child-admission and terminal lifecycle/restart history assertions. Source +35/−5; tests +238/−0; 278 changed source/test lines (M). Ruff,46 targeted cases across focused runs and full670 Python tests pass (423.97s); logs/XML in isolated validation check-results/t1-b3-*. No projection/backfill or live activation.

2026-11-17 · T1.3b-b7 planning · Focused Astra read-only review and coordinator acceptance of transfer replacement design. Explicit pending admission models complete-row replay; unchanged pending retry retains age; exact episode/identity guards refuse stale callbacks. Planned exact preview identity/terminal supersession and Store cancellation are separate bounded cards; historical UUID diagnostics remain mandatory b14/d1 acceptance. No schema/config/reasons, retry cadence or live activation changed. Documentation only; production delta zero and phase-wide net-negative forecast remains unfunded.

2026-11-17 · T1.3b-b4 · this commit · Luna implementation, independent review and coordinator integration. Monitor action creation and pending supersession (now with an existing agent_action_changed event), same-transaction cancellation supersession of pending actions, and exact expected snapshots for cancellation/controller/archived readback callbacks. Source +30/−8; tests +97/−1. Ruff, 33 targeted and all 672 Python tests pass (413.62s). No live activation.
2026-11-17 · T1.3b-b5b · this commit · Luna implementation, review fixes, coordinator integration. Intent insert before dispatch, accepted scheduler ID, missing-ID adoption and failed-output recovery terminal save synchronize job obligations in their transactions. New `job_obligations.locked_job` re-reads saved identity in the write transaction; `sync_backend` keeps ledger conflicts off the scientific ValueError path. Accepted-ID save refuses a changed row (replay of the same ID is a no-op) and adoption refreshes the saved snapshot. Source +37/−2; tests +291. Ruff, 26 targeted and all 678 Python tests pass (421.58s). No live activation.
2026-11-17 · T1.3b-b6 · this commit · Luna candidate with three review fixes, re-integrated by DeepSeek Flash onto b5b with coordinator review. Observation (transfer-pending accounting, normal observation, validation failure), cancellation adoption/terminal settlement, archived legacy settlement and steering observations re-read the saved job via `locked_job` and synchronize obligations in the same transaction. Adoption refreshes the saved snapshot; parent cancellation is rechecked inside the observation write; mapper conflicts take the backend-error path, never scientific failure. Source +53/−11; tests +196/−2. Ruff, 94 targeted and all 682 Python tests pass (430.96s). No live activation.
2026-11-17 · T1.3b-b7a · this commit · Luna candidate reviewed and tightened by DeepSeek Flash, coordinator review. Transfer admission saves pending before each operation (complete→pending replay opens a new episode; unchanged pending retry keeps its age), rechecks parent cancellation and immutable scope in that transaction and emits the existing state_changed event. One outcome helper verifies the admitted episode before saving success/failure; stale callbacks refuse, cancelled rows never dispatch, and an unchanged pending failure emits no event. New `core/transfer_obligations.py`. Source +166/−13; tests +187. Ruff, 25 targeted and all 688 Python tests pass (411.45s). No live activation.
2026-11-17 · T1.3b-b8+b9 · this commit · DeepSeek Flash implementation, coordinator review. Every report_versions writer (creation seam, report_state, artifact registration to publishing, cancellation) synchronizes a backend publication obligation keyed by exact (experiment, version) in its transaction; ready satisfies, failed/cancelled abandons, readiness stays separate from artifact delivery. Final publish writes no report row and needs no hook. New shared `obligation_sources.sync_episode` also replaces the transfer mapper's own branches (−5). Source +142/−29; tests +191. Ruff, 55 targeted and all 696 Python tests pass (433.45s). No live activation.
2026-11-17 · T1.3b-b7b/c · this commit · DeepSeek Flash implementation, coordinator review. Nonterminal downloads use a deterministic per-job preview identity; terminal-delivery admission cancels only that job's pending preview (abandoning its episode) in the same transaction and later preview admission is refused. Store cancellation abandons exact transfer obligations in its existing transaction. Source +44/−7; tests +205/−4. Validated together with R1 and b12 (independent files except store.py imports): Ruff, targeted and all 711 Python tests pass (429.17s). No live activation.
2026-11-17 · T1-R1 · this commit · DeepSeek Flash implementation, coordinator review. One caller-transaction `action_obligations.transition` (re-read, exact expected-snapshot check, UPDATE, sync, saved row) replaces eight duplicated dispatch/turn/_action_state/correction blocks in Workflow and Conversation; the guarded reconciliation updates share `expected_saved`. Event names/payloads, extra writes and comparison choices unchanged. Source +85/−112 (net −27); tests +46. Validated with b7b/c and b12: all 711 Python tests pass. No live activation.
2026-11-17 · T1.3b-b12 · this commit · DeepSeek Flash implementation, coordinator reduced its mapper to `sync_episode` (now carrying due_at). Every controller agent_wakeups writer synchronizes a backend timer obligation with the saved due time in its transaction; queueing the saved action or completion satisfies, supersession/failure abandons. Validated with b7b/c and R1: all 711 Python tests pass. No live activation.
2026-11-17 · T1.3b-b10/b11 · this commit · DeepSeek Flash implementations (b11 built on b10), coordinator review/merge; landed together because both hook Store.update and cancellation. Pending requests carry owner_answer obligations keyed by exact saved request ID (recorded answer satisfies; clearing/replacement abandons; malformed saved requests are diagnosed; one synthetic fixture now uses the real saved shape). Plan-review generations carry owner_approval obligations keyed by exact saved revision across Store.update, cancellation, review command, automatic/selective repair, planning steering and the Autopilot approval writer (approval satisfies; replacement, rejection or cancellation abandons; same-revision drafting revalidation keeps the generation open). Source +210/−8; tests +340/−1. Ruff and all 734 Python tests pass (478.63s). No live activation.
2026-11-17 · T1.4a · this commit · DeepSeek Flash implementation with two supervisor decisions, coordinator review. New `resolver.py` owns every saved intent without a scheduler identity: one match is adopted once (shared `adopt` now used by observation, cancellation and archived settlement), several matches quarantine with the existing hold/notice, an unreadable scheduler concludes nothing, and no record after `accounting_lag_seconds` (new per-execution-context setting, default 600) while still inside the seven-day accounting window ends the intent as failed `not_submitted:` with a `job_reconciled` receipt and abandons its backend obligation. Only such proven-absent intents stop counting as active predecessors, so selective repair submits a new intent; the old intent is never resubmitted. Source +194/−53; tests +414 (card exceeded its M band by test volume; accepted after review). Ruff, 123 targeted and all 746 Python tests pass (434.64s). Live repair dispatch after proven absence is not exercised in demo mode. No live activation.
2026-11-17 · T1.4b · this commit · DeepSeek Flash implementation, coordinator merge into the shared `resolver.py`. `TurnResolver` runs in the tick after dispatch drainage and before advancement: it reads the saved thread through new `CodexAgent.read_thread`, adopts a completed matching turn through the operator readback (shared `adopt_turn`/`dispatch_prompt_hash`), proves absence after 5 minutes on a readable thread (failed `turn_not_started` plus exactly one continuation in the same transaction; monitor actions excepted), quarantines several matches, and after six unreadable reads holds only that experiment with the existing hold and one notice. Demo threads refuse readback, so absence is never inferred there. Source +218/−46; tests +229/−3. Ruff and all 749 Python tests pass (454.25s). Discussion absence and multi-match quarantine lack committed scenario tests. No live activation.
2026-11-17 · T1 handoff · this commit · Coordinator stop at owner request. Uncommitted reviewed candidates R2 (core consolidation, src −68) and b13/c1b (advancement lifetime) and the T1.5a brief are described in notes/t1-handoff-2026-11-17b.md, with the read-only reduction audit. Phase production delta is +1282; net-negative needs an owner decision (see note). No live activation.
2026-11-17 · T1 net-source rule · this commit · Owner delegated the decision; coordinator waived the Phase 2 net-negative target and replaced it with a per-card replacement rule plus named retirement cards (README; t1-owner-decisions.md §7). Phase delta +1282 recorded; owner is told above +2,500. Documentation only.
2026-11-17 · T1 scope cut · this commit · Owner decision: defer T1.3c c1c–c4b and T1.7a–f (they only reshape the contract-3 engine deleted by T4.8). T1 remaining: b13, b14, d1a/b, T1.5a–c, T1.6a–c, plus R2 cleanup. Faster working method recorded in README and t1-owner-decisions.md §8. Documentation only.
2026-11-17 · T1.3b-b13 · cf87b56 · DeepSeek Flash candidate, coordinator review and integration. Contract-3 creation opens one backend advancement lifetime; every terminal phase writer (completion, failure, finish_agent, Store.update, commands, steering replacement, cancellation) closes it in its transaction (complete satisfies, otherwise abandons) and a proven terminal-to-active retry/repair opens the next episode. Cancellation opens a separate cancellation reconcile responsibility (pending/unconfirmed) that the existing cancellation procedure satisfies. Legacy/archived experiments get none; decisions read the closed lifetime as the final record. Includes c1b. Source +167/−26; tests +179/−14. Replaces the c1a "unsupported until c1b" refusals. Validated with R2: Ruff, 134 obligation tests and all 756 Python tests pass (463.92s). No live activation.
2026-11-17 · T1-R2 · 37404d2 · DeepSeek Flash candidate, coordinator extended it to b13. Mappers share one transaction guard, history lookup, project lookup, saved-JSON reader and `terminalize`; request mapping uses `sync_episode`; advancement/cancellation use one `_open` and `terminalize`; nine private copies deleted. Source +124/−234 (net −110), no test changes; churn 358 accepted as a mechanical wiring card. Validated with b13 (756 passed). No live activation.
2026-11-17 · T1.5a · 91d3f5d · DeepSeek Flash implementation, coordinator review. `Workflow.tick` is a manual adapter over twelve named steps in the original order (intake, runtime, observe, post-observe control, resolver, maintenance gate, advance, watchdog, finish, supervisor, research, retention, backup); moved code unchanged; `HEALTH_COUNTED_STEPS` fixes the health set (control, observe, research, supervisor, watchdog, retention, resolver). Source +75/−5; tests +83. Validated with b14. No live activation.
2026-11-17 · T1.3b-b14 · 6e81545 · DeepSeek Flash implementation, coordinator trimmed wrappers. `core/obligation_backfill.py` replays every saved live source (actions, jobs, transfers, requests, reviews, report versions, wakeups, contract-3 advancement lifetimes, outstanding cancellations) through its existing mapper with `before=None`, in rowid keyset batches with one savepoint per candidate; conflicts are counted and logged, never manufactured. Runs once in `Workflow.start` right after the legacy archive sweep. Source +173/−13; tests +248. New capability; all mapping reuses the mappers. Ruff and all 765 Python tests pass (453.15s). Intermediate-hook conflicts are diagnostic only. T1.3b complete. No live activation.
2026-11-17 · T1.5b · a97b3d4 · DeepSeek Flash implementation, coordinator review. Confirmed race: observation reading a submitting intent while `sbatch` is in flight proved false absence or wrote a false `submission_uncertain` hold. A lane-shared in-memory `submissions_in_flight` token covers the intent from its commit until the accepted-ID/uncertain save; observation skips it and `resolver.resolve` re-reads the saved row before reading the scheduler and before every hold write. Existing CAS adoption/absence reused. Source +68/−21; tests +309. Validated in batch (797 passed). No live activation.
2026-11-17 · T1.6a-1 · c193841 · DeepSeek Flash implementation, coordinator review. New `agent_pool.py` shadow registry keyed (source_table, action_id) with typed owner keys; `Workflow.spawn_agent` claims before `create_task` at all seven admission sites (stage, two title sites, monitor, discussion, research, supervisor) and settles from the source row on task end (terminal/pending release; dispatching/running/uncertain/received keep logical ownership). `Workflow.start` reconstructs from the three source tables before `connect_agent`. Nothing reads it yet. Source +194/−7; tests +328 (529 lines, accepted). Retirement targets for a-2/a-3/b recorded in the worker report: `_request` research fallback, lane scans by `active_action`, quota/shutdown lane loops, global role guards. Validated in batch. No live activation.
2026-11-17 · T1.3d-d1a · 9707a13 · DeepSeek Flash implementation, coordinator made unsupported pairs a reported cause. Pure `core/liveness.py` classifies every open obligation against its exact saved source (turn identity, owner notice, job/transfer row, wakeup due time, advancement dependency) or process-local backend service evidence with one interval of restart grace, and reports contract-3 experiments missing a lifetime. Read-only; no watchdog change. Source +222; tests +233. Batch: Ruff and all 797 Python tests pass (451.84s). No live activation.
2026-11-17 · T1.5c · 10f1b7e · DeepSeek Flash implementation, coordinator review. New `engine/runner.py` replaces the API tick loop with named periodic tasks (control, observe, watchdog, research, supervisor, retention, backup) at the existing cadences; control/research/supervisor share one primary-agent lock; the turn resolver stays inside control after draining and before advancement; gated tasks skip under maintenance as before; `maintenance.launch()` follows each successful control pass. `/healthz` uses the oldest last success over `HEALTH_COUNTED_STEPS` plus the latest failure. Shutdown stops scheduling, awaits the threaded retention pass, then bridge, workflow, store and lock. Removed the API loop and its state fields; `Workflow.tick` stays the manual adapter. Source +215/−32; tests +270/−2. Validated in batch (815 passed). No live activation.
2026-11-17 · T1.6a-2 · d5cad0e · DeepSeek Flash implementation, coordinator replaced per-notification table scans with a reservation-by-primary-key lookup. Callbacks carry the App Server process start count; requests, activity and answers route only to the reservation whose saved thread/turn matches on that exact process; answers are bound in memory to (process, reservation) and sent at most once, otherwise expired. Removed the research request fallback, `active_action` routing in `_request`/`_activity` and research's task-liveness reply check; unmatched and supervisor requests fail closed. Source +136/−48; tests +293/−18. Ruff and all 815 Python tests pass (464.08s). No live activation.
2026-11-17 · T1.3d-d1b · aaf67d4 · DeepSeek Flash implementation, coordinator review. `engine_watchdog` now runs `liveness.inspect` in one read transaction and emits one deduplicated `engine_stuck` notice per (experiment, cause, obligation); project-scope defects are logged only. Control and observation passes record process-local service evidence (`CONTROL_SERVICE`, `OBSERVE_SERVICE`) on return in both the runner and the manual tick. With derived holds deferred, an advancement lifetime without another open obligation is live while its saved revision moved within the interval or its hold is owner-owned. The interim STUCK_SQL heuristic is deleted. Source +93/−94; tests +216/−112 (515 lines, accepted). T1.3d complete. Validated in batch. No live activation.
2026-11-17 · T1.6a-3 · fe05393 · DeepSeek Flash implementation, coordinator review. Quota interruption, owner pauses, cancellation, maintenance blockers, supervisor/research interruption and readback enumerate reservations and act only through the reservation's live process (readback may use any configured connection, never a new turn); out-of-task settlements re-settle reservations; `Workflow.close` sets closing first, then closes roles, drains checks, withdraws every lane task and closes each transport once. Removed `hold_active_action`, both quota lane loops, the three-way busy probe and the lane/`active_action`/assignment scans. Logical-only reservations are never interrupted through another process (they keep their saved uncertainty). Operator `reconcile_action` still reads through the primary agent. Source +213/−99; tests +315/−7 (634 lines, accepted as retirement-heavy). Ruff and all 823 Python tests pass (463.80s). T1.6a complete. No live activation.
2026-11-17 · T1.6b-1 · 5250ade · DeepSeek Flash implementation; coordinator restored the lane cluster-snapshot copy and fixed one test helper the worker missed. All seven admission sites keep their deterministic preparation and authority checks, then offer a `Proposal`; `AgentPool.admit()` sorts by priority class (conversation > experiment/monitor > research > supervisor > title), each role's scan rank and saved identity, and claims a free ready connection plus a free typed owner (live claims and saved dispatching/running/uncertain/received rows) in one synchronous section before `create_task`. Selection runs at the end of the control pass and of the supervisor and research steps (under the primary-agent lock). Legacy busy flags are published by the pool; all existing global guards remain eligibility rules. Removed `spawn_agent`, `agent_lane`, `agent_assignments` and the stage lane-delegation recursion. Source +210/−60; tests +297/−45 (607 lines, accepted). Full suite: 828 passed plus the 4 fixed helper cases (508.33s). No live activation.

2026-11-17 · T1.6b-2 · 75bba92 · DeepSeek Flash implementation, coordinator review. `AgentPool.quota(eid=/run=/origin=)` centralizes cached selection and post-session binding admission using the existing Autopilot reserve policy and weekly plenty threshold (moved to quota.py); role-local copies replaced. Never-sent claims release their legacy busy flags without consuming result-drain paths for sent work. Monitor/discussion member turns now respect run reserves as required by the design. Source +92/−37; tests +145/−6. Ruff and all 836 Python tests pass (485.68s). Phase source delta from 42829f4 is +2592, over the +2500 notification threshold; owner notified. No live activation.

2026-11-17 · T1.6b-3 · 04f0878 · DeepSeek Flash implementation after timeout recovery; coordinator found and required the negative-capacity regression before acceptance. All roles dispatch on assigned lanes; conservative capacity counts live claims plus unclaimed dispatching/running/uncertain identities across all three action tables once, and rejects zero/negative capacity after shrink. Received results retain owner exclusion without turn debt; unsent live claims reserve capacity, unlike unclaimed pending rows. Removed supervisor experiment-wide suppression, research lane/global discussion waits, global stage/title/monitor/conversation unresolved blockers and lane-local admission checks; retained per-experiment exclusion and binding authority/quota checks. One control selection merges proposals from all role steps. Source +125/−110; tests +421/−11 (667 changed lines, reviewed overrun; required safety follow-up below landed in the same batch). Synthetic overlap/debt tests do not complete the T1.6c recovery matrix. No live activation.
2026-11-17 · T1.6b-3 safety · 243ea59 · Coordinator review required two blockers fixed before activation: reconnect eligibility now checks live reservations on the exact process across all source tables (not just lane.agent_task); selection uses a deterministic head-of-role merge instead of a non-transitive comparator, skipping ineligible heads without hiding successors. Source +47/−24; tests +87/−26 (184 changed lines). Both commits pass Ruff and the full isolated 848-test Python suite (460.20s), XML/log at check-results/t1-6b3-full.* in the validation checkout. T1.6b complete; T1.6c scenarios remain. No live activation.

2026-11-17 · T1.6c-1 · 35c4499 · DeepSeek Flash implementation, coordinator review. Separate multi_scenario fixture clones no-argument synthetic connections against one explicit FakeCodexBackend; one-slot fixture assertion preserved. Added finish gates, quota knobs and call logs by slot/instance/start count; driver awaits deduplicated lane/research/supervisor tasks. Six real-engine scenarios prove experiment+research, experiment+weekly synthesis and discussion+unrelated experiment overlap with independent result application, one-slot serialization, title→approved implementation same-owner exclusion with spare-slot progress, and research+weekly typed-project exclusion (weekly started before research activation because simultaneous weekly enqueue under active research is prohibited). Production source 0; tests +591/−15 (606 changed lines, reviewed overrun for shared fixture plus six scenarios). Ruff and all 854 Python tests pass (451.78s). c-2/c-3 remain; fake process counters alone are not managed-runtime acceptance. No live activation.

2026-11-17 · T1.6c-2 · 91723b8 · DeepSeek Flash implementation, coordinator review. Four synthetic recovery scenarios preserve accepted-then-lost experiment/project turns across restart (no invented project lost-start adoption), prove 2→1/debt-two negative capacity with exact readback and eventual positive progress, keep unreadable turns counted while independent work uses remaining capacity, and exercise the production CodexAgent callback/request generation path against an in-memory stub transport. Normal answers reply once to the originating connection; obsolete answers expire, wrong-connection requests fail closed, other-owner state/activity unchanged. Shared fake logs read_thread/read_turn/interrupt/reply; MultiScenario.restart supports shrink. Direct-row fixtures are labeled as accounting/routing evidence, not dispatch integration. Production source 0; tests +602/−2 (604 changed lines, reviewed overrun for four scenarios plus stub). Ruff and all 858 Python tests pass (449.92s). c-3 remains; JSONL/process behavior and live managed-runtime acceptance are not claimed. No live activation.

2026-11-17 · T1.6c-3 · 46206f6 · DeepSeek Flash implementation, coordinator review. Test-only shared-backend scenarios cover post-session fresh quota denial on experiment/project dispatch, exact per-process interrupts across two occupied role slots, unreadable outcomes surviving fresh quota and client shutdown, and full-slot shutdown with experiment+weekly/research occupants (closing before role shutdown, each transport closed once, saved identities unchanged after store close). Production source 0; tests +407/−0. Ruff and all 864 Python tests pass (459.13s). Audit found two open acceptance issues: research quota-only binding denial supersedes instead of retaining the pending action (repair in progress), and cloned Workflow transport lanes remain despite owner §7 retirement requirement (new explicit a-4 card). Do not claim T1 complete. No live activation.

2026-11-17 · T1.6b-2 acceptance repair · b880ac6 · DeepSeek Flash implementation, coordinator review. Research dispatch separates post-session authority supersession from quota denial: quota alone preserves the same pending decision and records the existing inference_hold transaction via a shared hold helper; fresh quota later begins that exact decision once. Authority/control-revision changes still supersede. New shared-backend gated-session regression fails on the old source, asserts released claim/busy flags, zero begin on denial and exact later progress. Source +15/−7; tests +80/−1 (103 changed lines). Ruff and all 865 Python tests pass (505.25s). Only explicit cloned-Workflow retirement a-4 remains before final T1 acceptance. No live activation.

2026-11-17 · T1.6a-4a · 349a0c2 · DeepSeek Flash implementation, supervisor-approved green split and coordinator review. Mechanical vocabulary/owner-alias consolidation removes pool_owner throughout production/tests; temporary cloned slots remain only until immediately following a-4b. Source +136/−137; tests +62/−63 (398 changed lines). Worker targeted suites green on this intermediate commit. No live activation.
2026-11-17 · T1.6a-4b · cefa44e · DeepSeek Flash implementation, coordinator review. AgentPool owns explicit AgentSlot transport/task state; one Workflow owns execution services/registries. Deleted cloned Workflow construction and manual field patching, slot identity scans and cluster-snapshot copies; slot-pinned callbacks and explicit dispatch context preserve process generations and saved identities. Fixed primary compatibility properties share canonical slot-zero state. Source +179/−133; tests +229/−170 (711 changed lines, approved mechanical wiring band). Final isolated Node24 make check passes: Ruff, 866 Python, 29 web, 45 gateway tests, web format, gateway build/types. Final phase production delta +2683, previously notified threshold exceeded. T1 complete under owner §8 scope cut; final evidence/boundaries in notes/t1-acceptance.md. No live activation.
