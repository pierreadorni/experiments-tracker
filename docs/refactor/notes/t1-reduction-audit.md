# T1 Phase 2 production source-reduction audit (read-only)

Audit of `/share/castor/home/adorni/experiments-tracker`, accounting set
`src gateway/src web/src scripts recipes`, baseline `42829f4`
(`git diff --numstat 42829f4 HEAD -- src gateway/src web/src scripts recipes`).

No file in the repository was modified. All commands were read-only
(`git diff/log/ls-tree`, `rg`, `wc`, `ast` scans, `sed`/`awk` reads).

**Baseline drift during the audit:** `HEAD` moved twice while I was reading
(`9481524` → `803e70b`, "feat: track owner request and plan-review
responsibilities (T1.3b-b10/b11)"). The measured phase delta therefore moved
from **+759** to **+971** net production lines during this audit. Every line
range below is from `803e70b`; re-anchor before applying.

```
42829f4..803e70b  added 2904  deleted 1933  net +971
src/experimenttracker/core/  = 1323 lines in 10 modules (all new this phase)
```

Driver of the deficit is unambiguous: the five→eight C2 mapper modules
(+1323) plus `legacy_archive.py` (+143), `legacy_archive_actions.py` (+68),
`codex_client.py` (+266, mostly clock migration + demo), `store.py` (+69),
against `workflow.py` −600, `artifacts.py` −172, `remote_worker.py` −131.

---

## Ranked opportunities (saving ÷ risk)

| # | Opportunity | Where | Est. net | Risk | Covers by |
|---|---|---|---|---|---|
| 1 | Delete `web/src/mock/` design study (not in the shipped bundle) | `web/src/mock/*` (4 files) | **−3689** | owner decision, not code risk | no tests; `tsc -b` only |
| 2 | Shared mapper helpers in `core/` (7 mappers) | `core/*.py` | **−55 … −70** | low | 9 obligation test files |
| 3 | One scheduler terminal-state source | 6 modules | **−38 … −46** | low–medium (REVOKED/COMPLETED trap) | steering/early-repair/legacy/remote tests |
| 4 | `Workflow.dispatch`/`Conversation.dispatch` repeated blocks | `workflow.py`, `conversation.py` | **−17 … −21** | low | workflow/conversation/discussion tests |
| 5 | `Store.command` unsettled-work guard helper | `store.py` (4 blocks) | **−14 … −20** | low–medium | auto-repair/steering/obligation tests |
| 6 | Shared readback-settle adapter (R4 groundwork) | 8 call sites | **−12 … −25** | medium | recovery/controller/knowledge tests |
| 7 | `Store.cancel_experiment` retire-loop helper | `store.py:892-957` | **−6 … −10** | low–medium | cancellation/report/transfer tests |
| 8 | `maintenance.py` detached-spawn helper (3 copies) | `maintenance.py` | **−6 … −14** | low–medium | `test_maintenance.py` |
| 9 | Transition+emit pairs (dispatch intent / turn started) | `workflow.py`, `conversation.py` | **−8 … −12** | low–medium | dispatch/obligation tests |
| 10 | Dead no-op `discard_failed_implementation` | `workflow.py:916-919` + call `592` | **−5** | none | `test_agent_output_recovery.py` |
| | **Total excluding #1** | | **−163 … −229** (realistic ≈ **−190**) | | |
| | **Total including #1** | | **≈ −3 850** | | |

**Headline:** the *code* consolidation reserve is ≈190 lines against a
**+971** deficit that is still growing by ~200 lines per commit. The only
reserve large enough to flip the phase is item 1 — a scope decision about a
non-shipped design mock, not a refactor. My honest numbers say: without an
owner decision on item 1 (or a reduction in the remaining C2/C4/C5 additions),
`D` stays positive by roughly **+780**.

---

## 1. `web/src/mock/` — 3 689 lines of non-shipped design study  (−3689)

```
web/src/mock/main.tsx        1-1101   (1101)
web/src/mock/mock.css        1-2241   (2241)
web/src/mock/ProgressStudy.tsx 1-225  ( 225)
web/src/mock/Select.tsx      1-122    ( 122)
```

Evidence that it is not production:
* `web/index.html:19` loads only `/src/main.tsx`; Vite builds from
  `web/index.html` only. The mock has its own dev entry `web/mock/index.html:15`
  (`<script type="module" src="/src/mock/main.tsx">`).
* `web/mock/README.md:3` states: *"this mock is not included in its production
  bundle"*; it is a "separate, interactive frontend mock for visual iteration".
* No file outside `web/src/mock/` imports anything from it
  (`rg -n "mock" web/src --glob '!web/src/mock/**'` → no matches).
* `docs/refactor/audit.md:119-120` lists it as "tracked design mocks
  (`web/src/mock`, `web/mock`)".
* It is unchanged since `1a95f8a` (initial release), so deleting it is a clean
  −3689 against the phase measure with no interaction with other work.

Behaviour risk: none at runtime. Non-code risks: (a) it is an owner-visible
design artifact referenced by `web/mock/README.md` and `web/mock/index.html`
(both outside the accounting set, so their edits earn no credit); (b) it is
currently typechecked by `web/tsconfig.json` (`"include": ["src"]`), so
`npm run typecheck` after deletion must be re-run but will simply check less.
Tests: no automated test imports it; `web/scripts/*.mjs` Playwright drives the
real app.

Accounting caveat: moving the directory out of `web/src` (e.g. under
`web/mock/`) would earn **zero** credit under the stated rule ("do not obtain
credit by moving implementation to a directory outside the accounting set").
Deletion is the only path that yields the lines. **This needs an owner
decision; it is the single item that changes the arithmetic.**

---

## 2. Shared `core/` mapper helpers — 7 call sites, 3 duplicated mechanisms  (−55 … −70)

The eight mappers repeat three mechanisms verbatim.

**(a) Obligation-history scan SQL (3 copies, identical).**
```
core/obligation_sources.py:169-190   _history()          (prefix query + contiguity/scope validation)
core/job_obligations.py:141-161      _check_saved_identity() inner scan
core/transfer_obligations.py:72-91   _check_history()
```
The literal `"SELECT * FROM obligations WHERE substr(id,1,?)=? ORDER BY id"`
appears at `obligation_sources.py:177`, `job_obligations.py:145`,
`transfer_obligations.py:78`. Extract
`obligation_sources.history_rows(db, source_type, saved_id)` returning the raw
`dict` rows; `_history` keeps its contiguity/scope validation on top, and the
job/transfer checks keep their *own* (stronger) field and owner/kind
validation. **−20.** Must preserve `ORDER BY id` (lexicographic) and the
`row["id"].rsplit(":",1)[0] == source_key` filter, which is what makes
`a:b` vs `a:b:c` sibling IDs distinct (`tests/test_obligation_sources.py:95`).

**(b) experiment → contract → project_id lookup (7 copies).**
```
core/action_obligations.py:105-112   (different: needs contract+phase+deleted_at → keep)
core/job_obligations.py:59-63
core/transfer_obligations.py:36-42
core/report_obligations.py:34-39
core/wakeup_obligations.py:24-31
core/request_obligations.py:36-38 (+47, +60)
core/review_obligations.py:130-135
```
Add ~7-line `obligation_sources.source_project_id(db, experiment_id, label)`
that performs the `SELECT contract FROM experiments WHERE id=?`, raises the
existing per-caller `ValueError("saved <label> experiment does not exist: …")`
message (message text differs per caller, so pass the label), and returns
`(json.loads(contract).get("project") or {}).get("id")`. Six sites collapse to
one line each; `report/wakeup/transfer` also drop their now-unused
`import json`. **−16 … −22.**

**(c) `_terminalize` (3 copies, same body, different `_SOURCE`).**
```
core/action_obligations.py:177-184
core/job_obligations.py:163-170
core/request_obligations.py:68-77
```
Move to `obligation_sources.terminalize(db, source_type, latest, saved_id,
operation)` (~7 lines). **−10.**

**(d) Caller-transaction guard (12 inline copies).**
```
core/obligations.py:158-160, core/obligation_sources.py:225-227,
core/action_obligations.py:92-93, core/report_obligations.py:208-209,
core/wakeup_obligations.py:255-256, core/transfer_obligations.py:301-302,
core/job_obligations.py:41-42, core/advancement_decisions.py:191-193,
core/request_obligations.py:17-18 and 32-33, core/review_obligations.py:110-111 and 126-127
```
One 3-line helper; each `if not db.in_transaction: raise RuntimeError(...)`
becomes a 1-line call. Messages already only assert the substring
`"active transaction"` in tests (`tests/test_advancement_decisions.py:32`,
`tests/test_job_obligations.py:210`), so wording can be unified. **−8 … −10.**

**(e) `saved_pending` / `saved_review` (near-identical readers).**
`core/request_obligations.py:15-22` and `core/review_obligations.py:108-115`
differ only by column name and decoder → one `_saved_json(db, eid, column,
label)` helper. **−5.**

Behaviour risk (whole item): low, *provided* the stronger job checks are not
weakened (the design note's warning stands: job identity, scheduler wait and
owner/kind must survive) and the handoff `SAVEPOINT` path is untouched. Do not
merge the payload validators (`advancement_decisions._decode`,
`request._request`, `review._review`): their rejection semantics differ.

Tests: `tests/test_obligation_sources.py`, `test_action_obligations.py`,
`test_job_obligations.py`, `test_transfer_obligations.py`,
`test_report_obligations.py`, `test_wakeup_obligations.py`,
`test_request_obligations.py`, `test_review_obligations.py`,
`test_iteration_obligations.py`, `test_job_observation_obligations.py`,
`test_legacy_archive_jobs.py`, `test_action_source_integration.py`.

---

## 3. Scheduler terminal-state constants — 6 definitions → 1  (−38 … −46)

```
src/experimenttracker/workflow.py:46-55          TERMINAL_BAD        (9 states, no COMPLETED)
src/experimenttracker/early_repair.py:11-22      TERMINAL            (10, +COMPLETED, +REVOKED)
src/experimenttracker/autopilot_budget.py:9-20   TERMINAL            (same 10)
src/experimenttracker/legacy_archive.py:6-16     TERMINAL_BAD        (same 9)
src/experimenttracker/steering.py:11-21          TERMINAL_JOBS       (9 states, no REVOKED)
src/experimenttracker/remote.py:575-585          inline literal set  (same 9 as steering)
```
There are exactly **three distinct sets**, and the current split is
intentional:
* `TERMINAL` = 10 states including `COMPLETED` **and** `REVOKED`
  (`autopilot_budget`, `early_repair`),
* `TERMINAL_BAD` = `TERMINAL − {COMPLETED}` (`legacy_archive`, `workflow`,
  also consumed by `core/job_obligations.py:114`),
* `TERMINAL_JOBS` = `TERMINAL − {REVOKED}` (`steering`, `cancellation`, and the
  inline copy in `remote.fetch`).

Put the canonical 10-state set plus the two derived sets in `slurm.py`
(it already owns `Observation` and scheduler parsing; it has no constants
today, so a `TERMINAL = frozenset({...})` block needs ~8 lines there because of
the 110-char line limit):

```python
TERMINAL = frozenset({"COMPLETED", "FAILED", "CANCELLED", "TIMEOUT", "OUT_OF_MEMORY", "NODE_FAIL", "PREEMPTED", "BOOT_FAIL", "DEADLINE", "REVOKED"})
TERMINAL_BAD = TERMINAL - {"COMPLETED"}
TERMINAL_JOBS = tuple(sorted(TERMINAL - {"REVOKED"}))
```
then one alias line per module (`TERMINAL_BAD = slurm.TERMINAL_BAD`, …) so the
existing public names and `tests/test_legacy_archive_jobs.py:101` keep
working. 67 → ~13 lines. **−40 typical.**

Behaviour risk: medium only if someone "unifies" the three sets. Preserve them
by derivation; keep `TERMINAL_JOBS` a tuple because `steering.py:53-55` expands
it positionally into `IN (?,?,…)` and `cancellation.py` imports it by name.
Tests: `test_steering.py`, `test_iteration_early_repair.py`, `test_auto_repair.py`,
`test_cancellation.py`, `test_legacy_archive_jobs.py`, `test_remote_execution.py`,
`test_job_observation_obligations.py`, `test_stage_progress.py`.

---

## 4. `Workflow`/`Conversation` dispatch repeated blocks  (−17 … −21)

**(a) pending-request expiry `finally` block, 3 identical copies (4 lines each):**
```
workflow.py:866-870        (dispatch_title)
workflow.py:3507-3511      (dispatch)
conversation.py:571-575    (Conversation.dispatch)
```
→ `Workflow._expire_pending(eid)` (4 lines); callers keep 1 line. **−5.**
(Do *not* fold in `workflow.py:363-369` (start-up expiry with a different
`hold_reason`) or `autopilot_controller.py:144-156` (different table).)

**(b) `agent_input_refreshed` block, 3 identical copies (6 lines each) inside
`Workflow.dispatch`:**
```
workflow.py:3245-3250, 3253-3258, 3310-3315
    prompt = await asyncio.to_thread(self.prompt, exp, <phase>)
    if legacy_archive.execution_frozen(self.store.experiment(eid)): return
    with self.store.tx() as db:
        db.execute("UPDATE agent_actions SET input=? WHERE id=?", (prompt, action["id"]))
        self.store.emit(db, eid, "agent_input_refreshed", {"action_id": action["id"]})
    action = {**action, "input": prompt}
```
→ `_refresh_input(eid, action, prompt)` returning the rebound action. **−6.**
Keep the three distinct conditions (`plan`/`prepare` + live, `interpret` +
`agent_experiment_v1`, isolated `prepare`); only the saved write is shared.
The frozen check must stay *before* the write in each case.

**(c) duplicated evidence-row query in `advance_reports`:**
```
workflow.py:3644-3651  and  3663-3670   (identical `SELECT * FROM artifacts … [AND evidence_revision=?]`)
```
→ `_evidence_rows(exp, eid)`. **−7.**

Tests: `tests/test_iteration.py`, `test_workflow.py`,
`test_iteration_declared_outputs.py`, `test_report_obligations.py`,
`test_conversation.py`, `test_discussion_*`, `test_iteration_agent_stages.py`.

---

## 5. `Store.command` unsettled-work guards  (−14 … −20)

Four blocks repeat the same two `SELECT 1` probes with 3–5 line formatting:
```
store.py:1180-1190   delete:  jobs('submitting','submitted') OR agent_actions(live)
store.py:1224-1231   repair_implementation: agent_actions(live+pending) OR steering(pending,cancelling)
store.py:1323-1330   session/recovery retry: agent_actions(live) OR steering(pending,cancelling)
store.py:1388-1395   agent retry:       agent_actions(live) OR steering(pending,cancelling)
```
Plus `store.py:1191-1195` (report publication) as a third member of the delete
guard. Add two small `Store` helpers, e.g.
`_live_actions(db, eid, *, include_pending=False)` and
`_pending_steering(db, eid)` (or one `_unsettled(db, eid, pending=False)`),
and keep the delete path's extra jobs probe. **−16 typical.**

Behaviour risk: low–medium — the trap is `repair_implementation`
(`store.py:1225`) which includes `'pending'` in its action-state list while
the other three do not. Parameterize it, do not "normalize" it. Tests:
`tests/test_auto_repair.py`, `test_steering.py`,
`test_action_source_integration.py`, `test_legacy_archive_activation.py`,
`test_iteration_agent_stages.py`.

---

## 6. Shared readback-settle adapter (also R4/C5 groundwork)  (−12 … −25)

Eight sites run the same three-way settle, each ~8–10 lines:
```
controller.py:623-631          wf._action_state(...)          (completed/failed/uncertain)
conversation.py:367-376        wf._action_state(...)          (all exceptions → uncertain, no re-raise)
autopilot_controller.py:215-223 self.state(...)                (encode(result), [:2000], 'received')
knowledge.py:725-733           memory.state(...)              (hold_reason variants)
legacy_archive_actions.py:34-42 workflow._action_state(...)   (else-branch form)
cancellation.py:57-63          lane._action_state(...)        (no Exception→uncertain; appends error)
workflow.py:656-665            advance_title (validation in the middle)
workflow.py:2331-2341          agent_stage
```
Common core: `read_turn` → `TerminalTurnError` → failed, `Exception` →
uncertain, else completed. Behaviour-preserving shape:
`await settle_turn(agent, thread_id, turn_id, settle, *, truncate=None)`
where `settle(state, result=..., error=...)` is a per-role 2–3-line adapter
(the design note's "explicit source adapters"). **−15 realistic** for the four
sites whose shapes actually match (`controller`, `workflow.advance_title`,
`workflow.agent_stage`, `legacy_archive_actions`); `cancellation` and
`conversation` differ in exception policy and should be left alone until C5.

Behaviour risk: medium — terminal-versus-unknown classification, error
truncation lengths and post-settle hold updates differ per role. Tests:
`tests/test_agent_output_recovery.py`, `test_iteration_output_recovery.py`,
`test_controller.py`, `test_knowledge.py`, `test_cancellation.py`,
`test_legacy_archive_actions.py`, `test_action_source_integration.py`.

Strategic note: this item's real value is that R4/C5 need it anyway; budget it
as shared cost, not as deletion credit.

---

## 7. `Store.cancel_experiment` retire-triple  (−6 … −10)

`store.py:892-957` repeats "select rows → bulk update → sync obligation per
row" three times (transfers 899-913, actions 914-925, report versions 926-941).
A local helper `_retire(db, select_sql, update_sql, sync, now, eid)` (or three
small closures) removes the repeated select/update scaffolding. **−8.**

Behaviour risk: low–medium (parameterized SQL strings; the obligation sync
callbacks have different signatures). Tests: `tests/test_cancellation.py`,
`test_report_obligations.py`, `test_transfer_obligations.py`,
`test_iteration.py`.

---

## 8. `maintenance.py` detached-spawn helper (3 copies)  (−6 … −14)

```
maintenance.py:316-331   coordinator launch   (argv, coordinator.log)
maintenance.py:589-606   release serve        (argv, log, cwd=root, env=env)
maintenance.py:702-721   prepare              (argv, <commit>.log)
```
All three: `with log.open("ab") as handle: os.chmod(handle.name, 0o600);
subprocess.Popen(argv, stdin=DEVNULL, stdout=handle, stderr=handle,
start_new_session=True)`. One `detached(argv, log_path, *, cwd=None,
env=None)` helper (~7 lines) removes 5–7 lines per site. **−8 typical.**

Behaviour risk: low–medium — the file handle must stay open exactly until
`Popen` returns, and `os.chmod` must stay inside the `with`. Tests:
`tests/test_maintenance.py` (incl. `test_coordinator_unknown_launch_is_not_retried`,
`test_real_demo_process_handover_without_model_or_scheduler`).

---

## 9. Transition+emit pairs (dispatch intent / turn started)  (−8 … −12)

```
workflow.py:3411-3425 (+ 3460-3466)   dispatch:  transition(dispatch_changes)+emit 'agent_dispatch_intent'
                                      then transition(turn_changes)+emit 'agent_turn_started'
workflow.py:830-841                   dispatch_title: same pair, smaller payload
conversation.py:525-533 / 547-553     Conversation.dispatch: same pair, 'discussion_*' events
```
`action_obligations.transition` is already shared (T1-R1), so what remains is
the `with self.store.tx(): transition(...); emit(...)` wrapper. One
`self._turn_intent(eid, action, changes, expected, kind, payload)` helper
(~8 lines) and one `_turn_started(...)` saves ~3 lines per call site. **−10.**
Event names/payload keys differ per role and must be passed in verbatim.
Tests: `tests/test_iteration_obligations.py`, `test_action_source_integration.py`,
`test_conversation.py`, `test_iteration_agent_stages.py`,
`test_workflow_job_obligations.py`.

---

## 10. Dead no-op  (−5)

```python
# workflow.py:916-919
def discard_failed_implementation(self):
    # Working environments, checkpoints and job-source records survive
    # repair. Retention is an owner operation, never automatic recomputation.
    return
```
Called once at `workflow.py:592`. Delete both. `tests/test_agent_output_recovery.py:201-202`
calls it twice (asserts nothing) and must be updated in the same change.
Behaviour risk: none (function has no effect).

Also considered and **rejected**: `submit_prepared_stage` (`workflow.py:1353-1355`)
looks like a pass-through to `_submit_prepared_stage` but is the public,
heavily-used entry point (18 call sites in tests); inlining it saves 2 lines and
adds churn.

---

## Verified dead-code / unreachable-branch reserve: essentially zero

Method: AST-extracted all 405 top-level names (functions, classes, module
constants) in `src/experimenttracker/**/*.py` and counted occurrences across
`src tests scripts tools recipes web gateway docs Makefile pyproject.toml`
(3 131 files). **Every top-level name has at least one non-local reference.**
A second pass over all class methods found only pydantic validators
(`CreateProject.valid_folder`, `ReportCell.check_cell`, `ReportBlock.check_block`,
`ResearchPlan.unique_stages`, `DemoAgent.demo_*`, …) and two same-file helpers
(`Workflow.cluster_for`, `Workflow.compact_plan`) — all live. Module-import graph:
no orphan module. `rg` shows no `if False`, `and False`, or
`NotImplementedError` paths.

Therefore the only genuinely unreferenced production artifacts are:
* `workflow.discard_failed_implementation` (item 10, 5 lines), and
* `core/advancement_decisions.py` — **193 lines with no production importer**
  (`rg -n advancement_decisions src scripts` → nothing; only
  `tests/test_advancement_decisions.py` imports it). It is an approved "inert
  helper" for c1a/b1 that earns no deletion credit. Recommendation, not a
  saving: land it **with its first caller** so the phase never carries 193
  unwired production lines. If c1b will add them anyway, the sequencing choice
  is the owner's, not a refactor.

**Retired-contract candidates I deliberately did not count** (they are
compatibility migrations that still mutate live rows, so removing them needs
DB evidence, not code review):
* `Workflow.recover_terminal_actions` (`workflow.py:872-914`, ~40 lines) —
  rewrites pre-existing `hold_reason='agent_recovery_exhausted'` rows on every
  tick.
* `Store.archive_legacy_experiments` (`store.py:772-798`, ~25 lines) — marks
  non-contract-3 rows archived; called twice per start/tick
  (`workflow.py:359`, `470`).
* `local_migrate`/`migrate` chains (`store.py:117-451`, `autopilot.py:27-107`,
  `knowledge.py:118-141`) — schema upgrade chain for existing installations.
* `legacy_archive.py` (143) and `legacy_archive_actions.py` (68) — archive
  readers, explicitly out of scope ("keep archived historical contracts and
  evidence readable").

## Negative findings worth recording

* **Two-line `if legacy_archive.execution_frozen(...): return` guards cannot be
  collapsed to one line.** There are 22 such guards in `workflow.py`; ruff
  selects `E7` in `pyproject.toml:41`, so E701 (multiple statements on one
  line) forbids `if …: return`. A `def frozen(self, eid)` helper still needs 2
  lines per site, so there is no saving. This is the obvious "cheap" −22 that
  does not exist.
* **SQL literal duplication is real but not compressible.** 607 distinct SQL
  literals; `SELECT * FROM agent_actions WHERE id=?` appears 31×,
  `SELECT 1 FROM agent_actions … state IN ('dispatching','running','uncertain')`
  10×, `SELECT * FROM jobs WHERE intent_id=?` 7×. Most are one-liners; only the
  multi-line `db.execute(...)` forms (items 4–7) yield lines. Introducing a
  query builder would raise risk and roughly net zero.
* **Docstrings/comments excluded** per instruction; several files would show a
  large but ineligible "saving" if comments were counted (e.g.
  `core/obligation_sources.py`, `core/obligations.py` header/argument prose).
* TS side (`gateway/src`, `web/src` excluding `mock`) is net +40 for the phase
  and has no unreferenced exports of consequence
  (`ThemePreference`, `checkedBytes`, `MaintenanceStatus`, `ReportVersion`,
  `ModelOption`, `pushSubscriptionSchema` are all used in-file or by generated
  accessors). No meaningful TS reserve.

## Verification commands (read-only, re-runnable)

```sh
git diff --numstat 42829f4 HEAD -- src gateway/src web/src scripts recipes | awk '{a+=$1;d+=$2} END {print a,d,a-d}'
wc -l web/src/mock/*                                   # 3689
rg -n "advancement_decisions" src scripts              # no production importer
rg -n "TERMINAL|TERMINAL_BAD|TERMINAL_JOBS" src         # 6 definitions
rg -n "SELECT \* FROM obligations WHERE substr" src/experimenttracker/core
sed -n '1180,1190p;1224,1231p;1323,1330p;1388,1395p' src/experimenttracker/store.py
sed -n '916,919p' src/experimenttracker/workflow.py
```

## Recommended order of work (saving ÷ risk, behaviour-preserving)

1. Item 2 (core helpers) — biggest safe code credit, all behind existing tests.
2. Item 3 (scheduler constants) — one mechanical commit, derived sets.
3. Item 4 + 10 (workflow repeated blocks + dead no-op).
4. Item 5 (store guards) and Item 7 (cancel retire).
5. Item 8 (maintenance spawn) and Item 9 (transition/emit pairs).
6. Item 6 only when R4/C5 need it.
7. Item 1 as an owner decision, separately from the refactor.

## Residual risk of this audit

* `HEAD` moved twice during the audit; line numbers will drift again within
  hours (three concurrent cards were landing).
* Net-line estimates are local, per item, and assume the helper is written once
  and every listed call site migrated in the same card (the design note's
  "inert helper earns no credit" rule). Applying only part of an item yields
  close to zero or a small increase.
* No tests were run (read-only audit); every risk statement is from reading
  the code and the test names that cover it.
