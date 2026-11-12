# Audit: why experiments stall and what to change

Date: 2026-11-12. Scope: source at `fe68364`, private operating records, and one
live project folder inspected read-only. The live database lives on the owner host
and was not inspected. All 452 Python tests pass (about 2.5 minutes).

## 1. Summary

ExperimentTracker's core design treats the agent as a **function**: a turn must
return a large JSON manifest (`ResearchPlan`, `Implementation`, `Analysis`) that
Python validates *after* the turn ends. The backend then drives a stage graph
through a polling state machine that rebuilds its decisions on every tick.

The October 9 workflow audit already proposed the right fix: one persistent agent
session per experiment, a few backend tools, and the backend owning only job
submission. The fix was applied by **adding** layers while keeping every earlier
contract working. The result is four execution generations in one engine, about
48 sticky hold reasons, about 60 SQLite tables, and many places where work can be
accepted but never resumed. The November 12 continuity proposal reached the same
diagnosis a month later.

## 2. Findings

### F1. Contract accretion
- The toy recipe (`toy_compare_v1`), plan schemas 1/2/3, execution contract ≤2 vs 3,
  `strict_reproducibility`, auto-repair and early repair all coexist.
- There are about 50 `iteration.enabled(exp)` branches, 41 in `workflow.py`.
  `Workflow.advance()` (`workflow.py:1125`) handles every generation inline.
- `make demo` exercises the legacy toy recipe (`cli.py:126`), not the contract-3
  path that real experiments use.

### F2. Manifest turns with validation afterwards
- Many constraints live only in validators or in prose (`workflow.py:2955-3075`,
  `models.py`): reserved IDs, the source-size limit, `EXPERIMENT_*` variables,
  TensorBoard tags, metric/variant uniqueness, and so on.
- Any mismatch costs a full continuation turn. `check_manifest` reduces the problem
  but does not remove it.

### F3. One sticky `hold_reason` string is both control flow and display text
- About 48 values exist. Whichever check runs last overwrites the field.
- `agent_stage_blocked`, `implementation_blocked` and `automatic_repair_pending`
  make `advance()` return immediately (`workflow.py:1127-1131`). Only one specific
  path can clear each of them.
- Steering with `mode=later` (which Autopilot hardcodes, `autopilot.py:515`) cannot
  resume a blocked stage, and manual retry refuses while steering is pending
  (`store.py` ActionCommand handling).
- Nothing guarantees that every unfinished obligation has a resolver.

### F4. Internal errors fail experiments permanently
`tick()` maps `ValueError`/`KeyError`/`OSError` raised in `advance()` to
`fail(eid, "validation_failed")` (`workflow.py:573`). A code bug or a brief NFS
error therefore ends an experiment.

### F5. God object and sequential tick
- `workflow.py` is 233 KB in one class. The largest methods are `agent_stage`
  (462 lines), `observe_jobs` (314), `generic_prompt` (310) and `dispatch` (283).
- Every subsystem receives the whole `Workflow`.
- `tick()` runs about 25 subsystems in sequence, so slow rsync, NFS or backup work
  delays everything. Owner shutdowns stalled several times and needed a manual stop.

### F6. Agent work is serialized
- Multiple sessions are implemented by cloning whole `Workflow` objects as "lanes",
  each with its own Codex server (`workflow.py:247`).
- `agent_busy`, `research.busy` (`autopilot_controller.py:30`) and
  `supervisor.blocks_dispatch` (`knowledge.py:576`) each stop dispatch globally.

### F7. Every event stores a full projection
- `Store.emit()` writes the complete experiment projection to the outbox for every
  state change (`store.py:657`).
- The live database reached 14 GB. `retention.py`, a compaction tool and a retention
  document were built to treat the symptom.
- Only the latest revision is ever published with a payload. Replay of older events
  sends identity and revision only (`gateway_client.py:195`).

### F8. Uncertain outcomes halt work instead of being resolved
- Submissions carry an intent comment, and turns can be read back from their
  thread, so most uncertainty can be resolved automatically.
- In practice, operators adopted turns by hand, copied rollouts and stopped owners.

### F9. LLM usage
- Each experiment needs at least 4–5 turns (title, plan, implementation,
  agent stages, interpretation), each starting from a freshly assembled context.
- Prompts combine a long static instruction block with a JSON payload: project
  knowledge, cluster snapshot, resource config, plan, output refs and jobs. One
  input reached 227,590 characters.
- Separate roles add more turns: monitor diagnostics, discussion, supervisor and
  weekly synthesis, the Autopilot controller (a new session per decision plus a
  seven-document foundation gate), and early-repair preparation.
- An earlier regression spent about 24.5 M input tokens on timer-driven monitor turns.

### F10. Code workspaces: work never returns to the main repository
Behaviour (`workflow.py:prepare_workspace`, `workspaces.py`, `projects.py:366`):
- In `auto` mode with no other active experiment, the agent edits the **main
  checkout directly**, without a branch or commit.
- Otherwise it gets a detached, locked worktree at
  `<repo>/.experimenttracker/experiments/<id>/checkout`, with uncommitted edits
  copied in (limit 32 MiB).
- Each experiment also gets a gitignored `work/` folder with its own `.venv`.
  Repairs create further candidate folders.
- Jobs never run the checkout. Declared sources are copied into each attempt
  (`project-source-NNNN.py`).
- Nothing is ever committed, named or merged.

Observed in one live project: 2 commits on `main`; 11 detached worktrees each with
1–14 uncommitted changed files; 24 separate Python environments across 26
experiment folders; per-experiment interpreter downloads and pip build folders;
untracked analysis files in the main checkout. Results cannot easily be traced to
code, space is wasted, and extracting publication code is manual.

### F11. Heavier than the product needs
- **Two backends:** FastAPI (38 routes) and the Worker (`index.ts`, 92 KB, 10 D1
  migrations, about 23 tables) both implement commands and projections.
- **Self-update system:** `maintenance.py` with its own schema, migration and UI
  exists mainly because restarts are risky. They are risky because of F3–F5 and F8.
- **Docs and process:** a hedged documentation style, several mandatory docs per
  development session, and repeated synthetic-versus-live acceptance wording.
- **Tests:** they mirror implementation paths but not autonomy. Timing-sensitive
  tests with fixed tick counts are flaky; one was deleted instead of fixed.
- **Frontend:** a 2,054-line `web/src/main.tsx` and tracked design mocks
  (`web/src/mock`, `web/mock`).

## 3. What to keep

- SQLite with a single writer and an ownership lock.
- Intent recorded before dispatch, and reconciliation through the Slurm comment.
- Quota gating on managed authentication.
- From contract 3: direct output references, attempt identities and explicit reuse.
- The PWA and gateway concept.
- Cancellation semantics.
- Contract 3's decision to drop hashing, sealing and the final validation barrier.

## 4. Recommendations (summary)

Details are in [design.md](design.md); tasks are in [todo.md](todo.md).

**Tier 0 – days, fewer stalls now**
1. Internal errors hold an experiment with backoff instead of failing it (C1).
2. Steering and answers resume blocked stages and implementations (C2 interim).
3. Add a liveness watchdog that uses no inference (C2).
4. Store the outbox by reference and build projections at publish time (C3).
5. Remove global agent serialization (C4 interim).
6. Make timing-sensitive tests deterministic (C12).

**Tier 1 – weeks, consolidation**
1. Freeze legacy contracts as read-only and move the demo to the current path (C10).
2. Replace `hold_reason` with derived waits from an obligations table (C2).
3. Split `Workflow` into services; one agent pool replaces lanes (C4, C11).
4. Resolve uncertain outcomes automatically (C5).
5. Git-native workspaces: a branch per experiment, backend commits, jobs run from
   commits, a shared environment cache in the project folder, and an automatic
   integration/merge queue (C6).

**Tier 2 – the large refactor**
1. One persistent research loop per experiment with a tool API. Plan, implementation
   and interpretation manifests disappear (C7).
2. Autopilot becomes a policy layer on the same loop (C8).
3. The gateway becomes a thin relay (C9).
4. Rewrite docs and process, and accept behaviour through scenario tests (C12, C13).
