# Refactor workspace

| File | Content |
|---|---|
| [audit.md](audit.md) | Findings (F1–F11), what to keep, recommendation summary |
| [design.md](design.md) | Target components C1–C13 and the migration strategy |
| [todo.md](todo.md) | Ordered task cards with status, model tier, acceptance and tests |
| `notes/` | Investigation notes produced by tasks (created when needed) |

## Owner decisions (2026-11-12)

1. Integration merges automatically once conflict resolution and the project check
   pass. Approval is opt-in per project; revert is available.
2. Worktrees live at `<repo>/.worktrees/<slug>`.
3. Shared environments live in each project's folder:
   `<repo>/.experimenttracker/envs/<lockhash>/`.
4. Implementation uses DeepSeek Flash subagents, with stronger-model review where a
   card says F+R or S. The first implementation batch is complete; see
   [todo.md](todo.md) for status and validation. Continue under **Order** below.

## T1 rollout decisions (2026-11-13)

The owner approved [decisions 1–6](notes/t1-owner-decisions.md): production-source
net reduction is measured across Phase 2, with additive intermediate cards and
separate source/test/doc counts. Use bounded reviewed subcards; ship the contract-3
demo and migrate execution fixtures before activating archival. Defer rejecting
toy creates until T1.1h, preserve archived readers, and use contract-3 browser
export assertions. Unresolved remote turns conservatively consume turn capacity;
reconciliation remains ungated. At archival, settle exact in-flight identities
without advancing legacy stages, and finish queued reports from saved evidence.
Code publication does not authorize live activation.

## How we work

- **Roles.** The coordinator (the main session) chooses the next task, writes the
  brief, reviews the diff, runs the full checks, commits, and updates `todo.md`.
  Implementers are DeepSeek Flash subagents.
- **One task, one child, one commit.** A child writes only inside its `cwd`.
  Parallel children need separate worktrees and must not touch the same hot files:
  `workflow.py`, `store.py`, `gateway/src/index.ts`, `web/src/main.tsx`.
- **One-line commit messages.** `<type>: <what changed> (<task ID>)`, as in the
  existing history. No test counts, acceptance disclaimers, review narratives or
  "no live activation" trailers in the message; evidence lives in `todo.md`'s log,
  not in the commit subject.
- **Size budget per card.** The card's `Size` band in [todo.md](todo.md) is a hard
  budget: S < 150, M < 500 changed lines (source plus tests, excluding generated
  files). Beyond **2×** that budget the child stops and asks; the coordinator
  reviews the overrun before anything is committed, and splits the card rather
  than accepting the drift.
- **Report net source lines.** Every commit reports `git show --numstat` totals for
  source and for tests/docs, so review sees whether code was removed.
- **Replacement, not layering (owner waiver 2026-11-17).** The former Phase 2
  net-negative line target is waived; see
  [the decision](notes/t1-owner-decisions.md#7-net-negative-waiver-2026-11-17).
  New required capability may add production code, but every card names the
  old code it replaces and deletes it in the same card or in a named retirement
  card. A helper whose callers keep their old copies is not accepted. Temporary
  dual paths have a named removal card, and T1 is not done while one is pending
  (or, for contract-3-only paths, the engine deletion T4.8).
  A card adding more than 150 net production lines records in the log why
  existing code could not be reused. Keep consolidation cards when found; never
  delete unrelated or shipped code, or tests, to move the numbers. Tell the owner
  if the phase delta from `42829f4` passes +2,500 production lines.
- **F+R and S tasks.** A fresh-context reviewer, or the coordinator, checks the diff
  against the task card and design.md before the commit.
- **Live state is off limits.** No edits to `.local/`, no live owner restarts, no
  Slurm or managed-model calls, no gateway deployment. Release preparation and
  installation stay owner-authorized (`docs/executor-updates.md`).
- **Additive schema.** SQLite migrations are additive; D1 migrations append-only.
  Saved job, action and turn identities keep their meaning.
- **No new ceremony.** Do not add hashing, sealing, inventories, mandatory validation
  stages, or new `hold_reason` values outside C2. Do not add tables or migrations,
  hold/failure reasons, error codes, config keys or environment variables unless the
  card names them; if a card seems to need one, stop and ask the owner first.
- **Checks before commit** (coordinator, Node 24, outside restrictive sandboxes):
  - `uv run --locked ruff check src tests scripts`
  - `uv run --locked ruff format --check src tests scripts`
  - the task's targeted tests, then `uv run --locked pytest -q`
  - `make check` whenever web or gateway code changed.

## Working fast (owner decision 2026-11-17)

Measured on 2026-11-17: twelve cards landed in one afternoon with DeepSeek Flash
workers. The full Python suite takes about 7 minutes and spreads evenly over
files; most cost was the coordinator, not the workers. Use these defaults:

- **Approved designs are enough.** Do not commission extra design reviews or
  separate reviewer agents for cards whose design note is already approved. The
  coordinator reviews the diff, concentrating on identity, transaction and
  rollback logic. Ask a stronger model only for a genuinely new design question.
- **Workers commit on their own branch.** Create each lane with
  `git worktree add -b t1/<card> .worktrees/<card> main`; the worker commits there
  (one-line message). The coordinator rebases/merges and reviews with
  `git show`, instead of copying patch files. Parallel lanes still avoid editing
  the same functions; import-line conflicts are expected and trivial.
- **Batch the full suite.** Run each card's targeted tests per card. Run the full
  suite once per batch of 2–3 independent cards in `.worktrees/t1-validation`,
  then commit the cards separately with the batch result in the log. If the batch
  fails, bisect by card.
- **Wiring cards may reach 800 changed lines.** Cards that repeat one reviewed
  pattern across call sites (source wiring, backfill, mechanical moves) use an
  800-line budget. New-behaviour cards (runner, pool, resolvers) keep 500.
- **Short briefs, quiet coordination.** Briefs point to the card and design note
  and state only the decisions they leave open (see the template). Let async
  workers wake you; do not poll. Read diffs and reports, not whole files.
- **Fresh worktrees** lack the untracked `recipes/` directory, so
  `test_maintenance.py::test_real_demo_process_handover_without_model_or_scheduler`
  fails there only; ignore it outside the validation checkout.
- **Untried:** `pytest-xdist` could cut the suite to ~2 minutes, but the tests
  share a fake clock, subprocesses and SQLite files. Worth one 30-minute trial
  (needs a locked dev dependency); drop it at the first flaky failure.

## Brief template for a child

```
Goal: <task ID and title from docs/refactor/todo.md, one sentence of intent>
Repo/cwd: /share/castor/home/adorni/experiments-tracker (or the lane worktree), ref <sha>
Read first: docs/refactor/todo.md card <ID>; docs/refactor/design.md section <Cn>;
            the files listed in the card (read the cited line ranges before editing).
Edit boundary: only the files listed in the card, plus new test files. Ask before
            touching anything else. Never touch .local/, live state, or other repos.
Facts and decisions already made: <paste anything the card leaves open>
Do: <steps from the card>
Success criteria: <acceptance from the card>
Validate: <exact pytest command from the card>; ruff check/format on changed files.
Output: a short report covering the changed files, decisions made in judgment calls,
            test results, the net source-line delta (`git show --numstat`), and
            anything left unresolved. Do not commit.
Stop and ask if: acceptance conflicts with existing tests in a way the card does
            not cover, a change would alter saved identities, schema or config
            meaning, adds a table, reason or config key the card does not name, or
            the change exceeds the card's Size budget by more than 2x.
```

## Order

**T4.8 is current.** Contract 4 is the default for new experiments. Contract 3
retirement still requires fresh drain evidence and a separate archive step. Phase 5
follows retirement. See [the task cards](todo.md#phase-4--research-loop-execution-contract-4-c7).

T1 is complete under the owner-approved scope cut; see
[notes/t1-acceptance.md](notes/t1-acceptance.md). Historical handoff notes describe
prior states, not the current task status.

The phase order in todo.md is the intended sequence. Within Phase 1, T0.1, T0.3,
T0.6 and T0.7 are independent (first batch); T0.2 and T0.4 touch `store.py`, so
run them one after the other afterwards. Phase 3 can start once T1.1b and T1.2 are done; it does not need
contract 4.
