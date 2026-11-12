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

## How we work

- **Roles.** The coordinator (the main session) chooses the next task, writes the
  brief, reviews the diff, runs the full checks, commits, and updates `todo.md`.
  Implementers are DeepSeek Flash subagents.
- **One task, one child, one commit.** A child writes only inside its `cwd`.
  Parallel children need separate worktrees and must not touch the same hot files:
  `workflow.py`, `store.py`, `gateway/src/index.ts`, `web/src/main.tsx`.
- **F+R and S tasks.** A fresh-context reviewer, or the coordinator, checks the diff
  against the task card and design.md before the commit.
- **Live state is off limits.** No edits to `.local/`, no live owner restarts, no
  Slurm or managed-model calls, no gateway deployment. Release preparation and
  installation stay owner-authorized (`docs/executor-updates.md`).
- **Additive schema.** SQLite migrations are additive; D1 migrations append-only.
  Saved job, action and turn identities keep their meaning.
- **No new ceremony.** Do not add hashing, sealing, inventories, mandatory validation
  stages, or new `hold_reason` values outside C2.
- **Checks before commit** (coordinator, Node 24, outside restrictive sandboxes):
  - `uv run --locked ruff check src tests scripts`
  - `uv run --locked ruff format --check src tests scripts`
  - the task's targeted tests, then `uv run --locked pytest -q`
  - `make check` whenever web or gateway code changed.

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
            test results, and anything left unresolved. Do not commit.
Stop and ask if: acceptance conflicts with existing tests in a way the card does
            not cover, a change would alter saved identities or schema meaning, or
            the change exceeds the card's size estimate by more than 2x.
```

## Order

The phase order in todo.md is the intended sequence. Within Phase 1, T0.1, T0.3,
T0.6 and T0.7 are independent (first batch); T0.2 and T0.4 touch `store.py`, so
run them one after the other afterwards. Phase 3 can start once T1.1b and T1.2 are done; it does not need
contract 4.
