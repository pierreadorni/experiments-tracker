# Project agent conventions

ExperimentTracker runs each experiment in its own named branch worktree and makes the
commits. These conventions apply to agent turns in this project. Replace this file
when you prefer different project rules.

- Put experiment-specific code in `experiments/<slug>/`.
- Keep shared-code changes backward compatible: add flags or configuration and keep
  existing defaults. Add or adjust tests when shared code changes.
- Edit files only in the experiment worktree; the backend commits them. Do not run
  `git commit`, `checkout`, `merge`, `rebase` or `push`, and do not create branches.
  Read-only Git commands are fine. Integration merges go through the backend queue.
- Keep data, checkpoints and outputs out of Git. Read data only through the
  declared data roots.
