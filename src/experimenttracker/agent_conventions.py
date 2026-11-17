"""Project conventions offered to owners, and their prompt form for named worktree runs."""

from pathlib import Path

PROJECT_AGENTS = Path(__file__).parent / "templates" / "project-AGENTS.md"

WORKTREE_CONVENTIONS = (
    "Edit files only in this experiment's named branch worktree; the tracker backend commits them. "
    "Do not run git commit, checkout, merge, rebase or push, and do not create branches. "
    "Put experiment-specific code in experiments/<slug>/. "
    "Keep shared-code changes backward compatible: add flags or configuration, keep existing defaults. "
    "Keep data, checkpoints and outputs out of Git; read data only through the declared data roots. "
    "Add or adjust tests when shared code changes."
)


def project_agents_template() -> str:
    return PROJECT_AGENTS.read_text(encoding="utf-8")


def worktree_conventions(named: bool) -> str:
    """Only named worktree runs get these conventions."""
    return WORKTREE_CONVENTIONS if named else ""
