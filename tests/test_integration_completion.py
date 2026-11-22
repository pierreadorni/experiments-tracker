"""The final interpretation opts in to integration when the run completes (T3.4).

The saved analysis carries ``integrate`` and its reason. Only a named branch is
eligible, and only the existing Store ``integrate`` command queues it, inside the
transaction that marks the run complete. Missing or false decisions, the Don't
merge flag and a request that is already saved queue nothing; the main checkout
and Git stay untouched. Replaying the boundary reuses the same deterministic
request ID.
"""

import json

import pytest
from helpers import IterationAgent, approve_plan, seed_saved_contract3, until
from pydantic import ValidationError
from test_branch_workspaces import git_project

from experimenttracker.models import Analysis
from experimenttracker.workflow import Workflow


def saved_analysis(**extra):
    return {
        "title": "Synthetic result",
        "summary": "Synthetic 42.",
        "observations": [{"text": "Synthetic 42.", "evidence_ids": ["evidence"]}],
        "limitations": ["Synthetic infrastructure test."],
        "next_ideas": [],
        "scientific_outcome": "inconclusive",
        **extra,
    }


class OptInAgent(IterationAgent):
    """Return the usual synthetic analysis with the requested integration decision."""

    def __init__(self, decision):
        super().__init__()
        self.decision = decision

    async def begin(self, thread_id, prompt, schema, *, workspace=None, **kwargs):
        payload = json.loads(prompt.split("\nPAYLOAD:\n", 1)[1])
        turn = await super().begin(thread_id, prompt, schema, workspace=workspace, **kwargs)
        if payload["phase"] == "interpret":
            self.results[(thread_id, turn)] |= self.decision
        return turn


def integration_rows(store, eid):
    rows = store.rows("SELECT * FROM obligations WHERE kind='integration' AND experiment_id=?", (eid,))
    return [(row, json.loads(row["detail"])) for row in rows]


def integrate_commands(store):
    return store.rows("SELECT * FROM commands WHERE kind='integrate'")


async def completed(runtime, agent, tmp_path=None):
    """Drive one synthetic experiment to completion; the caller closes the workflow."""
    store, scheduler, _, old = runtime
    source = git_project(old, store, tmp_path) if tmp_path else None
    workflow = Workflow(old.config, store, scheduler, agent)
    eid = seed_saved_contract3(
        store,
        "Test an entirely new method and show its frontier.",
        project_id="anyup-rs" if source else "test-project",
        branch_workspace=source is not None,
    )
    await approve_plan(workflow, store, eid)
    await until(workflow, store, eid, lambda e: e["phase"] == "complete")
    return workflow, store, eid, source


def test_old_saved_analyses_stay_valid_and_an_opt_in_needs_a_reason():
    saved = Analysis.model_validate(saved_analysis())
    assert (saved.integrate, saved.integrate_reason) == (False, None)
    assert Analysis.model_validate(saved_analysis(integrate=True, integrate_reason="Keep it.")).integrate
    with pytest.raises(ValidationError, match="needs a reason"):
        Analysis.model_validate(saved_analysis(integrate=True))
    with pytest.raises(ValidationError, match="needs a reason"):
        Analysis.model_validate(saved_analysis(integrate=True, integrate_reason="  "))
