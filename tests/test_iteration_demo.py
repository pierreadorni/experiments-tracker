"""The CLI demo creates contract-3 work and finishes it with bounded synthetic compute.

This module keeps its own name so the historical fixture migration in
``conftest.py`` leaves the real execution contract in place.
"""

import asyncio

from experimenttracker import artifacts, cli
from experimenttracker.codex_client import DEMO_AGENT_SOURCE
from experimenttracker.store import Store

SUPPORTED_BRIEF = "Compare linear and quadratic regression on the fixed shared synthetic dataset."
UNSUPPORTED_BRIEF = "Train a neural classifier and draw its decision frontier."


def forbid_scientific_traversal(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("scientific provenance traversal")

    monkeypatch.setattr(artifacts, "local_output_identity", forbidden)
    monkeypatch.setattr(artifacts, "local_output_manifest", forbidden)


def run_demo(config, brief):
    return asyncio.run(cli.demo(config, brief, 60))


def read_store(config):
    return Store.read_only(config.state_dir / "workflow.sqlite")


def test_demo_recipe_keeps_held_out_points_off_the_training_grid():
    namespace = {"__name__": "demo_recipe_source"}
    exec(compile(DEMO_AGENT_SOURCE, "recipe.py", "exec"), namespace)
    assert (len(namespace["TRAIN_X"]), len(set(namespace["TEST_X"]))) == (24, 8)
    assert set(namespace["TRAIN_X"]).isdisjoint(namespace["TEST_X"])


def test_demo_refuses_a_live_configuration(monkeypatch, config):
    def forbidden(*args, **kwargs):
        raise AssertionError("the demo must not start a live runtime")

    monkeypatch.setattr(cli, "runtime", forbidden)
    assert run_demo(config.model_copy(update={"mode": "live"}), SUPPORTED_BRIEF) == 1
    assert not (config.state_dir / "workflow.sqlite").exists()
