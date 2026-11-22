"""The shared wait helper ticks on demand, fails on time, and reports why."""

import asyncio
import time
from types import SimpleNamespace

import pytest
from helpers import wait_for_experiment, wait_until


async def test_satisfied_predicate_returns_without_ticking():
    ticks = []

    def tick():
        ticks.append("tick")

    assert await wait_until(lambda: "already settled", ticks=tick) == "already settled"
    assert ticks == []


async def test_sync_tick_advances_until_the_predicate_holds():
    state = {"ticks": 0}

    def tick():
        state["ticks"] += 1

    assert await wait_until(lambda: state["ticks"] == 3, ticks=tick) is True
    assert state["ticks"] == 3


async def test_async_tick_advances_until_the_predicate_holds():
    state = {"ticks": 0}

    async def tick():
        await asyncio.sleep(0)
        state["ticks"] += 1

    assert await wait_until(lambda: state["ticks"] == 2, ticks=tick) is True
    assert state["ticks"] == 2


async def test_unmet_condition_fails_with_the_timeout_and_description():
    with pytest.raises(pytest.fail.Exception) as failure:
        await wait_until(
            lambda: None,
            ticks=lambda: None,
            timeout=0.05,
            describe=lambda: {"phase": "training", "hold_reason": None},
        )
    assert "condition not reached within 0.05s" in str(failure.value)
    assert "'phase': 'training'" in str(failure.value)


async def test_a_blocked_tick_is_bounded_by_the_wall_timeout():
    entered = asyncio.Event()

    async def blocked():
        entered.set()
        await asyncio.Event().wait()

    started = time.monotonic()
    with pytest.raises(pytest.fail.Exception, match="within 0.2s"):
        await wait_until(lambda: None, ticks=blocked, timeout=0.2)
    assert entered.is_set()
    assert 0.2 <= time.monotonic() - started < 5


async def test_experiment_wait_bounds_its_mandatory_initial_tick():
    entered = asyncio.Event()

    async def blocked(*, force_observe):
        assert force_observe
        entered.set()
        await asyncio.Event().wait()

    workflow = SimpleNamespace(tick=blocked)
    store = SimpleNamespace(experiment=lambda eid: {"phase": "failed"})
    with pytest.raises(pytest.fail.Exception, match="within 0.05s"):
        await wait_for_experiment(workflow, store, "test", lambda exp: True, timeout=0.05)
    assert entered.is_set()


async def test_a_tick_timeout_is_not_reported_as_an_unmet_condition():
    def tick():
        raise TimeoutError("tick timed out")

    with pytest.raises(TimeoutError, match="tick timed out"):
        await wait_until(lambda: None, ticks=tick, timeout=5)
