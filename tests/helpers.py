"""Deterministic waiting for tests that drive the owner step by step.

A test advances the system explicitly (a workflow tick, a controller advance)
and a wait re-checks its condition after such a tick. Nothing counts ticks: a
wait ends when its predicate holds or when ``timeout`` seconds of wall time
pass, so a loaded machine makes a passing wait take longer instead of making it
give up too early. The timeout covers the whole wait, including a tick that
blocks, so a broken wait fails the suite with a diagnostic instead of hanging.

The predicate is checked before the first tick: a condition that already holds
advances nothing, so waiting never dispatches work on its own.

The short pause between ticks is what lets subprocess- and timer-driven work
(the demo scheduler, owner threads) finish while the test waits.
"""

import asyncio
import inspect

import pytest

POLL_SECONDS = 0.01


async def wait_until(predicate, *, ticks, timeout=30.0, describe=None):
    """Return the first truthy ``predicate()``, ticking until ``timeout`` expires.

    ``ticks`` advances the system by one step and may be sync or async. On
    timeout the test fails with ``describe``. A ``TimeoutError`` raised by the
    tick itself is not an unmet condition and propagates unchanged.
    """
    limit = asyncio.timeout(timeout)
    try:
        async with limit:
            while True:
                value = predicate()
                if value:
                    return value
                result = ticks()
                if inspect.isawaitable(result):
                    await result
                await asyncio.sleep(POLL_SECONDS)
    except TimeoutError:
        if not limit.expired():
            raise
        detail = f": {describe()}" if describe else ""
        pytest.fail(f"condition not reached within {timeout}s{detail}")


async def wait_for_experiment(workflow, store, eid, predicate, *, timeout=30.0):
    """Tick ``workflow`` until ``predicate(experiment)`` holds and return the experiment.

    Owner waits take one tick before the first predicate check: they drive an
    owner that starts settled, for example at a failed phase that only repairs
    or continues on the next tick.
    """
    ticked = False

    async def tick():
        nonlocal ticked
        await workflow.tick(force_observe=True)
        ticked = True

    await wait_until(
        lambda: ticked and predicate(store.experiment(eid)),
        ticks=tick,
        timeout=timeout,
        describe=lambda: store.experiment(eid),
    )
    return store.experiment(eid)
