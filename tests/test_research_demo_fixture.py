"""The reusable synthetic C4 result fixture is inert, valid and byte-stable."""

import json

from experimenttracker.models import ExperimentResult
from experimenttracker.research.demo_fixture import result_bytes


def test_c4_demo_fixture_is_valid_bounded_and_deterministic():
    first = result_bytes()
    assert len(first) < 1024
    assert first == result_bytes()

    result = ExperimentResult.model_validate(json.loads(first))
    assert result.protocol.endswith("not scientific evidence.")
    assert result.sample_count == 4
    assert [(metric.variant, metric.value) for metric in result.metrics] == [
        ("linear", 1.0),
        ("quadratic", 0.25),
    ]
