"""Small deterministic contract-4 result fixture; it performs no dispatch or I/O."""

import json

from ..models import ExperimentResult

RESULT = ExperimentResult(
    schema_version=1,
    protocol="Two synthetic measurements; fixture only, not scientific evidence.",
    sample_count=4,
    metrics=[
        {"name": "mse", "variant": "linear", "value": 1.0, "unit": "squared units"},
        {"name": "mse", "variant": "quadratic", "value": 0.25, "unit": "squared units"},
    ],
)


def result_bytes() -> bytes:
    """Return the bounded, canonical JSON result for a future opt-in C4 demo."""
    return json.dumps(RESULT.model_dump(mode="json"), sort_keys=True, separators=(",", ":")).encode()
