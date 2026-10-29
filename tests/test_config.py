from pathlib import Path

import pytest
from pydantic import ValidationError

from experimenttracker.config import GatewayConfig, QuotaConfig


@pytest.mark.parametrize(
    "url",
    [
        "http://gateway.test",
        "https://user:password@gateway.test",
        "https://gateway.test/api",
        "https://gateway.test?token=secret",
        "https://gateway.test#secret",
        "https://",
    ],
)
def test_gateway_accepts_only_a_credential_free_https_origin(url):
    with pytest.raises(ValidationError):
        GatewayConfig(url=url, credential_file=Path("/tmp/token"))


def test_credentials_are_explicit_and_recovery_preserves_hysteresis():
    with pytest.raises(ValidationError):
        GatewayConfig(url="https://gateway.test", credential_file=Path("relative-token"))
    with pytest.raises(ValidationError):
        QuotaConfig(five_hour_min_remaining=35, recovery_remaining=(30, 15))
    with pytest.raises(ValidationError):
        QuotaConfig(recovery_remaining=(101, 15))
    assert GatewayConfig(url="https://gateway.test", credential_file=Path("/tmp/token")).url


@pytest.mark.parametrize("limit", [0, -1, True, 1.5])
def test_executor_job_limit_requires_a_positive_integer(limit):
    from experimenttracker.config import SlurmConfig

    with pytest.raises(ValidationError):
        SlurmConfig(max_concurrent_jobs=limit)


def test_executor_job_limit_can_be_unbounded():
    from experimenttracker.config import SlurmConfig

    assert SlurmConfig(max_concurrent_jobs=None).max_concurrent_jobs is None
