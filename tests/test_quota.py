from experimenttracker.config import QuotaConfig
from experimenttracker.quota import QuotaGate


def snapshot(five=10, week=10):
    return {
        "ordinaryUsageAllowed": True,
        "rateLimitsByLimitId": {
            "codex": {
                "primary": {"windowDurationMins": 10080, "usedPercent": week},
                "secondary": {"windowDurationMins": 300, "usedPercent": five},
            }
        },
    }


def test_duration_identity_and_hysteresis():
    gate = QuotaGate(QuotaConfig())
    gate.update(snapshot(), now=100)
    assert gate.decide(now=101).admitted
    gate.update(snapshot(five=76), now=102)
    assert not gate.decide(now=103).admitted
    gate.update(snapshot(five=72), now=104)
    assert not gate.decide(now=105).admitted
    gate.update(snapshot(five=70), now=106)
    assert gate.decide(now=107).admitted


def test_sparse_update_preserves_window_without_refreshing_it():
    gate = QuotaGate(QuotaConfig())
    gate.update(snapshot(), now=100)
    gate.update(
        {"rateLimitsByLimitId": {"codex": {"primary": {"windowDurationMins": 300, "usedPercent": 20}}}},
        now=150,
    )
    assert gate.decide(now=155).remaining["10080"] == 90
    assert gate.decide(now=165).reason in ("quota snapshot stale", "ordinary subscription permission stale")


def test_unknown_bucket_and_backend_denial_hold():
    gate = QuotaGate(QuotaConfig())
    assert not gate.decide().admitted
    gate.update({"rateLimitsByLimitId": {"other": snapshot()["rateLimitsByLimitId"]["codex"]}})
    assert not gate.decide().admitted
    gate.update({**snapshot(), "ordinaryUsageAllowed": False})
    assert gate.decide().reason == "ordinary subscription usage unavailable"


def test_account_change_invalidates_other_window():
    gate = QuotaGate(QuotaConfig())
    gate.update({**snapshot(), "accountId": "one"})
    gate.update(
        {
            "accountId": "two",
            "ordinaryUsageAllowed": True,
            "rateLimits": {"primary": {"windowDurationMins": 300, "usedPercent": 0}},
        }
    )
    assert gate.decide().reason == "required quota windows unknown"
