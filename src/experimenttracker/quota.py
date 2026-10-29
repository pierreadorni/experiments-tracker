import math
import time
from dataclasses import dataclass

from .config import QuotaConfig


@dataclass
class Decision:
    admitted: bool
    reason: str | None
    remaining: dict
    observed_at: float | None


class QuotaGate:
    """Track freshness per window; sparse updates never erase or refresh an absent window."""

    def __init__(self, config: QuotaConfig):
        self.config = config
        self.windows = {}
        self.held = False
        self.ordinary_allowed = None
        self.permission_observed_at = None
        self.account_id = None

    def update(self, payload, now=None):
        now = time.time() if now is None else now
        account = payload.get("accountId")
        if self.account_id and account and account != self.account_id:
            self.windows.clear()
            self.ordinary_allowed = None
            self.permission_observed_at = None
        if account:
            self.account_id = account
        if "ordinaryUsageAllowed" in payload:
            self.ordinary_allowed = payload["ordinaryUsageAllowed"]
            self.permission_observed_at = now
        buckets = payload.get("rateLimitsByLimitId")
        if buckets is not None:
            bucket = buckets.get(self.config.bucket)
        else:
            bucket = payload.get("rateLimits")
            if bucket and bucket.get("limitId") not in (None, self.config.bucket):
                bucket = None
        if not bucket:
            return
        for key in ("primary", "secondary"):
            window = bucket.get(key)
            if not isinstance(window, dict):
                continue
            duration = window.get("windowDurationMins")
            used = window.get("usedPercent")
            if duration not in (300, 10080):
                continue
            if not isinstance(used, (int, float)) or isinstance(used, bool) or not math.isfinite(used):
                self.windows.pop(duration, None)
                continue
            if not 0 <= used <= 100:
                self.windows.pop(duration, None)
                continue
            self.windows[duration] = {**window, "observed_at": now}

    def decide(self, now=None):
        now = time.time() if now is None else now
        remaining = {str(k): 100 - v["usedPercent"] for k, v in self.windows.items()}
        observed = min((w["observed_at"] for w in self.windows.values()), default=None)
        reason = None
        if self.ordinary_allowed is False:
            reason = "ordinary subscription usage unavailable"
        elif self.ordinary_allowed is not True:
            reason = "ordinary subscription permission unknown"
        elif now - self.permission_observed_at > self.config.snapshot_max_age_seconds:
            reason = "ordinary subscription permission stale"
        elif not {300, 10080} <= self.windows.keys():
            reason = "required quota windows unknown"
        elif any(
            now - w["observed_at"] > self.config.snapshot_max_age_seconds for w in self.windows.values()
        ):
            reason = "quota snapshot stale"
        else:
            thresholds = (
                self.config.recovery_remaining
                if self.held
                else (self.config.five_hour_min_remaining, self.config.weekly_min_remaining)
            )
            if remaining["300"] < thresholds[0] or remaining["10080"] < thresholds[1]:
                reason = "subscription reserve reached"
        self.held = reason is not None
        return Decision(not self.held, reason, remaining, observed)
