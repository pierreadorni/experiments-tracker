"""Versioned researcher authority; no model tool can revise this envelope."""

import math
from typing import Literal
from uuid import UUID
from zoneinfo import ZoneInfo

from pydantic import Field, model_validator

from .config import StrictModel


class AutopilotPolicy(StrictModel):
    schema_version: Literal[1] = 1
    subject: str = Field(min_length=1, max_length=8000)
    scope: str = Field(min_length=1, max_length=16000)
    instructions: str = Field(default="", max_length=16000)
    exclusions: str = Field(default="", max_length=8000)
    prerequisites: str = Field(default="", max_length=8000)
    publication_goal: str = Field(min_length=1, max_length=4000)
    venue: str = Field(min_length=1, max_length=1000)
    guideline_links: list[str] = Field(default_factory=list, max_length=20)
    paper_format: str = Field(default="LaTeX", max_length=2000)
    start_at: float
    handoff_at: float
    submission_at: float
    timezone: str = "Europe/Paris"
    phase_percent: tuple[float, float, float, float] = (1, 59, 30, 10)
    borrowing_percent: float = Field(default=5, ge=0, le=30)
    protected_writing_seconds: int = Field(default=0, ge=0)
    weekly_gpu_hours: float = Field(ge=0, le=1000000)
    anchor_at: float
    max_open_experiments: int = Field(default=2, ge=1, le=20)
    five_hour_reserve: int = Field(default=50, ge=0, le=90)
    weekly_reserve: int = Field(default=25, ge=0, le=90)
    model: str | None = Field(default=None, max_length=200)
    reasoning_effort: Literal["none", "minimal", "low", "medium", "high", "xhigh", "max"] | None = None
    may_abandon: bool = True
    weekly_report_at: float | None = None
    notifications: bool = True

    @model_validator(mode="after")
    def valid(self):
        numbers = (
            self.start_at,
            self.handoff_at,
            self.submission_at,
            self.anchor_at,
            self.weekly_gpu_hours,
            self.borrowing_percent,
            *self.phase_percent,
        )
        if not all(math.isfinite(v) for v in numbers):
            raise ValueError("policy numbers must be finite")
        if not self.start_at < self.handoff_at <= self.submission_at:
            raise ValueError("require start < handoff <= submission")
        if any(v <= 0 for v in self.phase_percent) or abs(sum(self.phase_percent) - 100) > 1e-8:
            raise ValueError("four positive phase shares must sum to 100")
        duration = self.handoff_at - self.start_at
        if self.protected_writing_seconds > duration * self.phase_percent[3] / 100:
            raise ValueError("protected writing duration exceeds writing allocation")
        if self.borrowing_percent >= self.phase_percent[2]:
            raise ValueError("borrowing must leave ablation time")
        if self.weekly_report_at is not None and not math.isfinite(self.weekly_report_at):
            raise ValueError("report date must be finite")
        ZoneInfo(self.timezone)
        for link in self.guideline_links:
            if not link.startswith("https://") or len(link) > 2000:
                raise ValueError("guidelines require bounded HTTPS links")
        return self


class AutopilotCommand(StrictModel):
    schema_version: Literal[1] = 1
    request_id: UUID
    kind: Literal[
        "configure",
        "activate",
        "revise",
        "pause",
        "resume",
        "stop",
        "complete",
        "message",
        "answer",
        "control",
    ]
    expected_revision: int = Field(ge=0)
    policy: AutopilotPolicy | None = None
    text: str = Field(default="", max_length=16000)
    experiment_id: UUID | None = None
    control: Literal["defer", "resume", "abandon", "steer"] | None = None
    input_id: str | None = Field(default=None, max_length=100)
