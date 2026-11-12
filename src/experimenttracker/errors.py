"""Explicit experiment failures.

Unexpected exceptions from `Workflow.advance` are bugs: they hold one experiment
with backoff and stay visible in the inbox. Only a raised `ExperimentFailure`
ends an experiment, so the reason must be a real scientific, validation or
budget decision rather than an internal invariant violation.
"""


class ExperimentFailure(Exception):
    """A saved experiment cannot continue; `reason` becomes the `hold_reason`."""

    def __init__(self, reason, message):
        super().__init__(message)
        self.reason = reason
        self.message = message
