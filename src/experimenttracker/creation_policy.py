"""Internal create-boundary policy for the contract cutover.

Keep this source-controlled mode single and private: a rollout is not a user
configuration option. Candidate mode is exercised by focused tests before it is
selected for normal creates.
"""

MODE = "legacy"


def defaults_to_research_loop():
    return MODE == "candidate"


def select_contract(*, requested, opted_in):
    """Select the saved contract for a new create; never rewrite an existing one."""
    if requested is not None:
        return requested
    return 4 if defaults_to_research_loop() or opted_in else 3


def refuses_unbound_hosted_create():
    """Whether the candidate boundary refuses new hosted payloads lacking intent."""
    return MODE == "candidate"
