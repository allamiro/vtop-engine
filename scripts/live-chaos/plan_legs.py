#!/usr/bin/env python3
"""Plan the legs of the chaos campaign (#728): how many hours each leg runs and
what job timeout it gets. Reads HOURS, LEGS and RUNNER from the environment and
writes `legs`, `hours_per_leg` and `timeout_minutes` as GITHUB_OUTPUT lines.

A GitHub-hosted job stops at 6 hours whatever its timeout says; a self-hosted
one may run for 5 days. Which one a runner label names is decided by the label
itself: the labels GitHub provides begin with `ubuntu-`, `windows-` or `macos-`
(`ubuntu-latest`, `ubuntu-24.04`, `ubuntu-24.04-arm`, ...), and any other label
is taken to be a self-hosted runner. Telling them apart by comparing against
the one label `ubuntu-latest` let a versioned hosted image through with the
self-hosted allowance (review of #904).
"""

import json
import math
import os
import re
import sys

HOSTED_LIMIT_MINUTES = 360
SELF_HOSTED_LIMIT_MINUTES = 7200  # 5 days
SETUP_MINUTES = 30  # the build and the last scenario, on top of a leg's campaign time

_HOSTED_LABEL = re.compile(r"^(ubuntu|windows|macos)-")


def is_github_hosted(runner: str) -> bool:
    return _HOSTED_LABEL.match(runner.strip()) is not None


def plan(hours: float, legs: int, runner: str) -> dict:
    """The plan for `hours` over `legs`; raises ValueError for one that cannot run."""
    if hours <= 0 or not 1 <= legs <= 32:
        raise ValueError(f"hours must be positive and legs 1 to 32, got {hours} and {legs}")
    per = hours / legs
    # A leg's job gets its campaign time plus 30 minutes for the build and the
    # last scenario, so the timeout never cuts a leg short (review of #904).
    timeout = math.ceil(per * 60) + SETUP_MINUTES
    hosted = is_github_hosted(runner)
    limit = HOSTED_LIMIT_MINUTES if hosted else SELF_HOSTED_LIMIT_MINUTES
    if timeout > limit:
        kind = "a GitHub-hosted" if hosted else "a self-hosted"
        raise ValueError(
            f"{hours} h over {legs} legs is {per:.2f} h a leg, which with {SETUP_MINUTES} minutes of "
            f"setup passes the {limit} minutes {kind} runner ({runner}) allows a job: raise legs to at "
            f"least {math.ceil(hours * 60 / (limit - SETUP_MINUTES))}"
        )
    return {"legs": list(range(1, legs + 1)), "hours_per_leg": per, "timeout_minutes": timeout}


def main() -> None:
    try:
        p = plan(float(os.environ["HOURS"]), int(os.environ["LEGS"]), os.environ["RUNNER"])
    except ValueError as e:
        sys.exit(str(e))
    print(f"legs={json.dumps(p['legs'])}")
    print(f"hours_per_leg={p['hours_per_leg']:.4f}")
    print(f"timeout_minutes={p['timeout_minutes']}")


if __name__ == "__main__":
    main()
