"""Token estimate used to reserve rate-limit budget before calling upstream.

estimate = ceil(len(json.dumps(messages)) / 4) + (max_tokens or max_completion_tokens or default)

If the client sent neither ``max_tokens`` nor ``max_completion_tokens`` the gateway injects
``max_tokens = default`` into the forwarded body, so the completion can never exceed what was
reserved.
"""

from __future__ import annotations

import json
import math
from typing import Any

from .errors import InvalidRequest


def _completion_budget(body: dict[str, Any]) -> tuple[str, int] | None:
    """First present of max_tokens, max_completion_tokens. Both are validated when present.

    A present value of 0 is honoured as 0 (an explicit client choice), not treated as absent.
    """
    found: list[tuple[str, int]] = []
    for field in ("max_tokens", "max_completion_tokens"):
        value = body.get(field)
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise InvalidRequest(f"'{field}' must be a non-negative integer", param=field)
        found.append((field, value))
    return found[0] if found else None


def estimate_tokens(body: dict[str, Any], default_max_tokens: int) -> tuple[int, dict[str, Any]]:
    """Return (estimated tokens, body to forward). The input dict is not mutated."""
    forward = dict(body)
    prompt = math.ceil(len(json.dumps(body.get("messages", []))) / 4)
    budget = _completion_budget(body)
    if budget is None:
        forward["max_tokens"] = default_max_tokens
        completion = default_max_tokens
    else:
        completion = budget[1]
    return prompt + completion, forward
