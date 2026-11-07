"""Turn pydantic validation errors into readable, non-leaking messages.

Only ``loc``, ``type`` and ``msg`` are used. ``input``, ``ctx`` and ``url`` are never
surfaced, because arguments may contain customer PII.
"""

import unicodedata
from dataclasses import dataclass

from pydantic import ValidationError

from customer_mcp.config import CustomerIdCharset

MAX_ECHO = 64  # cap for any client-supplied name echoed back or logged
_MAX_ISSUES = 20

_ID_HINTS: dict[CustomerIdCharset, str] = {
    "digits": "must look like CUST-XXXXX, where each X is a digit (e.g. CUST-00042)",
    "alnum": (
        "must look like CUST-XXXXX, where each X is an uppercase letter or digit "
        "(e.g. CUST-A1B2C)"
    ),
}


@dataclass(frozen=True)
class FieldIssue:
    field: str
    code: str
    message: str


def _message(field: str, code: str, msg: str, charset: CustomerIdCharset) -> str:
    if code == "missing":
        return "is required"
    if code == "extra_forbidden":
        return "is not a recognised argument"
    if code == "string_pattern_mismatch" and field == "customer_id":
        return _ID_HINTS[charset]
    return msg.removeprefix("Value error, ")


def safe_name(name: str) -> str:
    """Escape control/format characters and cap the length of a client-supplied name."""
    escaped = "".join(
        f"\\u{ord(c):04x}" if unicodedata.category(c) in ("Cc", "Cf") else c for c in name
    )
    return escaped if len(escaped) <= MAX_ECHO else escaped[:MAX_ECHO] + "…"


def describe_validation_error(exc: ValidationError, charset: CustomerIdCharset) -> list[FieldIssue]:
    errors = exc.errors(include_input=False, include_context=False, include_url=False)
    issues = []
    for err in errors[:_MAX_ISSUES]:
        raw_field = ".".join(str(p) for p in err["loc"]) or "<arguments>"
        code = err["type"]
        # The message is chosen from the raw name; only the displayed name is escaped.
        message = _message(raw_field, code, err["msg"], charset)
        issues.append(FieldIssue(safe_name(raw_field), code, message))
    if len(errors) > _MAX_ISSUES:
        more = len(errors) - _MAX_ISSUES
        issues.append(FieldIssue("<arguments>", "too_many_errors", f"… and {more} more"))
    return issues


def render_issues(tool_name: str, issues: list[FieldIssue]) -> str:
    lines = [f"Invalid arguments for {tool_name}:"]
    lines.extend(f"- {i.field}: {i.message}" for i in issues)
    return "\n".join(lines)
