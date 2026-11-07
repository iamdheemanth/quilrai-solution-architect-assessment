"""Turn pydantic validation errors into readable, non-leaking messages.

Only ``loc``, ``type`` and ``msg`` are used. ``input``, ``ctx`` and ``url`` are never
surfaced, because arguments may contain customer PII.
"""

from dataclasses import dataclass

from pydantic import ValidationError

from customer_mcp.config import CustomerIdCharset

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


def describe_validation_error(exc: ValidationError, charset: CustomerIdCharset) -> list[FieldIssue]:
    issues = []
    for err in exc.errors(include_input=False, include_context=False, include_url=False):
        field = ".".join(str(p) for p in err["loc"]) or "<arguments>"
        code = err["type"]
        issues.append(FieldIssue(field, code, _message(field, code, err["msg"], charset)))
    return issues


def render_issues(tool_name: str, issues: list[FieldIssue]) -> str:
    lines = [f"Invalid arguments for {tool_name}:"]
    lines.extend(f"- {i.field}: {i.message}" for i in issues)
    return "\n".join(lines)
