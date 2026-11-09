"""PII patterns, validators and batch redaction (Task 3 streaming proof assumptions).

Assumptions the streaming redactor relies on: no empty match; lookbehind <= 1 char (K);
negative lookaheads only, <= 2 chars (R); bounded, ASCII-class quantifiers (no ReDoS).
"""

import regex

REPL = "[REDACTED]"

# Local part is possessive ('@' is not in the class); labels use a greedy repeat so that
# 'a@b.com.' backtracks; the right boundary forbids a following label char or '.label'.
EMAIL = (
    r"[A-Za-z0-9._%+-]{1,64}+@"
    r"(?:[A-Za-z0-9-]{1,63}+\.){1,8}[A-Za-z]{2,63}"
    r"(?![A-Za-z0-9-]|\.[A-Za-z0-9])"
)
# Validity (000/666/9xx, group 00, serial 0000) is checked by `valid`.
SSN = r"(?<![0-9])[0-9]{3}-[0-9]{2}-[0-9]{4}(?![0-9])"
CC = (
    r"(?<![0-9])(?:[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{1,7}|[0-9]{4} [0-9]{4} [0-9]{4} [0-9]{1,7}"
    r"|3[47][0-9]{2}-[0-9]{6}-[0-9]{5}|3[47][0-9]{2} [0-9]{6} [0-9]{5}|[0-9]{13,19})(?![0-9])"
)
COMBINED = rf"(?P<EMAIL>{EMAIL})|(?P<SSN>{SSN})|(?P<CC>{CC})"

K = 1  # max lookbehind width before a match
R = 2  # max lookahead width after a match

# Tool-argument channels hold JSON-escaped text. A match may start after a backslash only if
# its first char is not a JSON escape letter (so '\n' + 'john@x.com' is safe, while the second
# backslash of an escaped '\\' does not block a match). Lookbehind stays 1 char (K).
_GUARD = r"(?:(?<![\\])|(?<=[\\])(?![nrtbfu/\"\\]))"

P = regex.compile(COMBINED)
# Can never complete, so a partial=True search yields only true partial matches.
Q = regex.compile(rf"(?:{COMBINED})(?!)")
P_ARGS = regex.compile(rf"{_GUARD}(?:{COMBINED})")
Q_ARGS = regex.compile(rf"(?:{P_ARGS.pattern})(?!)")


def luhn_ok(digits: str) -> bool:
    total = 0
    for i, ch in enumerate(reversed(digits)):
        n = ord(ch) - 48
        if i % 2:
            n = n * 2 - 9 if n > 4 else n * 2
        total += n
    return total % 10 == 0


def valid(m: "regex.Match") -> bool:
    """Decide whether a pattern match is redacted (a rejected match is emitted verbatim).

    A guardrail should fail toward redaction, so shape alone decides for anything that is
    written like an SSN or a separated card number, whether or not the digits are a
    "real" number (made-up and test values are still PII-shaped). The Luhn check is kept
    only for unseparated 13 to 19 digit runs, which would otherwise catch timestamps,
    order numbers and other identifiers.
    """
    if m.lastgroup == "CC":
        text = m.group()
        if text.isdigit():
            return luhn_ok(text)
        return True
    return True


def redaction_type(m: "regex.Match") -> str:
    return m.lastgroup


def batch_redact(text: str, args: bool = False) -> str:
    pattern = P_ARGS if args else P
    return pattern.sub(lambda m: REPL if valid(m) else m.group(), text)
