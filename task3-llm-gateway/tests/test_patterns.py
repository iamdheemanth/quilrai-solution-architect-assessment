import json
import time

import pytest

from llm_gateway.patterns import (
    CC,
    COMBINED,
    EMAIL,
    P_ARGS,
    Q_ARGS,
    REPL,
    SSN,
    K,
    P,
    Q,
    R,
    batch_redact,
    luhn_ok,
    redaction_type,
    valid,
)

R_ = "[REDACTED]"

CASES = [
    ("mail john.doe@example.com now", "mail [REDACTED] now"),
    ("a@b.com.", "[REDACTED]."),
    ("a@b.com.au rocks", "[REDACTED] rocks"),
    ("user.123-45-6789@corp.io!", "[REDACTED]!"),
    ("no at sign", "no at sign"),
    ("x@y", "x@y"),
    ("SSN 123-45-6789 ok", "SSN [REDACTED] ok"),
    ("000-12-3456", "000-12-3456"),
    ("666-12-3456", "666-12-3456"),
    ("900-12-3456", "900-12-3456"),
    ("999-12-3456", "999-12-3456"),
    ("123-00-4567", "123-00-4567"),
    ("123-45-0000", "123-45-0000"),
    ("1234-56-7890", "1234-56-7890"),
    ("123456789", "123456789"),
    ("card 4111 1111 1111 1111.", "card [REDACTED]."),
    ("4111-1111-1111-1111", R_),
    ("4111111111111111", R_),
    ("3782 822463 10005", R_),
    ("id 4111 1111 1111 1112 x", "id 4111 1111 1111 1112 x"),
    ("id 4111 123-45-6789 x", "id 4111 [REDACTED] x"),
]


@pytest.mark.parametrize(("text", "expected"), CASES)
def test_batch_redact_table(text, expected):
    assert batch_redact(text) == expected


def test_constants():
    assert REPL == "[REDACTED]"
    assert (K, R) == (1, 2)
    assert COMBINED == f"(?P<EMAIL>{EMAIL})|(?P<SSN>{SSN})|(?P<CC>{CC})"
    guard = r"(?:(?<![\\])|(?<=[\\])(?![nrtbfu/\"\\]))"
    assert P_ARGS.pattern == guard + "(?:" + COMBINED + ")"
    assert Q.pattern == f"(?:{COMBINED})(?!)"
    assert Q_ARGS.pattern == "(?:" + P_ARGS.pattern + ")(?!)"


BS = chr(92)
GUARD_CASES = [
    (json.dumps({"p": BS + "a@b.co"}), json.dumps({"p": BS + "[REDACTED]"})),
    (json.dumps({"p": "x" + BS + "123-45-6789"}), json.dumps({"p": "x" + BS + "[REDACTED]"})),
    (json.dumps({"p": BS + "4111111111111111"}), json.dumps({"p": BS + "[REDACTED]"})),
    (json.dumps({"note": "Contact:\njohn@x.com"}), json.dumps({"note": "Contact:\n[REDACTED]"})),
    (
        json.dumps({"path": "C:" + BS + "Users" + BS + "123-45-6789.txt"}),
        json.dumps({"path": "C:" + BS + "Users" + BS + "[REDACTED].txt"}),
    ),
    (json.dumps({"x": "\t123-45-6789"}), json.dumps({"x": "\t[REDACTED]"})),
]


@pytest.mark.parametrize(("raw", "expected"), GUARD_CASES)
def test_args_guard_after_escaped_backslash(raw, expected):
    out = batch_redact(raw, args=True)
    assert out == expected
    json.loads(out)


def test_tool_args_keep_json_valid():
    raw = '{"note": "Contact:\\njohn@x.com"}'
    out = batch_redact(raw, args=True)
    assert out == '{"note": "Contact:\\n[REDACTED]"}'
    assert json.loads(out) == {"note": "Contact:\n[REDACTED]"}


def test_without_guard_escape_is_eaten():
    raw = '{"note": "Contact:\\njohn@x.com"}'
    assert batch_redact(raw) == '{"note": "Contact:\\[REDACTED]"}'


@pytest.mark.parametrize(
    "text", ["a" * 20000 + "@", "1" * 20000, "a@" + "b." * 10000, "1-" * 10000]
)
def test_redos_bounded_time(text):
    t0 = time.perf_counter()
    batch_redact(text)
    assert time.perf_counter() - t0 < 0.5


def test_redaction_type_and_valid():
    kinds = [redaction_type(m) for m in P.finditer("a@b.com 123-45-6789 4111111111111111")]
    assert kinds == ["EMAIL", "SSN", "CC"]
    bad = P.search("000-12-3456")
    assert not valid(bad)
    assert valid(P.search("a@b.com"))


@pytest.mark.parametrize(
    ("digits", "ok"),
    [
        ("4111111111111111", True),
        ("4111111111111112", False),
        ("378282246310005", True),
        ("0", True),
        ("79927398713", True),
        ("79927398710", False),
    ],
)
def test_luhn(digits, ok):
    assert luhn_ok(digits) is ok
