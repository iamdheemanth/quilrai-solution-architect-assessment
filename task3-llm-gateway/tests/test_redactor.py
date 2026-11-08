import itertools
import time
from collections import Counter

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from llm_gateway.patterns import REPL, batch_redact
from llm_gateway.redactor import StreamRedactor
from tests.test_patterns import GUARD_CASES

PII = [
    "john.doe@example.com",
    "a@b.co",
    "x.y+tag@sub.mail.example.co.uk",
    "123-45-6789",
    "078-05-1121",
    "4111 1111 1111 1111",
    "4111-1111-1111-1111",
    "4111111111111111",
    "3782 822463 10005",
    "378282246310005",
    "5555555555554444",
    "000-12-3456",
    "4111111111111112",
    "user.123-45-6789@corp.io",
]
FILL = [
    "hello ", "world", " ", ".", ", ", "\n", "call ", "1", "2024", "-", "@",
    "mail me at ", "a", "x", "...", "foo.bar", " 4111", "123-45", "-6789", "co",
    "m", ".au", "9", "0", "#", "[REDACTED]",
]  # fmt: skip
BS = chr(92)
ARG_FILL = [
    '{"a": "', '"}', "\\n", "\\\\", "\\", "\\t", '", "b": "',
    BS * 2, BS + "n", BS + "t", BS + "u0040",
]  # fmt: skip

text_st = st.lists(st.sampled_from(PII + FILL), max_size=25).map("".join)
args_text_st = st.lists(st.sampled_from(PII + FILL + ARG_FILL), max_size=25).map("".join)


def split_at(text, cuts):
    pts = [0, *sorted(cuts), len(text)]
    return [text[a:b] for a, b in itertools.pairwise(pts)]


def make_case(text_strategy):
    @st.composite
    def case(draw):
        t = draw(text_strategy)
        cuts = draw(st.lists(st.integers(0, len(t)), max_size=max(1, len(t))))
        return t, split_at(t, cuts)

    return case()


def run(chunks, **kw):
    r = StreamRedactor(**kw)
    out = [r.feed(c) for c in chunks]
    out.append(r.close())
    return "".join(out), r


@settings(max_examples=6000, deadline=None, suppress_health_check=list(HealthCheck))
@given(make_case(text_st))
def test_property_stream_equals_batch(case):
    text, chunks = case
    got, _ = run(chunks)
    assert got == batch_redact(text)


@settings(max_examples=6000, deadline=None, suppress_health_check=list(HealthCheck))
@given(make_case(args_text_st))
def test_property_stream_equals_batch_args(case):
    text, chunks = case
    got, _ = run(chunks, args=True)
    assert got == batch_redact(text, args=True)


FIXTURES = [
    "mail john.doe@example.com now",
    "a@b.com.",
    "a@b.com.au rocks",
    "user.123-45-6789@corp.io!",
    "x@y",
    "SSN 123-45-6789 ok",
    "000-12-3456",
    "1234-56-7890",
    "card 4111 1111 1111 1111.",
    "4111-1111-1111-1111",
    "3782 822463 10005",
    "id 4111 1111 1111 1112 x",
    "id 4111 123-45-6789 x",
    "[REDACTED]a@b.co",
]


@pytest.mark.parametrize("text", FIXTURES)
def test_exhaustive_splits(text):
    n = len(text)
    want = batch_redact(text)
    assert run(list(text))[0] == want
    for i in range(n + 1):
        assert run([text[:i], text[i:]])[0] == want
        for j in range(i, n + 1):
            assert run([text[:i], text[i:j], text[j:]])[0] == want


@pytest.mark.parametrize(
    ("text", "literal"),
    [
        ("mail john.doe@example.com now", "john.doe@example.com"),
        ("ssn 123-45-6789 ok", "123-45-6789"),
        ("card 4111 1111 1111 1111.", "4111 1111 1111 1111"),
        ("end a@b.co", "a@b.co"),
    ],
)
def test_no_leak_at_any_chunk_size(text, literal):
    for size in (1, 2, 3):
        r = StreamRedactor()
        emitted = ""
        for i in range(0, len(text), size):
            emitted += r.feed(text[i : i + size])
            assert literal not in emitted
        emitted += r.close()
        assert literal not in emitted
        assert REPL in emitted


def test_bounded_hold_on_prose():
    words = "the quick brown fox jumps over the lazy dog and then writes code".split()
    doc = " ".join(words[i % len(words)] for i in range(800))
    doc += " contact jane.roe@example.org or 123-45-6789, card 4111 1111 1111 1111. end"
    assert len(doc) > 4000
    r = StreamRedactor()
    for i in range(0, len(doc), 7):
        r.feed(doc[i : i + 7])
        assert r.held <= 650
    r.close()
    assert r.max_held < 100


def test_ttft_immediate_release():
    assert StreamRedactor().feed("hello ") == "hello "
    assert StreamRedactor().feed("Hi ") == "Hi "
    r = StreamRedactor()
    assert r.feed("Sur") == ""
    assert r.feed("e, ") == "Sure, "


def test_worked_example():
    r = StreamRedactor()
    outs = [r.feed(c) for c in ["Sure", ", mail jo", "hn@x.com", " today", "."]]
    assert outs == ["", "Sure, mail ", "", "[REDACTED] ", ""]
    assert r.close() == "today."


def test_adversarial_one_char_stream_is_fast_and_bounded():
    unit = "a" * 30 + "1234567" + "." * 5 + "a@" * 3 + " "
    blob = (unit * 250)[:10000]
    assert len(blob) == 10000
    r = StreamRedactor()
    times = []
    for ch in blob:
        t0 = time.perf_counter()
        r.feed(ch)
        times.append(time.perf_counter() - t0)
    r.close()
    times.sort()
    assert times[int(len(times) * 0.99)] < 0.005
    assert times[-1] < 0.1
    assert r.max_held <= 128


def test_cap_fails_closed():
    r = StreamRedactor(max_hold=8)
    out = r.feed("mail john.doe@exam")
    assert "john" not in out
    assert REPL in out
    assert r.counts["CAP"] == 1
    out += r.feed("ple.com now") + r.close()
    assert "john.doe" not in out
    assert r.held == 0


def test_max_length_email_does_not_hit_default_cap():
    email = "a" * 64 + "@" + ".".join(["b" * 63] * 8) + ".com"
    r = StreamRedactor()
    out = "".join(r.feed(ch) for ch in email + " ") + r.close()
    assert out == REPL + " "
    assert r.counts["CAP"] == 0
    assert r.max_held <= 650


def test_discard_drops_pending():
    r = StreamRedactor()
    assert r.feed("john@exa") == ""
    assert r.held == 8
    r.discard()
    assert r.held == 0
    assert r.close() == ""


def test_counts_by_type():
    r = StreamRedactor()
    out = r.feed("a@b.co 123-45-6789 4111111111111111 ") + r.close()
    assert out == f"{REPL} {REPL} {REPL} "
    assert r.counts == Counter({"EMAIL": 1, "SSN": 1, "CC": 1})


def test_rejected_matches_not_counted():
    r = StreamRedactor()
    out = r.feed("000-12-3456 ") + r.close()
    assert out == "000-12-3456 "
    assert sum(r.counts.values()) == 0


def test_args_mode_does_not_eat_escape():
    got, _ = run(['{"n": "Contact:\\', 'njohn@x.com"}'], args=True)
    assert got == '{"n": "Contact:\\n[REDACTED]"}'


def test_max_held_is_high_water_after_feed():
    r = StreamRedactor()
    seen = []
    for chunk in ["john@exa", "mple.com ", "ok "]:
        r.feed(chunk)
        seen.append(r.held)
    r.close()
    assert seen[0] == 8
    assert r.max_held == max(seen)
    assert r.max_held > seen[-1]


def run_variant(chunks, *skip):
    return run(chunks, _skip=frozenset(skip))[0]


GROW = ["a@b.co", "m"]  # a complete match that can still grow
LOOKAHEAD = ["a@b.com.", "au"]  # '.au' would extend the match past its '.'
HIDDEN = ["user.", "123-45-6789@corp.io"]  # a later complete match hides an earlier partial


@pytest.mark.parametrize("chunks", [GROW, LOOKAHEAD, HIDDEN])
def test_mutation_baseline_matches_batch(chunks):
    assert run(chunks)[0] == batch_redact("".join(chunks))


def test_mutation_without_q_search_leaks_earlier_partial():
    # Dropping the separate Q search: the later complete SSN hides the live email prefix.
    assert run_variant(HIDDEN, "q_search") != batch_redact("".join(HIDDEN))
    assert run_variant(list("a@b.com.au"), "q_search") != batch_redact("a@b.com.au")


@pytest.mark.parametrize("skip", [("fullmatch",), ("margin",), ("fullmatch", "margin")])
def test_fullmatch_and_margin_are_redundant_safeguards_given_q(skip):
    # With the real Q search these checks are defence in depth (assumption A4), not
    # independently necessary: dropping them does not change the output on these fixtures.
    for chunks in (GROW, LOOKAHEAD, HIDDEN):
        assert run_variant(chunks, *skip) == batch_redact("".join(chunks))


@pytest.mark.parametrize(("raw", "expected"), GUARD_CASES)
def test_args_guard_cases_streamed_in_every_split(raw, expected):
    want = batch_redact(raw, args=True)
    assert want == expected
    assert run(list(raw), args=True)[0] == want
    n = len(raw)
    for i in range(n + 1):
        assert run([raw[:i], raw[i:]], args=True)[0] == want
        for j in range(i, n + 1):
            assert run([raw[:i], raw[i:j], raw[j:]], args=True)[0] == want
