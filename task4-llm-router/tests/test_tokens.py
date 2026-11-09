import json
import math

import pytest

from llm_router.errors import InvalidRequest
from llm_router.tokens import estimate_tokens

MSGS = [{"role": "user", "content": "hello there, how are you today?"}]
PROMPT = math.ceil(len(json.dumps(MSGS)) / 4)


def test_prompt_part_is_ceil_of_serialized_length_over_4():
    # 64 chars of JSON -> ceil(64/4) = 16
    assert len(json.dumps(MSGS)) == 64
    assert PROMPT == 16


def test_default_max_tokens_injected_when_absent():
    body = {"model": "m", "messages": MSGS}
    est, fwd = estimate_tokens(body, 1024)
    assert est == 16 + 1024
    assert fwd["max_tokens"] == 1024
    assert "max_tokens" not in body  # caller's dict is not mutated
    assert fwd["messages"] == MSGS


def test_max_tokens_used_and_not_overwritten():
    body = {"model": "m", "messages": MSGS, "max_tokens": 200}
    est, fwd = estimate_tokens(body, 1024)
    assert est == 16 + 200
    assert fwd["max_tokens"] == 200
    assert "max_completion_tokens" not in fwd


def test_max_completion_tokens_used_and_nothing_injected():
    body = {"model": "m", "messages": MSGS, "max_completion_tokens": 300}
    est, fwd = estimate_tokens(body, 1024)
    assert est == 16 + 300
    assert "max_tokens" not in fwd
    assert fwd["max_completion_tokens"] == 300


def test_max_tokens_takes_precedence_over_max_completion_tokens():
    body = {"messages": MSGS, "max_tokens": 10, "max_completion_tokens": 999}
    est, _ = estimate_tokens(body, 1024)
    assert est == 16 + 10


def test_empty_messages():
    est, fwd = estimate_tokens({"messages": []}, 50)
    assert est == 1 + 50  # json "[]" is 2 chars -> ceil(2/4) = 1
    assert fwd["max_tokens"] == 50


def test_explicit_zero_max_tokens_is_honoured_not_defaulted():
    est, fwd = estimate_tokens({"messages": MSGS, "max_tokens": 0}, 1024)
    assert est == 16 + 0
    assert fwd["max_tokens"] == 0


def test_invalid_max_completion_tokens_rejected_even_with_valid_max_tokens():
    body = {"messages": MSGS, "max_tokens": 10, "max_completion_tokens": "lots"}
    with pytest.raises(InvalidRequest) as ei:
        estimate_tokens(body, 1024)
    assert ei.value.param == "max_completion_tokens"


@pytest.mark.parametrize("bad", [-1, "100", 1.5, True])
def test_invalid_max_tokens_rejected(bad):
    with pytest.raises(InvalidRequest) as ei:
        estimate_tokens({"messages": MSGS, "max_tokens": bad}, 1024)
    assert ei.value.param == "max_tokens"
