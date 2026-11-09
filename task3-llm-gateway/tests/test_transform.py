import json

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from llm_gateway.patterns import batch_redact
from llm_gateway.transform import (
    ERROR_FRAME,
    StreamTransformer,
    redact_completion,
    redact_error_body,
)
from tests.helpers import chunk, events, parse_out, run, sse, tool


def content_of(objs, index=0):
    return "".join(
        c["delta"].get("content") or ""
        for o in objs
        if o != "[DONE]"
        for c in o.get("choices", [])
        if c["index"] == index
    )


def test_error_frame_exact():
    assert ERROR_FRAME == (
        b'data: {"error":{"message":"Upstream stream failed","type":"upstream_error",'
        b'"code":"upstream_error"}}\n\n'
    )


def test_email_split_over_three_deltas():
    tx = StreamTransformer()
    out = run(
        tx,
        sse(chunk("mail john@")),
        sse(chunk("x.c")),
        sse(chunk("om now")),
        sse(chunk(finish="stop")),
        sse("[DONE]"),
    )
    objs = parse_out(out)
    assert content_of(objs) == batch_redact("mail john@x.com now")
    assert "john" not in out.decode()


def test_unchanged_frames_are_byte_identical():
    tx = StreamTransformer()
    frames = [sse(chunk("hello ", role="assistant")), sse(chunk("world "))]
    for f in frames:
        (ev,) = events(f)
        assert tx.on_event(ev) == ev.raw


def test_tail_before_finish():
    tx = StreamTransformer()
    out = run(tx, sse(chunk("email john@x.com")), sse(chunk(finish="stop")), sse("[DONE]"))
    objs = parse_out(out)
    assert objs[-1] == "[DONE]"
    fin = [i for i, o in enumerate(objs) if o != "[DONE]" and o["choices"][0]["finish_reason"]]
    assert len(fin) == 1
    f = fin[0]
    assert objs[f]["choices"][0]["delta"] == {}
    assert objs[f]["choices"][0]["finish_reason"] == "stop"
    assert "[REDACTED]" in content_of(objs[:f])
    assert content_of(objs) == "email [REDACTED]"
    synth = objs[f - 1]
    assert synth["choices"][0]["finish_reason"] is None
    assert synth["id"] == "chatcmpl-1" and synth["model"] == "m"
    assert tx.on_event(events(sse("[DONE]"))[0]) == b""


def test_finish_chunk_own_text_moved_to_synthetic():
    tx = StreamTransformer()
    out = run(tx, sse(chunk("to john@x.com", finish="stop")))
    objs = parse_out(out)
    assert len(objs) == 2
    assert objs[0]["choices"][0]["delta"]["content"] == "to [REDACTED]"
    assert objs[1]["choices"][0]["delta"] == {"content": ""}


def test_finish_without_held_text_passes_through_identical():
    tx = StreamTransformer()
    run(tx, sse(chunk("hello ")))
    (ev,) = events(sse(chunk(finish="stop")))
    assert tx.on_event(ev) == ev.raw


def test_n2_choices_independent():
    tx = StreamTransformer()
    out = run(
        tx,
        sse(chunk("a@b", index=0)),
        sse(chunk("x@y", index=1)),
        sse(chunk(".com ", index=0)),
        sse(chunk(".org ", index=1)),
        sse(chunk(finish="stop", index=0)),
        sse(chunk(finish="stop", index=1)),
        sse("[DONE]"),
    )
    objs = parse_out(out)
    assert content_of(objs, 0) == "[REDACTED] "
    assert content_of(objs, 1) == "[REDACTED] "


def test_refusal_redacted():
    tx = StreamTransformer()
    out = run(
        tx,
        sse(chunk(refusal="no, 123-45-")),
        sse(chunk(refusal="6789 ok ")),
        sse(chunk(finish="stop")),
        sse("[DONE]"),
    )
    text = "".join(
        o["choices"][0]["delta"].get("refusal") or "" for o in parse_out(out) if o != "[DONE]"
    )
    assert text == "no, [REDACTED] ok "


def test_tool_calls_redacted_and_valid_json():
    tx = StreamTransformer()
    frames = [
        sse(chunk(tool_calls=[tool(0, '{"a":"x\\n', id="c0", type="function")])),
        sse(chunk(tool_calls=[tool(1, '{"b":"q')])),
        sse(chunk(tool_calls=[tool(0, "jo")])),
        sse(chunk(tool_calls=[tool(0, "hn@x.")])),
        sse(chunk(tool_calls=[tool(1, '"}')])),
        sse(chunk(tool_calls=[tool(0, 'com"}')])),
        sse(chunk(finish="tool_calls")),
        sse("[DONE]"),
    ]
    objs = parse_out(run(tx, *frames))
    args = {0: "", 1: ""}
    for o in objs:
        if o == "[DONE]":
            continue
        for tc in o["choices"][0]["delta"].get("tool_calls", []):
            args[tc["index"]] += tc["function"]["arguments"]
    assert json.loads(args[0]) == {"a": "x\n[REDACTED]"}
    assert json.loads(args[1]) == {"b": "q"}
    first = objs[0]["choices"][0]["delta"]["tool_calls"][0]
    assert first["id"] == "c0" and first["type"] == "function"


def test_function_call_redacted():
    tx = StreamTransformer()
    objs = parse_out(
        run(
            tx,
            sse(chunk(function_call={"name": "f", "arguments": '{"e":"a@b.'})),
            sse(chunk(function_call={"arguments": 'com"}'})),
            sse(chunk(finish="function_call")),
            sse("[DONE]"),
        )
    )
    args = "".join(
        o["choices"][0]["delta"].get("function_call", {}).get("arguments", "")
        for o in objs
        if o != "[DONE]"
    )
    assert json.loads(args) == {"e": "[REDACTED]"}
    assert objs[0]["choices"][0]["delta"]["function_call"]["name"] == "f"


def test_logprobs_nulled():
    tx = StreamTransformer()
    out = run(tx, sse(chunk("hi ", logprobs={"content": [{"token": "hi"}]})))
    (o,) = parse_out(out)
    assert o["choices"][0]["logprobs"] is None
    assert o["choices"][0]["delta"]["content"] == "hi "


def test_usage_chunk_passthrough_after_close():
    tx = StreamTransformer()
    run(tx, sse(chunk("mail a@b.com")))
    usage = sse(chunk(choices=[], usage={"total_tokens": 3}))
    (ev,) = events(usage)
    out = tx.on_event(ev)
    assert out.endswith(usage)
    head = parse_out(out[: -len(usage)])
    assert content_of(head) == "[REDACTED]"


def test_eof_after_finish_synthesizes_done():
    tx = StreamTransformer()
    run(tx, sse(chunk("hi ")), sse(chunk(finish="stop")))
    assert tx.on_eof() == b"data: [DONE]\n\n"
    assert tx.done
    assert tx.on_eof() == b""


def test_eof_with_unfinished_choice_fails_closed():
    tx = StreamTransformer()
    out = run(tx, sse(chunk("mail john@x")))
    assert "john" not in out.decode()
    assert tx.on_eof() == ERROR_FRAME
    assert tx.on_eof() == b""


def test_discard_never_emits_held():
    tx = StreamTransformer()
    run(tx, sse(chunk("mail john@x")))
    tx.discard()
    assert tx.on_eof() == b""
    assert run(tx, sse(chunk(finish="stop")), sse("[DONE]")) == b""


def test_upstream_error_frame():
    tx = StreamTransformer()
    run(tx, sse(chunk("mail john@x")))
    out = run(tx, sse({"error": {"message": "bad john@x.com", "type": "e"}}))
    (o,) = parse_out(out)
    assert o["error"]["message"] == "bad [REDACTED]"
    assert o["error"]["type"] == "e"
    assert tx.done
    assert run(tx, sse(chunk(finish="stop"))) == b""


def test_bad_json_raises():
    tx = StreamTransformer()
    (ev,) = events(b"data: {not json\n\n")
    with pytest.raises(ValueError):
        tx.on_event(ev)


def test_non_object_json_raises():
    tx = StreamTransformer()
    (ev,) = events(b"data: [1]\n\n")
    with pytest.raises(ValueError):
        tx.on_event(ev)


def test_comment_becomes_ping():
    tx = StreamTransformer()
    (ev,) = events(b": keepalive\n\n")
    assert tx.on_event(ev) == b": ping\n\n"


def test_fully_held_delta_emits_empty_content():
    tx = StreamTransformer()
    (o,) = parse_out(run(tx, sse(chunk("john@x"))))
    assert o["choices"][0]["delta"]["content"] == ""


def test_non_ascii_changed_frame_is_ascii_escaped():
    tx = StreamTransformer()
    out = run(tx, sse(chunk("café a@b.com ")), sse(chunk(finish="stop")))
    assert out.isascii()
    assert content_of(parse_out(out)) == "café [REDACTED] "


def test_unknown_fields_preserved():
    tx = StreamTransformer()
    obj = chunk("a@b.com ")
    obj["obfuscation"] = "zz"
    obj["choices"][0]["extra"] = 5
    (o,) = parse_out(run(tx, sse(obj)))
    assert o["obfuscation"] == "zz" and o["choices"][0]["extra"] == 5


def test_stats():
    tx = StreamTransformer()
    run(tx, sse(chunk("a@b.com and 123-45-6789 ")), sse(chunk(finish="stop")), sse("[DONE]"))
    s = tx.stats()
    assert s["counts"]["EMAIL"] == 1 and s["counts"]["SSN"] == 1
    assert s["max_held"] >= 1


def test_redact_completion():
    obj = {
        "id": "x",
        "usage": {"n": 1},
        "choices": [
            {
                "index": 0,
                "finish_reason": "stop",
                "logprobs": {"content": []},
                "message": {
                    "role": "assistant",
                    "content": "mail a@b.com",
                    "refusal": "ssn 123-45-6789",
                    "tool_calls": [
                        {"id": "c", "function": {"name": "f", "arguments": '{"e":"a@b.com"}'}}
                    ],
                    "function_call": {"name": "g", "arguments": '{"e":"a@b.com"}'},
                },
            }
        ],
    }
    out = redact_completion(obj)
    m = out["choices"][0]["message"]
    assert m["content"] == "mail [REDACTED]"
    assert m["refusal"] == "ssn [REDACTED]"
    assert json.loads(m["tool_calls"][0]["function"]["arguments"]) == {"e": "[REDACTED]"}
    assert json.loads(m["function_call"]["arguments"]) == {"e": "[REDACTED]"}
    assert m["tool_calls"][0]["id"] == "c"
    assert out["choices"][0]["logprobs"] is None
    assert out["usage"] == {"n": 1} and out["id"] == "x"
    assert "a@b.com" in json.dumps(obj)  # input not mutated


def test_redact_error_body():
    out = redact_error_body({"error": {"message": "bad a@b.com", "code": "x"}})
    assert out == {"error": {"message": "bad [REDACTED]", "code": "x"}}


def _feed_until_raise(tx, *frames):
    """Run frames, returning (output_before_raise, raised)."""
    out = b""
    try:
        for f in frames:
            for ev in events(f):
                out += tx.on_event(ev)
    except ValueError:
        return out, True
    return out, False


def test_text_after_finish_fails_closed():
    tx = StreamTransformer()
    out, raised = _feed_until_raise(
        tx, sse(chunk("mail john@", finish="stop")), sse(chunk("x.com bye"))
    )
    assert raised
    assert b"john@x" not in out and b"x.com" not in out


def test_text_after_empty_choices_usage_chunk_fails_closed():
    tx = StreamTransformer()
    out, raised = _feed_until_raise(
        tx,
        sse(chunk("mail john@")),
        sse(chunk(choices=[], usage={"total_tokens": 1})),
        sse(chunk("x.com")),
    )
    assert raised
    assert b"john@x" not in out and b"x.com" not in out


def test_new_tool_text_after_finish_fails_closed():
    tx = StreamTransformer()
    _, raised = _feed_until_raise(
        tx, sse(chunk(finish="stop")), sse(chunk(tool_calls=[tool(0, "a@b")]))
    )
    assert raised


def test_empty_choices_without_usage_does_not_close():
    tx = StreamTransformer()
    out = run(tx, sse(chunk("mail john@")))
    empty = sse(chunk(choices=[]))
    (ev,) = events(empty)
    assert tx.on_event(ev) == ev.raw
    out += run(tx, sse(chunk("x.com ok ")), sse(chunk(finish="stop")), sse("[DONE]"))
    assert content_of(parse_out(out)) == "mail [REDACTED] ok "


def _raw(data: str) -> bytes:
    return b"data: " + data.encode() + b"\n\n"


@pytest.mark.parametrize(
    "data",
    [
        '{"choices":[{"index":0,"delta":{"content":["john@x.com"]}}]}',
        '{"choices":[{"index":0,"delta":"john@x.com "}]}',
        '{"choices":"john@x.com"}',
        '{"choices":null}',
        '{"choices":["john@x.com"]}',
        '{"choices":[{"index":0,"delta":{"refusal":5}}]}',
        '{"choices":[{"index":0,"delta":{"function_call":{"arguments":{"a":"b"}}}}]}',
        '{"choices":[{"index":0,"delta":{"function_call":"john@x.com"}}]}',
        '{"choices":[{"index":0,"delta":{"tool_calls":"john@x.com"}}]}',
        '{"choices":[{"index":0,"delta":{"tool_calls":["john@x.com"]}}]}',
        '{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":"a@b.com"}]}}]}',
        '{"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":1}}]}}]}',
    ],
)
def test_malformed_shapes_raise(data):
    tx = StreamTransformer()
    (ev,) = events(_raw(data))
    with pytest.raises(ValueError):
        tx.on_event(ev)


def test_error_null_is_a_normal_chunk():
    tx = StreamTransformer()
    obj = chunk("hello ")
    obj["error"] = None
    (ev,) = events(sse(obj))
    assert tx.on_event(ev) == ev.raw
    assert not tx.done


def test_eof_without_any_finished_choice_fails_closed():
    assert StreamTransformer().on_eof() == ERROR_FRAME
    tx = StreamTransformer()
    run(tx, sse(chunk(choices=[], usage={"total_tokens": 1})))
    assert tx.on_eof() == ERROR_FRAME


def test_duplicate_keys_raise():
    tx = StreamTransformer()
    data = '{"choices":[{"index":0,"delta":{"content":"john@x.com ","content":"ok. "}}]}'
    (ev,) = events(_raw(data))
    with pytest.raises(ValueError):
        tx.on_event(ev)


def test_discard_empties_every_redactor():
    tx = StreamTransformer()
    run(tx, sse(chunk("mail john@x", index=0)), sse(chunk("a b 123-45-", index=1)))
    assert any(r.held for r in tx._all)
    tx.discard()
    assert tx._all and all(r.held == 0 for r in tx._all)


_PIECES = st.sampled_from(
    ["john@x.com", "a@b.org", "123-45-6789", "4111 1111 1111 1111", "hi ", " ", "@", ".", "-", "9"]
    + ["café ", "x", "42 ", "\n", "123-45-"]
)


@settings(max_examples=600, deadline=None)
@given(
    texts=st.lists(st.lists(_PIECES, max_size=8).map("".join), min_size=1, max_size=2),
    seed=st.randoms(use_true_random=False),
    logprobs=st.booleans(),
    usage=st.booleans(),
    reasoning=st.booleans(),
)
def test_transformer_property(texts, seed, logprobs, usage, reasoning):
    # split each choice's text into random deltas (empty ones included)
    queues = []
    for t in texts:
        cuts = sorted(seed.randint(0, len(t)) for _ in range(seed.randint(0, 5)))
        pieces = [t[a:b] for a, b in zip([0, *cuts], [*cuts, len(t)], strict=True)]
        queues.append(pieces)
    lp = {"content": [{"token": "t"}]} if logprobs else None
    frames = []
    pending = [(i, list(q)) for i, q in enumerate(queues)]
    while any(q for _, q in pending):
        i, q = seed.choice([p for p in pending if p[1]])
        piece = q.pop(0)
        last = not q
        obj = chunk(piece, index=i, logprobs=lp, finish="stop" if last else None)
        if reasoning:
            obj["choices"][0]["delta"]["reasoning_content"] = piece
        frames.append(sse(obj))
    if usage:
        frames.append(sse(chunk(choices=[], usage={"total_tokens": 1})))
    frames.append(sse("[DONE]"))
    tx = StreamTransformer()
    out = run(tx, *frames)
    objs = parse_out(out)
    assert objs[-1] == "[DONE]" and objs.count("[DONE]") == 1
    for i, t in enumerate(texts):
        assert content_of(objs, i) == batch_redact(t)
        if reasoning:
            got = "".join(
                c["delta"].get("reasoning_content") or ""
                for o in objs[:-1]
                for c in o["choices"]
                if c["index"] == i
            )
            assert got == batch_redact(t)
    for o in objs[:-1]:
        for c in o["choices"]:
            assert c.get("logprobs") is None
    assert tx.on_event(events(sse(chunk("zz ")))[0]) == b""


# ---- final-review fixes (H1-H3, H5, H6) ----


def test_error_frame_redacts_entire_frame():
    tx = StreamTransformer()
    frame = {
        "error": {"message": "x", "metadata": {"raw": "user john@x.com"}},
        "choices": [{"index": 0, "delta": {"content": "ssn 123-45-6789 "}}],
    }
    out = run(tx, sse(frame))
    assert b"john@x.com" not in out and b"123-45-6789" not in out
    (o,) = parse_out(out)
    assert isinstance(o["error"], dict)
    assert tx.saw_upstream_error and tx.done
    assert run(tx, sse("[DONE]")) == b""


def test_error_frame_string_error_and_tool_args():
    tx = StreamTransformer()
    obj = {
        "error": "bad john@x.com",
        "choices": [{"delta": {"tool_calls": [tool(0, '{"e":"a@b.com"}')], "content": "a@b.com "}}],
    }
    out = run(tx, sse(obj))
    assert b"@b.com" not in out and b"@x.com" not in out
    (o,) = parse_out(out)
    assert o["error"] == "bad [REDACTED]"
    args = o["choices"][0]["delta"]["tool_calls"][0]["function"]["arguments"]
    assert json.loads(args) == {"e": "[REDACTED]"}


def _channel_stream(field, pieces):
    frames = [sse(_with_delta({field: p})) for p in pieces]
    frames.append(sse(chunk(finish="stop")))
    frames.append(sse("[DONE]"))
    return frames


def _with_delta(delta, index=0, finish=None):
    obj = chunk("")
    obj["choices"] = [{"index": index, "delta": delta, "logprobs": None, "finish_reason": finish}]
    return obj


@pytest.mark.parametrize("field", ["reasoning_content", "reasoning"])
def test_reasoning_channels_equal_batch_and_flush_before_finish(field):
    text = "think john@example.com and 123-45-6789 ok mail a@b.co"
    cuts = [0, 7, 15, 20, 31, 40, len(text)]
    pieces = [text[a:b] for a, b in zip(cuts, cuts[1:], strict=False)]
    out = run(StreamTransformer(), *_channel_stream(field, pieces))
    objs = parse_out(out)
    got = "".join(c["delta"].get(field) or "" for o in objs[:-1] for c in o["choices"])
    assert got == batch_redact(text)
    assert "john@example.com" not in out.decode()
    finish_at = next(
        n for n, o in enumerate(objs[:-1]) if o["choices"][0].get("finish_reason") == "stop"
    )
    assert got == "".join(
        c["delta"].get(field) or "" for o in objs[:finish_at] for c in o["choices"]
    )


def test_reasoning_non_string_fails_closed():
    with pytest.raises(ValueError):
        run(StreamTransformer(), sse(_with_delta({"reasoning_content": 5})))


def test_audio_transcript_redacted():
    obj = _with_delta({"audio": {"transcript": "ssn 123-45-6789", "id": "a"}})
    out = run(StreamTransformer(), sse(obj))
    assert b"123-45-6789" not in out
    assert parse_out(out)[0]["choices"][0]["delta"]["audio"]["transcript"] == "ssn [REDACTED]"


def test_frame_without_choices_redacted():
    out = run(StreamTransformer(), sse({"id": "c", "note": "john@x.com"}))
    assert parse_out(out) == [{"id": "c", "note": "[REDACTED]"}]


def test_message_object_in_stream_chunk_redacted():
    obj = _with_delta({})
    obj["choices"][0]["message"] = {"content": "john@x.com"}
    out = run(StreamTransformer(), sse(obj))
    assert b"john@x.com" not in out


def test_pii_free_frame_with_unknown_fields_byte_identical():
    obj = _with_delta({"audio": {"transcript": "hello"}, "x": [1, "y"]})
    obj["note"] = {"k": "v"}
    raw = b"data: " + json.dumps(obj).encode() + b"\n\n"  # non-compact: proves raw reuse
    assert run(StreamTransformer(), raw) == raw


def test_too_deep_unknown_field_fails_closed():
    deep: object = "x"
    for _ in range(80):
        deep = [deep]
    with pytest.raises(ValueError):
        run(StreamTransformer(), sse({"id": "c", "note": deep}))


def test_non_stream_tool_args_redacted_once_and_valid_json():
    from llm_gateway.transform import redact_json_body

    args = json.dumps({"a": "\n@x.com"})
    obj = {
        "choices": [{"message": {"tool_calls": [{"function": {"name": "f", "arguments": args}}]}}]
    }
    out = redact_json_body(obj)
    fixed = out["choices"][0]["message"]["tool_calls"][0]["function"]["arguments"]
    assert json.loads(fixed) == json.loads(batch_redact(args, args=True))
    assert chr(92) + "[REDACTED]" not in fixed


def test_usage_null_empty_choices_does_not_seal():
    tx = StreamTransformer()
    run(tx, sse(chunk("hello ")))
    run(tx, sse(chunk(choices=[]) | {"usage": None}))
    out = run(tx, sse(chunk("world john@x.com ")), sse(chunk(finish="stop")), sse("[DONE]"))
    objs = parse_out(out)
    assert objs[-1] == "[DONE]"
    assert "[REDACTED]" in content_of(objs)
