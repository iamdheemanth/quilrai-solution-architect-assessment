import json

import httpx2
import pytest

from llm_gateway.sse import SSEParser
from mock_llm.server import DEFAULT_TOKENS, build_mock_app

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def post(app, body, *, stream=True):
    payload = {"model": "m-x", "messages": [{"role": "user", "content": "hi"}]}
    payload.update(body)
    if stream:
        payload["stream"] = True
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://test"
    ) as client:
        return await client.post("/v1/chat/completions", json=payload)


def parse(raw: bytes) -> list[str]:
    return [e.data for e in SSEParser().feed(raw) if e.kind == "data"]


def content_of(frames: list[str], index: int = 0) -> str:
    out = ""
    for f in frames:
        if f == "[DONE]":
            continue
        for ch in json.loads(f).get("choices", []):
            if ch["index"] == index:
                out += ch["delta"].get("content") or ""
    return out


def test_default_tokens_split_pii_and_end_with_email():
    joined = "".join(DEFAULT_TOKENS)
    assert "john.doe@example.com" in joined
    assert "123-45-6789" in joined
    assert "4111 1111 1111 1111" in joined
    assert joined.endswith("jane@example.org")
    assert not any("john.doe@example.com" in t for t in DEFAULT_TOKENS)
    assert not any("123-45-6789" in t for t in DEFAULT_TOKENS)


async def test_stream_chunk_sequence():
    resp = await post(build_mock_app(tokens=["a", "b"]), {})
    assert resp.headers["content-type"].startswith("text/event-stream")
    frames = parse(resp.content)
    assert frames[-1] == "[DONE]"
    objs = [json.loads(f) for f in frames[:-1]]
    assert len(objs) == 4
    assert objs[0]["choices"][0]["delta"] == {"role": "assistant", "content": ""}
    assert objs[1]["choices"][0]["delta"] == {"content": "a"}
    assert objs[2]["choices"][0]["delta"] == {"content": "b"}
    assert objs[3]["choices"][0]["delta"] == {}
    assert objs[3]["choices"][0]["finish_reason"] == "stop"
    for o in objs:
        assert o["object"] == "chat.completion.chunk"
        assert o["model"] == "m-x"
        assert {"id", "created", "system_fingerprint"} <= o.keys()
        assert o["choices"][0]["logprobs"] is None


async def test_default_content_roundtrip():
    resp = await post(build_mock_app(), {})
    assert content_of(parse(resp.content)) == "".join(DEFAULT_TOKENS)


async def test_n2_independent_choices():
    resp = await post(build_mock_app(tokens=["x", "y"]), {"n": 2})
    frames = parse(resp.content)
    assert content_of(frames, 0) == "xy"
    assert content_of(frames, 1) == "xy"
    order = [
        json.loads(f)["choices"][0]["index"]
        for f in frames[:-1]
        if json.loads(f)["choices"][0]["delta"].get("content") in ("x", "y")
    ]
    assert order == [0, 1, 0, 1]


async def test_choices_override():
    resp = await post(build_mock_app(choices=[["a"], ["b", "c"]]), {})
    frames = parse(resp.content)
    assert content_of(frames, 0) == "a"
    assert content_of(frames, 1) == "bc"


async def test_include_usage_final_chunk():
    resp = await post(
        build_mock_app(tokens=["a", "b"]), {"stream_options": {"include_usage": True}}
    )
    frames = parse(resp.content)
    assert frames[-1] == "[DONE]"
    usage = json.loads(frames[-2])
    assert usage["choices"] == []
    assert usage["usage"]["completion_tokens"] == 2
    assert usage["usage"]["total_tokens"] == (
        usage["usage"]["prompt_tokens"] + usage["usage"]["completion_tokens"]
    )


async def test_no_usage_chunk_by_default():
    resp = await post(build_mock_app(tokens=["a"]), {})
    assert all(json.loads(f)["choices"] for f in parse(resp.content)[:-1])


async def test_non_streaming():
    resp = await post(build_mock_app(), {}, stream=False)
    body = resp.json()
    assert body["object"] == "chat.completion"
    assert body["model"] == "m-x"
    choice = body["choices"][0]
    assert choice["message"] == {"role": "assistant", "content": "".join(DEFAULT_TOKENS)}
    assert choice["finish_reason"] == "stop"
    assert choice["logprobs"] is None
    assert body["usage"]["total_tokens"] > 0


async def test_default_model():
    resp = await post(build_mock_app(), {"model": None}, stream=False)
    assert resp.json()["model"] == "mock-1"


async def test_fault_error_frame():
    resp = await post(build_mock_app(tokens=list("abcd"), fault="error_frame"), {})
    frames = parse(resp.content)
    assert "[DONE]" not in frames
    err = json.loads(frames[-1])
    assert err["error"]["code"] == "mock_error"
    assert err["error"]["type"] == "server_error"
    assert 0 < len(content_of(frames[:-1])) < 4


async def test_fault_drop():
    resp = await post(build_mock_app(tokens=list("abcd"), fault="drop"), {})
    frames = parse(resp.content)
    assert frames
    assert "[DONE]" not in frames
    assert 0 < len(content_of(frames)) < 4


async def test_fault_crlf():
    resp = await post(build_mock_app(tokens=["a"], fault="crlf"), {})
    assert b"\r\n\r\n" in resp.content
    assert b"\n\n" not in resp.content.replace(b"\r\n", b"")
    assert content_of(parse(resp.content)) == "a"


async def test_fault_cr():
    resp = await post(build_mock_app(tokens=["a"], fault="cr"), {})
    assert b"\n" not in resp.content
    assert b"\r\r" in resp.content
    assert content_of(parse(resp.content)) == "a"


async def test_fault_bom():
    resp = await post(build_mock_app(tokens=["a"], fault="bom"), {})
    assert resp.content.startswith(b"\xef\xbb\xbf")
    assert content_of(parse(resp.content)) == "a"


async def test_events_per_write_batches_without_changing_body():
    app1 = build_mock_app(tokens=list("abcde"))
    app3 = build_mock_app(tokens=list("abcde"), events_per_write=3)
    a = await post(app1, {})
    b = await post(app3, {})
    assert a.content == b.content


async def test_delay_still_completes():
    resp = await post(build_mock_app(tokens=["a", "b"], delay=0.001), {})
    assert content_of(parse(resp.content)) == "ab"


def test_mock_fault_env_var_validated_and_passed(monkeypatch):
    import mock_llm.server as server

    seen = {}
    monkeypatch.setattr(server.uvicorn, "run", lambda app, **kw: seen.update(kw))
    monkeypatch.setattr(server, "build_mock_app", lambda **kw: seen.update(app=kw) or object())
    monkeypatch.setenv("MOCK_FAULT", "crlf")
    server.main()
    assert seen["app"]["fault"] == "crlf"
    monkeypatch.setenv("MOCK_FAULT", "bogus")
    with pytest.raises(SystemExit) as exc:
        server.main()
    assert exc.value.code == 2
    monkeypatch.delenv("MOCK_FAULT")
    server.main()
    assert seen["app"]["fault"] is None
