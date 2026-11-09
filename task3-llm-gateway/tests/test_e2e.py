"""Layer 4: real uvicorn servers (mock + gateway) driven by the official OpenAI SDK."""

from __future__ import annotations

import contextlib
import os
import re
import socket
import statistics
import subprocess
import sys
import threading
import time
from collections.abc import Iterator

import httpx2
import openai
import pytest
import uvicorn
from openai import AsyncOpenAI

from llm_gateway.app import create_app
from llm_gateway.config import load_settings
from llm_gateway.patterns import batch_redact
from mock_llm.server import DEFAULT_TOKENS, build_mock_app

pytestmark = pytest.mark.anyio

MESSAGES = [{"role": "user", "content": "hello"}]
EXPECTED = batch_redact("".join(DEFAULT_TOKENS))
RAW_PII = ("john.doe@example.com", "jane@example.org", "123-45-6789", "4111 1111 1111 1111")


@pytest.fixture
def anyio_backend():
    return "asyncio"


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def start(app, started: list) -> int:
    """Run ``app`` under uvicorn in a thread; register it in ``started`` before waiting."""
    port = free_port()
    server = uvicorn.Server(
        uvicorn.Config(app, host="127.0.0.1", port=port, log_level="warning", lifespan="on")
    )
    thread = threading.Thread(target=server.run, daemon=True)
    started.append((server, thread))
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise RuntimeError("uvicorn server did not start")
        time.sleep(0.05)
    return port


def stop_all(started: list) -> None:
    for server, _ in started:
        server.should_exit = True
    hung = []
    for _server, thread in reversed(started):
        thread.join(timeout=10)
        if thread.is_alive():
            hung.append(thread.name)
    assert not hung, f"uvicorn thread(s) still alive after shutdown: {hung}"


def gateway_app(mock_port: int):
    return create_app(
        load_settings(
            {"UPSTREAM_BASE_URL": f"http://127.0.0.1:{mock_port}/v1", "UPSTREAM_API_KEY": "sk-gw"}
        )
    )


@contextlib.contextmanager
def stack_for(**mock_kwargs) -> Iterator[tuple[int, int]]:
    """Mock (with ``mock_kwargs``) + gateway; yields ``(mock_port, gateway_port)``."""
    started: list = []
    try:
        mock_port = start(build_mock_app(**mock_kwargs), started)
        gw_port = start(gateway_app(mock_port), started)
        yield mock_port, gw_port
    finally:
        stop_all(started)


@pytest.fixture(scope="module")
def stack() -> Iterator[tuple[int, int]]:
    with stack_for(delay=0.0) as ports:
        yield ports


@contextlib.asynccontextmanager
async def sdk(port: int):
    client = AsyncOpenAI(
        base_url=f"http://127.0.0.1:{port}/v1",
        api_key="sk-client",
        http_client=httpx2.AsyncClient(),
        max_retries=0,
    )
    try:
        yield client
    finally:
        await client.close()


async def stream_text(client: AsyncOpenAI) -> str:
    parts = []
    stream = await client.chat.completions.create(model="m", messages=MESSAGES, stream=True)
    async for ev in stream:
        for ch in ev.choices:
            if ch.delta.content:
                parts.append(ch.delta.content)
    return "".join(parts)


async def test_streaming_content_is_redacted(stack):
    async with sdk(stack[1]) as client:
        text = await stream_text(client)
    assert text == EXPECTED
    assert "[REDACTED]" in text
    for literal in RAW_PII:
        assert literal not in text


async def test_non_streaming_matches_streaming(stack):
    async with sdk(stack[1]) as client:
        resp = await client.chat.completions.create(model="m", messages=MESSAGES)
        streamed = await stream_text(client)
    content = resp.choices[0].message.content
    assert content == EXPECTED
    assert content == streamed
    for literal in RAW_PII:
        assert literal not in content


@pytest.mark.parametrize("fault", ["error_frame", "drop"])
async def test_upstream_fault_raises_api_error(fault):
    with stack_for(delay=0.0, fault=fault) as (_mock, gw):
        async with sdk(gw) as client:
            seen = []
            with pytest.raises(openai.APIError):
                stream = await client.chat.completions.create(
                    model="m", messages=MESSAGES, stream=True
                )
                async for ev in stream:
                    seen.extend(c.delta.content or "" for c in ev.choices)
    for literal in RAW_PII:
        assert literal not in "".join(seen)


async def ttft(client: AsyncOpenAI) -> float:
    t0 = time.perf_counter()
    stream = await client.chat.completions.create(model="m", messages=MESSAGES, stream=True)
    first = None
    async for ev in stream:
        if first is None and any(c.delta.content for c in ev.choices):
            first = time.perf_counter() - t0
    assert first is not None
    return first


async def test_ttft_through_gateway_close_to_direct():
    with stack_for(tokens=["Hi ", "there ", "friend ", "ok"], delay=0.02) as (mock, gw):
        async with sdk(mock) as direct, sdk(gw) as via:
            await ttft(direct)  # warm both connection pools
            await ttft(via)
            d = statistics.median([await ttft(direct) for _ in range(7)])
            g = statistics.median([await ttft(via) for _ in range(7)])
    assert g <= d + 0.050, f"gateway TTFT {g * 1000:.1f} ms vs direct {d * 1000:.1f} ms"


def minimal_env(**extra: str) -> dict[str, str]:
    keep = ("PATH", "SYSTEMROOT", "TEMP", "TMP")
    return {k: os.environ[k] for k in keep if k in os.environ} | extra


def test_main_bad_config_exits_2():
    proc = subprocess.run(
        [sys.executable, "-m", "llm_gateway"],
        env=minimal_env(UPSTREAM_BASE_URL="ftp://x"),
        capture_output=True,
        timeout=30,
    )
    assert proc.returncode == 2
    assert b"UPSTREAM_BASE_URL" in proc.stderr
    assert re.search(rb"llm-gateway: configuration error: ", proc.stderr)
    assert proc.stdout == b""
