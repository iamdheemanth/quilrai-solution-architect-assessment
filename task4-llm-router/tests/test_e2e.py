"""Real uvicorn servers (router + two mock providers) driven by the official OpenAI SDK."""

from __future__ import annotations

import contextlib
import os
import socket
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

from llm_router.app import create_app
from llm_router.config import load_settings
from mock_providers.server import LEAK_CANARIES, build_provider_app

pytestmark = pytest.mark.anyio

TENANT_KEY = "tk-e2e-tenant-key-0001"
PRIMARY_KEY = "pk-e2e-primary-secret"
SECONDARY_KEY = "sk-e2e-secondary-secret"
MESSAGES = [{"role": "user", "content": "hello"}]
DEADLINE_MS = 300


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
        uvicorn.Config(
            app,
            host="127.0.0.1",
            port=port,
            log_level="warning",
            lifespan="on",
            server_header=False,  # as in __main__.main()
        )
    )
    thread = threading.Thread(target=server.run, daemon=True)
    started.append((server, thread))
    thread.start()
    deadline = time.monotonic() + 10
    while not server.started:
        if time.monotonic() > deadline or not thread.is_alive():
            raise RuntimeError("uvicorn server did not start")
        time.sleep(0.02)
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


def router_settings(db_path: str, primary_port: int, secondary_port: int, **extra: str):
    env = {
        "ROUTER_TENANT_KEYS": f"{TENANT_KEY}=e2e",
        "ROUTER_KEY_PEPPER": "e2e-pepper-0123456789abcdef",
        "ROUTER_DB_PATH": db_path,
        "PRIMARY_BASE_URL": f"http://127.0.0.1:{primary_port}/v1",
        "PRIMARY_API_KEY": PRIMARY_KEY,
        "PRIMARY_MODEL": "primary-model",
        "SECONDARY_BASE_URL": f"http://127.0.0.1:{secondary_port}/v1",
        "SECONDARY_API_KEY": SECONDARY_KEY,
        "SECONDARY_MODEL": "secondary-model",
        "PRIMARY_TIMEOUT_MS": str(DEADLINE_MS),
        "SECONDARY_TIMEOUT_MS": str(DEADLINE_MS),
    }
    env.update(extra)
    return load_settings(env)


@contextlib.contextmanager
def stack(tmp_path, primary: dict, secondary: dict, **env: str) -> Iterator[int]:
    """Two mock providers + the router (with its own lifespan); yields the router port."""
    started: list = []
    try:
        p = start(build_provider_app(model="primary-model", **primary), started)
        s = start(build_provider_app(model="secondary-model", **secondary), started)
        settings = router_settings(str(tmp_path / "router.db"), p, s, **env)
        yield start(create_app(settings), started)
    finally:
        stop_all(started)


@contextlib.asynccontextmanager
async def sdk(port: int):
    client = AsyncOpenAI(
        base_url=f"http://127.0.0.1:{port}/v1",
        api_key=TENANT_KEY,
        http_client=httpx2.AsyncClient(),
        max_retries=0,
    )
    try:
        yield client
    finally:
        await client.close()


async def create_raw(client: AsyncOpenAI, **kwargs):
    return await client.chat.completions.with_raw_response.create(
        model="client-model", messages=MESSAGES, **kwargs
    )


async def test_success_via_primary(tmp_path):
    with stack(tmp_path, {"behavior": "ok"}, {"behavior": "ok"}) as port:
        async with sdk(port) as client:
            raw = await create_raw(client)
    completion = raw.parse()
    assert completion.choices[0].message.content == "Hello from primary-model"
    assert completion.usage.total_tokens == 50
    assert raw.headers["x-gateway-provider"] == "primary"
    assert "x-gateway-fallback-reason" not in raw.headers
    assert raw.headers["x-ratelimit-limit-tokens"] == "50000"
    assert raw.headers["x-ratelimit-reset-tokens"] == "60s"
    assert len(raw.headers["x-request-id"]) == 32
    assert "server" not in raw.headers


async def test_primary_429_is_served_by_secondary(tmp_path):
    with stack(tmp_path, {"behavior": "rate_limit"}, {"behavior": "ok"}) as port:
        async with sdk(port) as client:
            raw = await create_raw(client)
    assert raw.parse().choices[0].message.content == "Hello from secondary-model"
    assert raw.headers["x-gateway-provider"] == "secondary"
    assert raw.headers["x-gateway-fallback-reason"] == "rate_limited"


async def test_slow_primary_times_out_and_secondary_serves(tmp_path):
    with stack(tmp_path, {"behavior": "slow", "delay_s": 1.0}, {"behavior": "ok"}) as port:
        async with sdk(port) as client:
            t0 = time.perf_counter()
            raw = await create_raw(client)
            elapsed = time.perf_counter() - t0
    assert raw.parse().choices[0].message.content == "Hello from secondary-model"
    assert raw.headers["x-gateway-provider"] == "secondary"
    assert raw.headers["x-gateway-fallback-reason"] == "timeout"
    assert DEADLINE_MS / 1000 * 0.9 <= elapsed < 0.9


async def test_trickling_primary_hits_total_deadline(tmp_path):
    primary = {"behavior": "trickle", "delay_s": 0.05}  # ~20 chunks: well past 300 ms total
    with stack(tmp_path, primary, {"behavior": "ok"}) as port:
        async with sdk(port) as client:
            raw = await create_raw(client)
    assert raw.headers["x-gateway-fallback-reason"] == "timeout"


async def test_both_429_raises_rate_limit_error_with_upstream_code(tmp_path):
    with stack(tmp_path, {"behavior": "rate_limit"}, {"behavior": "rate_limit"}) as port:
        async with sdk(port) as client:
            with pytest.raises(openai.RateLimitError) as info:
                await client.chat.completions.create(model="m", messages=MESSAGES)
    exc = info.value
    assert exc.status_code == 429
    assert exc.code == "upstream_rate_limited"
    assert exc.body["code"] == "upstream_rate_limited"
    assert exc.body["request_id"] == exc.response.headers["x-request-id"]
    assert exc.response.headers["retry-after"] == "1"


async def test_gateway_rate_limit_raises_rate_limit_error_with_retry_after(tmp_path):
    # usage_tokens=1000 is reconciled after the first call, so a second estimate (~1006)
    # cannot fit in a 1500-token window.
    with stack(
        tmp_path,
        {"behavior": "ok", "usage_tokens": 1000},
        {"behavior": "ok"},
        ROUTER_TOKENS_PER_MINUTE="1500",
    ) as port:
        async with sdk(port) as client:
            first = await client.chat.completions.create(
                model="m", messages=MESSAGES, max_tokens=1000
            )
            with pytest.raises(openai.RateLimitError) as info:
                await client.chat.completions.create(model="m", messages=MESSAGES, max_tokens=1000)
    assert first.usage.total_tokens == 1000
    exc = info.value
    assert exc.code == "gateway_rate_limited"
    assert 59 <= int(exc.response.headers["retry-after"]) <= 60
    assert "x-gateway-provider" not in exc.response.headers


@pytest.mark.parametrize(
    ("primary", "secondary", "exc_type", "code"),
    [
        ({"behavior": "error500", "leak": True}, {"behavior": "ok"}, openai.InternalServerError,
         "upstream_error"),
        ({"behavior": "rate_limit", "leak": True}, {"behavior": "rate_limit", "leak": True},
         openai.RateLimitError, "upstream_rate_limited"),
        ({"behavior": "rate_limit", "leak": True}, {"behavior": "error500", "leak": True},
         openai.InternalServerError, "upstream_error"),
    ],
    ids=["primary-500", "both-429", "429-then-500"],
)  # fmt: skip
async def test_leaky_upstream_never_reaches_the_client(
    tmp_path, primary, secondary, exc_type, code
):
    with stack(tmp_path, primary, secondary) as port:
        async with sdk(port) as client:
            with pytest.raises(exc_type) as info:
                await client.chat.completions.create(model="m", messages=MESSAGES)
    exc = info.value
    assert exc.code == code
    assert set(exc.body) == {"message", "type", "param", "code", "request_id"}
    surfaces = [
        str(exc.body),
        exc.message,
        str(exc),
        exc.response.text,
        *exc.response.headers.values(),
    ]
    for canary in (*LEAK_CANARIES, PRIMARY_KEY, SECONDARY_KEY, "127.0.0.1"):
        for surface in surfaces:
            assert canary not in surface, canary


async def test_wrong_tenant_key_is_authentication_error(tmp_path):
    with stack(tmp_path, {"behavior": "ok"}, {"behavior": "ok"}) as port:
        client = AsyncOpenAI(
            base_url=f"http://127.0.0.1:{port}/v1",
            api_key="not-a-tenant",
            http_client=httpx2.AsyncClient(),
            max_retries=0,
        )
        try:
            with pytest.raises(openai.AuthenticationError) as info:
                await client.chat.completions.create(model="m", messages=MESSAGES)
        finally:
            await client.close()
    assert info.value.code == "invalid_api_key"


def minimal_env(**extra: str) -> dict[str, str]:
    keep = ("PATH", "SYSTEMROOT", "TEMP", "TMP")
    return {k: os.environ[k] for k in keep if k in os.environ} | extra


def test_main_bad_config_exits_2_without_echoing_secrets():
    proc = subprocess.run(
        [sys.executable, "-m", "llm_router"],
        env=minimal_env(ROUTER_TENANT_KEYS=f"{TENANT_KEY}=t", ROUTER_KEY_PEPPER="tiny-pep"),
        capture_output=True,
        timeout=30,
    )
    assert proc.returncode == 2
    assert proc.stdout == b""
    assert proc.stderr.replace(b"\r\n", b"\n") == (
        b"llm-router: configuration error: ROUTER_KEY_PEPPER must be at least 16 bytes\n"
    )
    assert TENANT_KEY.encode() not in proc.stderr
    assert b"tiny-pep" not in proc.stderr
