"""Contract of the mock provider used by the end-to-end tests."""

from __future__ import annotations

import os
import subprocess
import sys
import time

import httpx2
import pytest

from mock_providers.server import LEAK_CANARIES, build_provider_app

pytestmark = pytest.mark.anyio

BODY = {"model": "client-model", "messages": [{"role": "user", "content": "hi"}]}


@pytest.fixture
def anyio_backend():
    return "asyncio"


async def call(app, body=BODY) -> tuple[httpx2.Response, float]:
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=app), base_url="http://mock"
    ) as client:
        t0 = time.perf_counter()
        resp = await client.post("/v1/chat/completions", json=body)
        return resp, time.perf_counter() - t0


async def test_ok_returns_a_chat_completion_with_usage():
    resp, _ = await call(build_provider_app(behavior="ok", model="mock-p", usage_tokens=42))
    assert resp.status_code == 200
    body = resp.json()
    assert body["object"] == "chat.completion"
    assert body["model"] == "mock-p"
    assert body["choices"][0]["message"] == {"role": "assistant", "content": "Hello from mock-p"}
    assert body["choices"][0]["finish_reason"] == "stop"
    assert body["usage"]["total_tokens"] == 42


async def test_rate_limit_returns_429_json_error():
    resp, _ = await call(build_provider_app(behavior="rate_limit"))
    assert resp.status_code == 429
    assert resp.json()["error"]["code"] == "rate_limit_exceeded"
    assert not any(c in resp.text for c in LEAK_CANARIES)


@pytest.mark.parametrize("behavior", ["rate_limit", "error500"])
async def test_leak_mode_plants_every_canary(behavior):
    resp, _ = await call(build_provider_app(behavior=behavior, leak=True))
    assert resp.status_code == (429 if behavior == "rate_limit" else 500)
    for canary in LEAK_CANARIES:
        assert canary in resp.text


async def test_error500_returns_500_json():
    resp, _ = await call(build_provider_app(behavior="error500"))
    assert resp.status_code == 500
    assert resp.json()["error"]["type"] == "server_error"


async def test_slow_waits_delay_before_answering():
    resp, elapsed = await call(build_provider_app(behavior="slow", delay_s=0.3))
    assert resp.status_code == 200
    assert resp.json()["usage"]["total_tokens"] == 50
    assert elapsed >= 0.28


async def test_trickle_streams_a_valid_body_slowly():
    resp, elapsed = await call(build_provider_app(behavior="trickle", delay_s=0.05))
    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/json"
    assert resp.json()["object"] == "chat.completion"
    assert elapsed >= 0.2


async def test_rejects_non_object_body():
    async with httpx2.AsyncClient(
        transport=httpx2.ASGITransport(app=build_provider_app(behavior="ok")),
        base_url="http://mock",
    ) as client:
        resp = await client.post("/v1/chat/completions", content=b"[1]")
    assert resp.status_code == 400


def test_build_rejects_unknown_behavior():
    with pytest.raises(ValueError):
        build_provider_app(behavior="nope")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "env",
    [
        {"MOCK_PROVIDER_BEHAVIOR": "nope"},
        {"MOCK_PROVIDER_PORT": "70000"},
        {"MOCK_PROVIDER_PORT": "abc"},
        {"MOCK_PROVIDER_DELAY": "-1"},
        {"MOCK_PROVIDER_DELAY": "inf"},
    ],
)
def test_main_bad_env_exits_2(env):
    keep = ("PATH", "SYSTEMROOT", "TEMP", "TMP")
    proc = subprocess.run(
        [sys.executable, "-m", "mock_providers.server"],
        env={k: os.environ[k] for k in keep if k in os.environ} | env,
        capture_output=True,
        timeout=30,
    )
    assert proc.returncode == 2
    assert next(iter(env)).encode() in proc.stderr
