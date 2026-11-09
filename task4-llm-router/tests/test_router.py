import json
import logging
import random
import time
from collections.abc import AsyncIterator, Awaitable, Callable

import anyio
import httpx2
import pytest

from llm_router.config import load_settings
from llm_router.errors import UpstreamError, UpstreamRateLimited, UpstreamTimeout
from llm_router.router import RouteResult, route

DEADLINE_S = 0.2
PRIMARY_KEY = "pk-primary-secret"
SECONDARY_KEY = "sk-secondary-secret"
BODY = {"model": "client-model", "messages": [{"role": "user", "content": "hi"}]}

Handler = Callable[[httpx2.Request], Awaitable[httpx2.Response]]


def make_settings(timeout_ms: int = int(DEADLINE_S * 1000)):
    return load_settings(
        env={
            "ROUTER_TENANT_KEYS": "tk=tenant",
            "ROUTER_KEY_PEPPER": "p" * 32,
            "PRIMARY_BASE_URL": "http://primary.internal:9001/v1/",
            "PRIMARY_API_KEY": PRIMARY_KEY,
            "PRIMARY_MODEL": "primary-model",
            "SECONDARY_BASE_URL": "http://secondary.internal:9002/v1",
            "SECONDARY_API_KEY": SECONDARY_KEY,
            "SECONDARY_MODEL": "secondary-model",
            "PRIMARY_TIMEOUT_MS": str(timeout_ms),
            "SECONDARY_TIMEOUT_MS": str(timeout_ms),
        }
    )


class Upstreams:
    """One MockTransport that dispatches by host and records every call."""

    def __init__(self, primary: Handler, secondary: Handler) -> None:
        self.primary = primary
        self.secondary = secondary
        self.calls: list[str] = []
        self.requests: list[httpx2.Request] = []

    async def __call__(self, request: httpx2.Request) -> httpx2.Response:
        self.requests.append(request)
        if request.url.host == "primary.internal":
            self.calls.append("primary")
            return await self.primary(request)
        self.calls.append("secondary")
        return await self.secondary(request)

    def client(self, **kwargs) -> httpx2.AsyncClient:
        return httpx2.AsyncClient(transport=httpx2.MockTransport(self), **kwargs)

    @property
    def secondary_calls(self) -> int:
        return self.calls.count("secondary")


def ok(name: str) -> Handler:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json={"served_by": name, "usage": {"total_tokens": 7}})

    return handler


def status(code: int) -> Handler:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(code, json={"error": {"message": f"upstream said {code}"}})

    return handler


def stall(flags: dict) -> Handler:
    """Waits on an Event that is never set; records whether it was cancelled."""

    async def handler(request: httpx2.Request) -> httpx2.Response:
        flags["entered"] = True
        if "entered_event" in flags:
            flags["entered_event"].set()
        try:
            await anyio.Event().wait()
        except anyio.get_cancelled_exc_class():
            flags["cancelled"] = True
            raise
        raise AssertionError("unreachable")

    return handler


def run(upstreams: Upstreams, settings=None, body=BODY, **client_kwargs):
    settings = settings or make_settings()

    async def main():
        async with upstreams.client(**client_kwargs) as client:
            return await route(client, settings, body)

    return anyio.run(main)


def run_timed(upstreams: Upstreams, settings=None):
    t0 = time.perf_counter()
    try:
        return run(upstreams, settings), time.perf_counter() - t0
    except Exception as exc:
        exc.elapsed = time.perf_counter() - t0
        raise


# --- happy path and failover triggers --------------------------------------------------


def test_primary_success_never_calls_secondary():
    up = Upstreams(ok("primary"), ok("secondary"))
    result = run(up)
    assert result == RouteResult(
        provider="primary",
        fallback_reason=None,
        status=200,
        body={"served_by": "primary", "usage": {"total_tokens": 7}},
    )
    assert up.calls == ["primary"]


def test_primary_201_status_is_relayed():
    async def created(request):
        return httpx2.Response(201, json={"x": 1})

    result = run(Upstreams(created, ok("secondary")))
    assert (result.provider, result.status, result.body) == ("primary", 201, {"x": 1})


def test_primary_429_fails_over_with_reason_rate_limited_after_primary_closed():
    state = {"primary_closed": False, "closed_when_secondary_started": None}

    class TrackedStream(httpx2.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b'{"error": "rate limited"}'

        async def aclose(self) -> None:
            state["primary_closed"] = True

    async def primary(request):
        return httpx2.Response(429, headers={"retry-after": "1"}, stream=TrackedStream())

    async def secondary(request):
        state["closed_when_secondary_started"] = state["primary_closed"]
        return await ok("secondary")(request)

    up = Upstreams(primary, secondary)
    result = run(up)
    assert result.provider == "secondary"
    assert result.fallback_reason == "rate_limited"
    assert result.status == 200
    assert result.body["served_by"] == "secondary"
    assert up.calls == ["primary", "secondary"]
    assert state["closed_when_secondary_started"] is True


def test_primary_stall_times_out_at_deadline_and_primary_is_cancelled():
    flags: dict = {}
    up = Upstreams(stall(flags), ok("secondary"))
    result, elapsed = run_timed(up)
    assert result.provider == "secondary"
    assert result.fallback_reason == "timeout"
    assert result.body["served_by"] == "secondary"
    assert flags == {"entered": True, "cancelled": True}
    assert up.calls == ["primary", "secondary"]
    assert DEADLINE_S * 0.9 <= elapsed < 0.6


def test_trickling_body_hits_total_deadline_not_per_read_timeout():
    """Chunks arrive every 0.08 s for 0.5 s: each read is well under the per-read backstop
    (deadline + 0.5 s), so only a total deadline over the whole attempt can stop it."""
    state = {"chunks": 0, "closed": False}

    class Trickle(httpx2.AsyncByteStream):
        async def __aiter__(self) -> AsyncIterator[bytes]:
            yield b'{"served_by": "primary", "pad": "'
            for _ in range(6):
                await anyio.sleep(0.08)
                state["chunks"] += 1
                yield b"x"
            yield b'"}'

        async def aclose(self) -> None:
            state["closed"] = True

    async def primary(request):
        return httpx2.Response(200, stream=Trickle())

    up = Upstreams(primary, ok("secondary"))
    result, elapsed = run_timed(up)
    assert result.provider == "secondary"
    assert result.fallback_reason == "timeout"
    assert 1 <= state["chunks"] < 6
    assert state["closed"] is True
    assert DEADLINE_S * 0.9 <= elapsed < 0.6


def test_timeout_is_logged_with_provider_name_not_url(caplog):
    up = Upstreams(stall({}), ok("secondary"))
    with caplog.at_level(logging.WARNING, logger="llm_router"):
        run(up)
    [record] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert record.getMessage() == "upstream primary timed out after 200 ms (TimeoutError)"
    assert record.exc_info is None


def test_httpx_timeout_exception_counts_as_timeout(caplog):
    async def primary(request):
        raise httpx2.ReadTimeout("read timed out at primary.internal", request=request)

    up = Upstreams(primary, ok("secondary"))
    with caplog.at_level(logging.WARNING, logger="llm_router"):
        result = run(up)
    assert (result.provider, result.fallback_reason) == ("secondary", "timeout")
    [record] = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert record.getMessage() == "upstream primary timed out after 200 ms (ReadTimeout)"
    assert record.exc_info is None


def test_failover_is_not_logged_at_info(caplog):
    """The app logs exactly one INFO line per request; the router must not add another."""
    up = Upstreams(status(429), ok("secondary"))
    with caplog.at_level(logging.DEBUG, logger="llm_router"):
        run(up)
    assert [r for r in caplog.records if r.levelno == logging.INFO] == []
    [debug] = [r for r in caplog.records if r.levelno == logging.DEBUG]
    assert debug.getMessage() == "failing over to secondary reason=rate_limited"


# --- the deadline boundary race --------------------------------------------------------


@pytest.mark.slow
def test_boundary_race_uses_exactly_one_answer_and_never_calls_secondary_after_a_result():
    rng = random.Random(4)
    settings = make_settings()
    outcomes = {"primary": 0, "secondary": 0}

    async def one_run(jitter: float):
        state = {"completed": False}

        async def primary(request):
            await anyio.sleep(DEADLINE_S + jitter)
            state["completed"] = True
            return httpx2.Response(200, json={"served_by": "primary"})

        up = Upstreams(primary, ok("secondary"))
        async with up.client() as client:
            result = await route(client, settings, BODY)
        return result, state["completed"], up

    async def main():
        for _ in range(50):
            jitter = rng.uniform(-0.03, 0.03)
            result, completed, up = await one_run(jitter)
            assert isinstance(result, RouteResult)
            outcomes[result.provider] += 1
            if result.provider == "primary":
                assert completed is True
                assert result.body == {"served_by": "primary"}
                assert result.fallback_reason is None
                assert up.calls == ["primary"]
                assert up.secondary_calls == 0
            else:
                assert completed is False
                assert result.fallback_reason == "timeout"
                assert result.body["served_by"] == "secondary"
                assert up.calls == ["primary", "secondary"]

    anyio.run(main)
    assert outcomes["primary"] + outcomes["secondary"] == 50
    # Jitter straddles the deadline, so both sides of the race must have been exercised.
    assert outcomes["primary"] > 0
    assert outcomes["secondary"] > 0


# --- outer cancellation ----------------------------------------------------------------


def test_outer_cancellation_propagates_and_never_triggers_failover():
    flags: dict = {"entered_event": anyio.Event()}
    up = Upstreams(stall(flags), ok("secondary"))
    settings = make_settings(timeout_ms=5000)
    seen: list[str] = []

    async def main():
        async with up.client() as client:

            async def runner():
                try:
                    await route(client, settings, BODY)
                except anyio.get_cancelled_exc_class():
                    seen.append("cancelled")
                    raise
                seen.append("returned")

            async with anyio.create_task_group() as tg:
                tg.start_soon(runner)
                await flags["entered_event"].wait()
                tg.cancel_scope.cancel()
            return tg.cancel_scope.cancelled_caught

    t0 = time.perf_counter()
    cancelled_caught = anyio.run(main)
    elapsed = time.perf_counter() - t0
    assert seen == ["cancelled"]
    assert cancelled_caught is True
    assert flags["cancelled"] is True
    assert up.secondary_calls == 0
    assert up.calls == ["primary"]
    assert elapsed < 1.0


def test_cancel_landing_with_primary_timeout_never_starts_secondary():
    """Deterministic test of the checkpoint_if_cancelled guard.

    The primary handler cancels the outer scope and raises ReadTimeout with no await in
    between, so the router sees a timeout while a cancellation is pending but not yet
    delivered. Without the guard the secondary (whose handler never awaits) would be called
    and its answer returned; with it, cancellation propagates and the secondary is untouched.
    """
    holder: dict = {}

    async def primary(request):
        holder["scope"].cancel()
        raise httpx2.ReadTimeout("read timed out", request=request)

    up = Upstreams(primary, ok("secondary"))
    settings = make_settings(timeout_ms=5000)
    seen: list[str] = []

    async def main():
        async with up.client() as client:
            with anyio.CancelScope() as scope:
                holder["scope"] = scope
                try:
                    result = await route(client, settings, BODY)
                except anyio.get_cancelled_exc_class():
                    seen.append("cancelled")
                    raise
                seen.append(f"returned {result.provider}")
            return scope.cancelled_caught

    assert anyio.run(main) is True
    assert seen == ["cancelled"]
    assert up.calls == ["primary"]
    assert up.secondary_calls == 0


def test_outer_cancellation_during_secondary_propagates():
    pflags: dict = {}
    sflags: dict = {"entered_event": anyio.Event()}
    up = Upstreams(stall(pflags), stall(sflags))
    settings = make_settings()
    seen: list[str] = []

    async def main():
        async with up.client() as client:
            with anyio.CancelScope() as scope:
                async with anyio.create_task_group() as tg:

                    async def canceller():
                        await sflags["entered_event"].wait()
                        scope.cancel()

                    tg.start_soon(canceller)
                    try:
                        await route(client, settings, BODY)
                    except anyio.get_cancelled_exc_class():
                        seen.append("cancelled")
                        raise
            return scope.cancelled_caught

    assert anyio.run(main) is True
    assert seen == ["cancelled"]
    assert sflags["cancelled"] is True
    assert up.calls == ["primary", "secondary"]


# --- error mapping ---------------------------------------------------------------------


def test_both_429_raises_upstream_rate_limited():
    up = Upstreams(status(429), status(429))
    with pytest.raises(UpstreamRateLimited) as info:
        run(up)
    assert info.value.status == 429
    assert info.value.retry_after == 1
    assert info.value.message == UpstreamRateLimited.message
    assert up.calls == ["primary", "secondary"]


def test_primary_429_then_secondary_timeout_raises_upstream_timeout():
    sflags: dict = {}
    up = Upstreams(status(429), stall(sflags))
    t0 = time.perf_counter()
    with pytest.raises(UpstreamTimeout) as info:
        run(up)
    elapsed = time.perf_counter() - t0
    assert info.value.status == 504
    assert sflags["cancelled"] is True
    assert DEADLINE_S * 0.9 <= elapsed < 0.6


def test_both_timeout_raises_upstream_timeout_within_two_deadlines():
    up = Upstreams(stall({}), stall({}))
    t0 = time.perf_counter()
    with pytest.raises(UpstreamTimeout):
        run(up)
    elapsed = time.perf_counter() - t0
    assert up.calls == ["primary", "secondary"]
    assert 2 * DEADLINE_S * 0.9 <= elapsed < 0.9


def test_primary_timeout_then_secondary_429_raises_upstream_rate_limited():
    up = Upstreams(stall({}), status(429))
    with pytest.raises(UpstreamRateLimited):
        run(up)
    assert up.calls == ["primary", "secondary"]


@pytest.mark.parametrize("code", [400, 401, 404, 500, 503])
def test_primary_non_429_error_raises_upstream_error_without_failover(code):
    up = Upstreams(status(code), ok("secondary"))
    with pytest.raises(UpstreamError) as info:
        run(up)
    assert info.value.status == 502
    assert info.value.message == UpstreamError.message
    assert up.calls == ["primary"]


@pytest.mark.parametrize("code", [400, 500, 502])
def test_secondary_error_after_failover_raises_upstream_error(code):
    up = Upstreams(status(429), status(code))
    with pytest.raises(UpstreamError):
        run(up)
    assert up.calls == ["primary", "secondary"]


@pytest.mark.parametrize(
    "content",
    [b"not json at all", b"[1, 2, 3]", b'"a string"', b"null", b"\xff\xfe"],
    ids=["invalid", "list", "string", "null", "bad-bytes"],
)
def test_primary_2xx_with_non_object_or_invalid_json_raises_upstream_error(content):
    async def primary(request):
        return httpx2.Response(200, content=content)

    up = Upstreams(primary, ok("secondary"))
    with pytest.raises(UpstreamError):
        run(up)
    assert up.calls == ["primary"]


def test_secondary_malformed_json_after_failover_raises_upstream_error():
    async def secondary(request):
        return httpx2.Response(200, content=b"{oops")

    up = Upstreams(status(429), secondary)
    with pytest.raises(UpstreamError):
        run(up)


def test_connect_error_with_internal_hostname_is_sanitized_and_logged(caplog):
    planted = "connect to db-01.corp.internal:5432 refused (secret-host)"

    async def primary(request):
        raise httpx2.ConnectError(planted, request=request)

    up = Upstreams(primary, ok("secondary"))
    with caplog.at_level(logging.WARNING, logger="llm_router"):
        with pytest.raises(UpstreamError) as info:
            run(up)
    err = info.value
    assert up.calls == ["primary"]
    assert err.message == "The upstream provider returned an error."
    assert str(err) == ""
    assert err.__cause__ is None
    assert err.__suppress_context__ is True
    assert "secret-host" not in err.message
    records = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(records) == 1
    assert "primary" in records[0].getMessage()
    assert "primary.internal" not in records[0].getMessage()
    assert records[0].exc_info is not None
    assert isinstance(records[0].exc_info[1], httpx2.ConnectError)


def test_secondary_connect_error_after_failover_raises_upstream_error():
    async def secondary(request):
        raise httpx2.ConnectError("refused by secondary.internal", request=request)

    up = Upstreams(status(429), secondary)
    with pytest.raises(UpstreamError) as info:
        run(up)
    assert info.value.message == UpstreamError.message


def test_upstream_error_body_never_reaches_the_raised_error():
    async def primary(request):
        return httpx2.Response(
            500, json={"error": "Traceback (most recent call last): sk-leaked-upstream-key"}
        )

    with pytest.raises(UpstreamError) as info:
        run(Upstreams(primary, ok("secondary")))
    rendered = repr(info.value) + str(info.value) + info.value.message
    assert "Traceback" not in rendered
    assert "sk-leaked" not in rendered


# --- forwarding ------------------------------------------------------------------------


def test_forwarded_request_url_body_and_headers_for_primary():
    up = Upstreams(ok("primary"), ok("secondary"))
    body = {**BODY, "temperature": 0.3, "max_tokens": 50}
    original = json.loads(json.dumps(body))
    run(
        up,
        body=body,
        headers={"Authorization": "Bearer client-key", "Cookie": "session=abc"},
        cookies={"jar": "cookie-value"},
    )
    [req] = up.requests
    assert req.method == "POST"
    assert str(req.url) == "http://primary.internal:9001/v1/chat/completions"
    assert json.loads(req.content) == {**original, "model": "primary-model"}
    assert body == original, "client body must not be mutated"
    assert req.headers["authorization"] == f"Bearer {PRIMARY_KEY}"
    assert req.headers.get_list("authorization") == [f"Bearer {PRIMARY_KEY}"]
    assert req.headers["content-type"] == "application/json"
    assert "cookie" not in req.headers
    assert "client-key" not in str(dict(req.headers))
    assert set(req.headers.keys()) == {"host", "authorization", "content-type", "content-length"}
    timeout = req.extensions["timeout"]
    assert timeout["read"] == pytest.approx(DEADLINE_S + 0.5)


def test_forwarded_request_to_secondary_uses_secondary_model_and_key():
    up = Upstreams(status(429), ok("secondary"))
    run(up)
    primary_req, secondary_req = up.requests
    assert json.loads(primary_req.content)["model"] == "primary-model"
    assert str(secondary_req.url) == "http://secondary.internal:9002/v1/chat/completions"
    assert json.loads(secondary_req.content) == {**BODY, "model": "secondary-model"}
    assert secondary_req.headers["authorization"] == f"Bearer {SECONDARY_KEY}"
    assert PRIMARY_KEY not in str(dict(secondary_req.headers))


# --- a 2xx body that carries an upstream error ------------------------------------------


def error_in_200() -> Handler:
    async def handler(request: httpx2.Request) -> httpx2.Response:
        return httpx2.Response(200, json={"error": {"message": "Traceback sk-LEAK"}})

    return handler


def test_primary_2xx_error_without_choices_raises_upstream_error_without_failover():
    up = Upstreams(error_in_200(), ok("secondary"))
    with pytest.raises(UpstreamError) as info:
        run(up)
    assert info.value.status == 502
    assert info.value.message == UpstreamError.message
    assert up.calls == ["primary"]


def test_secondary_2xx_error_without_choices_after_failover_raises_upstream_error():
    up = Upstreams(status(429), error_in_200())
    with pytest.raises(UpstreamError) as info:
        run(up)
    assert info.value.status == 502
    assert up.calls == ["primary", "secondary"]


def test_2xx_body_with_both_error_and_choices_is_a_success():
    async def both(request):
        return httpx2.Response(200, json={"error": {"message": "warn"}, "choices": []})

    result = run(Upstreams(both, ok("secondary")))
    assert (result.provider, result.status) == ("primary", 200)
    assert result.body["choices"] == []
