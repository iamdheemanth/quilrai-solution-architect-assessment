# Task 4: token-aware rate-limiting and failover LLM router

## 1. What it is

An OpenAI-compatible HTTP router. It accepts `POST /v1/chat/completions`, enforces a **token-aware sliding-window rate limit per tenant API key** (default 50,000 tokens per minute) stored in an **on-disk SQLite** file, and forwards the request to a primary model provider. If the primary answers **429 or does not finish within 3000 ms**, it fails over to a secondary provider. Every error leaves the router as one standardized payload that never carries upstream stack traces, URLs, hostnames, keys or file paths.

> Concurrent requests from one tenant never admit more than the budget. A primary answer that exists at the deadline is always the one used, and the secondary is never called after it. Failover charges the tenant once, and a request that ends in an error refunds its reservation.

It is built with Starlette, uvicorn, httpx2 and anyio. The project is self-contained (own `pyproject.toml` and lock file). Two scripted mock providers are included, so everything runs locally. Non-streaming only: `stream: true` is rejected with 400.

How it maps to the three things this task is graded on:

| Graded criterion | Where to look | Code | Tests |
|---|---|---|---|
| Async concurrency and timeout race conditions | [Section 5](#5-failover-design) (total deadline, the boundary race, disconnects, charging once); [Section 4](#4-rate-limiter-design) (`BEGIN IMMEDIATE`, concurrency proof) | `router.py`, `app.py`, `limiter.py` | `test_router.py::test_boundary_race_uses_exactly_one_answer_and_never_calls_secondary_after_a_result`, `test_trickling_body_hits_total_deadline_not_per_read_timeout`, `test_outer_cancellation_propagates_and_never_triggers_failover`, `test_cancel_landing_with_primary_timeout_never_starts_secondary`; `test_limiter.py::test_threads_32x20_reserves_of_700_admit_exactly_71`, `test_processes_admit_exactly_floor_limit_over_est`, `test_async_tasks_admit_exactly_floor_limit_over_est`; `test_app.py::test_disconnect_during_stalled_primary_stops_failover_and_refunds`; `test_e2e.py::test_real_socket_close_during_stalled_primary_refunds_without_failover` |
| Accurate state eviction and token tracking | [Section 4](#4-rate-limiter-design) (sliding log, eviction, reserve/reconcile/refund, window boundary) | `limiter.py`, `tokens.py` | `test_limiter.py::test_window_boundary_59999_rejected_60000_admitted`, `test_window_slides_rather_than_fixed_reset`, `test_eviction_deletes_old_rows_for_that_tenant_only`, `test_reconcile_down_frees_tokens`, `test_refund_frees_tokens`, `test_retry_after_is_time_until_enough_old_rows_age_out`, `test_clock_is_read_only_inside_the_write_transaction`; `test_tokens.py`; `test_app.py::test_failover_on_primary_429_is_charged_once_at_secondary_usage` |
| Graceful fallback and error sanitization | [Section 5](#5-failover-design) (what fails over and what does not); [Section 6](#6-error-contract-and-sanitization) | `router.py`, `errors.py`, `app.py` | `test_app.py::test_no_canary_in_any_response`, `test_429_upstream_rate_limited_when_both_429_and_refunds`, `test_504_upstream_timeout_and_refunds`, `test_502_upstream_error_and_refunds`; `test_router.py::test_connect_error_with_internal_hostname_is_sanitized_and_logged`, `test_upstream_error_body_never_reaches_the_raised_error`; `test_e2e.py::test_leaky_upstream_never_reaches_the_client`; `test_errors.py` |

```
                      +------------------------- llm-router :8004 --------------------------+
                      |  authenticate: HMAC(pepper, key) -> tenant id   (401 invalid_api_key)|
 client -- POST ----> |  read body (1 MiB cap), parse, reject stream:true (413 / 400)        |
 /v1/chat/completions |  estimate = ceil(len(messages JSON)/4) + max_tokens                  |
                      |  limiter.reserve  --- BEGIN IMMEDIATE, evict, sum, insert (SQLite)   |
                      |       over budget -> 429 gateway_rate_limited + Retry-After          |
                      |                                                                      |
                      |  route (a watcher cancels it if the client disconnects)              |
                      |    primary  --- one 3000 ms deadline over connect..full body ---+    |
                      |       2xx -> use it                                             |    |
                      |       429 or timeout -> secondary (same deadline, same ledger)  |    |
                      |       other error -> 502, no failover                           |    |
                      |  settle: reconcile to usage.total_tokens, or refund on any error    |
 <-- JSON + headers --|  one INFO log line per request; detail only in the server log       |
                      +----------------------------------------------------------------------+
                                  |                                   |
                         primary provider (/v1)            secondary provider (/v1)
```

## 2. Quick start

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/). All commands run from `task4-llm-router/`.

```bash
uv sync --locked
uv run pytest -q -W error     # 289 tests, about 25 s
```

### bash

```bash
# terminal 1: primary mock, always answers 429
MOCK_PROVIDER_BEHAVIOR=rate_limit MOCK_PROVIDER_PORT=9001 uv run mock-provider

# terminal 2: secondary mock, answers normally
MOCK_PROVIDER_BEHAVIOR=ok MOCK_PROVIDER_PORT=9002 uv run mock-provider

# terminal 3: the router (the keys below are placeholders)
export ROUTER_TENANT_KEYS="demo-key-not-real=acme"
export ROUTER_KEY_PEPPER="demo-pepper-not-a-secret-0123"
export PRIMARY_BASE_URL="http://127.0.0.1:9001/v1"   PRIMARY_API_KEY="fake-primary-key"     PRIMARY_MODEL="primary-model"
export SECONDARY_BASE_URL="http://127.0.0.1:9002/v1" SECONDARY_API_KEY="fake-secondary-key" SECONDARY_MODEL="secondary-model"
uv run llm-router

# terminal 4
curl -si http://127.0.0.1:8004/v1/chat/completions \
  -H "Authorization: Bearer demo-key-not-real" -H "Content-Type: application/json" \
  -d '{"model":"any","messages":[{"role":"user","content":"hello"}]}'
```

### PowerShell

```powershell
# terminals 1 and 2
$env:MOCK_PROVIDER_BEHAVIOR = "rate_limit"; $env:MOCK_PROVIDER_PORT = "9001"; uv run mock-provider
$env:MOCK_PROVIDER_BEHAVIOR = "ok";         $env:MOCK_PROVIDER_PORT = "9002"; uv run mock-provider

# terminal 3
$env:ROUTER_TENANT_KEYS = "demo-key-not-real=acme"
$env:ROUTER_KEY_PEPPER = "demo-pepper-not-a-secret-0123"
$env:PRIMARY_BASE_URL = "http://127.0.0.1:9001/v1";   $env:PRIMARY_API_KEY = "fake-primary-key";     $env:PRIMARY_MODEL = "primary-model"
$env:SECONDARY_BASE_URL = "http://127.0.0.1:9002/v1"; $env:SECONDARY_API_KEY = "fake-secondary-key"; $env:SECONDARY_MODEL = "secondary-model"
uv run llm-router

# terminal 4 (curl.exe, not the Invoke-WebRequest alias)
$body = '{"model":"any","messages":[{"role":"user","content":"hello"}]}'
$body | curl.exe -si http://127.0.0.1:8004/v1/chat/completions `
  -H "Authorization: Bearer demo-key-not-real" -H "Content-Type: application/json" --data-binary "@-"
```

The mock reads `MOCK_PROVIDER_BEHAVIOR` (`ok`, `rate_limit`, `slow`, `error500`, `trickle`), `MOCK_PROVIDER_PORT` (default 9001), `MOCK_PROVIDER_DELAY` (seconds, for `slow` and `trickle`) and `MOCK_PROVIDER_HOST`. Anything invalid exits 2.

### Pointing at real providers

Base URLs **must include the `/v1`** (or whatever prefix the provider uses): the router appends `/chat/completions` and nothing else.

```bash
export PRIMARY_BASE_URL="https://api.openai.com/v1"
export PRIMARY_API_KEY="sk-your-primary-key"
export PRIMARY_MODEL="gpt-4o-mini"
```

The official OpenAI SDK works unmodified against the router (`OpenAI(base_url="http://127.0.0.1:8004/v1", api_key="demo-key-not-real")`). The client's key selects the tenant and is never forwarded; each provider gets only its own key.

### Demo output

Real output from the setup above (primary mock in `rate_limit` mode, secondary `ok`, `ROUTER_TOKENS_PER_MINUTE=3000` so the limit is easy to hit, a temp database file). Request ids differ per run. The demo ran on an ephemeral port (49809), which is why the URLs below differ from the quick start's port 8004.

**A successful fallback.** The primary returned 429, the secondary served. The body is the mock's reply, relayed unchanged; the headers are the router's.

```
$ curl -si http://127.0.0.1:49809/v1/chat/completions -H "Authorization: Bearer demo-key-not-real" \
    -H "Content-Type: application/json" \
    -d '{"model":"any","max_tokens":100,"messages":[{"role":"user","content":"hello"}]}'
HTTP/1.1 200 OK
x-request-id: fb245d5040fe4ace935515ee40876395
x-gateway-provider: secondary
x-ratelimit-limit-tokens: 3000
x-ratelimit-remaining-tokens: 2890
x-ratelimit-reset-tokens: 60s
x-gateway-fallback-reason: rate_limited
content-length: 281
content-type: application/json

{"id":"chatcmpl-mock","object":"chat.completion","created":1700000000,"model":"mock-x","choices":[{"index":0,"message":{"role":"assistant","content":"Hello from mock-x"},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":10,"completion_tokens":40,"total_tokens":50}}
```

Remaining is 2890 because the reservation was the estimate (110 tokens: 10 for the 38-character serialized messages, rounded up from 38/4, plus `max_tokens` 100) at the moment of admission. The log line below shows it being reconciled to the 50 tokens the provider reported, so later requests see 2950 left.

**An over-limit 429 with `Retry-After`.** Two requests had been served (50 tokens each after reconciliation), then a request whose estimate (2960) no longer fits in the 3000-token window:

```
$ curl -si ... -d '{"model":"any","max_tokens":2950,"messages":[{"role":"user","content":"hello"}]}'
HTTP/1.1 429 Too Many Requests
retry-after: 56
x-request-id: 58802cefb27441a79b627ed1901296aa
content-length: 226
content-type: application/json

{"error":{"message":"Rate limit exceeded for this API key. Retry after the time in the Retry-After header.","type":"rate_limit_error","param":null,"code":"gateway_rate_limited","request_id":"58802cefb27441a79b627ed1901296aa"}}
```

`Retry-After` is the whole seconds until enough old rows age out of the window for this request to fit (here the second 50-token row is the one that has to age out: freeing only the first still leaves 3010 > 3000, and that row was a few seconds old, so 56 of the 60). It is not a fixed guess.

**Sanitized errors.** A wrong key, a `stream: true` request and a wrong method:

```
$ curl -si ... -H "Authorization: Bearer wrong" -d '{"messages":[]}'
HTTP/1.1 401 Unauthorized
x-request-id: b713ae4aa4a84a4999d50e5aa17fdc86
{"error":{"message":"Missing or invalid API key.","type":"authentication_error","param":null,"code":"invalid_api_key","request_id":"b713ae4aa4a84a4999d50e5aa17fdc86"}}

$ curl -si ... -d '{"stream":true,"messages":[]}'
HTTP/1.1 400 Bad Request
x-request-id: 2aa7e38c3efa4b9485716b281695cd74
{"error":{"message":"Streaming is not supported by this gateway","type":"invalid_request_error","param":"stream","code":"invalid_request","request_id":"2aa7e38c3efa4b9485716b281695cd74"}}

$ curl -si -X GET http://127.0.0.1:49809/v1/chat/completions
HTTP/1.1 405 Method Not Allowed
allow: POST
x-request-id: b599fccae60f49458ea3c87fd1da6f12
{"error":{"message":"Method not allowed.","type":"invalid_request_error","param":null,"code":"method_not_allowed","request_id":"b599fccae60f49458ea3c87fd1da6f12"}}
```

The router log for those requests (stderr; one INFO line per request, the tenant shown as an 8-character prefix of its HMAC, never the key or any prompt text):

```
INFO llm_router [fb245d50...]: request request_id=fb245d5040fe4ace935515ee40876395 tenant=f7e36ce7 status=200 outcome=ok provider=secondary fallback_reason=rate_limited est_tokens=110 actual_tokens=50 duration_ms=17
INFO llm_router [58802cef...]: request request_id=58802cefb27441a79b627ed1901296aa tenant=f7e36ce7 status=429 outcome=gateway_rate_limited provider=- fallback_reason=- est_tokens=2960 actual_tokens=- duration_ms=1
INFO llm_router [b713ae4a...]: request request_id=b713ae4aa4a84a4999d50e5aa17fdc86 tenant=- status=401 outcome=invalid_api_key provider=- fallback_reason=- est_tokens=- actual_tokens=- duration_ms=0
```

(The request id inside the brackets is the full id in the real log; it is shortened here only to fit.)

## 3. Configuration

Read once at startup from the environment. A missing or invalid value prints one line to stderr naming the variable (never its value) and exits with status 2.

| Variable | Default | Meaning |
|---|---|---|
| `ROUTER_TENANT_KEYS` | required | `key=tenant,key=tenant`. Keys and tenants non-empty, no duplicate keys. Never logged or shown in `repr` or errors |
| `ROUTER_KEY_PEPPER` | required | Secret for the HMAC that turns an API key into a tenant id. At least 16 bytes |
| `ROUTER_TOKENS_PER_MINUTE` | `50000` | Token budget per tenant per window, integer >= 1 |
| `ROUTER_WINDOW_SECONDS` | `60` | Window length, integer >= 1 |
| `ROUTER_DEFAULT_MAX_TOKENS` | `1024` | Completion budget assumed (and injected into the forwarded body) when the client sends neither `max_tokens` nor `max_completion_tokens` |
| `ROUTER_DB_PATH` | `./router.db` | SQLite file. Created on first use, WAL mode (so `-wal` and `-shm` files appear beside it) |
| `PRIMARY_BASE_URL` | required | `http` or `https` URL with a host. **Must include `/v1`**; `/chat/completions` is appended |
| `PRIMARY_API_KEY` | required | Sent as `Authorization: Bearer ...` to the primary only |
| `PRIMARY_MODEL` | required | Replaces the client's `model` field in the forwarded body |
| `SECONDARY_BASE_URL`, `SECONDARY_API_KEY`, `SECONDARY_MODEL` | required | Same, for the secondary |
| `PRIMARY_TIMEOUT_MS` | `3000` | Total deadline for the primary attempt (the brief's 3000 ms) |
| `SECONDARY_TIMEOUT_MS` | `3000` | Total deadline for the secondary attempt. Both exist so tests can use small deadlines |
| `ROUTER_HOST` | `127.0.0.1` | Listen address |
| `ROUTER_PORT` | `8004` | Listen port, 1 to 65535 |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` or `CRITICAL`, case-insensitive |

The client's key may arrive as `Authorization: Bearer <key>`, `api-key: <key>` or `x-api-key: <key>`, in that order of precedence. It is compared against every configured key without short-circuiting, using `hmac.compare_digest`.

The upstream client is one shared `httpx2.AsyncClient` with `follow_redirects=False` and its default headers cleared. Each upstream request is built by hand, so the upstream sees exactly `Authorization` and `Content-Type`: the client's auth, cookie and other headers are never forwarded.

## 4. Rate limiter design

### Sliding log, not bucket counters

Every admitted request is one row: `usage(id, tenant, ts_ms, tokens, request_id UNIQUE)` with an index on `(tenant, ts_ms)`. The window is `(now - W, now]`, so a row exactly `W` old is already out. A fixed-window or bucket counter approximates this and is wrong in both directions: it lets a tenant spend a full budget at the end of one bucket and again at the start of the next, and it keeps charging tokens that have long since aged out. The log is exact, and the table stays small because a tenant's expired rows are deleted on its next reserve.

### Reserve, reconcile, refund

Real token usage is only known after the provider answers, but admission has to happen before. So:

1. **Reserve** (before the call). Estimate `ceil(len(json.dumps(messages)) / 4) + max_tokens` (falling back to `max_completion_tokens`, then `ROUTER_DEFAULT_MAX_TOKENS`, which is also injected into the forwarded body so the completion cannot exceed what was reserved). Insert a row with that estimate if it fits.
2. **Reconcile** (after a 2xx). Update the row to the provider's `usage.total_tokens`. If the provider omits or garbles `usage`, the estimate stays: it never under-charges.
3. **Refund** (any other exit: every gateway error, an unexpected exception, a client disconnect). Update the row to 0 tokens. The row stays but weighs nothing.

Reconcile and refund run shielded from cancellation, so a client that disconnects can never leave the ledger wrong. If the settle itself fails (SQLite error), it is logged and the estimate stays charged, which errs toward over-charging, not under-charging.

**Honest limit:** chars/4 is a rough estimate, not a tokenizer. It only has to be a reasonable reservation, because it is replaced by the provider's real count as soon as the answer arrives. A tiktoken estimator is listed under extensions.

**Another honest limit:** a refund on a client disconnect or a timeout means the tenant is not charged for an upstream attempt that may still have generated (and been billed by the provider). The reservation only holds capacity while the request is in flight.

### Oversized requests

A request whose estimate alone exceeds the whole budget could never be admitted, however long the client waited. A `Retry-After` for it would be a lie, so it gets a 400 `request_exceeds_rate_limit` with no `Retry-After` instead of a 429. No row is written.

### The reserve transaction

One `BEGIN IMMEDIATE` transaction does everything:

1. take the write lock (`BEGIN IMMEDIATE`),
2. read the clock,
3. `DELETE` this tenant's rows with `ts_ms <= now - W`,
4. `SUM` the rest,
5. admit iff `sum + estimate <= limit`, and `INSERT` the row, then `COMMIT`.

On a rejection the transaction is still committed, so the eviction is kept; no row is written. The connection runs in WAL mode with `busy_timeout=5000` and `autocommit=True` (so the `BEGIN` is explicit and nothing opens a transaction behind our back). The async facade runs the synchronous core in a worker thread through a capacity-1 limiter, so the event loop never blocks on SQLite.

**The clock is read inside the write transaction, after `BEGIN IMMEDIATE`.** A request that waited on the lock (or on `busy_timeout`) is evicted and timestamped at the moment it is actually admitted, not at the moment it arrived. Otherwise a stale `now` could keep a row that has already expired, or timestamp an admission earlier than a row committed just before it. `test_clock_is_read_only_inside_the_write_transaction` and `test_waiter_is_timestamped_at_admission_not_at_arrival` pin this.

### Concurrency proof

With a 50,000-token budget and 700-token reservations, exactly `floor(50000 / 700) = 71` can be admitted. Three tests hammer one tenant and each assert the admitted count is exactly 71 (the thread and process tests also assert the table holds exactly 71 rows totalling 49,700 tokens). The thread test gives every thread its own connection to the same file:

| Test | Load | Admitted |
|---|---|---|
| `test_threads_32x20_reserves_of_700_admit_exactly_71` | 32 threads x 20 reserves | 71 |
| `test_processes_admit_exactly_floor_limit_over_est` | 6 separately spawned processes sharing the file | 71 |
| `test_async_tasks_admit_exactly_floor_limit_over_est` | 200 async tasks through `AsyncLimiter` | 71 (this one proves the async facade; it serializes calls itself, so the database-level proof is the two rows above) |

These tests can fail. When the transaction was removed during development (reads and writes autocommitted separately), the thread test admitted 90 and the process test admitted 72, so they do detect over-admission. One more honest detail: a plain deferred `BEGIN` also passed these tests here. That is because the first statement of the transaction is the `DELETE`, which itself takes the write lock before the `SUM` is read. `BEGIN IMMEDIATE` makes that lock explicit instead of depending on statement order, which is the point of using it. I am not claiming it is measurably stronger on this workload.

### The window boundary

`test_window_boundary_59999_rejected_60000_admitted`: a request at +59,999 ms after a full-budget row is rejected, and at +60,000 ms it is admitted, because eviction uses `ts_ms <= now - W` on integer milliseconds (the clock is rounded, not truncated, so float seconds cannot land a millisecond short). The clock is injectable, so this is tested without sleeping.

### Durability

The file uses WAL with `synchronous=NORMAL`. On a power loss or OS crash, the last few commits can be lost; the database cannot be corrupted. For a rate limiter that is the right trade: losing a few seconds of recent charges briefly under-counts, never over-blocks, and the window is a minute. A process crash (the common case) loses nothing. If every charge had to survive power loss, use `synchronous=FULL` at the cost of an fsync per commit.

### Keys are never stored

The tenant column holds `HMAC-SHA256(pepper, key)` in hex. The raw API key never reaches the limiter, the database, or the logs (`test_raw_key_never_in_db_file`). The log shows only the first 8 hex characters. The pepper means a stolen database cannot be used to test guesses of a key offline without it.

## 5. Failover design

### What "3000 ms" means

Each provider attempt runs under **one total deadline** covering connect, send, response headers **and the full body read**, via `anyio.fail_after(3.0)`. It is not three separate timeouts and not a per-read timeout. For a non-streaming call, "times out after 3000 ms" can only sensibly mean the whole call, so that is what is enforced.

httpx2's own timeouts are not enough, because they are per phase: a `read` timeout restarts on every chunk, so a provider that trickles a body a few bytes at a time never trips it and the call can run for minutes. `test_trickling_body_hits_total_deadline_not_per_read_timeout` covers exactly this. httpx2 timeouts are still set, to `deadline + 0.5 s`, as a backstop that never fires first. Both `TimeoutError` (the total deadline) and `httpx2.TimeoutException` (a backstop) count as a timeout.

The worst case is therefore two deadlines back to back (about 6 s with defaults), and `test_both_timeout_raises_upstream_timeout_within_two_deadlines` asserts it.

### What fails over, and what does not

| Primary outcome | Result |
|---|---|
| 2xx | served by the primary |
| 429 | secondary, `x-gateway-fallback-reason: rate_limited` |
| timeout | secondary, `x-gateway-fallback-reason: timeout` |
| any other 4xx or 5xx, connection error, malformed JSON | 502 `upstream_error`, **no failover** (the brief names only 429 and timeout) |

A 2xx whose JSON body holds a truthy `error` and no `choices` is not a success: it is treated like any other upstream error (502 `upstream_error`, refunded, no failover), and its text is never relayed. A body that has `choices` is relayed as usual.

If the secondary also fails, **the secondary's outcome alone decides** the result, whatever the primary did: a 429 gives 429 `upstream_rate_limited` (`Retry-After: 1`), a timeout gives 504 `upstream_timeout`, anything else gives 502 `upstream_error`. So primary timeout then secondary 429 is a 429, primary timeout then secondary 5xx is a 502, and primary 429 or timeout then secondary timeout is a 504. The primary's response is fully read and closed before the secondary starts.

### The race conditions and how each is handled

**A primary answer landing right at the deadline.** The decision is "was a result produced?", not "did the deadline pass?". The attempt's result is assigned as the **last statement inside the deadline scope**, with no checkpoint after it. anyio's `fail_after` raises `TimeoutError` only if its cancel scope actually caught a cancellation, and nothing can be cancelled after the result is assigned. So a primary result that exists always wins, and the secondary is never called after it. If the deadline fires first, nothing was assigned and the attempt is a timeout. Exactly one provider's answer is ever used (`test_boundary_race_uses_exactly_one_answer_and_never_calls_secondary_after_a_result`).

**Cancellation from outside is never failover.** Only the timeout exceptions are caught. A cancellation from outside (the client leaving) is not a timeout, so it propagates. When the primary fails, the router also calls `checkpoint_if_cancelled()` before starting the secondary, so a cancel that lands at the same moment as a primary timeout does not start a second upstream call (`test_cancel_landing_with_primary_timeout_never_starts_secondary`, `test_outer_cancellation_during_secondary_propagates`).

**Client disconnect.** Starlette delivers no cancellation when the client goes away, so without help a client that left during a stalled primary would still trigger failover and be charged for it. A watcher task runs alongside `route`, reads the ASGI `http.disconnect` message, and cancels it. A client that leaves gets no failover and its reservation is refunded. The watcher only starts after the body is fully read, so it can never consume a body message. A result that was already produced is kept even if the disconnect lands at the same moment. The event appears in the log as `outcome=client_disconnected` with status 499; the client is gone, so the 499 response goes to a closed socket and is never delivered. Tests: `test_disconnect_during_stalled_primary_stops_failover_and_refunds`, `test_watcher_leaves_a_normal_request_untouched`, and, with a real socket, `test_real_socket_close_during_stalled_primary_refunds_without_failover`.

**Values that cannot be forwarded.** `json.loads` accepts `NaN`, `1e999` (which overflows to infinity) and lone surrogates, none of which can be re-encoded as strict JSON. A client body containing any of them gets a 400 before anything is reserved. The same value in an upstream 2xx body becomes a 502 with a refund and **no failover** (the upstream answered; the answer is the problem).

### Charging once

One reservation is made per gateway request. Failover reuses it, so the tenant is charged once, for the tokens the answering provider reports (`test_failover_on_primary_429_is_charged_once_at_secondary_usage`). Both providers failing refunds it (`test_429_upstream_rate_limited_when_both_429_and_refunds`, `test_504_upstream_timeout_and_refunds`).

## 6. Error contract and sanitization

Every error, from every code path, is exactly this shape, with an `x-request-id` header equal to `request_id`:

```json
{"error": {"message": "...", "type": "...", "param": null, "code": "...", "request_id": "..."}}
```

| Situation | Status | `code` | Notes |
|---|---|---|---|
| Missing or invalid key | 401 | `invalid_api_key` | Checked before the body is read |
| Bad JSON or shape, `stream: true`, or a value that cannot be forwarded | 400 | `invalid_request` | `param` names the field where there is one |
| Body over 1 MiB | 413 | `payload_too_large` | Enforced in the endpoint, on `Content-Length` and while reading chunks |
| Unknown path | 404 | `not_found` | JSON, not a plain-text page |
| Wrong method | 405 | `method_not_allowed` | JSON, with `Allow: POST` |
| Estimate larger than the whole budget | 400 | `request_exceeds_rate_limit` | No `Retry-After` |
| Tenant over its limit | 429 | `gateway_rate_limited` | `Retry-After`: whole seconds until the request would fit, at least 1 |
| Secondary rate limited after failover | 429 | `upstream_rate_limited` | `Retry-After: 1`. The secondary decides, so primary timeout then secondary 429 is also a 429 |
| Secondary timed out after failover | 504 | `upstream_timeout` | Primary 429 or timeout, then secondary timeout |
| Upstream error, connection error, malformed or unrepresentable body, a 2xx body carrying an `error` and no `choices`, or secondary 5xx after failover | 502 | `upstream_error` | A primary non-429 error does not fail over |
| SQLite failure | 503 | `rate_limiter_unavailable` | Fails closed: no upstream call |
| Anything else | 500 | `internal_error` | |
| Client disconnected | 499 | `client_disconnected` | Log only; the response goes to a closed socket |

### Sanitization approach

- Messages are **fixed strings per error class**. They are never built from `str(exc)`, an upstream body, a URL, a hostname, a key or a path. `GatewayError` deliberately does not pass its message to `Exception`, so `str(err)` is empty.
- Upstream responses are consumed inside the router and only the parsed JSON of a 2xx is relayed. Upstream error bodies, status text and headers are discarded; the client sees one of the fixed codes above.
- Full exception detail (tracebacks, connect-error text with hostnames, SQLite messages with paths) goes to the **server log only**, at WARNING with the traceback, tied to the `request_id`. Prompt content and keys are never logged.
- Unknown paths and methods, oversized bodies and unhandled exceptions all pass through the same error constructor, so nothing escapes as Starlette's default plain-text body.

`test_no_canary_in_any_response` plants a fake traceback, an `sk-` key, an internal hostname and a filesystem path in upstream error bodies and a connect error, then asserts none of those strings appear in any response body or header. `test_leaky_upstream_never_reaches_the_client` does the same end to end with the official SDK, and `test_sqlite_error_becomes_rate_limiter_unavailable_without_path` covers the database path.

## 7. Response headers

On every response: `x-request-id`.

On success, in addition:

| Header | Meaning |
|---|---|
| `x-gateway-provider` | `primary` or `secondary`: who actually answered |
| `x-gateway-fallback-reason` | `rate_limited` or `timeout`; present only when the secondary served |
| `x-ratelimit-limit-tokens` | The tenant's budget for the window |
| `x-ratelimit-remaining-tokens` | Budget left right after this admission, using the reservation estimate |
| `x-ratelimit-reset-tokens` | Whole seconds until the tenant's oldest row ages out, e.g. `60s` |

`x-ratelimit-remaining-tokens` is computed at admission, before reconciliation, so it is conservative: the next request sees any difference. On a 429 from the gateway, `Retry-After` carries the wait. Upstream response headers are never relayed.

## 8. Extensions (not built)

- **Streaming** with commit-at-first-chunk: once the first token is sent the response is committed to that provider, so failover would only be possible before it. The reservation would be reconciled when the stream ends.
- **Failover on 5xx and connection errors.** Not done because the brief names only 429 and timeout, but it is a small change in `route` (and the same retry policy question for POST idempotency).
- **Cooldown or circuit breaker.** After repeated primary failures, skip the primary for a while instead of paying a 3 s timeout on every request.
- **Hedged requests.** Start the secondary after a shorter delay and use whichever answers first, trading cost for tail latency.
- **A tiktoken (or provider) estimator** instead of chars/4, to tighten reservations.
- **`Retry-After`-aware scheduling.** Honour the primary's own `Retry-After` as a cooldown rather than only reacting per request.
- **Redis for multi-host.** The limiter is SQLite on one host: one file, one write lock. Several router instances on several machines need a shared store; the `reserve`/`reconcile`/`refund` interface maps onto a Lua script, and the sliding log onto a sorted set.
- **IETF `RateLimit` headers** as an option alongside the `x-ratelimit-*` ones.

## 9. Test evidence

```bash
uv sync --locked && uv run ruff check . && uv run ruff format --check . && uv run pytest -q -W error
```

```
294 passed in 30.31s
```

294 tests, all collected and all passing, with warnings treated as errors. By file:

| File | Tests | Covers |
|---|---|---|
| `test_limiter.py` | 25 | exact admission at the limit, window boundary (+59,999 / +60,000 ms), sliding rather than fixed reset, eviction scoped to the tenant, reconcile and refund, `Retry-After` computation, restart persistence, raw key absent from the file, clock read inside the transaction, rollback on error, the 32x20 thread, 6-process and 200-task concurrency proofs, shielded refund, sanitized SQLite errors |
| `test_router.py` | 38 | success, 429 failover, total deadline and trickling body, httpx2 timeout counted, the boundary race, outer cancellation (during primary, during secondary, landing with a timeout), every both-fail combination, no failover on a primary 5xx, malformed 2xx bodies and 2xx error bodies, sanitized connect errors, exact forwarded request |
| `test_app.py` | 86 | headers on success, single charge on failover, status and code for every row of the error table, 404, 405 and 413, 401 before body parsing, unforwardable bodies (400 and 502), disconnect (including during the body read) and watcher behaviour, a 2xx error body as a sanitized 502, no canary in any response, exactly one INFO line, lifespan ownership, logging setup |
| `test_e2e.py` | 12 | real sockets and the official OpenAI SDK: success, failover on 429 and on a slow or trickling primary, both 429, gateway 429 with `Retry-After`, leaky upstream, wrong key, real socket close mid-request, bad config exits 2 |
| `test_tokens.py` | 12 | estimate formula, `max_tokens` precedence, default injection, explicit zero, invalid values |
| `test_auth.py` | 24 | HMAC tenant id, header styles and precedence, invalid and non-ASCII keys, compares every key |
| `test_config.py` | 66 | defaults, overrides, required variables, bounds, pepper length in bytes, errors never contain secrets, `repr` hides secrets, frozen settings |
| `test_errors.py` | 17 | exact payload shape, `x-request-id`, `Retry-After` floor, unhandled exception becomes a sanitized 500 |
| `test_mock_providers.py` | 14 | every mock behaviour, leak mode canaries, bad environment exits 2 |
