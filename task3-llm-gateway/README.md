# Task 3: streaming PII-redaction LLM gateway

## 1. What it is

An HTTP gateway for OpenAI-compatible Chat Completions. It forwards `POST /v1/chat/completions` to an upstream provider and relays the reply to the client while replacing emails, US SSNs and credit card numbers with `[REDACTED]`. For streamed replies it does this **incrementally**: every character that can no longer become part of a match is sent immediately, and only the short suffix that might still grow into one is held back. Nothing buffers the full response.

> For any text and any way it is split into deltas (empty deltas included), the redacted stream concatenates to exactly what one-shot redaction of the whole text produces. A PII match is never split across chunks, and a match still open at the end of the stream is redacted before `finish_reason` and `[DONE]`.

It is built with Starlette, uvicorn and httpx2 plus the `regex` module (for its partial-match API). The project is self-contained (own `pyproject.toml` and lock file, nothing imported from other tasks). A scripted mock provider that splits PII across token boundaries is included, so everything runs locally.

How it maps to the three things this task is graded on:

| Graded criterion | Where to look | Code | Tests |
|---|---|---|---|
| Async stream chunking and buffer state management | [Section 5](#5-the-streaming-algorithm) (per-channel redactor state), [Section 6](#6-proxy-design) (byte-level SSE parser, per-choice transformer, terminal rules) | `redactor.py`, `sse.py`, `transform.py`, `proxy.py` | `test_redactor.py`, `test_sse.py`, `test_transform.py`, `test_streaming.py` |
| Performant regex over partial text streams | [Section 4](#4-what-is-redacted) (patterns), [Section 5](#5-the-streaming-algorithm) (viable-prefix holdback, correctness argument, bounds) | `patterns.py`, `redactor.py` | `test_patterns.py` (ReDoS bounds), `test_redactor.py` (stream equals batch property, exhaustive splits, mutation checks) |
| Memory efficiency and low-latency proxy design | [Sections 6 and 7](#6-proxy-design): two bounded buffers, no response accumulation, immediate release, backpressure, benchmark | `proxy.py`, `app.py`, `bench/bench.py` | `test_streaming.py` (incremental delivery, bounded memory, cleanup), `test_e2e.py` (TTFT with the official SDK) |

```
                        +------------------------- llm-gateway :8003 --------------------------+
                        |                                                                      |
 client  -- POST -----> | forward original body bytes; replace Authorization with gateway key  | --> upstream provider
 /v1/chat/completions   |                                                                      |     (OpenAI-compatible,
                        |  reply is text/event-stream?                                         |      default mock :8002)
                        |   yes: SSEParser (bytes) -> StreamTransformer                        |
        <-- SSE ------- |        one StreamRedactor per (choice, channel):                     | <-- SSE / JSON
   redacted, flushed    |        release what cannot start a match, hold the rest             |
   per upstream read    |        close + flush tails before finish_reason / [DONE]             |
                        |   no:  JSON body (capped) -> redact every string leaf -> reply       |
                        |  any failure mid-stream -> error frame, held text dropped, no [DONE] |
                        |  one log line per request: metadata and counts, never content        |
                        +----------------------------------------------------------------------+
```

## 2. Quick start

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/). All commands run from `task3-llm-gateway/`.

```bash
uv sync --locked
uv run pytest -q          # 262 tests, about 60 to 80 s (one slow property test dominates)
```

### bash

```bash
# terminal 1: scripted mock provider on 127.0.0.1:8002
uv run mock-llm

# terminal 2: gateway on 127.0.0.1:8003 (defaults point at the mock)
uv run llm-gateway

# terminal 3: streaming request
curl -N http://127.0.0.1:8003/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{"model":"mock-1","stream":true,"messages":[{"role":"user","content":"hi"}]}'
```

### PowerShell

```powershell
# terminal 1
uv run mock-llm

# terminal 2
uv run llm-gateway

# terminal 3 (curl.exe, not the Invoke-WebRequest alias)
$body = '{"model":"mock-1","stream":true,"messages":[{"role":"user","content":"hi"}]}'
$body | curl.exe -N http://127.0.0.1:8003/v1/chat/completions `
  -H "Content-Type: application/json" --data-binary "@-"
```

### Pointing at a real provider

The gateway holds the provider key; the key a client sends is never forwarded.

```bash
export UPSTREAM_BASE_URL="https://api.openai.com/v1"
export UPSTREAM_API_KEY="sk-your-provider-key"
uv run llm-gateway
```

```powershell
$env:UPSTREAM_BASE_URL = "https://api.openai.com/v1"
$env:UPSTREAM_API_KEY = "sk-your-provider-key"
uv run llm-gateway
```

### Demo output

The mock streams `Sure. Contact john.d` `oe@exa` `mple.com` ` or call about SSN 123-4` `5-6789` `; card 4111 11` `11 1111 1111` ` is on file. Reach me at ` `ja` `ne@exa` `mple.org`, so every PII value is split across deltas and the answer ends on an email with no trailing text. Through the gateway (JSON envelope trimmed to the `content` delta; real output):

```
$ curl -N http://127.0.0.1:8003/v1/chat/completions -H "Content-Type: application/json" \
    -d '{"model":"mock-1","stream":true,"messages":[{"role":"user","content":"hi"}]}'
data: {... "delta":{"role":"assistant","content":""} ...}
data: {... "delta":{"content":"Sure. Contact "} ...}
data: {... "delta":{"content":""} ...}
data: {... "delta":{"content":""} ...}
data: {... "delta":{"content":"[REDACTED] or call about SSN "} ...}
data: {... "delta":{"content":""} ...}
data: {... "delta":{"content":"[REDACTED]; card "} ...}
data: {... "delta":{"content":""} ...}
data: {... "delta":{"content":"[REDACTED] is on file. Reach me at "} ...}
data: {... "delta":{"content":""} ...}
data: {... "delta":{"content":""} ...}
data: {... "delta":{"content":""} ...}
data: {... "delta":{"content":"[REDACTED]"} ...}
data: {... "delta":{},"finish_reason":"stop"}
data: [DONE]
```

Held deltas go out as `"content": ""` (no frame is dropped), and the final `[REDACTED]` is a synthetic chunk emitted before the `finish_reason` chunk.

Non-streaming (same mock, no `stream` flag; real output):

```
$ curl -s http://127.0.0.1:8003/v1/chat/completions -H "Content-Type: application/json" \
    -d '{"model":"mock-1","messages":[{"role":"user","content":"hi"}]}'
{"id":"chatcmpl-mock","object":"chat.completion","created":1700000000,"model":"mock-1","system_fingerprint":"fp_mock","choices":[{"index":0,"message":{"role":"assistant","content":"Sure. Contact [REDACTED] or call about SSN [REDACTED]; card [REDACTED] is on file. Reach me at [REDACTED]"},"logprobs":null,"finish_reason":"stop"}],"usage":{"prompt_tokens":5,"completion_tokens":11,"total_tokens":16}}
```

The official OpenAI SDK works unmodified (`pip install openai`):

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:8003/v1", api_key="sk-your-provider-key")
stream = client.chat.completions.create(
    model="mock-1", stream=True, messages=[{"role": "user", "content": "hi"}]
)
print("".join(c.choices[0].delta.content or "" for c in stream if c.choices))
# Sure. Contact [REDACTED] or call about SSN [REDACTED]; card [REDACTED] is on file. Reach me at [REDACTED]
```

The log line the gateway wrote for the streaming request (stderr; counts by type, never content):

```
INFO llm_gateway: request method=POST route=chat_completions upstream_status=200 outcome=ok error=- redactions={"CC": 1, "EMAIL": 2, "SSN": 1} max_held=20 duration_ms=15.0
```

The mock also reads `MOCK_HOST`, `MOCK_PORT` (default 8002) and `MOCK_DELAY` (seconds between tokens, default 0), and `MOCK_FAULT` (one of `error_frame`, `drop`, `crlf`, `cr`, `bom`; anything else exits 2) for fault injection. The request body options `n` and `stream_options` are honoured; see `src/mock_llm/server.py`.

## 3. Configuration

Read once at startup from the environment. Invalid values print one line to stderr and exit with status 2. There are no other switches.

| Variable | Default | Meaning |
|---|---|---|
| `UPSTREAM_BASE_URL` | `http://127.0.0.1:8002/v1` | Provider base URL (`http` or `https`, host required); `/chat/completions` is appended |
| `UPSTREAM_API_KEY` | empty | Sent as `Authorization: Bearer ...`. If empty, no `Authorization` header is sent. Never logged or shown in `repr` or errors |
| `UPSTREAM_READ_TIMEOUT` | `120` | Seconds, float > 0. Applies per socket read, so it bounds the gap between tokens, not the stream length |
| `GATEWAY_HOST` | `127.0.0.1` | Listen address |
| `GATEWAY_PORT` | `8003` | Listen port, 1 to 65535 |
| `GATEWAY_MAX_BODY_BYTES` | `4194304` | Request body cap (413 above it) and cap on a non-streaming upstream JSON reply |
| `LOG_LEVEL` | `INFO` | `debug`, `info`, `warning` or `error`, case-insensitive |

Fixed upstream client settings: one shared `httpx2.AsyncClient`, `follow_redirects=False`, timeouts `connect=5, read=UPSTREAM_READ_TIMEOUT, write=30, pool=5`, default headers cleared.

The gateway has no client authentication: anyone who can reach it spends the gateway-held key. Do not bind beyond loopback without an auth layer in front (client auth is listed under Extensions).

## 4. What is redacted

### Patterns

All three are combined into one regex, in this order, so matches never overlap (`patterns.py`). Character classes are ASCII (`[0-9]`, never `\d` or `\w`) and every quantifier is bounded, which rules out ReDoS and fixes the holdback bounds.

| Type | Matches | Validator (a rejected match is emitted verbatim) |
|---|---|---|
| EMAIL | `local@label.label...tld`: local part up to 64 chars of `[A-Za-z0-9._%+-]`, 1 to 8 labels of up to 63 chars, alphabetic TLD of 2 to 63; may not be followed by a label character or `.label` | none |
| SSN | hyphenated `ddd-dd-dddd` only, not adjacent to other digits | area not `000`, `666` or `900`-`999`; group not `00`; serial not `0000` |
| CC | 13 to 19 digits as a plain run, `4-4-4-1..7` groups with `-` or space, or Amex `4-6-5` | separators stripped, 13 to 19 digits, Luhn check passes |

Replacement is exactly `[REDACTED]`. `a@b.com.` becomes `[REDACTED].` (a trailing period is not part of the email).

### Channels

Per choice, each text channel has its own redactor, so `n > 1` choices never splice into each other:

- `delta.content`, `delta.refusal` and the reasoning fields `delta.reasoning_content` and `delta.reasoning`;
- each `delta.tool_calls[k].function.arguments` (keyed by `k`) and legacy `delta.function_call.arguments`. These carry JSON-escaped text, so they use a variant of the patterns with an escape guard (below).
- `choices[].logprobs` is set to `null` whenever it is non-null. Logprobs repeat every sampled token, so passing them through would hand the client the raw PII next to the redacted `content`.

`role`, `finish_reason`, `usage`, the envelope (`id`, `object`, `created`, `model`, `system_fingerprint`, `service_tier`) and `obfuscation` pass through untouched. Every **other** string in a streamed frame (unknown delta fields such as `delta.audio.transcript`, `message` objects, top-level extras) is batch-redacted per frame; a frame with no such PII stays byte-identical. Non-streaming and any other JSON replies are redacted by content, at any status: `choices` get the same per-channel treatment, and every other string value anywhere in the document is redacted too.

**Tool-argument escape guard.** In raw JSON-escaped text, `\n` followed by `john@x.com` must not let the match swallow the `n` and corrupt the escape. The args patterns therefore start with `(?:(?<![\\])|(?<=[\\])(?![nrtbfu/"\\]))`: a match may start right after a backslash only if its first character is not a JSON escape letter. This keeps the argument valid JSON while still redacting PII that follows an escaped backslash (for example `C:\\Users\\123-45-6789.txt`).

### Not redacted, by design

- SSNs with area `000`, `666` or `9xx` (which includes ITINs), group `00` or serial `0000`.
- Unhyphenated 9-digit runs (ZIP+4 codes, order numbers and phone fragments collide with them).
- Digit runs that are Luhn-invalid.
- In tool arguments: PII hidden behind a JSON `\uXXXX` escape (for example `\u0040` for `@`), and the one character that the escape guard cannot separate in cases such as `C:\\new@x.com` (the `n` after the backslash pair leaks).
- PII in JSON object **keys** of error bodies (only string values are redacted).
- Anything outside email, SSN and card patterns: names, addresses, phone numbers, API keys, free-text identifiers. Obfuscated forms (spelled out, fullwidth digits) are also out of scope.
- PII split across several frames inside an unknown (non-channel) field: those strings are redacted per frame, not across frames, so a value cut by a frame boundary there is not caught.
- Request bodies: this redacts what comes back, not what the client sends.

## 5. The streaming algorithm

### Why naive chunking leaks

Redacting each delta independently misses `a@b.co` split as `a@b` + `.c` + `o`. The obvious fix with the `regex` module, searching with `partial=True` and flushing up to the partial match, also leaks, because `regex` prefers a *complete* match over a *partial* one even when the complete one was only cut short by the buffer end. Property probe over 6,000 random inputs and chunkings (compared with one-shot redaction of the whole text):

| Strategy | Diverged from batch (leaked PII) | Notes |
|---|---|---|
| Per-chunk redaction | 2,361 of 6,000 | |
| Flush up to the first partial match | 416 of 6,000 | |
| Viable-prefix holdback (this gateway) | **0 of 6,000** | mean 2.2 chars held on prose, max 16; adversarial input held at most 64 chars |

A fixed window (LiteLLM's `stream_holdback_chars` approach) is also correct but held 642 characters in the same probe. These numbers came from the design probes; the repository's equivalent is the hypothesis property `test_property_stream_equals_batch` and the exhaustive-split tests.

### Viable-prefix holdback

`StreamRedactor` (`redactor.py`) keeps one buffer: `K = 1` character of already-emitted context (needed by the SSN/CC left boundary) plus the pending text. After each delta it drains:

1. `m = P.search(buf, c)`: leftmost **complete** match.
2. `q = Q.search(buf, c, partial=True)`: leftmost **partial** match, where `Q = (?:P)(?!)` can never complete, so a partial search reports only true partials (text that could still grow into a match).
3. Everything before `min(m.start, q.start)` can never start a match and is emitted at once.
4. A complete match is released only when it is *final*:
   - it starts before any still-open partial (`s1 < s2`);
   - it ends at least `R = 2` characters before the buffer end (the lookahead margin);
   - it is not a prefix of a longer match (`P.fullmatch(buf, s1, partial=True) is None`).
5. Anything else stays pending. At end of a channel, `close()` runs batch semantics on the remainder.

Worked example (real output of the redactor; `[REDACTED]` appears only once the match is final):

| Delta fed | Released | Held after |
|---|---|---|
| `Reach ja` | `Reach ` | `ja` (could be an email local part) |
| `ne@exa` | | `jane@exa` |
| `mple.org` | | `jane@example.org` (complete, but ends at the buffer end and could grow, e.g. `.org.uk`) |
| ` now` | `[REDACTED] ` | `now` |
| end of stream | `now` | nothing |

### Correctness argument and what it rests on

The argument depends on four properties of the patterns, asserted in `patterns.py` and tested: no pattern matches the empty string; lookbehind is at most `K = 1` character; lookaheads are negative and at most `R = 2` characters; quantifiers are bounded and ASCII-class.

- **Safe prefix:** if a position has neither a complete nor a partial match in the buffer, no continuation can create a match starting there. A future match would either end inside the buffer (already complete) or run past it (already visible as partial).
- **Finality:** if a complete match has no partial before it and no path that can grow past the buffer end, every higher-priority alternative the engine tried failed on a real character, so the same match wins in every continuation. Induction over the cursor gives equality with batch redaction.

What the tests establish about the three checks, stated precisely: the separate `Q` partial search is **necessary** (without it `user.` + `123-45-6789@corp.io` releases `user.` early and leaks); the `fullmatch` check and the `+R` margin are **redundant safeguards** kept as defence in depth against surprises in `regex`'s partial-mode semantics, and the tests show they are redundant given `Q`, not independently required. The margin has a cost: when a chunk ends exactly one delimiter after a PII match, release waits one more token.

`test_property_stream_equals_batch` (hypothesis, random splits including empty deltas), `test_exhaustive_splits` and `test_no_leak_at_any_chunk_size` check equality with batch; `test_mutation_without_q_search_leaks_earlier_partial` shows the leak without the Q search; `test_fullmatch_and_margin_are_redundant_safeguards_given_q` shows the other two checks change nothing on these fixtures (defence in depth).

### Bounds

Held text is bounded by the longest possible match, not by the response. Prose holds a few characters; the worst legitimate candidate is an email-in-progress of at most about 64 + 1 + 8 x 64 + 63 characters. `MAX_HOLD = 1024` is a defensive cap that bounded patterns never reach (`test_max_length_email_does_not_hit_default_cap`). If it were ever exceeded, the redactor **fails closed**: it replaces the whole held span with `[REDACTED]` and counts a `CAP` event, rather than emitting the oldest characters of a possibly giant candidate. Worst-case cost: back-to-back maximal-length emails cost about 1 ms per character; this is bounded and only an upstream can trigger it. Cost is roughly 9 microseconds per delta in the design probes, so the redactor runs inline in the async generator, with no thread offload.

## 6. Proxy design

**Two bounded buffers, nothing else.** The SSE parser holds at most one partial line and one pending event (1 MiB limit each; over the limit raises and fails closed). The redactors hold at most the pending candidate (Section 5). No response-sized buffer exists on the streaming path. `test_memory_bounded` streams a large body and asserts flat memory.

**A byte-level SSE parser, not `aiter_lines`.** `httpx2`'s `aiter_lines()` splits like `str.splitlines`, i.e. also on U+2028, U+2029 and U+0085, which may legally appear raw inside a JSON string, producing broken JSON. `SSEParser` (`sse.py`) splits only on raw CR, LF and CRLF bytes (also when CRLF straddles two reads), strips a leading BOM, decodes data lines as UTF-8, and keeps the exact original bytes of each event. `test_u2028_single_event` and the split-invariance tests cover it.

**Byte-identical forwarding.** A frame whose text did not change is forwarded as the upstream's original bytes. A changed frame is re-serialised as compact ASCII-escaped JSON. The request body is forwarded as the client's original bytes. Upstream comment lines are re-emitted as `: ping` (content dropped), which keeps idle proxies from timing out on a silent reasoning model.

**One yield per upstream read.** All frames produced from one upstream read leave in a single write, and nothing between the redactor and the socket waits for more input. Released text reaches the client before the next upstream chunk arrives (`test_released_text_reaches_client_before_next_upstream_chunk`, gated mock).

**Terminal rules.** The tail rule is the bug class behind providers whose last words slip past a streaming filter. Here, per choice:

- A choice's finish chunk first closes that choice's redactors; released text and tails go out in **one synthetic chunk before** the finish chunk (same envelope, `finish_reason: null`; argument tails as `tool_calls:[{"index":k,"function":{"arguments":...}}]`).
- On `[DONE]`, or on the `choices: []` usage chunk, any unfinished choices are closed first.
- Once a choice has finished or been closed it is sealed: text arriving for it afterwards is an error.
- EOF without `[DONE]` synthesises `[DONE]` only if at least one choice finished and every seen choice finished; otherwise it fails closed.

**Fail closed.** Any exception mid-stream (transport error, invalid or non-object JSON, duplicate JSON keys, malformed shapes such as a non-list `choices`, text after finish, oversized line or event) discards all held text and emits one `data: {"error":{"message":"Upstream stream failed","type":"upstream_error","code":"upstream_error"}}` frame with **no `[DONE]`**. The official SDK raises `APIError` on it. An upstream error frame is forwarded with the **whole frame** redacted (every string leaf, including `error.metadata` and any `choices` text it carries), held text discarded, and no `[DONE]` follows; the request is logged as `outcome=upstream_error`. `test_upstream_drop_mid_pii_fails_closed` drops the connection in the middle of an email and asserts neither the partial value nor `[DONE]` reaches the client.

**Reply path follows the upstream.** `text/event-stream` takes the stream path; any other reply is read (capped by `GATEWAY_MAX_BODY_BYTES`), parsed and redacted by content at any status, with the upstream status preserved. A non-JSON, oversized or malformed reply (bad JSON, non-object, nesting deeper than 64) is a 502 in the OpenAI error shape. The request's `stream` flag is not consulted. Connect failure gives 502 and a timeout before headers gives 504, both `{"error":{"message","type","code"}}`.

**Client disconnect and backpressure.** The response subclass closes the body iterator when streaming ends or is cancelled (stock Starlette only does it at garbage collection), so the upstream response is closed deterministically, and the cleanup is shielded from cancellation (`test_client_disconnect_closes_upstream`). The relay is pull-based: when the client stops reading, uvicorn stops draining, the generator parks, httpx2 stops reading, and TCP flow control stalls the upstream.

**Headers and the key.** Streaming responses carry `content-type: text/event-stream; charset=utf-8`, `cache-control: no-cache` and `x-accel-buffering: no` (so nginx does not buffer), with no `content-length`. The client's `Authorization`, `x-api-key`, `api-key`, `proxy-authorization`, `cookie`, `host`, `content-length`, hop-by-hop headers, any header named in `Connection`, and `accept-encoding` are stripped; upstream gets `accept-encoding: identity` so the relayed body is never compressed, and `Authorization: Bearer <UPSTREAM_API_KEY>` if the key is set. Upstream response headers (`retry-after`, `x-ratelimit-*`, `x-request-id`) are not relayed to the client.

**Front door and logging.** Only `POST /v1/chat/completions` exists: other paths give 404, other methods 405 (with `Allow: POST`), no redirects, both as OpenAI-shaped JSON. A body over `GATEWAY_MAX_BODY_BYTES` gets Starlette's plain-text 413 (logged). Exactly one INFO line per request goes to stderr: method, a fixed route label (never the client's path), upstream status, outcome, error class name, redaction counts by type, max held characters, duration. Never content, prompts, deltas, query strings or keys. The count and max-held fields are filled for streamed replies; JSON replies log the outcome only.

## 7. Benchmark

`uv run python bench/bench.py` starts the mock and the gateway under real uvicorn on free ports and prints this table (about 60 s; `--runs` and `--mem-mb` shrink it).

| Metric | Direct to mock | Through gateway | Note |
|---|---|---|---|
| TTFT median, first token `Hi ` (15 runs, 30 ms delay) | 40.51 ms | 44.97 ms | +4.46 ms; released immediately |
| TTFT median, first token `Sur` (redaction effect) | 31.44 ms | 64.30 ms | +32.86 ms; one token held for exactness |
| Per-chunk inter-arrival p50 / p99 (delay 0, 400 chunks) | 13 us / 537 us | 13 us / 993 us | added p50 0 us, p99 455 us |
| Throughput, content chars/s (delay 0) | 333,336 | 250,393 | 75% of direct |
| tracemalloc peak, transformer path, 5 MB content | - | 0.16 MB | +0 B vs 5 MB run (3.9 s) |
| tracemalloc peak, transformer path, 50 MB content | - | 0.15 MB | -6,906 B vs 5 MB run (44.5 s) |

Environment: Windows 11 (10.0.26300), CPython 3.12.10, asyncio on the Windows Proactor loop, uvicorn 0.54.0 with the h11 server, httpx2 2.16.0 client, loopback only, 15 runs per latency metric.

These numbers are platform-specific. Windows timers have roughly 15.6 ms granularity, so the 30 ms token delay itself reads about 31 to 40 ms, and the per-chunk p50 of 13 us mostly reflects already-buffered TCP reads, so the p99 is the more meaningful per-chunk figure. The memory rows measure the SSE parser plus `StreamTransformer` in-process, not the whole server. How to read it:

- A first token with no possible match (`Hi `) is released at once: about 4 ms added.
- A first token that starts like an email (`Sur`, an email local part) is held until the next token shows it is not one, which costs about one inter-token interval. This is the price of exactness; a fixed-window design pays far more.
- Peak memory stays near 0.15 MB when the content grows 10x.

## 8. Comparison with production gateways

Public documentation of how common gateways handle streamed output. None of them documents streaming redaction that is both incremental and boundary-safe.

| Product / mode | Strategy | TTFT cost | Redacts on stream? | Documented leak or limit |
|---|---|---|---|---|
| [LiteLLM Presidio `apply_to_output`](https://github.com/BerriAI/litellm/blob/main/litellm/proxy/guardrails/guardrail_hooks/presidio.py) | Buffers every chunk, masks the assembled response, emits one synthesized chunk | About full generation time | Yes, after full buffer | Fails open on a masking error (yields the unmasked chunks); skips `/v1/responses` |
| [LiteLLM Bedrock guardrail](https://docs.litellm.ai/docs/proxy/guardrails/bedrock) | Default buffers until moderated; options for every-Nth-chunk sampling or background scan | High, or low with leaks | Block only | Background mode: text that streamed during the scan "has already reached the client" |
| [Portkey](https://portkey.ai/docs/guides/use-cases/streaming-output-guardrails) | Accumulates after the stream; sends results after `[DONE]` | None | No | Checks "on the complete text"; no deny for streamed output |
| [Cloudflare AI Gateway DLP](https://developers.cloudflare.com/ai-gateway/features/dlp/) | "buffers the full streamed response before scanning" | High | Flag or block only | No redact action |
| [NeMo Guardrails output rails](https://docs.nvidia.com/nemo/guardrails/configure-guardrails/yaml-schema/streaming/output-rail-streaming.md) | 200-token chunks with 50-token overlap; `stream_first: True` by default | None (or rail time per chunk) | Block or terminate | With `stream_first` the user "has already received the tokens" when a rail fires |
| [Bedrock Guardrails](https://docs.aws.amazon.com/bedrock/latest/userguide/guardrails-streaming.html) | Synchronous buffers "one or more response chunks"; asynchronous scans in background | Some, or none | Sync only | "doesn't support the masking of sensitive information with asynchronous mode" |
| [Azure OpenAI content filter](https://learn.microsoft.com/en-us/azure/ai-foundry/openai/concepts/content-streaming) | Default buffers chunks; Asynchronous Filter streams token by token | Some, or none | Block | Default: tail leak of the last ~15 to 30 words; async: signal within a ~1,000-character window |

LiteLLM's custom-guardrail API offers [`stream_holdback_chars`](https://docs.litellm.ai/docs/proxy/guardrails/custom_guardrail), a fixed trailing window "so a match never gets split across two chunks". That is the closest precedent; the viable-prefix holdback is its exact variant, holding only what can still become a match. Two lessons from the table are built in and tested: flush and redact the tail before `finish_reason` and `[DONE]` (Azure's tail leak is a missing flush), and fail closed (LiteLLM's Presidio fail-open branch emits unmasked text).

## 9. Extensions (not built)

- Prompt-side (request) redaction, which would re-serialise the request body.
- A logprobs-reject mode (explicit 400) instead of silently nulling them.
- Responses API and Anthropic Messages adapters over the same redaction core (the Responses API repeats full text in its aggregate `*.done` events and would need consistent rewriting).
- NFKC normalisation and fullwidth or other Unicode-evasion handling.
- Escape-decoded matching of tool arguments (closes the `\uXXXX` and `\\new@x.com` gaps).
- Bare 9-digit SSNs with context scoring, and card issuer (IIN) checks.
- NER-based detectors for names and addresses.
- Client authentication, reusing Task 2's JWT middleware.
- A concurrency benchmark on Linux with uvloop and httptools.

## 10. Test evidence

```
$ uv sync --locked && uv run ruff check . && uv run ruff format --check . && uv run pytest -q -W error
All checks passed!
24 files already formatted
262 passed in 63.06s
```

The pytest run takes about 60 to 80 s on the reference machine. Names below come from `uv run pytest --collect-only -q`; `(xN)` marks a parametrised test with N cases. The counts per file sum to 262.

**tests/test_patterns.py (41)**: `test_batch_redact_table` (x21), `test_constants`, `test_args_guard_after_escaped_backslash` (x6), `test_tool_args_keep_json_valid`, `test_without_guard_escape_is_eaten`, `test_redos_bounded_time` (x4), `test_redaction_type_and_valid`, `test_luhn` (x6)

**tests/test_redactor.py (44)**: `test_property_stream_equals_batch`, `test_property_stream_equals_batch_args`, `test_exhaustive_splits` (x14), `test_no_leak_at_any_chunk_size` (x4), `test_bounded_hold_on_prose`, `test_ttft_immediate_release`, `test_worked_example`, `test_adversarial_one_char_stream_is_fast_and_bounded`, `test_cap_fails_closed`, `test_max_length_email_does_not_hit_default_cap`, `test_discard_drops_pending`, `test_counts_by_type`, `test_rejected_matches_not_counted`, `test_args_mode_does_not_eat_escape`, `test_max_held_is_high_water_after_feed`, `test_mutation_baseline_matches_batch` (x3), `test_mutation_without_q_search_leaks_earlier_partial`, `test_fullmatch_and_margin_are_redundant_safeguards_given_q` (x3), `test_args_guard_cases_streamed_in_every_split` (x6)

**tests/test_sse.py (16)**: `test_basic_events`, `test_split_invariance_every_split_and_bytewise`, `test_split_invariance_exact_raw_for_lf_only_input`, `test_u2028_single_event`, `test_raw_fidelity_ends_with_blank_line`, `test_cr_only_bytewise_emits_on_final_cr_feed`, `test_crlf_split_after_cr_emits_on_first_feed_and_drops_lf`, `test_comment_ending_in_cr_emitted_immediately`, `test_bom_spanning_feeds_and_single_strip`, `test_comment_event`, `test_dataless_event_not_dispatched`, `test_data_space_stripping`, `test_incomplete_trailing_dropped_on_close`, `test_invalid_utf8_replaced`, `test_line_limit`, `test_event_limit`

**tests/test_transform.py (58)**: `test_error_frame_exact`, `test_email_split_over_three_deltas`, `test_unchanged_frames_are_byte_identical`, `test_tail_before_finish`, `test_finish_chunk_own_text_moved_to_synthetic`, `test_finish_without_held_text_passes_through_identical`, `test_n2_choices_independent`, `test_refusal_redacted`, `test_tool_calls_redacted_and_valid_json`, `test_function_call_redacted`, `test_logprobs_nulled`, `test_usage_chunk_passthrough_after_close`, `test_eof_after_finish_synthesizes_done`, `test_eof_with_unfinished_choice_fails_closed`, `test_discard_never_emits_held`, `test_upstream_error_frame`, `test_bad_json_raises`, `test_non_object_json_raises`, `test_comment_becomes_ping`, `test_fully_held_delta_emits_empty_content`, `test_non_ascii_changed_frame_is_ascii_escaped`, `test_unknown_fields_preserved`, `test_stats`, `test_redact_completion`, `test_redact_error_body`, `test_text_after_finish_fails_closed`, `test_text_after_empty_choices_usage_chunk_fails_closed`, `test_new_tool_text_after_finish_fails_closed`, `test_empty_choices_without_usage_does_not_close`, `test_malformed_shapes_raise` (x12), `test_error_null_is_a_normal_chunk`, `test_eof_without_any_finished_choice_fails_closed`, `test_duplicate_keys_raise`, `test_discard_empties_every_redactor`, `test_transformer_property`, `test_error_frame_redacts_entire_frame`, `test_error_frame_string_error_and_tool_args`, `test_reasoning_channels_equal_batch_and_flush_before_finish` (x2), `test_reasoning_non_string_fails_closed`, `test_audio_transcript_redacted`, `test_frame_without_choices_redacted`, `test_message_object_in_stream_chunk_redacted`, `test_pii_free_frame_with_unknown_fields_byte_identical`, `test_too_deep_unknown_field_fails_closed`, `test_non_stream_tool_args_redacted_once_and_valid_json`, `test_usage_null_empty_choices_does_not_seal`

**tests/test_streaming.py (6)**: `test_released_text_reaches_client_before_next_upstream_chunk`, `test_held_candidate_released_only_when_final`, `test_client_disconnect_closes_upstream` (x2), `test_upstream_drop_mid_pii_fails_closed`, `test_memory_bounded`

**tests/test_gateway.py (54)**: `test_stream_content_equals_batch_redact_with_sse_headers`, `test_non_streaming_redacted_and_logprobs_null`, `test_upstream_request_headers_url_and_body`, `test_empty_key_sends_no_authorization`, `test_connect_error_is_502_openai_shape`, `test_read_timeout_before_headers_is_504`, `test_upstream_400_json_relayed_and_redacted`, `test_non_json_upstream_is_502_and_closed`, `test_json_over_cap_is_502`, `test_sse_reply_to_stream_false_request_is_redacted_stream`, `test_other_path_404_json`, `test_other_methods_405` (x4), `test_oversized_body_413`, `test_transport_error_mid_stream_gives_error_frame_without_leak`, `test_non_json_data_frame_gives_error_frame`, `test_eof_without_done_before_finish_is_error_frame`, `test_upstream_closed_after_stream`, `test_one_log_record_per_stream_request_no_content`, `test_one_log_record_for_json_error_and_front_door`, `test_mid_stream_failure_logs_class_name_only_and_one_info`, `test_timeout_and_bad_gateway_outcomes_logged`, `test_every_json_reply_redacted_by_content_status_preserved` (x5), `test_normal_completion_non_pii_fields_unchanged`, `test_malformed_json_is_502_logged` (x6), `test_missing_content_type_is_502`, `test_trailing_slash_is_404_not_redirect`, `test_client_credentials_and_named_connection_headers_stripped`, `test_exactly_one_record_of_any_level_per_request` (x11), `test_front_door_duration_measured_and_path_not_logged`, `test_upstream_3xx_json_relayed_redacted`, `test_client_disconnect_outcome_and_upstream_closed`, `test_mid_stream_upstream_error_frame_logged_as_upstream_error`

**tests/test_mock_llm.py (17)**: `test_default_tokens_split_pii_and_end_with_email`, `test_stream_chunk_sequence`, `test_default_content_roundtrip`, `test_n2_independent_choices`, `test_choices_override`, `test_include_usage_final_chunk`, `test_no_usage_chunk_by_default`, `test_non_streaming`, `test_default_model`, `test_fault_error_frame`, `test_fault_drop`, `test_fault_crlf`, `test_fault_cr`, `test_fault_bom`, `test_events_per_write_batches_without_changing_body`, `test_delay_still_completes`, `test_mock_fault_env_var_validated_and_passed`

**tests/test_config.py (20)**: `test_defaults`, `test_every_variable_parses`, `test_bad_upstream_url` (x3), `test_bad_read_timeout` (x3), `test_non_finite_read_timeout` (x2), `test_bad_port` (x3), `test_bad_max_body_bytes`, `test_bad_log_level`, `test_repr_hides_key`, `test_errors_never_contain_key` (x4)

**tests/test_e2e.py (6)**, real uvicorn servers and the official OpenAI SDK: `test_streaming_content_is_redacted`, `test_non_streaming_matches_streaming`, `test_upstream_fault_raises_api_error` (x2), `test_ttft_through_gateway_close_to_direct`, `test_main_bad_config_exits_2`
