# Task 2: MCP security gateway

## 1. What it is

An HTTP reverse proxy that sits in front of an MCP server (Streamable HTTP, one `/mcp` endpoint). It authenticates every request with a bearer token, parses the JSON-RPC body strictly, and applies a method-level authorization rule before anything reaches the downstream server:

> A `tools/call` whose tool name starts with `admin_` requires the `admin` role. A `viewer` gets a JSON-RPC error `-32001 Unauthorized Tool Call` back directly, and the downstream is never contacted. Everything else is forwarded byte-for-byte and streamed back.

It is built with Starlette, uvicorn and httpx2 (all already dependencies of the official `mcp` SDK, 2.3.x) as a pure-ASGI middleware chain. The project is self-contained: it has its own `pyproject.toml` and lock file and does not import from Task 1. A mock downstream MCP server (three tools, one of them `admin_reset_key`) is included so the whole thing runs locally.

How it maps to the three things this task is graded on:

| Concern | Where to look |
|---|---|
| Parsing the JSON-RPC wire format correctly | [Section 4](#4-request-pipeline) stage 2 and [Section 7](#7-bypass-cases-blocked): strict I-JSON parsing, message classification, id handling. Code: `src/mcp_gateway/jsonrpc.py` |
| Proxy middleware construction and HTTP forwarding | [Sections 4, 8, 9](#4-request-pipeline): pure-ASGI auth and policy middlewares, header policy, original-bytes forwarding, SSE streaming. Code: `app.py`, `middleware.py`, `proxy.py` |
| Fine-grained method-level authorization and clean errors | [Sections 5 and 6](#5-authorization-rules-and-the--32001-response): the rule, the `-32001` response, and the full status/error matrix. Code: `policy.py`, `auth.py` |

```
                 +------------------------------ mcp-gateway :8000 -------------------------------+
                 |                                                                               |
 client  ------> | 1 BearerAuthMiddleware        2 JsonRpcPolicyMiddleware          5 proxy      | ------> mock-mcp-server
 Authorization:  |   verify JWT / static token     path, method, Content-Type          route     |         :8001/mcp
 Bearer <token>  |   401/400 + -32005 on failure   read body, strict parse (I-JSON)    httpx2    |
                 |                                 classify, header consistency        stream    |
        <------- |                               3 authorize: method allowlist,                  | <------
   response      |                                   admin_* needs the admin role                |   upstream response
   (or a local   |                               4 deny -> -32001, no upstream call              |   (SSE or JSON) relayed
   JSON-RPC      |                                                                               |
   error)        |  6 one audit line per decision on logger mcp_gateway.audit (stderr)           |
                 +-------------------------------------------------------------------------------+
```

## 2. Quick start

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/). All commands run from `task2-mcp-gateway/`.

```bash
uv sync --locked
uv run pytest -q          # 200 tests, about 2 s
```

The demo needs three terminals (or run the two servers in the background). The secret below is a placeholder; any value of 32 bytes or more works, and the gateway refuses to start with a shorter one.

### bash

```bash
# terminal 1: mock downstream on 127.0.0.1:8001
uv run mock-mcp-server

# terminal 2: gateway on 127.0.0.1:8000
export GATEWAY_JWT_SECRET="change-me-to-a-32-byte-or-longer-secret"
uv run mcp-gateway

# terminal 3: mint tokens and call the gateway
export GATEWAY_JWT_SECRET="change-me-to-a-32-byte-or-longer-secret"
VIEWER=$(uv run mint-token --role viewer)
ADMIN=$(uv run mint-token --role admin)

# 1. legacy initialize (viewer)
curl -s -i http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Bearer $VIEWER" \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-11-25","capabilities":{},"clientInfo":{"name":"demo","version":"0"}}}'

# 2. viewer: tools/list (transparent, all three tools are listed)
curl -s -i http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Bearer $VIEWER" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/list"}'

# 3. viewer: tools/call admin_reset_key  -> HTTP 200 + -32001, downstream not contacted
curl -s -i http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Bearer $VIEWER" \
  -d '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"user":"alice"}}}'

# 4. admin: the same call succeeds
curl -s -i http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -H "Authorization: Bearer $ADMIN" \
  -d '{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"user":"alice"}}}'

# 5. no token -> 401
curl -s -i http://127.0.0.1:8000/mcp \
  -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" \
  -d '{"jsonrpc":"2.0","id":5,"method":"tools/list"}'
```

### PowerShell (7.3 or later; use `curl.exe`, because `curl` is an alias for `Invoke-WebRequest`)

```powershell
# terminal 1
uv run mock-mcp-server

# terminal 2
$env:GATEWAY_JWT_SECRET = "change-me-to-a-32-byte-or-longer-secret"
uv run mcp-gateway

# terminal 3
$env:GATEWAY_JWT_SECRET = "change-me-to-a-32-byte-or-longer-secret"
$VIEWER = uv run mint-token --role viewer
$ADMIN  = uv run mint-token --role admin
$call = '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"admin_reset_key","arguments":{"user":"alice"}}}'

curl.exe -s -i http://127.0.0.1:8000/mcp -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" -H "Authorization: Bearer $VIEWER" -d $call   # -32001
curl.exe -s -i http://127.0.0.1:8000/mcp -H "Content-Type: application/json" -H "Accept: application/json, text/event-stream" -H "Authorization: Bearer $ADMIN"  -d $call   # success
```

On Windows PowerShell 5.1, native-command quoting differs: write the JSON to a file and pass `--data-binary "@call.json"` instead of `-d $call`.

### Without minting a JWT (static tokens)

For a quick test, switch the gateway to a fixed token map. Tokens come from the environment; nothing is hard-coded in the source.

```bash
GATEWAY_AUTH_MODE=static GATEWAY_STATIC_TOKENS="viewer-token=viewer,admin-token=admin" uv run mcp-gateway
# then use "Authorization: Bearer viewer-token" or "Bearer admin-token" in the curl commands above
```

```powershell
$env:GATEWAY_AUTH_MODE = "static"; $env:GATEWAY_STATIC_TOKENS = "viewer-token=viewer,admin-token=admin"; uv run mcp-gateway
```

### Observed output

Captured from a real run of the bash commands above. Long outputs are trimmed; `date` headers are removed from the 200/401 blocks except where shown.

Viewer, `tools/call admin_reset_key` (step 3). HTTP 200, a JSON-RPC error, and the id echoed:

```
HTTP/1.1 200 OK
content-type: application/json
content-length: 182

{"jsonrpc":"2.0","id":3,"error":{"code":-32001,"message":"Unauthorized Tool Call","data":{"reason":"forbidden_tool","tool":"admin_reset_key","requiredRole":"admin","role":"viewer"}}}
```

Admin, the same call (step 4). Forwarded; the downstream answers with an SSE event, relayed as it arrived:

```
HTTP/1.1 200 OK
cache-control: no-cache, no-transform
content-type: text/event-stream
x-accel-buffering: no
Transfer-Encoding: chunked

event: message
data: {"jsonrpc":"2.0","id":4,"result":{"content":[{"text":"{\n  \"user\": \"alice\",\n  \"key_reset\": true\n}","type":"text"}],"isError":false}}
```

No token (step 5):

```
HTTP/1.1 401 Unauthorized
content-type: application/json
content-length: 129
www-authenticate: Bearer realm="mcp-gateway"

{"jsonrpc":"2.0","id":null,"error":{"code":-32005,"message":"Authentication required","data":{"reason":"missing_authorization"}}}
```

Viewer `initialize` (step 1) and `tools/list` (step 2) return `200` with `content-type: text/event-stream` and a single `event: message` whose `data:` is the downstream's result. The `tools/list` result contains `get_status`, `list_users` and `admin_reset_key`: the list is not filtered (see [Section 11](#11-design-notes)). The mock is stateless, so `initialize` does not issue an `Mcp-Session-Id`.

The audit lines the gateway wrote for those requests (stderr, with the log-line prefix trimmed):

```
{"event":"authz.decision","ts":"2026-11-08T03:19:20.451039+00:00","decision":"deny","reason":"admin_tool_requires_admin_role","subject":"sha256:cebf292c038f","role":"viewer","era":"legacy","method":"tools/call","tool":"admin_reset_key","rpc_id":3,"request_id":"c1f998b93f71447b92a1d2578f13b095","upstream_contacted":false}
{"event":"authz.decision","ts":"2026-11-08T03:19:20.486920+00:00","decision":"allow","reason":"allowed","subject":"sha256:cebf292c038f","role":"admin","era":"legacy","method":"tools/call","tool":"admin_reset_key","rpc_id":4,"request_id":"7121be31d967496baca5610c4d73af2a","upstream_contacted":true}
{"event":"authz.decision","ts":"2026-11-08T03:19:20.524750+00:00","decision":"reject","reason":"missing_authorization","subject":null,"role":null,"era":"legacy","method":null,"tool":null,"rpc_id":null,"request_id":"6aefa08f233a4d07b09d556800a6b89f","upstream_contacted":false}
```

(`mint-token` in the demo used the default subject `demo-user`, so both principals share the same hashed `subject`.)

The same flow through the official SDK client (`mcp.Client`) is covered by `tests/test_e2e.py`, which runs the real gateway and mock servers in both protocol eras (legacy handshake and `2026-07-28`).

## 3. Configuration

All settings come from environment variables, are read once at startup, and are case-insensitive where they are enumerations. An invalid value prints `mcp-gateway: configuration error: ...` to stderr and exits with code `2`.

| Variable | Default | Notes |
|---|---|---|
| `GATEWAY_AUTH_MODE` | `jwt` | **Opt-in:** `static` uses a fixed token map instead of JWTs |
| `GATEWAY_DENY_HTTP_STATUS` | `200` | **Opt-in:** `403` returns the same `-32001` body with HTTP 403 |
| `GATEWAY_JWT_SECRET` | none, required in `jwt` mode | HS256 key, at least 32 bytes; a shorter value refuses to start (exit 2) |
| `GATEWAY_STATIC_TOKENS` | none, required in `static` mode | `token=role[,token=role]`, role `admin` or `viewer`, tokens unique and non-empty |
| `GATEWAY_JWT_AUDIENCE` | `mcp-gateway` | Required `aud` claim |
| `GATEWAY_JWT_ISSUER` | `mcp-gateway-dev` | Required `iss` claim |
| `GATEWAY_DOWNSTREAM_URL` | `http://127.0.0.1:8001/mcp` | The upstream MCP endpoint; must be `http://` or `https://` with a host, otherwise startup exits with code 2 |
| `GATEWAY_HOST` / `GATEWAY_PORT` | `127.0.0.1` / `8000` | Listen address |
| `GATEWAY_MAX_BODY_BYTES` | `4194304` (4 MiB) | Larger bodies get HTTP 413 |
| `LOG_LEVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR`; audit lines are INFO |

Mock downstream: `MOCK_HOST` (`127.0.0.1`), `MOCK_PORT` (`8001`), `MOCK_JSON_RESPONSE` (`false`, `true` makes it answer in JSON instead of SSE), `MOCK_STATELESS` (`true`).

`mint-token --role admin|viewer [--sub NAME] [--ttl SECONDS]` reads `GATEWAY_JWT_SECRET` (and the audience and issuer variables) and prints an HS256 token with `sub`, `role`, `aud`, `iss`, `iat` and `exp`; the default lifetime is one hour.

Source: `src/mcp_gateway/config.py`.

These two opt-ins are the only policy switches. Unknown methods are always denied, tool names are always matched literally, `tools/list` is always transparent, and legacy GET/DELETE are always forwarded after authentication. The reasoning is in [Section 11](#11-design-notes).

## 4. Request pipeline

The app is a Starlette application with one route (`/mcp`, POST/GET/DELETE) and two pure-ASGI middlewares, outermost first (`app.py`).

| Stage | What happens | Failure |
|---|---|---|
| 1. Authenticate (`BearerAuthMiddleware`) | Exactly one `Authorization: Bearer <b64token>` header (RFC 6750 syntax). JWT: HS256 only, `exp`/`iat`/`sub`/`role`/`aud`/`iss` required, 30 s leeway, `role` must be `admin` or `viewer`. Runs before the body is read, for every method, including `tools/list`, GET and DELETE. There is no default role. | 401 or 400, `-32005`, `id: null`, `WWW-Authenticate` |
| 2. Parse and classify (`JsonRpcPolicyMiddleware`) | Exact path `/mcp`; method POST/GET/DELETE; `Content-Type: application/json` (charset utf-8 only), no `Content-Encoding`; read the body (4 MiB cap); strict I-JSON parse; classify as request, notification or response; check mirrored routing headers against the body | 404, 405, 415, 413, 400 (`-32700`, `-32600`, `-32020`) |
| 3. Authorize | Method allowlist (default deny, `notifications/*` allowed), then for `tools/call`: validate `params.name`, apply the `admin_` rule on the **decoded body** value | `-32601`, `-32602`, `-32001` |
| 4. Forward and stream | Original request bytes go to the downstream with the header policy of [Section 8](#8-forwarding-and-header-policy); the response is relayed chunk by chunk ([Section 9](#9-streaming)) | 502/504 + `-32603` for gateway-side upstream failures; downstream 4xx/5xx relayed verbatim |
| 5. Error mapping | Every gateway-originated failure is a JSON-RPC error body with `Content-Type: application/json` ([Section 6](#6-status-and-error-matrix)) | |
| 6. Audit | One JSON line per decision ([Section 10](#10-audit-log-format)) | |

Parsing details (`src/mcp_gateway/jsonrpc.py`). The gateway authorizes on the decoded body, so it must reject every body that the downstream SDK might read differently from the way the gateway read it. The accepted profile is I-JSON (RFC 7493): UTF-8 only, BOM rejected, duplicate object keys rejected (compared after `\u` unescaping), `NaN`/`Infinity`/overflowing floats like `1e400` rejected, lone surrogates rejected, nesting depth at most 64. Any violation is HTTP 400, `-32700`, `id: null`, and nothing is forwarded. After parsing, a message is classified by `method`: an object with `method` and a string-or-integer `id` is a request, without `id` a notification; an object without `method` is a client response (valid only with an `id` and exactly one of `result`/`error`). Arrays (batches), scalars, a wrong `jsonrpc` version, `method` together with `result` or `error`, and booleans, floats, `null` or objects as `id` are `-32600`. A `tools/call` without an `id` is `-32600`, never treated as a notification.

Era detection (`policy.py`): the gateway reads `MCP-Protocol-Version` exactly as the SDK does. The first `MCP-Protocol-Version` value decides: it is "modern" if present and not one of the SDK's initialize-handshake versions (`mcp_types.version.HANDSHAKE_PROTOCOL_VERSIONS`, currently 2024-11-05 to 2025-11-25), so `2026-07-28` and any unknown value are modern; a missing header is "legacy". The era only changes one response: the status of `-32601` (see the matrix). Authorization is identical in both eras.

## 5. Authorization rules and the `-32001` response

The rule (`policy.py`, `is_admin_tool` and `authorize`):

| Request | viewer | admin |
|---|---|---|
| `tools/call` with `params.name` a string starting with `admin_` | `-32001`, not forwarded | forwarded |
| `tools/call` with any other name (`get_status`, `list_users`, ...) | forwarded | forwarded |
| `tools/list`, `initialize`, `ping` and the other allowlisted methods | forwarded | forwarded |
| Method not on the allowlist | `-32601`, not forwarded | `-32601`, not forwarded |

The match is a literal, case-sensitive prefix test on the decoded string. The decision uses only the decoded body: the `Mcp-Name` and `Mcp-Method` headers are checked for consistency with the body but never used to decide. The role comes only from the verified token, never from a header, the session id or `_meta`.

The allowlist (`ALLOWED_METHODS`) is `initialize`, `ping`, `server/discover`, `tools/list`, `tools/call`, `resources/list`, `resources/templates/list`, `resources/read`, `resources/subscribe`, `resources/unsubscribe`, `subscriptions/listen`, `prompts/list`, `prompts/get`, `logging/setLevel`, `completion/complete`, `tasks/get`, `tasks/result`, `tasks/list`, `tasks/cancel`, plus anything starting with `notifications/`. Client responses (messages without `method`, for example a reply to a server-initiated sampling request on the legacy stream) pass after the same strict parse. Everything else is `-32601`, because the SDK lets a server register arbitrary custom methods that could do privileged work without being `tools/call`.

The denial, exactly as returned:

```http
HTTP/1.1 200 OK
Content-Type: application/json

{"jsonrpc":"2.0","id":3,"error":{"code":-32001,"message":"Unauthorized Tool Call",
 "data":{"reason":"forbidden_tool","tool":"admin_reset_key","requiredRole":"admin","role":"viewer"}}}
```

- The `id` is echoed with its exact JSON type: `7` stays a number and `"7"` stays a string (`test_viewer_admin_tool_denied_without_upstream_call`, `test_string_id_echoed_with_type`).
- The body is JSON, never SSE, never 202, and carries no `Mcp-Session-Id`. The downstream is contacted zero times (asserted on a recording transport, and against the real SDK mock's call counter in `tests/test_integration.py`).
- The echoed tool name is passed through `safe_name` (control and format characters escaped, capped at 64 characters). The token is never included.
- HTTP 200 is the default because the brief asks for a JSON-RPC error returned directly, and 200 matches JSON-RPC convention and keeps the body visible to generic clients that raise on non-2xx. `GATEWAY_DENY_HTTP_STATUS=403` returns the identical body with 403, which is what the MCP authorization spec maps "insufficient permissions" to (`test_deny_status_403_opt_in`). Either way the official SDK client surfaces a JSON-RPC error with `.code == -32001`.

Four caveats about `-32001`, stated here because the brief mandates this code:

1. **It is legal.** JSON-RPC 2.0 reserves `-32000` to `-32099` for implementation-defined server errors ([JSON-RPC 2.0](https://www.jsonrpc.org/specification)).
2. **It sits in a band the current MCP spec discourages.** The 2026-07-28 revision labels `-32000` to `-32019` legacy, says new implementations SHOULD NOT use that sub-range, and tells receivers not to assume any specific meaning for those codes ([MCP 2026-07-28 basic](https://modelcontextprotocol.io/specification/2026-07-28/basic)).
3. **The Python SDK uses it locally.** `mcp_types` defines `REQUEST_TIMEOUT = -32001` as an SDK-side code, so SDK-based client logs can label a denial as a timeout. `data.reason: "forbidden_tool"` is there so machines can tell the two apart.
4. **Draft-era tooling may misread it.** The 2026 draft used `-32001` for header mismatch before renumbering it to `-32020` ([2026-07-28 changelog](https://modelcontextprotocol.io/specification/2026-07-28/changelog)).

A design free of the brief's constraint would use a code outside `-32768..-32000` or rely on HTTP 403 alone. Authentication failures use `-32005` and header mismatch uses `-32020`; `-32002` (retired in the spec) is never used.

## 6. Status and error matrix

Every response the gateway generates itself, in pipeline order. `id` is the request's id (exact type) where the body was parsed far enough to know it, otherwise `null`.

| Condition | HTTP | JSON-RPC code | `id` |
|---|---|---|---|
| No `Authorization`, or a scheme other than Bearer | 401, `WWW-Authenticate: Bearer realm="mcp-gateway"` | `-32005` | null |
| More than one `Authorization` header, or a malformed credential | 400, `error="invalid_request"` | `-32005` | null |
| Bad signature, expired, wrong `aud`/`iss`/algorithm, missing claim, unknown role, unknown static token | 401, `error="invalid_token"` | `-32005` | null |
| Path other than exactly `/mcp` | 404 | `-32600` "Not Found" | null |
| Method other than POST, GET, DELETE (including implicit HEAD) | 405, `Allow: POST, GET, DELETE` | `-32600` "Method Not Allowed" | null |
| Non-JSON `Content-Type`, charset other than UTF-8, any `Content-Encoding` | 415 | `-32600` | null |
| Body over the cap (chunked bodies count too) | 413, Starlette's plain-text response; **not audited** | none | none |
| Outside I-JSON: bad UTF-8, BOM, duplicate key, NaN/Infinity, lone surrogate, depth over 64, not JSON | 400 | `-32700` | null |
| Array, scalar, bad `jsonrpc`, `method` plus `result`/`error`, invalid id, `tools/call` without id | 400 | `-32600` | null |
| Mirrored header mismatch, or a duplicated `Mcp-Method`/`Mcp-Name` | 400 | `-32020` | echoed |
| Method not on the allowlist | 404 on 2026-era requests, 200 on legacy; 400 when sent as a notification (no id) | `-32601` | echoed (null for a notification) |
| `tools/call` with `params` not an object, or `name` missing, empty or not a string | 400 | `-32602` | echoed |
| Viewer calls an `admin_*` tool | 200 (403 with the opt-in) | `-32001` | echoed |
| Downstream connect failure | 502 | `-32603` "Downstream unavailable" | echoed |
| Downstream timeout before response headers | 504 | `-32603` "Downstream timeout" | echoed |
| Downstream answers 4xx/5xx | relayed unchanged | the downstream's | the downstream's |
| Downstream fails after streaming started | headers already sent: upstream closed, warning logged, stream ends | none possible | n/a |

Why `-32601` is 200 on legacy requests: the 2025-11-25 transport spec makes a client read a 404 as "session terminated, start a new session", whereas 2026-07-28 explicitly uses 404. The gateway picks the era the way the SDK does, from `MCP-Protocol-Version` (`test_modern_era_unknown_method_is_404`, `test_unknown_method_denied`).

A client that disconnects while the gateway is still reading its body writes no audit record either.

Sources: `middleware.py` (auth, 404/405/415, `-32001`), `jsonrpc.py` (`-32700`, `-32600`), `policy.py` (`-32020`, `-32601`, `-32602`), `proxy.py` (502/504).

## 7. Bypass cases blocked

The core risk of an authorizing proxy is that it and the downstream read the same bytes differently. These classes were probed against the real SDK (`mcp` 2.3.0): duplicate keys resolve last-wins in the downstream, `admin_reset_key` decodes before dispatch, and on the `2026-07-28` path a BOM or UTF-16 body was accepted by the downstream's parser and executed `admin_reset_key`. They are the interoperability classes described in [Bishop Fox's JSON interoperability write-up](https://bishopfox.com/blog/json-interoperability-vulnerabilities); the fix is a fail-closed strict parser plus deciding on decoded values. All tests below assert that the downstream received zero calls for denied or rejected requests.

| # | Attack input | Gateway behaviour | Proving test |
|---|---|---|---|
| 1 | Duplicate `name`, `params`, `method` or `id` keys (also `name` vs `n\u0061me`) | 400, `-32700`, id null | `test_gateway.py::test_bypass_bodies_never_reach_downstream` [dup-name, dup-params, dup-method, dup-id, dup-name-escaped]; `test_integration.py::test_duplicate_key_body_rejected_and_nothing_called` (real SDK mock) |
| 2 | `"\u0061dmin_reset_key"` (escaped tool name) | Authorized on the decoded value: viewer gets `-32001` | same test [escaped-admin-name]; `test_jsonrpc.py::test_escaped_name_is_decoded` |
| 3 | UTF-16, UTF-32 (with or without BOM), BOM-prefixed or CESU-8 surrogate body (plus `NaN`) | 400, `-32700` | same test [utf-16, utf-16-le-no-bom, utf-16-be-no-bom, utf-32-le-no-bom, utf-32-be-no-bom, cesu8-surrogate, bom, nan]; `test_jsonrpc.py::test_parse_rejects_with_32700` (UTF-8 BOM, UTF-16, UTF-32, `NaN`, `Infinity`, `1e400`, lone surrogates, depth) |
| 4 | Missing, duplicated, malformed, forged, expired, `alg=none`, HS512 or wrong-audience token, on `tools/list` and `tools/call` | 401/400 before the body is read; no default role | `test_gateway.py::test_auth_failures_never_reach_downstream` (12 cases); `test_auth.py::test_invalid_tokens_are_401_invalid_token`, `test_extract_bearer_errors`; `test_unparseable_body_without_token_is_401_not_400` |
| 5 | `method` together with `result` | 400, `-32600` | `test_bypass_bodies_never_reach_downstream` [method-plus-result] |
| 6 | `tools/call` with no id, or id `null`, `1.0`, `true` | 400, `-32600` (never 202, never forwarded) | [idless-tools-call, id-null, id-float, id-bool]; `test_jsonrpc.py::test_classify_rejects_with_32600` |
| 7 | Batch array (one or two calls), scalar body | 400, `-32600`, id null | [batch, batch-two]; `test_classify_rejects_with_32600` |
| 8 | `name` non-string or missing; `params` an array or null | 400, `-32602`, never a 500 | [name-not-string, name-missing, params-array]; `test_policy.py::test_bad_tools_call_params_32602` |
| 9 | `Tools/Call`, custom `admin/reset` | `-32601`, default deny | [method-case, custom]; `test_policy.py::test_unknown_method_denied` (legacy 200 / modern 404) |
| 10 | `Mcp-Name: get_status` over a body calling `admin_reset_key`; duplicated routing headers | 400, `-32020`; authorization never reads the headers | `test_gateway.py::test_mirrored_header_mismatch_rejected`; `test_policy.py::test_mirrored_header_mismatch` (also covers the duplicate-header case) |
| 11 | `Admin_reset_key`, `ADMIN_RESET`, leading space, fullwidth `ａ`, zero-width space | Not matched by the literal rule, so forwarded unchanged; the SDK matches tool names exactly and answers "Unknown tool", nothing runs. See [Section 12](#12-production-hardening-described-not-built) | `test_policy.py::test_literal_rule_lets_variants_through_to_exact_matching_downstream` |
| 12 | `text/plain`, form, non-UTF-8 charset, `Content-Encoding: gzip` | 415 | `test_gateway.py::test_wrong_content_type_415` |
| 13 | 5 MiB body; nesting depth over 64 | 413 (no upstream call); 400 `-32700` | `test_oversized_body_413_and_large_legit_body_forwarded` (also: a 1 MiB legitimate body is forwarded byte-identical); `test_jsonrpc.py::test_parse_rejects_with_32700` [nesting_too_deep]; `test_depth_64_is_accepted` |
| 14 | `/mcp/`, `/MCP`, `//mcp`, other paths; PUT/PATCH/OPTIONS/HEAD | 404 / 405 as JSON, audited, not forwarded | `test_other_paths_404`, `test_other_methods_405_json_and_audited`, `test_unauthenticated_head_is_401` |
| 15 | The client's token reaching the downstream | `Authorization`, `Cookie` and others stripped | `test_forwarded_headers_are_filtered` |
| 16 | Viewer sending an admin's `Mcp-Session-Id` | Role comes from the request's own token: `-32001` | `test_session_id_does_not_carry_role` |
| 17 | Log injection through a tool name or id | One JSON line, control characters escaped, strings capped | `test_policy.py::test_audit_event_is_single_escaped_json_line`, `test_safe_name_escapes_control_and_format_chars`, `test_safe_name_caps_at_64` |

Rows 1 to 13 match the probes; 14 to 17 follow from the specifications and are covered by the tests listed. Row 11 is the one place where the gateway deliberately relies on the downstream's exact-match behaviour; the unit test documents it rather than hiding it.

## 8. Forwarding and header policy

The gateway forwards the **original request bytes**, not a re-serialization of the parsed message. Once the strict parser has accepted a body (single object, UTF-8, no duplicate keys, finite numbers), re-serializing would add nothing to security, and forwarding the original is byte-transparent: no key reordering, no number reformatting, and a large legitimate body (1 MiB argument) arrives identical (`test_admin_admin_tool_forwarded_byte_identical`, `test_oversized_body_413_and_large_legit_body_forwarded`). The body is read once, parsed, and replayed to the route handler; GET and DELETE are forwarded with an empty body.

The token never travels downstream: the gateway is the client-facing resource server and `Authorization` is stripped. The MCP authorization spec forbids passing a received client token through to another service, because that defeats the downstream's own controls, breaks the audit trail and blurs trust boundaries. If the downstream needs to know the caller, that is a separate gateway-issued credential (see [Section 12](#12-production-hardening-described-not-built)).

Request headers use a denylist, because RFC 9110 says a proxy must forward header fields it does not recognise (so `Mcp-*`, `Last-Event-ID`, `Accept` and custom headers pass).

| Direction | Dropped | Set by the gateway |
|---|---|---|
| Client to downstream | `Authorization`, `Cookie`, `Host`, `Content-Length`, `Origin`, `Proxy-Authorization`, client `X-Request-Id`, any `X-Gateway-*`, `Accept-Encoding`, hop-by-hop (`Connection`, `Keep-Alive`, `Proxy-Connection`, `TE`, `Trailer`, `Transfer-Encoding`, `Upgrade`) and every header named in `Connection` | `Accept-Encoding: identity`, a gateway-generated `X-Request-Id`; `Host` and `Content-Length` are derived by httpx2 from the downstream URL and the body |
| Downstream to client | hop-by-hop headers, headers named in `Connection`, `Server`, `Date`, any `X-Gateway-*` | nothing; status, `Content-Type`, `Mcp-Session-Id`, `Cache-Control`, `X-Accel-Buffering` and the rest are relayed |

Notes:

- `Accept-Encoding: identity` is sent so the relayed bytes are never compressed and `Content-Length` stays correct. The httpx2 client's own default headers (`Accept: */*`, `User-Agent`) are cleared at startup so they never leak downstream.
- `Host` is dropped on purpose: the mock keeps the SDK's DNS-rebinding protection on, which accepts only `127.0.0.1:*`, `localhost:*` and `[::1]:*`. httpx2 derives `127.0.0.1:8001` from the downstream URL (`test_forwarded_headers_are_filtered`, `tests/test_mock_mcp.py::test_foreign_host_rejected_421`).
- `Mcp-Session-Id` is relayed in both directions, since a legacy client cannot continue without it. It is never used to carry a role.
- The query string is forwarded; redirects from the downstream are not followed. Upstream timeouts: connect 5 s, read 300 s, write 30 s, pool 5 s.

## 9. Streaming

The downstream answers a request with either a JSON body or an SSE stream. The gateway relays whatever it gets without buffering: it opens the upstream response with `stream=True`, yields raw chunks to a `StreamingResponse`, and relays the status and headers (including `Content-Type: text/event-stream`, `Cache-Control` and `X-Accel-Buffering`) unchanged.

- Incremental: the test drives the ASGI app directly with a recording `send` and an upstream that yields one SSE event, waits, then yields a second. The first chunk reaches the client before the upstream produces the second (`test_streaming.py::test_sse_is_relayed_incrementally`).
- Client disconnect: when the client goes away mid-stream the upstream response is closed (`test_client_disconnect_closes_upstream`), which the 2026-07-28 spec requires to be treated as cancellation.
- Upstream failure mid-stream: the status line is already sent, so no error status is possible; the gateway closes the upstream, logs a warning and ends the stream cleanly (`test_upstream_failure_mid_stream_closes_upstream`).
- Gateway-originated errors are never SSE or 202.

Observed with the official SDK client: a GET to the stateless downstream through the gateway is authenticated and forwarded; the downstream answers 200 with an SSE stream that stays open and empty, and the SDK client tolerates it.

## 10. Audit log format

One JSON line per decision (allow, deny and reject), written at INFO level through logger `mcp_gateway.audit` to stderr. Fields:

| Field | Meaning |
|---|---|
| `event`, `ts` | Always `authz.decision`; UTC ISO-8601 timestamp |
| `decision` | `allow`, `deny` or `reject` |
| `reason` | Machine-readable: `allowed`, `admin_tool_requires_admin_role`, `legacy_passthrough`, `client_response`, `missing_authorization`, `jwt_rejected`, `unknown_token`, `duplicate_key`, `unknown_method`, `header_mismatch`, `invalid_tool_name`, `unsupported_media_type`, ... |
| `subject` | `sha256:` plus 12 hex characters of the JWT `sub` (static mode: `static:` plus 12 hex characters of a per-process keyed HMAC-SHA256 of the token, so lines correlate within a run but a guessed token cannot be confirmed); never the raw value |
| `role`, `era` | `admin`/`viewer` or null; `legacy`/`modern` |
| `method`, `tool`, `rpc_id` | From the decoded body, after `safe_name` escaping (string ids too) |
| `request_id` | Generated per request and sent downstream as `X-Request-Id`, so the two logs correlate |
| `upstream_contacted` | A forward was attempted. `false` for every deny and reject. A 502/504 after an allow still reads `true`; the failure appears as an extra WARNING on logger `mcp_gateway.proxy` (exception class, `request_id`, outcome `downstream_unavailable` or `downstream_timeout`; no body or headers) |

Never logged: tool arguments, bodies, tokens, cookies, raw `Mcp-Session-Id` values. Every client-supplied string passes through `safe_name`, which escapes control and format characters and caps the value at 64 characters. The JSON encoder escapes CR/LF, so a tool named `x\nrefund decision=allow` cannot forge a second line. Auditing never raises: an unserializable field (a non-string dict key or a cycle; `default=str` absorbs everything else) degrades to an `unserializable_fields` record (`test_each_decision_writes_one_audit_record`, `test_audit_event_never_raises_on_odd_values`, `test_audit_event_unserializable_fields_fallback`). Source: `audit.py`.

## 11. Design notes

- **Pure ASGI, not FastAPI.** The gateway has one route and needs to control exactly when the body is read and what bytes leave. Raw ASGI middlewares make the order explicit (auth, then policy, then proxy), let the body be read once and replayed, and stream responses without a framework in the way. Starlette provides the routing, the body-size cap and `StreamingResponse`; httpx2 is the upstream client. All three are already dependencies of `mcp`, so there is nothing new to install.
- **Why JWT.** "Extract the user's role from the token" reads most naturally as reading a claim from a signed token. PyJWT with a pinned algorithm (`HS256`, which blocks `alg=none` and algorithm confusion) plus required `exp`, `aud` and `iss` demonstrates the checks RFC 8725 asks for. The secret must be at least 32 bytes and this is validated at startup, so a misconfigured gateway refuses to start instead of returning a 500 on every request. The static-token mode exists only for convenience testing.
- **Why strict I-JSON.** The authorization decision is only as good as the agreement between the gateway's parser and the downstream's. The SDK picks one of two different JSON parsers from the `MCP-Protocol-Version` header alone (pydantic-core on the legacy path, stdlib `json.loads` on the 2026-07-28 path), and the client controls that header. Rejecting anything outside the profile (RFC 7493) closes the gap regardless of which parser the downstream uses.
- **Why `tools/list` is transparent.** The brief asks for transparent forwarding, and hiding tools is not enforcement: a client can call a tool it never saw. Filtering would also require rewriting JSON and SSE responses, pagination cursors and caching headers. The enforcement point is `tools/call`, and it works whatever the list shows. Filtering is described in [Section 12](#12-production-hardening-described-not-built).
- **DNS-rebinding protection stays on in the mock.** The SDK enables it automatically for loopback binds. The gateway sets the `Host` header correctly instead of disabling a security control to make proxying work. Binding the mock to `0.0.0.0` silently disables the protection, so do not do that outside a trusted network.
- **Scope: "lightweight".** The brief asks for a gateway, not a platform, so only two behaviours are switchable (`GATEWAY_AUTH_MODE`, `GATEWAY_DENY_HTTP_STATUS`) and everything else is fixed at the brief's behaviour. Further options were considered and deliberately not built; they are listed in the next section.
- **Legacy GET and DELETE** (the server-to-client stream and session end of the 2025-11-25 transport) are authenticated and then forwarded. They carry no JSON-RPC request to authorize (`test_legacy_get_delete_forwarded_after_auth`).

## 12. Production hardening (described, not built)

Not implemented, on purpose. In rough priority order:

1. **Per-role tool allowlist instead of a prefix rule.** `startswith("admin_")` is a naming convention, not least privilege: it misses `admin-reset`, `admin.reset` and `reset_key`, and it fails open for any new tool whose name does not follow the convention. The least-privilege form maps each role to the tools it may call and denies the rest (Pomerium's MCP tool limits are built this way). The prefix rule is what the brief specifies, so it is the default here.
2. **Case-folding and a strict charset for tool names.** Against this SDK the literal match is complete: it resolves tool names by exact, case-sensitive comparison, and every case, whitespace, zero-width, fullwidth, Cyrillic and NUL variant returned "Unknown tool" (row 11 above). A different downstream might normalise names. The hardening is to reject names outside the SEP-986 charset `^[A-Za-z0-9._-]{1,128}$` with `-32602`, then apply the rule to `name.casefold()`. ASCII-only names make Unicode normalisation moot, and case-folding errs on the side of blocking too much. Never normalise a name and forward the normalised form: that authorizes one string and executes another.
3. **Per-role `tools/list` filtering.** This is hardening, not enforcement: it stops an agent planning around tools it cannot call and keeps admin tool descriptions and schemas out of the model's context (the 2026-07-28 tools spec allows a list to vary with the authorization presented; agentgateway and Kong both filter). Costs: buffering and rewriting only `tools/list` responses in both JSON and SSE framing, handling empty pages with a `nextCursor`, `notifications/tools/list_changed`, and marking caller-specific lists `cacheScope: "private"` so a shared cache cannot leak one role's list to another. A test must still prove that a viewer who learns `admin_reset_key` from an unfiltered list gets `-32001` when calling it.
4. **Tell the downstream who is calling.** Today the downstream sees an anonymous gateway: the mock enforces nothing, and the token is stripped. A real deployment should send a service credential (or a signed identity header that the downstream trusts only from the gateway's network position, with client-supplied copies stripped, which the `X-Gateway-*` drop already does) so the downstream can apply its own authorization and audit.
5. **403 versus 200 for denials.** 200 follows the brief and JSON-RPC convention; 403 is closer to the MCP authorization spec (and to Kong's behaviour), and lets HTTP-level tooling (WAFs, metrics, alerting) see denials. `GATEWAY_DENY_HTTP_STATUS=403` provides it. Neither fixes the `-32001` band concern in [Section 5](#5-authorization-rules-and-the--32001-response).
6. **JWKS and asymmetric keys.** A shared HS256 secret means the gateway can mint what it verifies and every verifier holds the signing key. Production should verify RS256/ES256 tokens from an identity provider's JWKS endpoint with key rotation, and add token revocation and short lifetimes.
7. **Connection pool sizing.** Long-lived legacy GET streams share httpx2's default pool (100 connections) with POSTs, so enough open streams can starve requests. Production would set explicit `httpx2.Limits` or use separate pools for streams and requests.
8. **Other items left out:** rate limiting and per-principal quotas, TLS termination and mTLS to the downstream, request timeouts per method, policy loaded from configuration rather than code, metrics, and an optional strict mode that refuses unknown tool names rather than leaving that to the downstream. Unknown-method forwarding, legacy GET/DELETE refusal and an `X-Gateway-Role` header were considered and not built; their defaults (deny, forward after auth, nothing sent) are the safe ones.

## 13. Test evidence

Final run from `task2-mcp-gateway/`:

```
$ uv sync --locked
$ uv run ruff check .
All checks passed!
$ uv run pytest -q -W error
238 passed in 2.1s
```

`uv run pytest --collect-only -q` collects 238 tests. The layers are: unit tests (`test_jsonrpc`, `test_policy`, `test_auth`, `test_config`, `test_tokens`), the gateway through an ASGI transport with a recording mock upstream (`test_gateway`), the gateway against the real SDK mock (`test_integration`, `test_mock_mcp`), incremental SSE relay and disconnect handling (`test_streaming`), and end to end with real servers and the official SDK client in both protocol eras (`test_e2e`).

Test names by file (parametrized tests are abbreviated to one line with the case count):

**`tests/test_auth.py`** (23)

- `test_realm_constant`
- `test_extract_bearer_errors` (8 cases)
- `test_extract_bearer_scheme_case_insensitive`
- `test_valid_jwt_gives_principal`
- `test_invalid_tokens_are_401_invalid_token` (10 cases)
- `test_static_verifier`
- `test_build_verifier_selects_by_mode`

**`tests/test_config.py`** (25)

- `test_defaults_with_jwt_secret`
- `test_jwt_mode_requires_32_byte_secret` (2 cases)
- `test_static_mode_parses_tokens`
- `test_rejects_invalid` (10 cases)
- `test_errors_never_leak_token_values`
- `test_downstream_url_must_be_http_with_host` (4 cases)
- `test_downstream_url_accepted` (3 cases)
- `test_deny_status_403_opt_in`
- `test_repr_hides_jwt_secret`
- `test_repr_hides_static_tokens`

**`tests/test_e2e.py`** (5)

- `test_admin_lists_and_calls_admin_tool` (2 cases)
- `test_viewer_allowed_tool_ok_and_admin_tool_denied` (2 cases)
- `test_main_refuses_short_secret`

**`tests/test_gateway.py`** (73)

- `test_viewer_admin_tool_denied_without_upstream_call`
- `test_string_id_echoed_with_type`
- `test_deny_status_403_opt_in`
- `test_admin_admin_tool_forwarded_byte_identical`
- `test_viewer_tools_list_forwarded_transparently`
- `test_bypass_bodies_never_reach_downstream` (26 cases)
- `test_modern_era_unknown_method_is_404`
- `test_auth_failures_never_reach_downstream` (12 cases)
- `test_unparseable_body_without_token_is_401_not_400`
- `test_mirrored_header_mismatch_rejected`
- `test_wrong_content_type_415` (4 cases)
- `test_json_charset_utf8_accepted`
- `test_oversized_body_413_and_large_legit_body_forwarded`
- `test_other_paths_404` (4 cases)
- `test_other_methods_405_json_and_audited` (5 cases)
- `test_unauthenticated_head_is_401`
- `test_forwarded_headers_are_filtered`
- `test_response_headers_relayed`
- `test_downstream_errors_relayed_verbatim` (2 cases)
- `test_downstream_unreachable_502`
- `test_downstream_timeout_504`
- `test_legacy_get_delete_forwarded_after_auth`
- `test_static_token_mode`
- `test_session_id_does_not_carry_role`
- `test_each_decision_writes_one_audit_record`
- `test_lifespan_created_client_is_closed_and_defaults_cleared`

**`tests/test_integration.py`** (4)

- `test_viewer_admin_call_denied_and_never_reaches_mock`
- `test_admin_admin_call_reaches_mock`
- `test_viewer_tools_list_is_transparent`
- `test_duplicate_key_body_rejected_and_nothing_called`

**`tests/test_jsonrpc.py`** (53)

- `test_parse_rejects_with_32700` (16 cases)
- `test_depth_64_is_accepted`
- `test_parse_strict_raises_only_value_errors`
- `test_classify_rejects_with_32600` (12 cases)
- `test_escaped_name_is_decoded`
- `test_notification_and_response_kinds`
- `test_classify_returns_frozen_message`
- `test_error_body_shape`
- `test_constants`
- `test_accepted_bodies_decode_identically_in_both_sdk_parsers` (18 cases)

**`tests/test_mock_mcp.py`** (3)

- `test_tools_list_has_three_tools`
- `test_call_increments_counter`
- `test_foreign_host_rejected_421`

**`tests/test_policy.py`** (43)

- `test_viewer_denied_admin_tool`
- `test_admin_allowed_admin_tool`
- `test_viewer_allowed_normal_tool_and_tools_list`
- `test_response_allowed`
- `test_literal_rule_lets_variants_through_to_exact_matching_downstream` (5 cases)
- `test_tools_call_without_id_rejected`
- `test_bad_tools_call_params_32602` (5 cases)
- `test_unknown_method_denied` (4 cases)
- `test_unknown_method_notification_is_400_in_both_eras`
- `test_notifications_prefix_allowed`
- `test_mirrored_header_mismatch`
- `test_mirrored_header_uri_and_prompts`
- `test_mirrored_header_base64_decoded`
- `test_mirrored_header_bad_encoding_is_mismatch` (2 cases)
- `test_mirrored_header_non_ascii_bytes_is_mismatch_not_exception`
- `test_mirrored_header_bad_params_is_mismatch`
- `test_protocol_era` (9 cases)
- `test_audit_event_is_single_escaped_json_line`
- `test_audit_event_never_raises_on_odd_values`
- `test_audit_event_unserializable_fields_fallback` (2 cases)
- `test_safe_name_caps_at_64`
- `test_safe_name_escapes_control_and_format_chars`

**`tests/test_streaming.py`** (4)

- `test_sse_is_relayed_incrementally`
- `test_client_disconnect_closes_upstream` (2 cases)
- `test_upstream_failure_mid_stream_closes_upstream`

**`tests/test_tokens.py`** (5)

- `test_mint_token_claims`
- `test_main_prints_verifiable_token`
- `test_main_honours_audience_and_issuer_env`
- `test_main_missing_secret_exits_2`
- `test_main_short_secret_exits_2`
