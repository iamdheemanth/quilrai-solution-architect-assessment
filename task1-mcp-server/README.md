# Task 1: Customer-service MCP server (stdio)

## What it is

A Python MCP server that talks JSON-RPC over stdio and exposes two tools against an in-memory customer database: `get_customer_record` (read-only lookup) and `trigger_refund` (moves money). It is built on the official `mcp` SDK (2.3.x) using the low-level `Server`, with strict Pydantic validation at the tool boundary. stdout carries JSON-RPC only; all logs go to stderr. Invalid input is rejected with standard JSON-RPC error codes (`-32602` for bad arguments, `-32700`/`-32600` for unparseable messages), as the brief requires, with a switch to the MCP 2025-11-25 tool-error style (see [Error policy](#error-policy)).

## Quick start

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                  # install dependencies
uv run customer-mcp      # run the server on stdio (waits for JSON-RPC on stdin)
uv run pytest -v         # 104 tests, about 13 s (many spawn the server as a subprocess)
```

Tool-error mode (`isError: true` results for invalid arguments, the MCP 2025-11-25 recommendation) instead of the default `-32602`:

```bash
MCP_VALIDATION_ERRORS=tool uv run customer-mcp                      # bash
```
```powershell
$env:MCP_VALIDATION_ERRORS="tool"; uv run customer-mcp   # PowerShell
```

Smoke test with the MCP Inspector CLI (needs Node):

```bash
npx @modelcontextprotocol/inspector --cli uv run customer-mcp --method tools/list
npx @modelcontextprotocol/inspector --cli uv run customer-mcp --method tools/call \
  --tool-name get_customer_record --tool-arg customer_id=CUST-00001
```

The Inspector's `--tool-arg` coerces argument types (everything on the command line is text), so it cannot demonstrate type strictness. That is proven by `tests/test_stdio.py`, which sends raw JSON-RPC.

## Claude Desktop / MCP client config

```json
{
  "mcpServers": {
    "customer-service": {
      "command": "uv",
      "args": ["--directory", "/absolute/path/to/task1-mcp-server", "run", "customer-mcp"],
      "env": {
        "MCP_VALIDATION_ERRORS": "protocol",
        "CUSTOMER_ID_CHARSET": "digits"
      }
    }
  }
}
```

On Windows use a path such as `C:\\path\\to\\task1-mcp-server`. The `env` block is optional; the values shown are the defaults. GUI clients (for example Claude Desktop on macOS) may not inherit `PATH`; if `uv` is not found, use the absolute path from `which uv` (`where uv` on Windows) as `"command"`.

## Tools

### `get_customer_record`

Read-only. Annotations: `readOnlyHint=true`, `destructiveHint=false`, `idempotentHint=true`, `openWorldHint=false`.

| Input | Rule |
|---|---|
| `customer_id` | string matching `^CUST-[0-9]{5}$` (ASCII digits only, case-sensitive, no surrounding whitespace or trailing newline) |

Output (`structuredContent`, also JSON-encoded in a text block): `customer_id`, `name`, `email`, `tier` (`standard` / `gold` / `platinum`), `account_status` (`active` / `suspended`), `refundable_balance`.

### `trigger_refund`

Moves money and is not idempotent: each successful call creates a new refund. Annotations: `readOnlyHint=false`, `destructiveHint=true`, `idempotentHint=false`, `openWorldHint=false`.

| Input | Rule |
|---|---|
| `customer_id` | same as above |
| `amount` | JSON number, > 0, finite, at most 2 decimal places. JSON integers are accepted; strings and booleans are rejected |
| `reason` | string of 10 to 500 characters, with at least 10 left after trimming whitespace and dropping control or zero-width (Unicode Cc/Cf) characters |

Unknown fields are rejected on both tools (`additionalProperties: false` in the published schema, enforced server-side too). Missing or `null` `arguments` report the missing fields rather than crashing.

Output: `refund_id` (`RF-000001`, incrementing), `customer_id`, `amount`, `status` (`processed`), `remaining_refundable_balance`.

Business rules, checked in this order: customer must exist; account must not be suspended; amount must not exceed the refundable balance. A full-balance refund is allowed. A rejected refund leaves the balance unchanged. A huge but valid amount such as `1e300` passes validation and is refused as exceeding the refundable balance.

### Seed customers

| ID | Name | Tier | Status | Refundable balance |
|---|---|---|---|---|
| `CUST-00001` | Ada Lovelace | gold | active | 500.00 |
| `CUST-00002` | Alan Turing | standard | active | 120.50 |
| `CUST-00003` | Grace Hopper | platinum | suspended | 1000.00 |
| `CUST-A1B2C` | Katherine Johnson | standard | active | 75.00 |

`CUST-A1B2C` is only addressable when `CUSTOMER_ID_CHARSET=alnum`; in the default mode its ID fails validation. State lives in memory and resets on restart.

## Configuration

Read once at startup, case-insensitively. An invalid value prints a message to stderr and exits with code `2`.

| Variable | Values | Default | Effect |
|---|---|---|---|
| `MCP_VALIDATION_ERRORS` | `protocol`, `tool` | `protocol` | How argument-validation failures are reported (see below) |
| `CUSTOMER_ID_CHARSET` | `digits`, `alnum` | `digits` | `^CUST-[0-9]{5}$` or `^CUST-[A-Z0-9]{5}$` |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR` | `INFO` | stderr log level |

## Error policy

| Situation | Response |
|---|---|
| Line that is not valid JSON | JSON-RPC error `-32700` `"Parse error"`, `id: null` |
| Valid JSON that is not a JSON-RPC message (batch array, `params` of the wrong type, non-string `method`) | JSON-RPC error `-32600` `"Invalid Request"`, `id: null` |
| Unknown method | JSON-RPC error `-32601` (SDK) |
| Malformed `tools/call` envelope (`arguments` not an object, `name` missing or not a string, `params` omitted) | JSON-RPC error `-32602` `"Invalid request parameters"` (SDK) |
| Unknown tool name | JSON-RPC error `-32602`, `"Unknown tool: <name>"` (both modes; name truncated to 64 chars) |
| Arguments fail a tool's rules, mode `protocol` (default) | JSON-RPC error `-32602`, `"Invalid arguments for tool <name>"`, `data: {"errors": [{field, code, message}, ...]}` |
| Arguments fail a tool's rules, mode `tool` | Result with `isError: true`; text lists each bad field; no `structuredContent` |
| Business rule: customer not found, account suspended, amount over balance | Result with `isError: true` and a readable message |
| Unexpected exception | JSON-RPC error `-32603`, `"Internal error"`; traceback goes to stderr only |

Error messages name the offending fields and never echo rejected argument values or the `reason`; unknown tool names and unrecognised argument names are echoed back escaped and truncated to 64 characters. Logs record field names only (same escaping and cap), folded into the single outcome line of the call. Rejected unparseable lines are never logged.

### Why `-32602` by default, and what `tool` mode is for

The brief asks for invalid formats to be rejected "with standard MCP JSON-RPC error codes", so the default answers a schema violation with `-32602 Invalid params` and a machine-readable field list in `error.data`. Business outcomes (unknown customer, suspended account, amount over balance) are not malformed input, so they stay tool results with `isError: true` in both modes.

MCP 2025-11-25 ([Tools, Error Handling](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)) recommends a different split: protocol errors only for unknown tools, requests that fail the `CallToolRequest` schema, and server errors, while "input validation errors (e.g., date in wrong format, value out of range)" SHOULD be returned as tool execution errors with `isError: true`. This resolved an ambiguity in 2025-06-18 via [SEP-1303](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/1303), and the 2026-07-28 revision keeps it. The rationale is that the model sees a tool result and can self-correct, while clients only MAY pass a JSON-RPC error on to the model. `MCP_VALIDATION_ERRORS=tool` gives exactly that behaviour; it is what the official SDKs' high-level APIs do by default. The two modes differ only in how a failed argument check is reported; validation itself is identical.

## STDIO isolation

The stdio transport requires that the server "MUST NOT write anything to its stdout that is not a valid MCP message" and allows stderr for logging ([Transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)). Enforcement:

- Logging is configured with `logging.basicConfig(stream=sys.stderr, force=True)` before the server starts; SDK loggers are held at WARNING.
- Ruff rule `T20` forbids `print` in the codebase (`uv run ruff check .`).
- Nothing rebinds `sys.stdout`.
- The SDK's stdio transport claims file descriptor 1 for the wire and points the process-level fd 1 at a copy of stderr, so stray writes from other code, including C extensions, land on stderr rather than in the protocol stream.
- Tracebacks, including crashes, go to stderr.

Proof is black-box: `tests/test_stdio.py` spawns the real server, speaks raw newline-delimited JSON-RPC, and asserts that every stdout line is valid UTF-8 and parses as JSON-RPC, that logs appear only on stderr, and that malformed input does not break the session. `test_stray_prints_cannot_corrupt_stdout` runs the server with a handler that calls `print("STRAY-PRINT")` and `os.write(1, b"STRAY-OSWRITE\n")`: the tool call still succeeds, neither string appears on stdout, and both appear on stderr, which demonstrates the fd-1 diversion directly.

## Malformed input on the wire

Every case below is exercised by `tests/test_stdio.py` against the real process; in all of them the server keeps serving.

| Input | Result | Produced by |
|---|---|---|
| Non-JSON line | `-32700` `"Parse error"`, `id: null` | `transport.py` |
| JSON array (batch), e.g. `[1, 2]` | `-32600` `"Invalid Request"`, `id: null` (batching was removed from MCP in 2025-06-18) | `transport.py` |
| Request whose `params` is an array or string | `-32600` `"Invalid Request"`, `id: null` | `transport.py` |
| `tools/call` with `arguments` as a string or array, `name` missing or not a string, or `params` omitted | `-32602` `"Invalid request parameters"` | SDK |
| Request before `initialize` | `-32602` `"Invalid request parameters"`; a later `initialize` and requests work normally | SDK |
| Unknown method | `-32601` | SDK |
| Notification | No response | SDK |
| stdin EOF | Process exits with code 0 | SDK |

Why `transport.py` exists: SDK 2.3.0's stdio reader turns a line it cannot parse into an exception item and the dispatcher drops it, so the client would get no reply and wait until it timed out. JSON-RPC 2.0 section 5 requires `-32700` for invalid JSON and `-32600` for an invalid request object, with `id: null` when the id cannot be determined. `transport.py` wraps the SDK's `stdio_server()` (keeping its fd-level stdout protection) and puts a small filter between the reader and the server: parse failures are answered with the right code, and everything else is passed through untouched. It relies only on the SDK's public stream protocol, and the rejected line is never logged.

## Design notes

- **Low-level `Server`, not the high-level `MCPServer`.** The high-level layer builds its input model from the function signature: it coerces `"100"` to `100.0` and ignores unknown keys, which defeats the strictness requirements. It also turns every argument failure into an `isError` result, so it cannot produce `-32602` for bad input. Here `tools/list` and `tools/call` are registered by hand and arguments go through Pydantic models with `strict=True, extra="forbid"`.
- **Module layout.** `config.py` (environment settings), `schemas.py` (strict argument and output models), `validation.py` (non-leaking error messages), `store.py` (in-memory customers and refund rules), `server.py` (tool registry and error mapping), `transport.py` (stdio with JSON-RPC parse errors), `__main__.py` (logging setup and startup).
- **`Decimal` for money.** Balances are held as `Decimal` quantized to cents, so repeated refunds don't accumulate float error. Floats appear only at the JSON boundary.
- **Annotations.** `get_customer_record` is marked read-only and idempotent; `trigger_refund` is marked destructive and non-idempotent so clients can gate it behind confirmation.
- **No idempotency key.** The brief specifies three inputs, so a retried `trigger_refund` issues a second refund. A production version would add an optional `idempotency_key` argument, store `key -> receipt`, and return the stored receipt on replay (rejecting a replay whose amount or customer differs).
- **Single process, in-memory state.** There is no locking because handlers never `await` between the balance check and the debit, so on a single event loop each refund runs atomically even though the SDK runs every request as its own task and one client can have several calls in flight. A networked or multi-process version would need transactional storage.

## Spec versions

Built for MCP 2025-11-25 (handshake-based) on `mcp` 2.3.x; the test harness negotiates `2025-11-25`. Revision 2026-07-28 redesigns the protocol to be stateless (no `initialize` handshake, per-request version and capabilities, `server/discover`). The behaviours that matter here (error codes, stdout purity, exit on stdin EOF) are unchanged by it, but this project was exercised only with the 2025-11-25 handshake flow.

## Evidence: test suite

`uv sync --locked && uv run ruff check . && uv run pytest -v` gives `All checks passed!` and `104 passed`. Test names from `uv run pytest --collect-only -q`, with parametrized cases abbreviated:

```
tests/test_config.py
  test_defaults_when_env_empty
  test_reads_and_normalises_values
  test_rejects_unknown_values[MCP_VALIDATION_ERRORS-strict | CUSTOMER_ID_CHARSET-hex | LOG_LEVEL-LOUD]

tests/test_schemas.py
  test_digits_accepts[CUST-00001 | CUST-99999]
  test_digits_rejects[CUST-1234 | CUST-123456 | cust-00001 | " CUST-00001" | "CUST-00001\n"
                      | Arabic-Indic digits | CUST-A1B2C | CUST_00001 | "" | 12345]
  test_alnum_accepts_letters
  test_alnum_rejects[CUST-a1b2c | cust-A1B2C | CUST-A1B2]
  test_amount_accepts_json_int_as_float
  test_amount_rejects["50" | True | 0 | -1 | 1e-09 | 10.005 | inf | nan | None]
  test_reason_rejects["too short" | ten spaces | "  short   " | 501 chars | 1234567890 (number)
                      | ten NULs | ten zero-width spaces | 5 zero-width + "abcd"]
  test_reason_accepts_exactly_10_chars
  test_reason_accepts_embedded_tab_and_newline
  test_extra_and_missing_fields_rejected
  test_published_schema_is_strict
  test_rendered_message_names_fields_but_never_echoes_input
  test_alnum_hint_wording
  test_huge_extra_key_is_capped
  test_extra_key_with_newline_is_escaped
  test_issue_list_is_capped_at_20

tests/test_server.py
  test_tools_list_publishes_strict_schemas_and_annotations
  test_get_customer_record_success
  test_trigger_refund_success
  test_invalid_args_tool_mode_returns_is_error_with_fields
  test_missing_arguments_reports_required_fields
  test_missing_arguments_default_mode_is_32602
  test_invalid_args_default_mode_raises_32602_with_data
  test_unknown_tool_is_32602_in_both_modes[tool | protocol]
  test_business_error_is_tool_error
  test_alnum_mode_accepts_letter_ids
  test_unexpected_exception_is_32603_without_leak
  test_unknown_tool_name_is_never_logged_raw
  test_unknown_tool_name_echo_is_truncated
  test_invalid_call_emits_exactly_one_log_record[tool | protocol]

tests/test_stdio.py
  test_handshake_and_tools_list
  test_notification_gets_no_response
  test_default_mode_bad_args_return_32602[3 cases]
  test_tool_mode_bad_args_return_is_error_result[3 cases]
  test_unknown_tool_32602
  test_unknown_method_32601
  test_junk_input_gets_jsonrpc_errors_and_server_keeps_serving
  test_wrong_type_params_get_invalid_request
  test_malformed_tools_call_arguments_is_32602
  test_stray_prints_cannot_corrupt_stdout
  test_request_before_initialize_does_not_break_server
  test_logs_go_to_stderr_only
  test_crash_traceback_on_stderr_not_stdout
  test_exits_cleanly_on_stdin_eof
  test_invalid_config_exits_2_with_stderr_message

tests/test_store.py
  test_errors_are_business_errors
  test_get_returns_record
  test_seeded_includes_alnum_and_suspended
  test_get_unknown_raises
  test_refund_unknown_raises
  test_refund_decrements_balance_exactly
  test_refund_full_balance_allowed
  test_refund_over_balance_rejected
  test_over_balance_message
  test_refund_suspended_rejected
  test_suspended_checked_before_balance
  test_rejected_refund_leaves_balance_unchanged
  test_seeded_stores_are_independent
  test_huge_amount_rejected_as_business_error[1e+30 | 1e+300]

tests/test_transport.py
  test_invalid_json_is_parse_error
  test_valid_json_that_is_not_a_message_is_invalid_request[4 cases]
  test_unexpected_exception_type_is_invalid_request

104 passed
```
