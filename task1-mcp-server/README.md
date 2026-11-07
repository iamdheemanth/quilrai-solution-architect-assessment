# Task 1: Customer-service MCP server (stdio)

## What it is

A Python MCP server that talks JSON-RPC over stdio and exposes two tools against an in-memory customer database: `get_customer_record` (read-only lookup) and `trigger_refund` (moves money). It is built on the official `mcp` SDK (2.3.x) using the low-level `Server`, with strict Pydantic validation at the tool boundary. stdout carries JSON-RPC only; all logs go to stderr. Error handling follows the MCP 2025-11-25 tools spec, with a switch for the stricter protocol-level reading (see [Error policy](#error-policy)).

## Quick start

Requires Python 3.12 and [uv](https://docs.astral.sh/uv/).

```bash
uv sync                  # install dependencies
uv run customer-mcp      # run the server on stdio (waits for JSON-RPC on stdin)
uv run pytest -v         # 83 tests, about 10 s (many spawn the server as a subprocess)
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
        "MCP_VALIDATION_ERRORS": "tool",
        "CUSTOMER_ID_CHARSET": "digits"
      }
    }
  }
}
```

On Windows use a path such as `C:\\path\\to\\task1-mcp-server`. The `env` block is optional; the values shown are the defaults.

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
| `MCP_VALIDATION_ERRORS` | `tool`, `protocol` | `tool` | How argument-validation failures are reported (see below) |
| `CUSTOMER_ID_CHARSET` | `digits`, `alnum` | `digits` | `^CUST-[0-9]{5}$` or `^CUST-[A-Z0-9]{5}$` |
| `LOG_LEVEL` | `DEBUG`, `INFO`, `WARNING`, `ERROR` | `INFO` | stderr log level |

## Error policy

| Situation | Response |
|---|---|
| Unknown tool name | JSON-RPC error `-32602`, `"Unknown tool: <name>"` (both modes; name truncated to 64 chars) |
| Arguments fail a tool's rules, mode `tool` (default) | Result with `isError: true`; text lists each bad field; no `structuredContent` |
| Arguments fail a tool's rules, mode `protocol` | JSON-RPC error `-32602`, `"Invalid arguments for tool <name>"`, `data: {"errors": [{field, code, message}, ...]}` |
| Business rule: customer not found, account suspended, amount over balance | Result with `isError: true` and a readable message |
| Unexpected exception | JSON-RPC error `-32603`, `"Internal error"`; traceback goes to stderr only |
| Parse errors, unknown methods, malformed `tools/call` envelope, requests before `initialize` | Left to the SDK (see [Observed SDK behaviour](#observed-sdk-behaviour)) |

Error messages name the offending fields and never echo the rejected value or the refund `reason`; logs record field names only.

### Why `isError` and not `-32602` by default

MCP 2025-11-25 ([Tools, Error Handling](https://modelcontextprotocol.io/specification/2025-11-25/server/tools)) limits protocol errors to unknown tools, requests that fail the `CallToolRequest` schema, and server errors. "Input validation errors (e.g., date in wrong format, value out of range)" are to be returned as tool execution errors with `isError: true`. This resolved an ambiguity in 2025-06-18 via [SEP-1303](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/1303); the 2026-07-28 revision keeps the classification. The rationale is that the model can see a tool result and self-correct, but typically cannot see a JSON-RPC error. A malformed `customer_id` and an out-of-range `amount` are exactly the spec's own examples, so `tool` mode is the default. Both official SDKs' high-level APIs behave the same way.

Setting `MCP_VALIDATION_ERRORS=protocol` gives the literal "invalid params means `-32602`" behaviour, with a machine-readable field list in `error.data`. The spec language here is guidance, so this is a legitimate choice, but it has a cost: clients only MAY pass protocol errors to the model, so a model may never see the validation message. Business outcomes stay `isError: true` in both modes.

## STDIO isolation

The stdio transport requires that the server "MUST NOT write anything to its stdout that is not a valid MCP message" and allows stderr for logging ([Transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports)). Enforcement:

- Logging is configured with `logging.basicConfig(stream=sys.stderr, force=True)` before the server starts; SDK loggers are held at WARNING.
- Ruff rule `T20` forbids `print` in the codebase (`uv run ruff check .`).
- Nothing rebinds `sys.stdout`.
- The SDK's stdio transport claims file descriptor 1 for the wire and points the process-level fd 1 at a copy of stderr, so stray writes from other code, including C extensions, land on stderr rather than in the protocol stream.
- Tracebacks, including crashes, go to stderr.

Proof is black-box: `tests/test_stdio.py` spawns the real server, speaks raw newline-delimited JSON-RPC, and asserts that every stdout line parses as JSON-RPC, that logs appear only on stderr, and that malformed input does not break the session.

## Observed SDK behaviour

Recorded by the stdio harness on `mcp` 2.3.0; the server adds no handling for these cases.

| Input | Result |
|---|---|
| Non-JSON line after `initialize` | No reply, no stderr output at INFO; server keeps serving |
| JSON array (batch), e.g. `[1, 2]` | No reply; server keeps serving |
| Request before `initialize` | Error `-32602` `"Invalid request parameters"`; a later `initialize` and requests work normally |
| Unknown method | Error `-32601` |
| Notification | No response |
| stdin EOF | Process exits with code 0 |

## Design notes

- **Low-level `Server`, not the high-level `MCPServer`.** The high-level layer builds its input model from the function signature: it coerces `"100"` to `100.0` and ignores unknown keys, which defeats the strictness requirements. Here `tools/list` and `tools/call` are registered by hand and arguments go through Pydantic models with `strict=True, extra="forbid"`.
- **`Decimal` for money.** Balances are held as `Decimal` quantized to cents, so repeated refunds don't accumulate float error. Floats appear only at the JSON boundary.
- **Annotations.** `get_customer_record` is marked read-only and idempotent; `trigger_refund` is marked destructive and non-idempotent so clients can gate it behind confirmation.
- **No idempotency key.** The brief specifies three inputs, so a retried `trigger_refund` issues a second refund. A production version would add an optional `idempotency_key` argument, store `key -> receipt`, and return the stored receipt on replay (rejecting a replay whose amount or customer differs).
- **Single process, in-memory state.** There is no locking because the stdio server handles one client; a networked version would need transactional storage.

## Spec versions

Built for MCP 2025-11-25 (handshake-based) on `mcp` 2.3.x; the test harness negotiates `2025-11-25`. Revision 2026-07-28 redesigns the protocol to be stateless (no `initialize` handshake, per-request version and capabilities, `server/discover`). The behaviours that matter here (error classification, stdout purity, exit on stdin EOF) are unchanged by it, but this project was exercised only with the 2025-11-25 handshake flow.

## Evidence: test suite

`uv sync --locked && uv run ruff check . && uv run pytest -v` gives `All checks passed!` and `83 passed`. Test names from `uv run pytest --collect-only -q`, with parametrized cases abbreviated:

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
  test_reason_rejects["too short" | ten spaces | "  short   " | 501 chars | 1234567890 (number)]
  test_reason_accepts_exactly_10_chars
  test_extra_and_missing_fields_rejected
  test_published_schema_is_strict
  test_rendered_message_names_fields_but_never_echoes_input
  test_alnum_hint_wording

tests/test_server.py
  test_tools_list_publishes_strict_schemas_and_annotations
  test_get_customer_record_success
  test_trigger_refund_success
  test_invalid_args_tool_mode_returns_is_error_with_fields
  test_missing_arguments_reports_required_fields
  test_invalid_args_protocol_mode_raises_32602_with_data
  test_unknown_tool_is_32602_in_both_modes[tool | protocol]
  test_business_error_is_tool_error
  test_alnum_mode_accepts_letter_ids
  test_unexpected_exception_is_32603_without_leak
  test_unknown_tool_name_is_never_logged_raw
  test_unknown_tool_name_echo_is_truncated

tests/test_stdio.py
  test_handshake_and_tools_list
  test_notification_gets_no_response
  test_bad_args_return_is_error_result[3 cases]
  test_protocol_mode_bad_args_return_32602
  test_unknown_tool_32602
  test_unknown_method_32601
  test_junk_input_does_not_break_server
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

83 passed
```
