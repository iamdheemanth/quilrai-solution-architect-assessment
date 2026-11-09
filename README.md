# QuilrAI Solution Architect Assessment

Solutions for the four-part QuilrAI Solution Architect take-home. Each task lives in its own folder with its own README.

| Task | Description | Status |
|---|---|---|
| 1 | [Customer-service MCP server over stdio](task1-mcp-server/) | Done |
| 2 | [MCP security gateway: bearer auth, strict JSON-RPC parsing, method-level tool authorization (`-32001`), streaming proxy](task2-mcp-gateway/) | Done |
| 3 | [Streaming PII-redaction LLM gateway: viable-prefix holdback redacts emails, SSNs and card numbers across SSE chunk boundaries with bounded memory](task3-llm-gateway/) | Done |
| 4 | [Token-aware rate-limiting and model-fallback router: SQLite sliding-log limiter (`BEGIN IMMEDIATE`, reserve/reconcile/refund), 3000 ms total-deadline failover on 429 or timeout, sanitized error contract](task4-llm-router/) | Done |

Prerequisites: Python 3.12 and [uv](https://docs.astral.sh/uv/). Each task folder is self-contained; run `uv sync` inside it.
