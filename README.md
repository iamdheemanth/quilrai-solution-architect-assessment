# QuilrAI Solution Architect Assessment

Solutions for the four-part QuilrAI Solution Architect take-home. Each task lives in its own folder with its own README.

| Task | Description | Status |
|---|---|---|
| 1 | [Customer-service MCP server over stdio](task1-mcp-server/) | Done |
| 2 | [MCP security gateway: bearer auth, strict JSON-RPC parsing, method-level tool authorization (`-32001`), streaming proxy](task2-mcp-gateway/) | Done |
| 3 | [Streaming PII-redaction LLM gateway: viable-prefix holdback redacts emails, SSNs and card numbers across SSE chunk boundaries with bounded memory](task3-llm-gateway/) | Done |
| 4 | Rate-limiting and model-fallback router (SQLite) | Coming |

Prerequisites: Python 3.12 and [uv](https://docs.astral.sh/uv/). Each task folder is self-contained; run `uv sync` inside it.
