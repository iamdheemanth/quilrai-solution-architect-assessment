"""Low-level MCP server: tools/list, tools/call and the error-mapping policy."""

import json
import logging
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass

import mcp_types as types
from mcp.server.lowlevel import Server
from mcp.shared.exceptions import MCPError
from pydantic import BaseModel, ValidationError

from customer_mcp.config import Settings
from customer_mcp.schemas import (
    CustomerRecord,
    RefundReceipt,
    StrictArgs,
    build_arg_models,
)
from customer_mcp.store import BusinessError, CustomerStore
from customer_mcp.validation import describe_validation_error, render_issues

SERVER_NAME = "customer-service-mcp"
SERVER_VERSION = "0.1.0"

log = logging.getLogger("customer_mcp.server")
_MAX_ECHO = 64


@dataclass(frozen=True)
class _ToolSpec:
    name: str
    description: str
    args_model: type[StrictArgs]
    output_model: type[BaseModel]
    annotations: types.ToolAnnotations
    run: Callable[[StrictArgs], BaseModel]


def _log_call(name: str, outcome: str, started: float) -> None:
    ms = int((time.perf_counter() - started) * 1000)
    log.info("tool=%s outcome=%s duration_ms=%d", name, outcome, ms)


def build_server(settings: Settings, store: CustomerStore) -> Server:
    models = build_arg_models(settings.customer_id_charset)

    def run_get(args: StrictArgs) -> BaseModel:
        return store.get(args.customer_id)  # type: ignore[attr-defined]

    def run_refund(args: StrictArgs) -> BaseModel:
        receipt = store.refund(
            args.customer_id,  # type: ignore[attr-defined]
            args.amount,  # type: ignore[attr-defined]
            args.reason,  # type: ignore[attr-defined]
        )
        log.info(
            "refund_issued refund_id=%s customer_id=%s amount=%s",
            receipt.refund_id,
            receipt.customer_id,
            receipt.amount,
        )
        return receipt

    specs = {
        spec.name: spec
        for spec in (
            _ToolSpec(
                name="get_customer_record",
                description=(
                    "Look up a customer's profile (name, email, tier, account status and "
                    "refundable balance) by customer ID. Read-only."
                ),
                args_model=models.get_customer_record,
                output_model=CustomerRecord,
                annotations=types.ToolAnnotations(
                    read_only_hint=True,
                    destructive_hint=False,
                    idempotent_hint=True,
                    open_world_hint=False,
                ),
                run=run_get,
            ),
            _ToolSpec(
                name="trigger_refund",
                description=(
                    "Issue a refund against a customer's refundable balance. Moves money and "
                    "is not idempotent: each call creates a new refund. Rejected for "
                    "suspended accounts or amounts above the refundable balance."
                ),
                args_model=models.trigger_refund,
                output_model=RefundReceipt,
                annotations=types.ToolAnnotations(
                    read_only_hint=False,
                    destructive_hint=True,
                    idempotent_hint=False,
                    open_world_hint=False,
                ),
                run=run_refund,
            ),
        )
    }

    async def list_tools(ctx, params) -> types.ListToolsResult:
        return types.ListToolsResult(
            tools=[
                types.Tool(
                    name=s.name,
                    description=s.description,
                    input_schema=s.args_model.model_json_schema(),
                    output_schema=s.output_model.model_json_schema(),
                    annotations=s.annotations,
                )
                for s in specs.values()
            ]
        )

    def tool_error(text: str) -> types.CallToolResult:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=text)], is_error=True
        )

    async def call_tool(ctx, params: types.CallToolRequestParams) -> types.CallToolResult:
        started = time.perf_counter()
        name = params.name
        spec = specs.get(name)
        if spec is None:
            # Client-controlled: never log the name (log forging); cap the wire echo.
            _log_call("<unknown>", "unknown_tool", started)
            shown = name if len(name) <= _MAX_ECHO else name[:_MAX_ECHO] + "…"
            raise MCPError(types.INVALID_PARAMS, f"Unknown tool: {shown}")

        try:
            args = spec.args_model.model_validate(params.arguments or {})
        except ValidationError as exc:
            issues = describe_validation_error(exc, settings.customer_id_charset)
            log.info("invalid arguments for %s: fields=%s", name, [i.field for i in issues])
            _log_call(name, "invalid_args", started)
            if settings.validation_errors == "protocol":
                raise MCPError(
                    types.INVALID_PARAMS,
                    f"Invalid arguments for tool {name}",
                    data={"errors": [asdict(i) for i in issues]},
                ) from None
            return tool_error(render_issues(name, issues))

        try:
            out = spec.run(args)
        except BusinessError as exc:
            _log_call(name, "business_error", started)
            return tool_error(str(exc))
        except Exception:
            log.exception("unexpected error in tool %s", name)
            _log_call(name, "internal_error", started)
            raise MCPError(types.INTERNAL_ERROR, "Internal error") from None

        payload = out.model_dump(mode="json")
        _log_call(name, "ok", started)
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(payload))],
            structured_content=payload,
        )

    return Server(
        SERVER_NAME,
        version=SERVER_VERSION,
        on_list_tools=list_tools,
        on_call_tool=call_tool,
    )
