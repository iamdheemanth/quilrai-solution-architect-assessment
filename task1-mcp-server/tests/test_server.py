import json
from contextlib import asynccontextmanager

import pytest
from mcp import Client
from mcp.shared.exceptions import MCPError

from customer_mcp.config import Settings
from customer_mcp.server import build_server
from customer_mcp.store import CustomerStore

pytestmark = pytest.mark.anyio


@pytest.fixture
def anyio_backend():
    return "asyncio"


@asynccontextmanager
async def client(settings: Settings | None = None):
    server = build_server(settings or Settings(), CustomerStore.seeded())
    async with Client(server) as c:
        yield c


async def test_tools_list_publishes_strict_schemas_and_annotations():
    async with client() as c:
        tools = {t.name: t for t in (await c.list_tools()).tools}
    assert set(tools) == {"get_customer_record", "trigger_refund"}
    assert tools["trigger_refund"].input_schema["additionalProperties"] is False
    assert tools["get_customer_record"].annotations.read_only_hint is True
    r = tools["trigger_refund"].annotations
    assert (r.read_only_hint, r.destructive_hint, r.idempotent_hint, r.open_world_hint) == (
        False,
        True,
        False,
        False,
    )
    assert tools["trigger_refund"].output_schema["properties"]["refund_id"]


async def test_get_customer_record_success():
    async with client() as c:
        res = await c.call_tool("get_customer_record", {"customer_id": "CUST-00001"})
    assert res.is_error is False and res.structured_content["name"] == "Ada Lovelace"
    assert json.loads(res.content[0].text) == res.structured_content


async def test_trigger_refund_success(caplog):
    caplog.set_level("INFO", logger="customer_mcp")
    args = {"customer_id": "CUST-00001", "amount": 25.5, "reason": "Duplicate charge on invoice"}
    async with client() as c:
        res = await c.call_tool("trigger_refund", args)
    assert res.is_error is False
    assert res.structured_content["refund_id"] == "RF-000001"
    assert res.structured_content["remaining_refundable_balance"] == 474.5
    assert "refund_issued refund_id=RF-000001 customer_id=CUST-00001 amount=25.5" in caplog.text
    assert "Duplicate charge" not in caplog.text
    assert "tool=trigger_refund outcome=ok duration_ms=" in caplog.text


TOOL_MODE = Settings(validation_errors="tool")


async def test_invalid_args_tool_mode_returns_is_error_with_fields(caplog):
    caplog.set_level("INFO", logger="customer_mcp")
    bad = {"customer_id": "CUST-1", "amount": "50", "reason": "short", "junk": 1}
    async with client(TOOL_MODE) as c:
        res = await c.call_tool("trigger_refund", bad)
    assert res.is_error is True and res.structured_content is None
    for field in ("customer_id", "amount", "reason", "junk"):
        assert field in res.content[0].text
    assert "tool=trigger_refund outcome=invalid_args" in caplog.text


async def test_missing_arguments_reports_required_fields():
    async with client(TOOL_MODE) as c:
        res = await c.call_tool("trigger_refund", None)
    assert res.is_error is True
    for field in ("customer_id", "amount", "reason"):
        assert field in res.content[0].text


async def test_missing_arguments_default_mode_is_32602():
    async with client() as c:
        with pytest.raises(MCPError) as e:
            await c.call_tool("trigger_refund", None)
    assert e.value.code == -32602
    assert {i["field"] for i in e.value.data["errors"]} == {"customer_id", "amount", "reason"}


async def test_invalid_args_default_mode_raises_32602_with_data():
    async with client() as c:  # default settings: protocol mode, as the brief requires
        with pytest.raises(MCPError) as e:
            await c.call_tool("get_customer_record", {"customer_id": "nope"})
    assert e.value.code == -32602
    assert e.value.message == "Invalid arguments for tool get_customer_record"
    assert e.value.data["errors"][0]["field"] == "customer_id"
    assert "nope" not in json.dumps(e.value.data)


@pytest.mark.parametrize("mode", ["tool", "protocol"])
async def test_unknown_tool_is_32602_in_both_modes(mode):
    async with client(Settings(validation_errors=mode)) as c:
        with pytest.raises(MCPError) as e:
            await c.call_tool("admin_reset_key", {})
    assert e.value.code == -32602
    assert e.value.message == "Unknown tool: admin_reset_key"


async def test_business_error_is_tool_error():
    async with client() as c:
        missing = await c.call_tool("get_customer_record", {"customer_id": "CUST-00404"})
        suspended = await c.call_tool(
            "trigger_refund",
            {"customer_id": "CUST-00003", "amount": 5, "reason": "Goodwill gesture please"},
        )
    assert missing.is_error is True and "No customer found" in missing.content[0].text
    assert suspended.is_error is True and "suspended" in suspended.content[0].text


async def test_alnum_mode_accepts_letter_ids():
    async with client(Settings(customer_id_charset="alnum")) as c:
        res = await c.call_tool("get_customer_record", {"customer_id": "CUST-A1B2C"})
    assert res.is_error is False and res.structured_content["name"] == "Katherine Johnson"


async def test_unexpected_exception_is_32603_without_leak(monkeypatch):
    def boom(self, cid):
        raise RuntimeError("db password=hunter2")

    monkeypatch.setattr(CustomerStore, "get", boom)
    async with client() as c:
        with pytest.raises(MCPError) as e:
            await c.call_tool("get_customer_record", {"customer_id": "CUST-00001"})
    assert (e.value.code, e.value.message) == (-32603, "Internal error")
    assert "hunter2" not in str(e.value.data)
    assert "hunter2" not in str(e.value)


async def test_unknown_tool_name_is_never_logged_raw(caplog):
    caplog.set_level("INFO", logger="customer_mcp")
    evil = "x" + chr(10) + "refund_issued refund_id=RF-999999 customer_id=CUST-00001 amount=499.99"
    async with client() as c:
        with pytest.raises(MCPError):
            await c.call_tool(evil, {})
    records = [r for r in caplog.records if r.name.startswith("customer_mcp")]
    assert len(records) == 1
    msg = records[0].getMessage()
    assert "tool=<unknown> outcome=unknown_tool" in msg
    assert "refund_issued" not in msg and chr(10) not in msg


async def test_unknown_tool_name_echo_is_truncated():
    async with client() as c:
        with pytest.raises(MCPError) as e:
            await c.call_tool("a" * 500, {})
    assert e.value.message == "Unknown tool: " + "a" * 64 + chr(0x2026)


@pytest.mark.parametrize("mode", ["tool", "protocol"])
async def test_invalid_call_emits_exactly_one_log_record(caplog, mode):
    caplog.set_level("DEBUG", logger="customer_mcp")
    async with client(Settings(validation_errors=mode)) as c:
        if mode == "protocol":
            with pytest.raises(MCPError):
                await c.call_tool("get_customer_record", {"customer_id": "bad", "junk": 1})
        else:
            await c.call_tool("get_customer_record", {"customer_id": "bad", "junk": 1})
    records = [r for r in caplog.records if r.name.startswith("customer_mcp")]
    assert len(records) == 1
    msg = records[0].getMessage()
    assert "outcome=invalid_args" in msg and "fields=" in msg
