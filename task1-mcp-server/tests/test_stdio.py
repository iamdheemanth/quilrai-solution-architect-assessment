"""Black-box tests: real subprocess, raw JSON-RPC lines over stdio."""

import json
import os
import sys
from pathlib import Path

import pytest

from tests.rawstdio import RawStdioSession

HERE = Path(__file__).parent
SERVER_CMD = [sys.executable, "-m", "customer_mcp"]
CRASH_CMD = [sys.executable, str(HERE / "crashing_server.py")]
PRINT_CMD = [sys.executable, str(HERE / "printing_server.py")]


def assert_pure(session: RawStdioSession) -> None:
    """Every non-empty stdout line must be a well-formed JSON-RPC 2.0 message."""
    for line in session.stdout_lines:
        if not line.strip():
            continue
        obj = json.loads(line)
        assert isinstance(obj, dict), line
        assert obj.get("jsonrpc") == "2.0", line
        if "method" in obj:
            assert "id" not in obj, f"notification with id: {line}"
        else:
            assert "id" in obj, line
            assert ("result" in obj) != ("error" in obj), line
            if "error" in obj:
                code = obj["error"]["code"]
                assert isinstance(code, int) and code != 0, line


@pytest.fixture
def make_session():
    sessions: list[RawStdioSession] = []

    def factory(env_overrides=None, cmd=None):
        env = dict(os.environ)
        for var in ("LOG_LEVEL", "MCP_VALIDATION_ERRORS", "CUSTOMER_ID_CHARSET"):
            env.pop(var, None)  # ambient config must not leak into the child
        env.update(env_overrides or {})
        session = RawStdioSession(cmd or SERVER_CMD, env=env)
        sessions.append(session)
        return session

    yield factory
    for session in sessions:
        session.close()
    for session in sessions:
        assert_pure(session)


@pytest.fixture
def s(make_session):
    return make_session()


def test_handshake_and_tools_list(s):
    init = s.initialize()
    assert init["result"]["capabilities"]["tools"] is not None
    tools = s.request("tools/list")["result"]["tools"]
    assert init["result"]["serverInfo"]["name"] == "customer-service-mcp"
    assert init["result"]["serverInfo"]["version"] == "1.0.0"
    assert {t["name"] for t in tools} == {"get_customer_record", "trigger_refund"}


def test_notification_gets_no_response(s):
    s.initialize()
    s.request("tools/list")
    s.close()
    assert len(s.stdout_lines) == 2  # initialize + tools/list, nothing for the notification


@pytest.mark.parametrize(
    "args,field",
    [
        ({"customer_id": "CUST-00001\n"}, "customer_id"),
        ({"customer_id": 12345}, "customer_id"),
        ({}, "customer_id"),
    ],
)
def test_bad_args_return_is_error_result(s, args, field):
    s.initialize()
    r = s.request("tools/call", {"name": "get_customer_record", "arguments": args})["result"]
    assert r["isError"] is True and field in r["content"][0]["text"]


def test_protocol_mode_bad_args_return_32602(make_session):
    s = make_session({"MCP_VALIDATION_ERRORS": "protocol"})
    s.initialize()
    resp = s.request("tools/call", {"name": "get_customer_record", "arguments": {}})
    assert resp["error"]["code"] == -32602


def test_unknown_tool_32602(s):
    s.initialize()
    resp = s.request("tools/call", {"name": "nope", "arguments": {}})
    assert resp["error"]["code"] == -32602
    assert resp["error"]["message"] == "Unknown tool: nope"


def test_unknown_method_32601(s):
    s.initialize()
    assert s.request("foo/bar")["error"]["code"] == -32601


def test_junk_input_does_not_break_server(s):
    s.initialize()
    s.send("this is not json")
    s.send("[1, 2]")
    assert s.request("tools/list")["result"]["tools"]  # still serving
    s.close()
    # SDK 2.3.0 drops both junk lines silently; the only replies are initialize and tools/list.
    replies = [json.loads(line) for line in s.stdout_lines]
    assert [r["id"] for r in replies] == [1, 2]


def test_malformed_tools_call_arguments_is_32602(s):
    s.initialize()
    resp = s.request("tools/call", {"name": "get_customer_record", "arguments": "x"})
    assert resp["error"]["code"] == -32602
    assert resp["error"]["message"] == "Invalid request parameters"
    assert s.request("tools/list")["result"]["tools"]  # still serving


def test_stray_prints_cannot_corrupt_stdout(make_session):
    s = make_session(cmd=PRINT_CMD)
    s.initialize()
    resp = s.request(
        "tools/call",
        {"name": "get_customer_record", "arguments": {"customer_id": "CUST-00001"}},
    )
    assert resp["result"]["isError"] is False
    s.close()
    for marker in ("STRAY-PRINT", "STRAY-OSWRITE"):
        assert all(marker not in line for line in s.stdout_lines)
        assert marker in s.stderr_text


def test_request_before_initialize_does_not_break_server(s):
    s.send({"jsonrpc": "2.0", "id": 900, "method": "tools/list"})
    s.initialize()
    assert s.request("tools/list")["result"]["tools"]


def test_logs_go_to_stderr_only(s):
    s.initialize()
    s.request(
        "tools/call", {"name": "get_customer_record", "arguments": {"customer_id": "CUST-1"}}
    )
    s.close()
    assert "outcome=invalid_args" in s.stderr_text
    assert all("outcome=" not in line for line in s.stdout_lines)


def test_crash_traceback_on_stderr_not_stdout(make_session):
    s = make_session(cmd=CRASH_CMD)
    s.initialize()
    resp = s.request(
        "tools/call",
        {"name": "get_customer_record", "arguments": {"customer_id": "CUST-12345"}},
    )
    assert resp["error"]["code"] == -32603 and resp["error"]["message"] == "Internal error"
    s.close()
    assert "Traceback" in s.stderr_text
    assert all("Traceback" not in line and "simulated" not in line for line in s.stdout_lines)


def test_exits_cleanly_on_stdin_eof(s):
    s.initialize()
    assert s.close(timeout=10) == 0


def test_invalid_config_exits_2_with_stderr_message(make_session):
    s = make_session({"CUSTOMER_ID_CHARSET": "hex"})
    assert s.close(timeout=10) == 2
    assert "CUSTOMER_ID_CHARSET" in s.stderr_text
    assert s.stdout_lines == []
