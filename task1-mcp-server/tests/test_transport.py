import mcp_types as types
import pytest
from pydantic import ValidationError

from customer_mcp.transport import rejection_for


def _sdk_parse_failure(line: str) -> ValidationError:
    """Reproduce the exception the SDK's stdio reader yields for an unparseable line."""
    with pytest.raises(ValidationError) as e:
        types.jsonrpc_message_adapter.validate_json(line, by_name=False)
    return e.value


def test_invalid_json_is_parse_error():
    error = rejection_for(_sdk_parse_failure("this is not json"))
    assert (error.code, error.message) == (-32700, "Parse error")


@pytest.mark.parametrize(
    "line",
    [
        "[1, 2]",
        '{"jsonrpc": "2.0", "id": 7, "method": "tools/call", "params": [1]}',
        '{"jsonrpc": "2.0", "id": 7, "method": 5}',
        '"just a string"',
    ],
)
def test_valid_json_that_is_not_a_message_is_invalid_request(line):
    error = rejection_for(_sdk_parse_failure(line))
    assert (error.code, error.message) == (-32600, "Invalid Request")


def test_unexpected_exception_type_is_invalid_request():
    error = rejection_for(RuntimeError("anything"))
    assert (error.code, error.message) == (-32600, "Invalid Request")
