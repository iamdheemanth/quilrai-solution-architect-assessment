import pytest

from customer_mcp.config import ConfigError, Settings, load_settings


def test_defaults_when_env_empty():
    assert load_settings({}) == Settings("tool", "digits", "INFO")


def test_reads_and_normalises_values():
    s = load_settings(
        {
            "MCP_VALIDATION_ERRORS": "Protocol",
            "CUSTOMER_ID_CHARSET": "ALNUM",
            "LOG_LEVEL": "debug",
        }
    )
    assert s == Settings("protocol", "alnum", "DEBUG")


@pytest.mark.parametrize(
    "var,value",
    [
        ("MCP_VALIDATION_ERRORS", "strict"),
        ("CUSTOMER_ID_CHARSET", "hex"),
        ("LOG_LEVEL", "LOUD"),
    ],
)
def test_rejects_unknown_values(var, value):
    with pytest.raises(ConfigError, match=var):
        load_settings({var: value})
