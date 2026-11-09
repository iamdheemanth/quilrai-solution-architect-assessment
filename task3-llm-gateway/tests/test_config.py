import pytest

from llm_gateway.config import ConfigError, Settings, load_settings

KEY = "sk-secret-123"


def test_defaults():
    s = load_settings({})
    assert s == Settings()
    assert s.upstream_base_url == "http://127.0.0.1:8002/v1"
    assert s.upstream_api_key == ""
    assert s.upstream_read_timeout == 120.0
    assert (s.host, s.port) == ("127.0.0.1", 8003)
    assert s.max_body_bytes == 4 * 1024 * 1024
    assert s.log_level == "INFO"


def test_every_variable_parses():
    s = load_settings(
        {
            "UPSTREAM_BASE_URL": "https://api.example.com/v1",
            "UPSTREAM_API_KEY": KEY,
            "UPSTREAM_READ_TIMEOUT": "2.5",
            "GATEWAY_HOST": "0.0.0.0",
            "GATEWAY_PORT": "9000",
            "GATEWAY_MAX_BODY_BYTES": "1024",
            "LOG_LEVEL": "debug",
        }
    )
    assert s.upstream_base_url == "https://api.example.com/v1"
    assert s.upstream_api_key == KEY
    assert s.upstream_read_timeout == 2.5
    assert s.host == "0.0.0.0"
    assert s.port == 9000
    assert s.max_body_bytes == 1024
    assert s.log_level == "DEBUG"


@pytest.mark.parametrize("url", ["127.0.0.1:8002/v1", "ftp://x/v1", "http:///v1"])
def test_bad_upstream_url(url):
    with pytest.raises(ConfigError, match="UPSTREAM_BASE_URL"):
        load_settings({"UPSTREAM_BASE_URL": url})


@pytest.mark.parametrize("value", ["0", "-1", "abc"])
def test_bad_read_timeout(value):
    with pytest.raises(ConfigError, match="UPSTREAM_READ_TIMEOUT"):
        load_settings({"UPSTREAM_READ_TIMEOUT": value})


@pytest.mark.parametrize("value", ["nan", "inf"])
def test_non_finite_read_timeout(value):
    with pytest.raises(ConfigError, match="UPSTREAM_READ_TIMEOUT"):
        load_settings({"UPSTREAM_READ_TIMEOUT": value})


@pytest.mark.parametrize("value", ["0", "70000", "x"])
def test_bad_port(value):
    with pytest.raises(ConfigError, match="GATEWAY_PORT"):
        load_settings({"GATEWAY_PORT": value})


def test_bad_max_body_bytes():
    with pytest.raises(ConfigError, match="GATEWAY_MAX_BODY_BYTES"):
        load_settings({"GATEWAY_MAX_BODY_BYTES": "0"})


def test_bad_log_level():
    with pytest.raises(ConfigError, match="LOG_LEVEL"):
        load_settings({"LOG_LEVEL": "chatty"})


def test_repr_hides_key():
    s = load_settings({"UPSTREAM_API_KEY": KEY})
    assert s.upstream_api_key == KEY
    assert KEY not in repr(s)


@pytest.mark.parametrize(
    "bad",
    [
        {"UPSTREAM_BASE_URL": "ftp://x/v1"},
        {"UPSTREAM_READ_TIMEOUT": "abc"},
        {"GATEWAY_PORT": "0"},
        {"LOG_LEVEL": "chatty"},
    ],
)
def test_errors_never_contain_key(bad):
    with pytest.raises(ConfigError) as exc:
        load_settings({"UPSTREAM_API_KEY": KEY, **bad})
    assert KEY not in str(exc.value)
