import pytest

from mcp_gateway.config import ConfigError, load_settings

SECRET = "s" * 32


def test_defaults_with_jwt_secret():
    s = load_settings({"GATEWAY_JWT_SECRET": SECRET})
    assert (s.auth_mode, s.deny_http_status) == ("jwt", 200)
    assert s.downstream_url == "http://127.0.0.1:8001/mcp"
    assert s.max_body_bytes == 4 * 1024 * 1024
    assert (s.host, s.port) == ("127.0.0.1", 8000)
    assert (s.jwt_audience, s.jwt_issuer) == ("mcp-gateway", "mcp-gateway-dev")
    assert s.log_level == "INFO"


@pytest.mark.parametrize("secret", ["", "x" * 31])
def test_jwt_mode_requires_32_byte_secret(secret):
    with pytest.raises(ConfigError, match="GATEWAY_JWT_SECRET") as exc:
        load_settings({"GATEWAY_JWT_SECRET": secret})
    if secret:
        assert secret not in str(exc.value)


def test_static_mode_parses_tokens():
    s = load_settings(
        {"GATEWAY_AUTH_MODE": "STATIC", "GATEWAY_STATIC_TOKENS": "a1=admin, v1=viewer"}
    )
    assert dict(s.static_tokens) == {"a1": "admin", "v1": "viewer"}
    assert s.jwt_secret == ""
    assert s.auth_mode == "static"


@pytest.mark.parametrize(
    ("env", "name"),
    [
        ({"GATEWAY_AUTH_MODE": "static"}, "GATEWAY_STATIC_TOKENS"),
        (
            {"GATEWAY_AUTH_MODE": "static", "GATEWAY_STATIC_TOKENS": "a1=root"},
            "GATEWAY_STATIC_TOKENS",
        ),
        (
            {"GATEWAY_AUTH_MODE": "static", "GATEWAY_STATIC_TOKENS": "a1=admin,a1=viewer"},
            "GATEWAY_STATIC_TOKENS",
        ),
        (
            {"GATEWAY_AUTH_MODE": "static", "GATEWAY_STATIC_TOKENS": "noequals"},
            "GATEWAY_STATIC_TOKENS",
        ),
        ({"GATEWAY_DENY_HTTP_STATUS": "418"}, "GATEWAY_DENY_HTTP_STATUS"),
        ({"GATEWAY_AUTH_MODE": "oauth"}, "GATEWAY_AUTH_MODE"),
        ({"GATEWAY_PORT": "abc"}, "GATEWAY_PORT"),
        ({"GATEWAY_PORT": "70000"}, "GATEWAY_PORT"),
        ({"GATEWAY_MAX_BODY_BYTES": "0"}, "GATEWAY_MAX_BODY_BYTES"),
        ({"LOG_LEVEL": "LOUD"}, "LOG_LEVEL"),
    ],
)
def test_rejects_invalid(env, name):
    if env.get("GATEWAY_AUTH_MODE", "jwt").lower() != "static":
        env = {**env, "GATEWAY_JWT_SECRET": SECRET}
    with pytest.raises(ConfigError, match=name):
        load_settings(env)


def test_errors_never_leak_token_values():
    with pytest.raises(ConfigError) as exc:
        load_settings({"GATEWAY_AUTH_MODE": "static", "GATEWAY_STATIC_TOKENS": "supersecret=root"})
    assert "supersecret" not in str(exc.value)
    with pytest.raises(ConfigError) as exc:
        load_settings(
            {"GATEWAY_AUTH_MODE": "static", "GATEWAY_STATIC_TOKENS": "dup-tok=admin,dup-tok=viewer"}
        )
    assert "dup-tok" not in str(exc.value)


def test_deny_status_403_opt_in():
    s = load_settings({"GATEWAY_JWT_SECRET": SECRET, "GATEWAY_DENY_HTTP_STATUS": "403"})
    assert s.deny_http_status == 403


def test_repr_hides_jwt_secret():
    s = load_settings({"GATEWAY_JWT_SECRET": SECRET})
    assert SECRET not in repr(s)
    assert "downstream_url" in repr(s)


def test_repr_hides_static_tokens():
    s = load_settings(
        {
            "GATEWAY_AUTH_MODE": "static",
            "GATEWAY_STATIC_TOKENS": "tok-admin-1=admin,tok-view-1=viewer",
        }
    )
    text = repr(s)
    assert "tok-admin-1" not in text
    assert "tok-view-1" not in text
    assert "auth_mode" in text
