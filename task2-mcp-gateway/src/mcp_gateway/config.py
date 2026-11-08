"""Startup settings, read once from the environment."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Literal

Role = Literal["admin", "viewer"]
AuthMode = Literal["jwt", "static"]

_AUTH_MODES = ("jwt", "static")
_ROLES = ("admin", "viewer")
_DENY_STATUSES = (200, 403)
_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")
_MIN_SECRET_BYTES = 32


class ConfigError(ValueError):
    """An environment variable holds an unsupported value."""


@dataclass(frozen=True)
class Settings:
    downstream_url: str = "http://127.0.0.1:8001/mcp"
    host: str = "127.0.0.1"
    port: int = 8000
    auth_mode: AuthMode = "jwt"
    jwt_secret: str = field(default="", repr=False)
    jwt_audience: str = "mcp-gateway"
    jwt_issuer: str = "mcp-gateway-dev"
    static_tokens: Mapping[str, Role] = field(default_factory=dict, repr=False)
    deny_http_status: Literal[200, 403] = 200
    max_body_bytes: int = 4 * 1024 * 1024
    log_level: str = "INFO"


def _text(env: Mapping[str, str], name: str, default: str) -> str:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


def _choice(env: Mapping[str, str], name: str, default: str, allowed: tuple[str, ...]) -> str:
    value = _text(env, name, default).lower()
    if value not in allowed:
        raise ConfigError(f"{name} must be one of {', '.join(allowed)} (case-insensitive)")
    return value


def _int(env: Mapping[str, str], name: str, default: int, low: int, high: int | None) -> int:
    raw = _text(env, name, str(default))
    bound = f">= {low}" if high is None else f"between {low} and {high}"
    try:
        value = int(raw)
    except ValueError:
        raise ConfigError(f"{name} must be an integer {bound}") from None
    if value < low or (high is not None and value > high):
        raise ConfigError(f"{name} must be an integer {bound}")
    return value


def _parse_static_tokens(raw: str) -> dict[str, Role]:
    name = "GATEWAY_STATIC_TOKENS"
    fmt = "token=role[,token=role] with role admin or viewer, unique non-empty tokens"
    tokens: dict[str, Role] = {}
    for item in raw.split(","):
        if not item.strip():
            continue
        token, sep, role = (part.strip() for part in item.partition("="))
        if not sep or not token or role.lower() not in _ROLES:
            raise ConfigError(f"{name} must be {fmt}")
        if token in tokens:
            raise ConfigError(f"{name} must be {fmt}; a token is listed more than once")
        tokens[token] = role.lower()  # type: ignore[assignment]
    if not tokens:
        raise ConfigError(f"{name} is required when GATEWAY_AUTH_MODE=static: {fmt}")
    return tokens


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from env (defaults to os.environ); raise ConfigError if invalid."""
    if env is None:
        env = os.environ
    auth_mode = _choice(env, "GATEWAY_AUTH_MODE", "jwt", _AUTH_MODES)
    jwt_secret = ""
    static_tokens: dict[str, Role] = {}
    if auth_mode == "jwt":
        jwt_secret = env.get("GATEWAY_JWT_SECRET", "")
        if len(jwt_secret.encode("utf-8")) < _MIN_SECRET_BYTES:
            raise ConfigError(
                f"GATEWAY_JWT_SECRET must be at least {_MIN_SECRET_BYTES} bytes "
                "when GATEWAY_AUTH_MODE=jwt"
            )
    else:
        static_tokens = _parse_static_tokens(env.get("GATEWAY_STATIC_TOKENS", ""))
    deny = _choice(env, "GATEWAY_DENY_HTTP_STATUS", "200", tuple(str(s) for s in _DENY_STATUSES))
    return Settings(
        downstream_url=_text(env, "GATEWAY_DOWNSTREAM_URL", Settings.downstream_url),
        host=_text(env, "GATEWAY_HOST", Settings.host),
        port=_int(env, "GATEWAY_PORT", Settings.port, 1, 65535),
        auth_mode=auth_mode,  # type: ignore[arg-type]
        jwt_secret=jwt_secret,
        jwt_audience=_text(env, "GATEWAY_JWT_AUDIENCE", Settings.jwt_audience),
        jwt_issuer=_text(env, "GATEWAY_JWT_ISSUER", Settings.jwt_issuer),
        static_tokens=static_tokens,
        deny_http_status=int(deny),  # type: ignore[arg-type]
        max_body_bytes=_int(env, "GATEWAY_MAX_BODY_BYTES", Settings.max_body_bytes, 1, None),
        log_level=_choice(env, "LOG_LEVEL", "INFO", tuple(x.lower() for x in _LOG_LEVELS)).upper(),
    )
