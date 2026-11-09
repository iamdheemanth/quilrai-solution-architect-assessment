"""Startup settings, read once from the environment."""

from __future__ import annotations

import math
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

_LOG_LEVELS = ("debug", "info", "warning", "error")


class ConfigError(ValueError):
    """An environment variable holds an unsupported value."""


@dataclass(frozen=True)
class Settings:
    upstream_base_url: str = "http://127.0.0.1:8002/v1"
    upstream_api_key: str = field(default="", repr=False)
    upstream_read_timeout: float = 120.0
    host: str = "127.0.0.1"
    port: int = 8003
    max_body_bytes: int = 4 * 1024 * 1024
    log_level: str = "INFO"


def _text(env: Mapping[str, str], name: str, default: str) -> str:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


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


def _positive_float(env: Mapping[str, str], name: str, default: float) -> float:
    raw = _text(env, name, str(default))
    try:
        value = float(raw)
    except ValueError:
        raise ConfigError(f"{name} must be a number > 0") from None
    if not math.isfinite(value) or value <= 0:
        raise ConfigError(f"{name} must be a number > 0")
    return value


def _upstream_url(env: Mapping[str, str]) -> str:
    name = "UPSTREAM_BASE_URL"
    url = _text(env, name, Settings.upstream_base_url)
    try:
        parts = urlsplit(url)
        valid = parts.scheme in ("http", "https") and bool(parts.hostname)
        _ = parts.port  # raises ValueError for an out-of-range or non-numeric port
    except ValueError:
        valid = False
    if not valid:
        raise ConfigError(f"{name} must be an http:// or https:// URL with a host")
    return url


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from env (defaults to os.environ); raise ConfigError if invalid."""
    if env is None:
        env = os.environ
    level = _text(env, "LOG_LEVEL", Settings.log_level).lower()
    if level not in _LOG_LEVELS:
        raise ConfigError(f"LOG_LEVEL must be one of {', '.join(_LOG_LEVELS)} (case-insensitive)")
    return Settings(
        upstream_base_url=_upstream_url(env),
        upstream_api_key=env.get("UPSTREAM_API_KEY", "").strip(),
        upstream_read_timeout=_positive_float(
            env, "UPSTREAM_READ_TIMEOUT", Settings.upstream_read_timeout
        ),
        host=_text(env, "GATEWAY_HOST", Settings.host),
        port=_int(env, "GATEWAY_PORT", Settings.port, 1, 65535),
        max_body_bytes=_int(env, "GATEWAY_MAX_BODY_BYTES", Settings.max_body_bytes, 1, None),
        log_level=level.upper(),
    )
