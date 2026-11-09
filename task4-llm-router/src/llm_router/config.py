"""Startup settings, read once from the environment.

ConfigError messages name the offending variable but never echo its value, because several
of the variables hold secrets (tenant keys, the HMAC pepper, provider API keys).
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from urllib.parse import urlsplit

_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")
_MIN_PEPPER_BYTES = 16


class ConfigError(ValueError):
    """An environment variable is missing or holds an unsupported value."""


@dataclass(frozen=True)
class Provider:
    name: str
    base_url: str
    api_key: str = field(repr=False)
    model: str
    timeout_s: float


@dataclass(frozen=True)
class Settings:
    tenant_keys: Mapping[str, str] = field(repr=False)
    key_pepper: str = field(repr=False)
    tokens_per_minute: int
    window_seconds: int
    default_max_tokens: int
    db_path: str
    primary: Provider
    secondary: Provider
    host: str
    port: int
    log_level: str


def _text(env: Mapping[str, str], name: str, default: str) -> str:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip()


def _required(env: Mapping[str, str], name: str) -> str:
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        raise ConfigError(f"{name} is required")
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


def _tenant_keys(env: Mapping[str, str]) -> dict[str, str]:
    name = "ROUTER_TENANT_KEYS"
    raw = _required(env, name)
    keys: dict[str, str] = {}
    for entry in raw.split(","):
        key, sep, tenant = entry.partition("=")
        key, tenant = key.strip(), tenant.strip()
        if not sep or not key or not tenant:
            raise ConfigError(
                f"{name} must be a comma-separated list of non-empty key=tenant pairs"
            )
        if key in keys:
            raise ConfigError(f"{name} contains a duplicate key")
        keys[key] = tenant
    return keys


def _pepper(env: Mapping[str, str]) -> str:
    name = "ROUTER_KEY_PEPPER"
    pepper = _required(env, name)
    if len(pepper.encode("utf-8")) < _MIN_PEPPER_BYTES:
        raise ConfigError(f"{name} must be at least {_MIN_PEPPER_BYTES} bytes")
    return pepper


def _base_url(env: Mapping[str, str], name: str) -> str:
    url = _required(env, name)
    try:
        parts = urlsplit(url)
        valid = parts.scheme in ("http", "https") and bool(parts.hostname)
        _ = parts.port  # raises ValueError for an out-of-range or non-numeric port
    except ValueError:
        valid = False
    if not valid:
        raise ConfigError(f"{name} must be an http:// or https:// URL with a host")
    return url


def _provider(env: Mapping[str, str], prefix: str) -> Provider:
    return Provider(
        name=prefix.lower(),
        base_url=_base_url(env, f"{prefix}_BASE_URL"),
        api_key=_required(env, f"{prefix}_API_KEY"),
        model=_required(env, f"{prefix}_MODEL"),
        timeout_s=_int(env, f"{prefix}_TIMEOUT_MS", 3000, 1, None) / 1000,
    )


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from env (defaults to os.environ); raise ConfigError if invalid."""
    if env is None:
        env = os.environ
    level = _text(env, "LOG_LEVEL", "INFO").upper()
    if level not in _LOG_LEVELS:
        raise ConfigError(f"LOG_LEVEL must be one of {', '.join(_LOG_LEVELS)} (case-insensitive)")
    return Settings(
        tenant_keys=_tenant_keys(env),
        key_pepper=_pepper(env),
        tokens_per_minute=_int(env, "ROUTER_TOKENS_PER_MINUTE", 50000, 1, None),
        window_seconds=_int(env, "ROUTER_WINDOW_SECONDS", 60, 1, None),
        default_max_tokens=_int(env, "ROUTER_DEFAULT_MAX_TOKENS", 1024, 1, None),
        db_path=_text(env, "ROUTER_DB_PATH", "./router.db"),
        primary=_provider(env, "PRIMARY"),
        secondary=_provider(env, "SECONDARY"),
        host=_text(env, "ROUTER_HOST", "127.0.0.1"),
        port=_int(env, "ROUTER_PORT", 8004, 1, 65535),
        log_level=level,
    )
