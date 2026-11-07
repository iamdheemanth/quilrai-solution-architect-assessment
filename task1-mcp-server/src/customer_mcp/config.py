"""Startup settings, read once from the environment."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Literal

ValidationErrorMode = Literal["tool", "protocol"]
CustomerIdCharset = Literal["digits", "alnum"]

_VALIDATION_MODES = ("tool", "protocol")
_CHARSETS = ("digits", "alnum")
_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR")


class ConfigError(ValueError):
    """An environment variable holds an unsupported value."""


@dataclass(frozen=True)
class Settings:
    validation_errors: ValidationErrorMode = "tool"
    customer_id_charset: CustomerIdCharset = "digits"
    log_level: str = "INFO"


def _read(env: Mapping[str, str], name: str, default: str, allowed: tuple[str, ...], upper: bool):
    raw = env.get(name)
    if raw is None or raw.strip() == "":
        return default
    value = raw.strip().upper() if upper else raw.strip().lower()
    if value not in allowed:
        raise ConfigError(f"{name} must be one of {', '.join(allowed)} (case-insensitive)")
    return value


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Build Settings from env (defaults to os.environ); raise ConfigError if invalid."""
    if env is None:
        env = os.environ
    return Settings(
        validation_errors=_read(env, "MCP_VALIDATION_ERRORS", "tool", _VALIDATION_MODES, False),
        customer_id_charset=_read(env, "CUSTOMER_ID_CHARSET", "digits", _CHARSETS, False),
        log_level=_read(env, "LOG_LEVEL", "INFO", _LOG_LEVELS, True),
    )
