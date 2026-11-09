"""Tenant authentication.

The tenant id is HMAC-SHA256(pepper, key) in hex; the raw key is never stored or logged.
"""

from __future__ import annotations

import hashlib
import hmac
from collections.abc import Mapping

from .config import Settings
from .errors import InvalidApiKey


def tenant_id(pepper: str, key: str) -> str:
    return hmac.new(pepper.encode("utf-8"), key.encode("utf-8"), hashlib.sha256).hexdigest()


def _presented_key(headers: Mapping[str, str]) -> str:
    """Pick the key: Authorization Bearer first, then api-key, then x-api-key."""
    auth = headers.get("authorization")
    if auth is not None:
        scheme, _, token = auth.strip().partition(" ")
        token = token.strip()
        if scheme.lower() != "bearer" or not token:
            raise InvalidApiKey()
        return token
    for name in ("api-key", "x-api-key"):
        value = headers.get(name)
        if value is not None:
            value = value.strip()
            if not value:
                raise InvalidApiKey()
            return value
    raise InvalidApiKey()


def authenticate(headers: Mapping[str, str], settings: Settings) -> str:
    """Return the tenant id for the presented key or raise InvalidApiKey."""
    presented = _presented_key(headers).encode("utf-8")
    matched: str | None = None
    for key in settings.tenant_keys:  # no short-circuit: compare against every key
        if hmac.compare_digest(presented, key.encode("utf-8")) and matched is None:
            matched = key
    if matched is None:
        raise InvalidApiKey()
    return tenant_id(settings.key_pepper, matched)
