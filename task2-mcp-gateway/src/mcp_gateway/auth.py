"""Bearer authentication: header extraction and token verification.

Fails closed: every problem raises AuthError; there is no default role.
Messages and reasons never include the presented token.
"""

from __future__ import annotations

import hashlib
import hmac
import re
import secrets
from collections.abc import Mapping
from dataclasses import dataclass

import jwt

from mcp_gateway.config import ROLES, Role, Settings

REALM = 'Bearer realm="mcp-gateway"'
_INVALID_REQUEST = f'{REALM}, error="invalid_request"'
_INVALID_TOKEN = f'{REALM}, error="invalid_token"'
_B64TOKEN = re.compile(r"[A-Za-z0-9\-._~+/]+=*")
_REQUIRED_CLAIMS = ["exp", "iat", "sub", "role", "aud", "iss"]
_LEEWAY_SECONDS = 30


@dataclass(frozen=True)
class Principal:
    subject: str
    role: Role


class AuthError(Exception):
    def __init__(self, status: int, www_authenticate: str, reason: str) -> None:
        super().__init__(reason)
        self.status = status
        self.www_authenticate = www_authenticate
        self.reason = reason


def _invalid_token(reason: str) -> AuthError:
    return AuthError(401, _INVALID_TOKEN, reason)


def extract_bearer(raw_headers: list[tuple[bytes, bytes]]) -> str:
    values = [value for name, value in raw_headers if name.lower() == b"authorization"]
    if not values:
        raise AuthError(401, REALM, "missing_authorization")
    if len(values) > 1:
        raise AuthError(400, _INVALID_REQUEST, "multiple_authorization_headers")
    value = values[0].decode("latin-1")
    scheme, _, rest = value.partition(" ")
    if scheme.lower() != "bearer":
        raise AuthError(401, REALM, "unsupported_scheme")
    token = rest.lstrip(" ")
    if not rest or not _B64TOKEN.fullmatch(token):
        raise AuthError(400, _INVALID_REQUEST, "malformed_bearer_token")
    return token


class JwtVerifier:
    def __init__(self, secret: str, audience: str, issuer: str) -> None:
        self._secret = secret
        self._audience = audience
        self._issuer = issuer

    def verify(self, token: str) -> Principal:
        try:
            claims = jwt.decode(
                token,
                self._secret,
                algorithms=["HS256"],
                audience=self._audience,
                issuer=self._issuer,
                options={"require": _REQUIRED_CLAIMS},
                leeway=_LEEWAY_SECONDS,
            )
        except jwt.InvalidTokenError:
            raise _invalid_token("jwt_rejected") from None
        role = claims.get("role")
        subject = claims.get("sub")
        if role not in ROLES:
            raise _invalid_token("unknown_role")
        if not isinstance(subject, str) or not subject:
            raise _invalid_token("invalid_subject")
        return Principal(subject, role)


class StaticVerifier:
    def __init__(self, tokens: Mapping[str, Role]) -> None:
        self._tokens = [(t.encode("utf-8"), r) for t, r in tokens.items()]
        # Keyed per process so a guessed token cannot be confirmed against an audit subject.
        self._subject_key = secrets.token_bytes(32)

    def verify(self, token: str) -> Principal:
        presented = token.encode("utf-8")
        matched: Role | None = None
        for known, role in self._tokens:  # no early exit: compare against every token
            if hmac.compare_digest(presented, known):
                matched = role
        if matched is None:
            raise _invalid_token("unknown_token")
        digest = hmac.new(self._subject_key, presented, hashlib.sha256).hexdigest()[:12]
        return Principal(f"static:{digest}", matched)


def build_verifier(settings: Settings) -> JwtVerifier | StaticVerifier:
    if settings.auth_mode == "static":
        return StaticVerifier(settings.static_tokens)
    return JwtVerifier(settings.jwt_secret, settings.jwt_audience, settings.jwt_issuer)
