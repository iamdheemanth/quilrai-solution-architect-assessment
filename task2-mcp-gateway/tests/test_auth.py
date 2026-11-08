import base64
import hashlib
import json
import re
import time
import warnings

import jwt
import pytest

from mcp_gateway.auth import (
    REALM,
    AuthError,
    JwtVerifier,
    Principal,
    StaticVerifier,
    build_verifier,
    extract_bearer,
)
from mcp_gateway.config import Settings
from mcp_gateway.tokens import mint_token

SECRET = "s" * 32
AUD = "mcp-gateway"
ISS = "mcp-gateway-dev"
INVALID_TOKEN = 'Bearer realm="mcp-gateway", error="invalid_token"'
INVALID_REQUEST = 'Bearer realm="mcp-gateway", error="invalid_request"'
v = JwtVerifier(SECRET, AUD, ISS)


def _claims(**over):
    now = int(time.time())
    claims = {"sub": "x", "role": "admin", "aud": AUD, "iss": ISS, "iat": now, "exp": now + 60}
    claims.update(over)
    return claims


def _b64(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _hs512_token() -> str:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", jwt.warnings.InsecureKeyLengthWarning)
        return jwt.encode(_claims(), SECRET, "HS512")


def _alg_none_token() -> str:
    header = _b64(json.dumps({"alg": "none", "typ": "JWT"}).encode())
    payload = _b64(json.dumps(_claims()).encode())
    return f"{header}.{payload}."


def test_realm_constant():
    assert REALM == 'Bearer realm="mcp-gateway"'


@pytest.mark.parametrize(
    "headers,status,www",
    [
        ([], 401, REALM),
        ([(b"authorization", b"Basic YTpi")], 401, REALM),
        (
            [(b"authorization", b"Bearer a"), (b"authorization", b"Bearer b")],
            400,
            INVALID_REQUEST,
        ),
        ([(b"authorization", b"Bearer")], 400, INVALID_REQUEST),
        ([(b"authorization", b"Bearer a b")], 400, INVALID_REQUEST),
        ([(b"authorization", b"Bearer a$b")], 400, INVALID_REQUEST),
        ([(b"authorization", b"Bearer =abc")], 400, INVALID_REQUEST),
        ([(b"authorization", b"Bearer \xff")], 400, INVALID_REQUEST),
    ],
)
def test_extract_bearer_errors(headers, status, www):
    with pytest.raises(AuthError) as e:
        extract_bearer(headers)
    assert (e.value.status, e.value.www_authenticate) == (status, www)


def test_extract_bearer_scheme_case_insensitive():
    headers = [(b"x-other", b"1"), (b"authorization", b"bearer   abc.def-_~+/=")]
    assert extract_bearer(headers) == "abc.def-_~+/="


def test_valid_jwt_gives_principal():
    assert v.verify(mint_token(SECRET, "viewer", subject="alice")) == Principal("alice", "viewer")


@pytest.mark.parametrize(
    "make",
    [
        lambda: mint_token(SECRET, "admin", ttl_seconds=-120),
        lambda: mint_token("t" * 32, "admin"),
        lambda: mint_token(SECRET, "admin", audience="other"),
        lambda: mint_token(SECRET, "admin", issuer="other"),
        lambda: jwt.encode(_claims(role="root"), SECRET, "HS256"),
        lambda: jwt.encode({k: x for k, x in _claims().items() if k != "role"}, SECRET, "HS256"),
        lambda: jwt.encode(_claims(sub=""), SECRET, "HS256"),
        _hs512_token,
        _alg_none_token,
        lambda: "not-a-jwt",
    ],
)
def test_invalid_tokens_are_401_invalid_token(make):
    token = make()
    with pytest.raises(AuthError) as e:
        v.verify(token)
    assert (e.value.status, e.value.www_authenticate) == (401, INVALID_TOKEN)
    assert token not in str(e.value) and token not in e.value.reason


def test_static_verifier():
    sv = StaticVerifier({"tok-admin": "admin", "tok-view": "viewer"})
    principal = sv.verify("tok-view")
    assert principal.role == "viewer"
    assert principal.subject.startswith("static:") and len(principal.subject) == len("static:") + 12
    assert sv.verify("tok-admin").role == "admin"
    assert re.fullmatch(r"static:[0-9a-f]{12}", principal.subject)
    assert sv.verify("tok-view").subject == principal.subject  # stable within one verifier
    assert sv.verify("tok-admin").subject != principal.subject
    # Keyed per process: a dictionary guess of the token cannot be confirmed from the log.
    assert principal.subject != "static:" + hashlib.sha256(b"tok-view").hexdigest()[:12]
    assert StaticVerifier({"tok-view": "viewer"}).verify("tok-view").subject != principal.subject
    with pytest.raises(AuthError) as e:
        sv.verify("unknown-secret-token")
    assert (e.value.status, e.value.www_authenticate) == (401, INVALID_TOKEN)
    assert "unknown-secret-token" not in str(e.value) + e.value.reason


def test_build_verifier_selects_by_mode():
    assert isinstance(build_verifier(Settings(jwt_secret=SECRET)), JwtVerifier)
    static = Settings(auth_mode="static", static_tokens={"t": "viewer"})
    assert isinstance(build_verifier(static), StaticVerifier)
