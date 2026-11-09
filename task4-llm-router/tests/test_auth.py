import hashlib
import hmac

import pytest
from starlette.datastructures import Headers

from llm_router.auth import authenticate, tenant_id
from llm_router.config import load_settings
from llm_router.errors import InvalidApiKey

PEPPER = "pepper-0123456789-abcdef"
KEY_A = "key-alpha-123"
KEY_B = "key-beta-456"


@pytest.fixture
def settings():
    return load_settings(
        env={
            "ROUTER_TENANT_KEYS": f"{KEY_A}=acme,{KEY_B}=globex",
            "ROUTER_KEY_PEPPER": PEPPER,
            "PRIMARY_BASE_URL": "http://127.0.0.1:9001/v1",
            "PRIMARY_API_KEY": "pk",
            "PRIMARY_MODEL": "m1",
            "SECONDARY_BASE_URL": "http://127.0.0.1:9002/v1",
            "SECONDARY_API_KEY": "sk",
            "SECONDARY_MODEL": "m2",
        }
    )


def test_tenant_id_is_hmac_sha256_hex():
    expected = hmac.new(PEPPER.encode(), KEY_A.encode(), hashlib.sha256).hexdigest()
    assert tenant_id(PEPPER, KEY_A) == expected
    assert tenant_id(PEPPER, KEY_A) == tenant_id(PEPPER, KEY_A)
    assert tenant_id(PEPPER, KEY_A) != KEY_A
    assert KEY_A not in tenant_id(PEPPER, KEY_A)
    assert tenant_id(PEPPER, KEY_A) != tenant_id(PEPPER, KEY_B)
    assert tenant_id("another-pepper-0123456", KEY_A) != tenant_id(PEPPER, KEY_A)


@pytest.mark.parametrize(
    "header",
    [
        {"Authorization": f"Bearer {KEY_A}"},
        {"authorization": f"bearer {KEY_A}"},
        {"api-key": KEY_A},
        {"x-api-key": KEY_A},
    ],
)
def test_authenticate_success_all_header_styles(settings, header):
    assert authenticate(Headers(header), settings) == tenant_id(PEPPER, KEY_A)


def test_authenticate_second_key(settings):
    got = authenticate(Headers({"Authorization": f"Bearer {KEY_B}"}), settings)
    assert got == tenant_id(PEPPER, KEY_B)


def test_bearer_takes_precedence_over_api_key_headers(settings):
    headers = Headers({"Authorization": f"Bearer {KEY_A}", "api-key": KEY_B, "x-api-key": KEY_B})
    assert authenticate(headers, settings) == tenant_id(PEPPER, KEY_A)


def test_api_key_takes_precedence_over_x_api_key(settings):
    headers = Headers({"api-key": KEY_A, "x-api-key": KEY_B})
    assert authenticate(headers, settings) == tenant_id(PEPPER, KEY_A)


def test_invalid_bearer_is_not_rescued_by_other_headers(settings):
    headers = Headers({"Authorization": "Bearer wrong", "api-key": KEY_A})
    with pytest.raises(InvalidApiKey):
        authenticate(headers, settings)


@pytest.mark.parametrize(
    "header",
    [
        {},
        {"Authorization": ""},
        {"Authorization": "Bearer"},
        {"Authorization": "Bearer "},
        {"Authorization": f"Basic {KEY_A}"},
        {"Authorization": KEY_A},
        {"Authorization": "Bearer unknown-key"},
        {"Authorization": f"Bearer {KEY_A}x"},
        {"Authorization": f"Bearer {KEY_A[:-1]}"},
        {"api-key": ""},
        {"api-key": "unknown-key"},
        {"x-api-key": ""},
        {"x-api-key": "unknown-key"},
    ],
)
def test_authenticate_failures(settings, header):
    with pytest.raises(InvalidApiKey):
        authenticate(Headers(header), settings)


def test_authenticate_non_ascii_key_is_rejected_not_crashed(settings):
    headers = Headers(raw=[(b"x-api-key", "kéy".encode("latin-1"))])
    with pytest.raises(InvalidApiKey):
        authenticate(headers, settings)


def test_authenticate_compares_every_key(settings, monkeypatch):
    calls = []
    real = hmac.compare_digest

    def spy(a, b):
        calls.append(b)
        return real(a, b)

    monkeypatch.setattr("llm_router.auth.hmac.compare_digest", spy)
    authenticate(Headers({"Authorization": f"Bearer {KEY_A}"}), settings)
    assert len(calls) == 2
