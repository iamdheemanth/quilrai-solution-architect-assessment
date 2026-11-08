import jwt

from mcp_gateway.auth import JwtVerifier
from mcp_gateway.tokens import main, mint_token

SECRET = "k" * 32


def test_mint_token_claims():
    token = mint_token(SECRET, "admin", subject="bob", ttl_seconds=100, now=1_000_000)
    claims = jwt.decode(
        token, SECRET, algorithms=["HS256"], audience="mcp-gateway", options={"verify_exp": False}
    )
    assert claims == {
        "sub": "bob",
        "role": "admin",
        "aud": "mcp-gateway",
        "iss": "mcp-gateway-dev",
        "iat": 1_000_000,
        "exp": 1_000_100,
    }


def test_main_prints_verifiable_token(monkeypatch, capsys):
    monkeypatch.setenv("GATEWAY_JWT_SECRET", SECRET)
    monkeypatch.delenv("GATEWAY_JWT_AUDIENCE", raising=False)
    monkeypatch.delenv("GATEWAY_JWT_ISSUER", raising=False)
    assert main(["--role", "viewer", "--sub", "alice"]) == 0
    out = capsys.readouterr().out
    assert len(out.splitlines()) == 1
    principal = JwtVerifier(SECRET, "mcp-gateway", "mcp-gateway-dev").verify(out.strip())
    assert (principal.subject, principal.role) == ("alice", "viewer")


def test_main_honours_audience_and_issuer_env(monkeypatch, capsys):
    monkeypatch.setenv("GATEWAY_JWT_SECRET", SECRET)
    monkeypatch.setenv("GATEWAY_JWT_AUDIENCE", "aud-x")
    monkeypatch.setenv("GATEWAY_JWT_ISSUER", "iss-x")
    assert main(["--role", "admin"]) == 0
    token = capsys.readouterr().out.strip()
    assert JwtVerifier(SECRET, "aud-x", "iss-x").verify(token).role == "admin"


def test_main_missing_secret_exits_2(monkeypatch, capsys):
    monkeypatch.delenv("GATEWAY_JWT_SECRET", raising=False)
    assert main(["--role", "viewer"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "GATEWAY_JWT_SECRET" in captured.err


def test_main_short_secret_exits_2(monkeypatch, capsys):
    monkeypatch.setenv("GATEWAY_JWT_SECRET", "short")
    assert main(["--role", "viewer"]) == 2
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "32" in captured.err
