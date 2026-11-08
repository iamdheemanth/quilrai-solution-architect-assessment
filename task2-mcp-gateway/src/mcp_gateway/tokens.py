"""Mint HS256 demo tokens for the gateway (library function and ``mint-token`` CLI)."""

from __future__ import annotations

import argparse
import os
import sys
import time

import jwt

from mcp_gateway.config import MIN_SECRET_BYTES, Role, Settings


def mint_token(
    secret: str,
    role: Role,
    subject: str = "demo-user",
    audience: str = "mcp-gateway",
    issuer: str = "mcp-gateway-dev",
    ttl_seconds: int = 3600,
    now: int | None = None,
) -> str:
    issued = int(time.time()) if now is None else now
    claims = {
        "sub": subject,
        "role": role,
        "aud": audience,
        "iss": issuer,
        "iat": issued,
        "exp": issued + ttl_seconds,
    }
    return jwt.encode(claims, secret, algorithm="HS256")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="mint-token", description="Mint a gateway JWT.")
    parser.add_argument("--role", required=True, choices=["admin", "viewer"])
    parser.add_argument("--sub", default="demo-user")
    parser.add_argument("--ttl", type=int, default=3600, help="lifetime in seconds")
    args = parser.parse_args(argv)

    secret = os.environ.get("GATEWAY_JWT_SECRET", "")
    if len(secret.encode("utf-8")) < MIN_SECRET_BYTES:
        sys.stderr.write(
            f"error: GATEWAY_JWT_SECRET must be set to at least {MIN_SECRET_BYTES} bytes\n"
        )
        return 2
    audience = os.environ.get("GATEWAY_JWT_AUDIENCE", "").strip() or Settings.jwt_audience
    issuer = os.environ.get("GATEWAY_JWT_ISSUER", "").strip() or Settings.jwt_issuer
    token = mint_token(secret, args.role, args.sub, audience, issuer, args.ttl)
    print(token)  # noqa: T201
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
