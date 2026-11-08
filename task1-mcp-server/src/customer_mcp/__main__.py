"""Process entry point: JSON-RPC on stdout, logs on stderr, exit 0 on stdin EOF."""

from __future__ import annotations

import logging
import sys

import anyio

from customer_mcp import __version__
from customer_mcp.config import ConfigError, load_settings
from customer_mcp.server import build_server
from customer_mcp.store import CustomerStore
from customer_mcp.transport import serve_stdio

log = logging.getLogger("customer_mcp.main")


def main() -> None:
    try:
        settings = load_settings()
    except ConfigError as e:
        sys.stderr.write(f"customer-mcp: configuration error: {e}\n")
        sys.exit(2)

    logging.basicConfig(
        stream=sys.stderr,
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        force=True,
    )
    logging.getLogger("mcp").setLevel(logging.WARNING)
    log.info(
        "starting customer-mcp version=%s validation_errors=%s customer_id_charset=%s",
        __version__,
        settings.validation_errors,
        settings.customer_id_charset,
    )
    server = build_server(settings, CustomerStore.seeded())
    anyio.run(serve_stdio, server)


if __name__ == "__main__":
    main()
