"""Console entry point: ``python -m llm_router`` / ``llm-router``."""

from __future__ import annotations

import logging
import sys

import uvicorn

from .app import create_app
from .config import ConfigError, load_settings
from .context import install_request_id_filter


def main() -> None:
    try:
        settings = load_settings()
    except ConfigError as exc:
        # ConfigError messages name the variable, never its value (several are secrets).
        sys.stderr.write(f"llm-router: configuration error: {exc}\n")
        raise SystemExit(2) from None
    logging.basicConfig(
        stream=sys.stderr,
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s [%(request_id)s]: %(message)s",
        force=True,
    )
    for handler in logging.getLogger().handlers:
        install_request_id_filter(handler)  # every record (uvicorn's too) gets request_id
    # httpx2 logs every upstream request URL at INFO; keep that out of the router log.
    for noisy in ("httpx2", "httpcore2"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    uvicorn.run(
        create_app(settings),
        host=settings.host,
        port=settings.port,
        server_header=False,
        log_level=settings.log_level.lower(),
    )


if __name__ == "__main__":
    main()
