"""Per-request context for logging.

The endpoint sets ``request_id_var`` for the duration of a request. ``RequestIdFilter`` copies it
onto every log record as ``record.request_id``, so WARNINGs from the router or limiter are tied
to the request without each call site passing the id. Outside a request it is ``"-"``.
"""

from __future__ import annotations

import logging
from contextvars import ContextVar

request_id_var: ContextVar[str] = ContextVar("request_id", default="-")


class RequestIdFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if not getattr(record, "request_id", None):
            record.request_id = request_id_var.get()
        return True


def install_request_id_filter(target: logging.Logger | logging.Handler) -> None:
    """Attach one RequestIdFilter to a logger or handler (idempotent)."""
    if not any(isinstance(f, RequestIdFilter) for f in target.filters):
        target.addFilter(RequestIdFilter())
