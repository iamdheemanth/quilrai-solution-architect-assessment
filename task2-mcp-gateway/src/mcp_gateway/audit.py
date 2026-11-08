"""Audit trail: one escaped JSON line per decision on logger ``mcp_gateway.audit``."""

from __future__ import annotations

import json
import logging
import unicodedata
from datetime import UTC, datetime

MAX_ECHO = 64

_log = logging.getLogger("mcp_gateway.audit")


def safe_name(value: str) -> str:
    """Escape control/format characters, then cap at ``MAX_ECHO`` characters plus an ellipsis."""
    out: list[str] = []
    for ch in value:
        if unicodedata.category(ch) in ("Cc", "Cf"):
            cp = ord(ch)
            out.append(f"\\u{cp:04x}" if cp <= 0xFFFF else f"\\U{cp:08x}")
        else:
            out.append(ch)
    text = "".join(out)
    return text[:MAX_ECHO] + "\u2026" if len(text) > MAX_ECHO else text


def audit_event(**fields: object) -> None:
    record = {"event": "authz.decision", "ts": datetime.now(UTC).isoformat(), **fields}
    try:
        line = json.dumps(record, ensure_ascii=True, separators=(",", ":"), default=str)
    except (TypeError, ValueError):  # e.g. non-str keys, circular refs
        line = json.dumps(
            {"event": "authz.decision", "ts": record["ts"], "error": "unserializable_fields"},
            separators=(",", ":"),
        )
    _log.info("%s", line)
