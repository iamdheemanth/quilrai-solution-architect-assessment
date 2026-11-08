"""Streaming redactor using viable-prefix holdback.

Invariant: ``"".join(feed(d) for d in deltas) + close()`` equals ``batch_redact(text)`` for any
split of the text. A pending match is released only when it is final: it must (1) start before
any still-growing partial match (Q search), (2) end at least R chars before the buffer end so
negative lookaheads are settled (+R margin), and (3) not be a prefix of a longer match
(``fullmatch(partial=True)``). Text before the earliest possible match start is emitted at once.
"""

from collections import Counter

from llm_gateway.patterns import P_ARGS, Q_ARGS, REPL, K, P, Q, R, redaction_type, valid


class StreamRedactor:
    """Redacts one text channel incrementally.

    ``_buf`` holds K chars of already-emitted context plus pending text; ``_c`` indexes the
    first pending char.

    ``held``: number of pending (not yet emitted) chars after the latest drain.
    ``max_held``: high-water mark of ``held``, measured after each ``feed``.
    ``counts``: redactions by type (EMAIL, SSN, CC); ``"CAP"`` counts fail-closed replacements
    made when pending text would exceed ``max_hold``.
    """

    def __init__(self, args: bool = False, max_hold: int = 1024, *, _skip=frozenset()):
        # _skip is test-only: names finality checks to disable (or q_naive) for mutation tests.
        self._p = P_ARGS if args else P
        self._q = Q_ARGS if args else Q
        self._max_hold = max_hold
        self._skip = _skip
        self._buf = ""
        self._c = 0
        self._max_held = 0
        self.counts: Counter[str] = Counter()

    @property
    def held(self) -> int:
        return len(self._buf) - self._c

    @property
    def max_held(self) -> int:
        return self._max_held

    def _emit_match(self, m, out: list[str]) -> None:
        if valid(m):
            out.append(REPL)
            self.counts[redaction_type(m)] += 1
        else:
            out.append(m.group())

    def _drain(self, eos: bool) -> str:
        buf, c, n, out = self._buf, self._c, len(self._buf), []
        skip = self._skip
        while c < n:
            m = self._p.search(buf, c)  # leftmost complete match
            s1 = m.start() if m else n
            if eos:  # buffer is the rest of the stream: batch semantics
                if not m:
                    break
                out.append(buf[c:s1])
                self._emit_match(m, out)
                c = m.end()
                continue
            if "q_search" in skip:
                s2 = n
            elif "q_naive" in skip:  # test-only: plain partial search, blind to lookahead-at-end
                q = self._p.search(buf, c, partial=True)
                s2 = q.start() if q is not None and q.partial else n
            else:
                q = self._q.search(buf, c, partial=True)  # leftmost partial match
                s2 = q.start() if q else n
            if (
                m
                and s1 < s2
                and ("margin" in skip or m.end() + R <= n)
                and ("fullmatch" in skip or self._p.fullmatch(buf, s1, partial=True) is None)
            ):
                out.append(buf[c:s1])
                self._emit_match(m, out)
                c = m.end()
                continue
            f = min(s1, s2)
            out.append(buf[c:f])  # [c, f) can never start a match: safe to emit
            c = f
            break
        if eos and c < n:
            out.append(buf[c:])
            c = n
        if n - c > self._max_hold:  # fail closed: never hold more than max_hold
            out.append(REPL)
            self.counts["CAP"] += 1
            c = n
        keep = max(0, c - K)
        self._buf, self._c = buf[keep:], c - keep
        return "".join(out)

    def feed(self, delta: str) -> str:
        self._buf += delta
        out = self._drain(False)
        self._max_held = max(self._max_held, self.held)
        return out

    def close(self) -> str:
        return self._drain(True)

    def discard(self) -> None:
        """Drop pending text without emitting it (fail closed)."""
        self._buf, self._c = "", 0
