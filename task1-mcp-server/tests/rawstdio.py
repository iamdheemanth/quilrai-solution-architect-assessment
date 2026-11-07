"""Black-box JSON-RPC-over-stdio driver: spawns a server and speaks raw lines to it."""

from __future__ import annotations

import json
import os
import subprocess
import threading
import time


class RawStdioSession:
    def __init__(self, cmd: list[str], env: dict[str, str] | None = None) -> None:
        child_env = dict(os.environ if env is None else env)
        child_env["PYTHONUNBUFFERED"] = "1"
        self._cond = threading.Condition()
        self._out: list[str] = []
        self._err: list[str] = []
        self._out_eof = False
        self._next_id = 1
        self._closed = False
        self._exit_code: int | None = None
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=child_env,
        )
        self._threads = [
            threading.Thread(
                target=self._pump, args=(self._proc.stdout, self._out, True), daemon=True
            ),
            threading.Thread(
                target=self._pump, args=(self._proc.stderr, self._err, False), daemon=True
            ),
        ]
        for t in self._threads:
            t.start()

    def _pump(self, pipe, sink: list[str], is_stdout: bool) -> None:
        try:
            for raw in iter(pipe.readline, b""):
                line = raw.decode("utf-8", errors="replace").rstrip("\r\n")
                with self._cond:
                    sink.append(line)
                    self._cond.notify_all()
        finally:
            with self._cond:
                if is_stdout:
                    self._out_eof = True
                self._cond.notify_all()

    @property
    def stdout_lines(self) -> list[str]:
        with self._cond:
            return list(self._out)

    @property
    def stderr_text(self) -> str:
        with self._cond:
            return "\n".join(self._err)

    def send(self, msg: dict | str) -> None:
        text = msg if isinstance(msg, str) else json.dumps(msg)
        self._proc.stdin.write((text + "\n").encode("utf-8"))
        self._proc.stdin.flush()

    def request(self, method: str, params: dict | None = None, timeout: float = 10) -> dict:
        req_id = self._next_id
        self._next_id += 1
        msg: dict = {"jsonrpc": "2.0", "id": req_id, "method": method}
        if params is not None:
            msg["params"] = params
        self.send(msg)
        deadline = time.monotonic() + timeout
        seen = 0
        with self._cond:
            while True:
                while seen < len(self._out):
                    line = self._out[seen]
                    seen += 1
                    try:
                        obj = json.loads(line)
                    except ValueError:
                        continue
                    if (
                        isinstance(obj, dict)
                        and obj.get("id") == req_id
                        and ("result" in obj or "error" in obj)
                    ):
                        return obj
                remaining = deadline - time.monotonic()
                if self._out_eof or remaining <= 0:
                    why = "stdout closed" if self._out_eof else f"no reply in {timeout}s"
                    raise AssertionError(
                        f"{method} (id={req_id}): {why}; stderr:\n" + "\n".join(self._err)
                    )
                self._cond.wait(remaining)

    def initialize(self) -> dict:
        resp = self.request(
            "initialize",
            {
                "protocolVersion": "2025-11-25",
                "capabilities": {},
                "clientInfo": {"name": "rawstdio", "version": "0"},
            },
        )
        self.send({"jsonrpc": "2.0", "method": "notifications/initialized"})
        return resp

    def close(self, timeout: float = 10) -> int:
        if self._closed:
            return self._exit_code
        self._closed = True
        try:
            self._proc.stdin.close()
        except OSError:
            pass
        timed_out = False
        try:
            self._exit_code = self._proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            timed_out = True
            self._proc.kill()
            self._proc.wait()
        for t in self._threads:
            t.join(timeout=5)
        self._proc.stdout.close()
        self._proc.stderr.close()
        if timed_out:
            raise AssertionError(
                f"server did not exit within {timeout}s of stdin EOF; stderr:\n" + self.stderr_text
            )
        return self._exit_code
