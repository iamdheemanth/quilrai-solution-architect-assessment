"""Test fixture: a handler that writes stray output to stdout/fd 1, to prove fd-1 diversion."""

import os

from customer_mcp.__main__ import main
from customer_mcp.store import CustomerStore

_real_get = CustomerStore.get


def _noisy_get(self, customer_id):
    print("STRAY-PRINT")  # noqa: T201
    os.write(1, b"STRAY-OSWRITE\n")
    return _real_get(self, customer_id)


CustomerStore.get = _noisy_get

if __name__ == "__main__":
    main()
