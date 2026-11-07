"""Test fixture: run the real server with a store that fails, to prove crash isolation."""

from customer_mcp.__main__ import main
from customer_mcp.store import CustomerStore


def _boom(self, customer_id):
    raise RuntimeError("simulated backend failure")


CustomerStore.get = _boom

if __name__ == "__main__":
    main()
