"""In-memory mock customer database and refund business rules."""

from collections.abc import Iterable
from decimal import Decimal

from customer_mcp.schemas import CustomerRecord, RefundReceipt

_CENT = Decimal("0.01")


class BusinessError(Exception):
    """Expected, client-visible failure; str(exc) is safe to return to the client."""


class CustomerNotFound(BusinessError):
    pass


class RefundRejected(BusinessError):
    pass


def _money(value: float | Decimal) -> Decimal:
    return Decimal(str(value)).quantize(_CENT)


class CustomerStore:
    def __init__(self, records: Iterable[CustomerRecord]) -> None:
        self._records: dict[str, CustomerRecord] = {}
        self._balances: dict[str, Decimal] = {}
        for rec in records:
            self._records[rec.customer_id] = rec.model_copy()
            self._balances[rec.customer_id] = _money(rec.refundable_balance)
        self._refund_counter = 0

    @classmethod
    def seeded(cls) -> "CustomerStore":
        def rec(cid: str, name: str, email: str, tier: str, status: str, bal: float):
            return CustomerRecord(
                customer_id=cid,
                name=name,
                email=email,
                tier=tier,
                account_status=status,
                refundable_balance=bal,
            )

        return cls(
            [
                rec("CUST-00001", "Ada Lovelace", "ada@example.com", "gold", "active", 500.00),
                rec("CUST-00002", "Alan Turing", "alan@example.com", "standard", "active", 120.50),
                rec(
                    "CUST-00003", "Grace Hopper", "grace@example.com",
                    "platinum", "suspended", 1000.00,
                ),
                rec(
                    "CUST-A1B2C", "Katherine Johnson", "katherine@example.com",
                    "standard", "active", 75.00,
                ),
            ]
        )

    def _find(self, customer_id: str) -> CustomerRecord:
        record = self._records.get(customer_id)
        if record is None:
            raise CustomerNotFound(f"No customer found with id {customer_id}")
        return record

    def get(self, customer_id: str) -> CustomerRecord:
        record = self._find(customer_id)
        return record.model_copy(
            update={"refundable_balance": float(self._balances[customer_id])}
        )

    def refund(self, customer_id: str, amount: float, reason: str) -> RefundReceipt:
        record = self._find(customer_id)
        if record.account_status == "suspended":
            raise RefundRejected(
                f"Account {customer_id} is suspended; refunds are not permitted"
            )
        value = _money(amount)
        balance = self._balances[customer_id]
        if value > balance:
            raise RefundRejected(
                f"Refund of {value:.2f} exceeds refundable balance of {balance:.2f} "
                f"for {customer_id}"
            )
        remaining = balance - value
        self._balances[customer_id] = remaining
        self._refund_counter += 1
        return RefundReceipt(
            refund_id=f"RF-{self._refund_counter:06d}",
            customer_id=customer_id,
            amount=float(value),
            status="processed",
            remaining_refundable_balance=float(remaining),
        )
