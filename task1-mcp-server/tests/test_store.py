import pytest

from customer_mcp.store import (
    BusinessError,
    CustomerNotFound,
    CustomerStore,
    RefundRejected,
)


@pytest.fixture
def store() -> CustomerStore:
    return CustomerStore.seeded()


def test_errors_are_business_errors():
    assert issubclass(CustomerNotFound, BusinessError)
    assert issubclass(RefundRejected, BusinessError)


def test_get_returns_record(store):
    rec = store.get("CUST-00001")
    assert rec.name == "Ada Lovelace"
    assert rec.email == "ada@example.com"
    assert rec.tier == "gold"
    assert rec.account_status == "active"
    assert rec.refundable_balance == 500.0


def test_seeded_includes_alnum_and_suspended(store):
    assert store.get("CUST-A1B2C").name == "Katherine Johnson"
    assert store.get("CUST-00003").account_status == "suspended"


def test_get_unknown_raises(store):
    with pytest.raises(CustomerNotFound, match="No customer found with id CUST-00404"):
        store.get("CUST-00404")


def test_refund_unknown_raises(store):
    with pytest.raises(CustomerNotFound, match="CUST-00404"):
        store.refund("CUST-00404", 1, "Refund for unknown")


def test_refund_decrements_balance_exactly(store):
    r1 = store.refund("CUST-00002", 0.1, "Duplicate charge refund")
    r2 = store.refund("CUST-00002", 0.2, "Duplicate charge refund")
    assert (r1.refund_id, r2.refund_id) == ("RF-000001", "RF-000002")
    assert r1.status == "processed"
    assert r1.amount == 0.1
    assert r2.remaining_refundable_balance == 120.20
    assert store.get("CUST-00002").refundable_balance == 120.20


def test_refund_full_balance_allowed(store):
    r = store.refund("CUST-00001", 500, "Full refund requested")
    assert r.remaining_refundable_balance == 0.0


def test_refund_over_balance_rejected(store):
    with pytest.raises(RefundRejected, match="exceeds"):
        store.refund("CUST-00001", 1e12, "Large refund request")


def test_over_balance_message(store):
    with pytest.raises(RefundRejected) as ei:
        store.refund("CUST-00001", 500.01, "Large refund request")
    assert str(ei.value) == (
        "Refund of 500.01 exceeds refundable balance of 500.00 for CUST-00001"
    )


def test_refund_suspended_rejected(store):
    with pytest.raises(RefundRejected, match="suspended") as ei:
        store.refund("CUST-00003", 10, "Refund for outage")
    assert str(ei.value) == "Account CUST-00003 is suspended; refunds are not permitted"


def test_suspended_checked_before_balance(store):
    with pytest.raises(RefundRejected, match="suspended"):
        store.refund("CUST-00003", 1e12, "Refund for outage")


def test_rejected_refund_leaves_balance_unchanged(store):
    with pytest.raises(RefundRejected):
        store.refund("CUST-00001", 1e12, "Large refund request")
    assert store.get("CUST-00001").refundable_balance == 500.0
    assert store.refund("CUST-00001", 1, "Follow up refund").refund_id == "RF-000001"


def test_seeded_stores_are_independent():
    a, b = CustomerStore.seeded(), CustomerStore.seeded()
    a.refund("CUST-00001", 100, "Refund for outage")
    assert b.get("CUST-00001").refundable_balance == 500.0
    assert b.refund("CUST-00001", 1, "Refund for outage").refund_id == "RF-000001"
