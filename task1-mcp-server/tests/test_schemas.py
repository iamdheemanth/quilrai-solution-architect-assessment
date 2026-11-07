import pytest
from pydantic import ValidationError

from customer_mcp.schemas import build_arg_models
from customer_mcp.validation import describe_validation_error, render_issues

m = build_arg_models("digits")
a = build_arg_models("alnum")

VALID_REFUND = {
    "customer_id": "CUST-00001",
    "amount": 25.5,
    "reason": "Duplicate charge on invoice",
}


@pytest.mark.parametrize("cid", ["CUST-00001", "CUST-99999"])
def test_digits_accepts(cid):
    model = m.get_customer_record.model_validate({"customer_id": cid})
    assert model.customer_id == cid


@pytest.mark.parametrize(
    "cid",
    [
        "CUST-1234",
        "CUST-123456",
        "cust-00001",
        " CUST-00001",
        "CUST-00001\n",
        "CUST-١٢٣٤٥",
        "CUST-A1B2C",
        "CUST_00001",
        "",
        12345,
    ],
)
def test_digits_rejects(cid):
    with pytest.raises(ValidationError):
        m.get_customer_record.model_validate({"customer_id": cid})


def test_alnum_accepts_letters():
    model = a.get_customer_record.model_validate({"customer_id": "CUST-A1B2C"})
    assert model.customer_id == "CUST-A1B2C"


@pytest.mark.parametrize("cid", ["CUST-a1b2c", "cust-A1B2C", "CUST-A1B2"])
def test_alnum_rejects(cid):
    with pytest.raises(ValidationError):
        a.get_customer_record.model_validate({"customer_id": cid})


def test_amount_accepts_json_int_as_float():
    assert m.trigger_refund.model_validate({**VALID_REFUND, "amount": 100}).amount == 100.0


BAD_AMOUNTS = ["50", True, 0, -1, 1e-9, 10.005, float("inf"), float("nan"), None]


@pytest.mark.parametrize("amount", BAD_AMOUNTS)
def test_amount_rejects(amount):
    with pytest.raises(ValidationError):
        m.trigger_refund.model_validate({**VALID_REFUND, "amount": amount})


@pytest.mark.parametrize(
    "reason",
    [
        "too short",
        " " * 10,
        "  short   ",
        "x" * 501,
        1234567890,
        "\x00" * 10,
        "​" * 10,
        "​" * 5 + "abcd",
    ],
)
def test_reason_rejects(reason):
    with pytest.raises(ValidationError):
        m.trigger_refund.model_validate({**VALID_REFUND, "reason": reason})


def test_reason_accepts_exactly_10_chars():
    model = m.trigger_refund.model_validate({**VALID_REFUND, "reason": "0123456789"})
    assert model.reason == "0123456789"


def test_reason_accepts_embedded_tab_and_newline():
    reason = "Duplicate\tcharge\non invoice"
    assert m.trigger_refund.model_validate({**VALID_REFUND, "reason": reason}).reason == reason


def test_extra_and_missing_fields_rejected():
    with pytest.raises(ValidationError) as e:
        m.trigger_refund.model_validate({"customer_id": "CUST-00001", "junk": 1})
    issues = describe_validation_error(e.value, "digits")
    assert {i.field for i in issues} == {"amount", "reason", "junk"}


def test_published_schema_is_strict():
    s = m.trigger_refund.model_json_schema()
    assert s["additionalProperties"] is False
    assert s["properties"]["customer_id"]["pattern"] == "^CUST-[0-9]{5}$"
    assert s["properties"]["amount"]["exclusiveMinimum"] == 0
    assert s["properties"]["reason"]["minLength"] == 10
    assert set(s["required"]) == {"customer_id", "amount", "reason"}


def test_rendered_message_names_fields_but_never_echoes_input():
    secret = "CUST-SECRET-ssn-123-45-6789"
    with pytest.raises(ValidationError) as e:
        m.trigger_refund.model_validate({"customer_id": secret, "amount": "50", "reason": "short"})
    text = render_issues("trigger_refund", describe_validation_error(e.value, "digits"))
    assert text.startswith("Invalid arguments for trigger_refund:")
    assert "customer_id" in text and "amount" in text and "reason" in text
    assert "CUST-XXXXX" in text and "digit" in text
    assert secret not in text and "123-45-6789" not in text and '"50"' not in text


def test_alnum_hint_wording():
    with pytest.raises(ValidationError) as e:
        a.get_customer_record.model_validate({"customer_id": "bad"})
    (issue,) = describe_validation_error(e.value, "alnum")
    assert "uppercase letter or digit" in issue.message
    assert "CUST-A1B2C" in issue.message


def test_huge_extra_key_is_capped():
    with pytest.raises(ValidationError) as e:
        m.get_customer_record.model_validate({"customer_id": "CUST-00001", "k" * 10_000: 1})
    issues = describe_validation_error(e.value, "digits")
    assert all(len(i.field) <= 65 for i in issues)


def test_extra_key_with_newline_is_escaped():
    with pytest.raises(ValidationError) as e:
        m.get_customer_record.model_validate({"customer_id": "CUST-00001", "a\nb\x00\u200b": 1})
    issues = describe_validation_error(e.value, "digits")
    assert all("\n" not in i.field and "\x00" not in i.field for i in issues)
    rendered = render_issues("get_customer_record", issues)
    assert len(rendered.splitlines()) == 1 + len(issues)  # no injected extra lines


def test_issue_list_is_capped_at_20():
    with pytest.raises(ValidationError) as e:
        extras = {f"x{i}": 1 for i in range(50)}
        m.get_customer_record.model_validate({"customer_id": "CUST-00001", **extras})
    issues = describe_validation_error(e.value, "digits")
    assert len(issues) == 21
    assert issues[-1].code == "too_many_errors" and issues[-1].field == "<arguments>"
    assert issues[-1].message.endswith("and 30 more")
