"""Contract models: strict tool arguments and tool outputs."""

from dataclasses import dataclass
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

from customer_mcp.config import CustomerIdCharset

CUSTOMER_ID_PATTERNS: dict[CustomerIdCharset, str] = {
    "digits": r"^CUST-[0-9]{5}$",
    "alnum": r"^CUST-[A-Z0-9]{5}$",
}

_ID_DESCRIPTIONS: dict[CustomerIdCharset, str] = {
    "digits": "Customer identifier: CUST- followed by exactly 5 digits (e.g. CUST-00042).",
    "alnum": (
        "Customer identifier: CUST- followed by exactly 5 uppercase letters or digits "
        "(e.g. CUST-A1B2C)."
    ),
}


class StrictArgs(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


@dataclass(frozen=True)
class ArgModels:
    get_customer_record: type[StrictArgs]
    trigger_refund: type[StrictArgs]


def build_arg_models(charset: CustomerIdCharset) -> ArgModels:
    pattern = CUSTOMER_ID_PATTERNS[charset]
    id_description = _ID_DESCRIPTIONS[charset]

    class GetCustomerRecordArgs(StrictArgs):
        customer_id: str = Field(pattern=pattern, description=id_description)

    class TriggerRefundArgs(StrictArgs):
        customer_id: str = Field(pattern=pattern, description=id_description)
        amount: Annotated[float, Field(gt=0, allow_inf_nan=False)] = Field(
            description="Refund amount; greater than 0, at most 2 decimal places.",
        )
        reason: str = Field(
            min_length=10,
            max_length=500,
            description="Why the refund is being issued; 10 to 500 characters of non-blank text.",
        )

        @field_validator("amount")
        @classmethod
        def _two_decimals(cls, v: float) -> float:
            if round(v, 2) != v:
                raise ValueError("must have at most 2 decimal places")
            return v

        @field_validator("reason")
        @classmethod
        def _not_blank(cls, v: str) -> str:
            if len(v.strip()) < 10:
                raise ValueError("must contain at least 10 characters after trimming whitespace")
            return v

    return ArgModels(get_customer_record=GetCustomerRecordArgs, trigger_refund=TriggerRefundArgs)


class CustomerRecord(BaseModel):
    customer_id: str
    name: str
    email: str
    tier: Literal["standard", "gold", "platinum"]
    account_status: Literal["active", "suspended"]
    refundable_balance: float


class RefundReceipt(BaseModel):
    refund_id: str
    customer_id: str
    amount: float
    status: Literal["processed"]
    remaining_refundable_balance: float
