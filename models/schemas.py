"""
LexSolv AI — Pydantic schemas for insolvency management data models.

These schemas standardise the data shapes exchanged between the platform core
and external accounting integrations (Xero, MYOB).
"""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from enum import Enum
from typing import Optional
from uuid import UUID, uuid4

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------

class TransactionType(str, Enum):
    """Broad classification of a financial transaction."""
    INVOICE = "invoice"
    CREDIT_NOTE = "credit_note"
    PAYMENT = "payment"
    JOURNAL = "journal"
    BANK_TRANSACTION = "bank_transaction"


class TransactionStatus(str, Enum):
    DRAFT = "draft"
    SUBMITTED = "submitted"
    AUTHORISED = "authorised"
    PAID = "paid"
    VOIDED = "voided"


class CreditorStatus(str, Enum):
    ACTIVE = "active"
    DISPUTED = "disputed"
    WRITTEN_OFF = "written_off"
    PAID = "paid"


# ---------------------------------------------------------------------------
# CompanyData
# ---------------------------------------------------------------------------

class CompanyData(BaseModel):
    """
    Represents the financial profile of a company under administration /
    insolvency proceedings.  Populated from an accounting integration.
    """

    id: UUID = Field(default_factory=uuid4, description="Internal unique identifier")
    legal_name: str = Field(..., min_length=1, description="Registered company name")
    trading_name: Optional[str] = Field(None, description="Trading / DBA name")
    abn: Optional[str] = Field(None, pattern=r"^\d{11}$", description="Australian Business Number (11 digits)")
    acn: Optional[str] = Field(None, pattern=r"^\d{9}$", description="Australian Company Number (9 digits)")

    # Financial snapshot
    total_assets: Decimal = Field(default=Decimal("0.00"), description="Total assets per last balance sheet")
    total_liabilities: Decimal = Field(default=Decimal("0.00"), description="Total liabilities per last balance sheet")
    total_creditors: Decimal = Field(default=Decimal("0.00"), description="Sum of all outstanding creditor claims")

    # Integration metadata
    source: Optional[str] = Field(None, description="Origin system, e.g. 'xero' or 'myob'")
    external_id: Optional[str] = Field(None, description="ID in the source accounting system")
    last_synced_at: Optional[datetime] = Field(None, description="Timestamp of last successful sync")

    class Config:
        json_schema_extra = {
            "example": {
                "legal_name": "Acme Holdings Pty Ltd",
                "trading_name": "Acme",
                "abn": "12345678901",
                "acn": "123456789",
                "total_assets": "1250000.00",
                "total_liabilities": "980000.00",
                "total_creditors": "640000.00",
                "source": "xero",
                "external_id": "xero-org-abc123",
            }
        }


# ---------------------------------------------------------------------------
# CreditorList / CreditorEntry
# ---------------------------------------------------------------------------

class CreditorEntry(BaseModel):
    """A single creditor claim against the company."""

    id: UUID = Field(default_factory=uuid4)
    creditor_name: str = Field(..., min_length=1)
    contact_email: Optional[str] = None
    contact_phone: Optional[str] = None

    amount_claimed: Decimal = Field(..., description="Original claim amount")
    amount_admitted: Optional[Decimal] = Field(None, description="Admitted amount after adjudication")
    currency: str = Field(default="AUD", max_length=3)

    status: CreditorStatus = Field(default=CreditorStatus.ACTIVE)
    category: Optional[str] = Field(
        None,
        description="Creditor category, e.g. 'secured', 'unsecured', 'priority', 'employee'"
    )
    due_date: Optional[date] = None

    # Traceability
    source_invoice_ids: list[str] = Field(default_factory=list, description="Invoice IDs from accounting system")
    notes: Optional[str] = None


class CreditorList(BaseModel):
    """
    Aggregated list of creditors for a company under insolvency proceedings.
    Typically built by pulling aged-payables data from the accounting system.
    """

    company_id: UUID = Field(..., description="Reference to the parent CompanyData record")
    as_at_date: date = Field(..., description="Cut-off date for the creditor snapshot")
    creditors: list[CreditorEntry] = Field(default_factory=list)

    @property
    def total_claimed(self) -> Decimal:
        return sum((c.amount_claimed for c in self.creditors), Decimal("0.00"))

    @property
    def total_admitted(self) -> Decimal:
        return sum(
            (c.amount_admitted for c in self.creditors if c.amount_admitted is not None),
            Decimal("0.00"),
        )

    @property
    def creditor_count(self) -> int:
        return len(self.creditors)

    class Config:
        json_schema_extra = {
            "example": {
                "company_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "as_at_date": "2026-02-01",
                "creditors": [
                    {
                        "creditor_name": "Supplier Co",
                        "amount_claimed": "45000.00",
                        "status": "active",
                        "category": "unsecured",
                    }
                ],
            }
        }


# ---------------------------------------------------------------------------
# Transaction
# ---------------------------------------------------------------------------

class Transaction(BaseModel):
    """
    A normalised financial transaction record sourced from Xero or MYOB.
    Used for ledger reconstruction and forensic analysis during insolvency.
    """

    id: UUID = Field(default_factory=uuid4)
    company_id: UUID = Field(..., description="Reference to the parent CompanyData record")

    transaction_type: TransactionType
    status: TransactionStatus = Field(default=TransactionStatus.AUTHORISED)

    reference: Optional[str] = Field(None, description="Invoice number, journal ref, etc.")
    description: Optional[str] = None

    date: date = Field(..., description="Transaction date")
    due_date: Optional[date] = None

    amount: Decimal = Field(..., description="Gross amount (positive)")
    tax_amount: Decimal = Field(default=Decimal("0.00"))
    currency: str = Field(default="AUD", max_length=3)

    account_code: Optional[str] = Field(None, description="Chart-of-accounts code from the source system")
    account_name: Optional[str] = None
    contact_name: Optional[str] = Field(None, description="Counter-party / vendor / debtor name")

    # Integration metadata
    source: Optional[str] = Field(None, description="'xero' or 'myob'")
    external_id: Optional[str] = Field(None, description="ID in the source accounting system")
    raw_payload: Optional[dict] = Field(None, description="Original API response payload for audit trail")

    class Config:
        json_schema_extra = {
            "example": {
                "company_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
                "transaction_type": "invoice",
                "status": "authorised",
                "reference": "INV-0042",
                "date": "2026-01-15",
                "due_date": "2026-02-15",
                "amount": "12500.00",
                "tax_amount": "1136.36",
                "account_code": "200",
                "account_name": "Sales",
                "contact_name": "Widget Buyers Pty Ltd",
                "source": "xero",
                "external_id": "xero-inv-xyz789",
            }
        }
