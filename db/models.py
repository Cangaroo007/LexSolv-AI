"""
LexSolv AI — SQLAlchemy ORM models.

These map directly to PostgreSQL tables and correspond to the Pydantic schemas
in models/schemas.py.  The Pydantic schemas handle API serialization; these
ORM models handle persistence.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Enum as SAEnum,
    ForeignKey,
    Index,
    Numeric,
    String,
    Text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from db.database import Base


# ---------------------------------------------------------------------------
# Company — the entity under insolvency / administration
# ---------------------------------------------------------------------------

class CompanyDB(Base):
    __tablename__ = "companies"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    legal_name = Column(String(255), nullable=False)
    trading_name = Column(String(255), nullable=True)
    abn = Column(String(11), nullable=True, index=True)
    acn = Column(String(9), nullable=True, index=True)

    # Financial snapshot
    total_assets = Column(Numeric(15, 2), default=0)
    total_liabilities = Column(Numeric(15, 2), default=0)
    total_creditors = Column(Numeric(15, 2), default=0)

    # Integration metadata
    source = Column(String(50), nullable=True)  # 'xero' or 'myob'
    external_id = Column(String(255), nullable=True)
    last_synced_at = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    creditors = relationship("CreditorDB", back_populates="company", cascade="all, delete-orphan")
    transactions = relationship("TransactionDB", back_populates="company", cascade="all, delete-orphan")
    integration_connections = relationship("IntegrationConnectionDB", back_populates="company", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_companies_source_external", "source", "external_id", unique=True),
    )

    def __repr__(self) -> str:
        return f"<Company {self.legal_name} ({self.id})>"


# ---------------------------------------------------------------------------
# Creditor — a single creditor claim against a company
# ---------------------------------------------------------------------------

class CreditorDB(Base):
    __tablename__ = "creditors"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)

    creditor_name = Column(String(255), nullable=False)
    contact_email = Column(String(255), nullable=True)
    contact_phone = Column(String(50), nullable=True)

    amount_claimed = Column(Numeric(15, 2), nullable=False)
    amount_admitted = Column(Numeric(15, 2), nullable=True)
    currency = Column(String(3), default="AUD")

    status = Column(
        SAEnum("active", "disputed", "written_off", "paid", name="creditor_status"),
        default="active",
        nullable=False,
    )
    category = Column(String(50), nullable=True)  # secured, unsecured, priority, employee
    due_date = Column(Date, nullable=True)

    source_invoice_ids = Column(JSONB, default=list)  # list of strings
    notes = Column(Text, nullable=True)

    as_at_date = Column(Date, nullable=False, server_default=func.current_date())

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    company = relationship("CompanyDB", back_populates="creditors")

    __table_args__ = (
        Index("ix_creditors_company_status", "company_id", "status"),
    )

    def __repr__(self) -> str:
        return f"<Creditor {self.creditor_name} — ${self.amount_claimed}>"


# ---------------------------------------------------------------------------
# Transaction — normalised financial transaction from Xero / MYOB
# ---------------------------------------------------------------------------

class TransactionDB(Base):
    __tablename__ = "transactions"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)

    transaction_type = Column(
        SAEnum("invoice", "credit_note", "payment", "journal", "bank_transaction", name="transaction_type"),
        nullable=False,
    )
    status = Column(
        SAEnum("draft", "submitted", "authorised", "paid", "voided", name="transaction_status"),
        default="authorised",
        nullable=False,
    )

    reference = Column(String(255), nullable=True)
    description = Column(Text, nullable=True)

    date = Column(Date, nullable=False)
    due_date = Column(Date, nullable=True)

    amount = Column(Numeric(15, 2), nullable=False)
    tax_amount = Column(Numeric(15, 2), default=0)
    currency = Column(String(3), default="AUD")

    account_code = Column(String(50), nullable=True)
    account_name = Column(String(255), nullable=True)
    contact_name = Column(String(255), nullable=True)

    # Integration metadata
    source = Column(String(50), nullable=True)
    external_id = Column(String(255), nullable=True)
    raw_payload = Column(JSONB, nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    company = relationship("CompanyDB", back_populates="transactions")

    __table_args__ = (
        Index("ix_transactions_company_date", "company_id", "date"),
        Index("ix_transactions_source_external", "source", "external_id"),
        Index("ix_transactions_type", "company_id", "transaction_type"),
    )

    def __repr__(self) -> str:
        return f"<Transaction {self.reference} {self.transaction_type} ${self.amount}>"


# ---------------------------------------------------------------------------
# IntegrationConnection — tracks which accounting system a company is linked to
# ---------------------------------------------------------------------------

class IntegrationConnectionDB(Base):
    __tablename__ = "integration_connections"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUID(as_uuid=True), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)

    provider = Column(String(50), nullable=False)  # 'xero' or 'myob'
    tenant_id = Column(String(255), nullable=True)  # Xero tenant ID or MYOB company file ID
    tenant_name = Column(String(255), nullable=True)

    is_active = Column(Boolean, default=True, nullable=False)
    connected_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    last_synced_at = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    company = relationship("CompanyDB", back_populates="integration_connections")
    oauth_tokens = relationship("OAuthTokenDB", back_populates="connection", cascade="all, delete-orphan")

    __table_args__ = (
        Index("ix_intconn_provider_tenant", "provider", "tenant_id", unique=True),
    )

    def __repr__(self) -> str:
        return f"<IntegrationConnection {self.provider}:{self.tenant_id}>"


# ---------------------------------------------------------------------------
# OAuthToken — stores encrypted OAuth2 tokens for each connection
# ---------------------------------------------------------------------------

class OAuthTokenDB(Base):
    __tablename__ = "oauth_tokens"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    connection_id = Column(
        UUID(as_uuid=True),
        ForeignKey("integration_connections.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    access_token = Column(Text, nullable=False)   # Encrypt at rest in production
    refresh_token = Column(Text, nullable=True)
    token_type = Column(String(50), default="Bearer")
    scope = Column(Text, nullable=True)
    expires_at = Column(DateTime(timezone=True), nullable=True)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    connection = relationship("IntegrationConnectionDB", back_populates="oauth_tokens")

    def __repr__(self) -> str:
        return f"<OAuthToken connection={self.connection_id} expires={self.expires_at}>"
