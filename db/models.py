"""
LexSolv AI — SQLAlchemy ORM models.

These map directly to database tables and correspond to the Pydantic schemas
in models/schemas.py.  The Pydantic schemas handle API serialization; these
ORM models handle persistence.

Supports both PostgreSQL (production) and SQLite (local development).
"""

from __future__ import annotations

import uuid
from datetime import date, datetime

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    TypeDecorator,
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from db.database import Base, IS_SQLITE

# ---------------------------------------------------------------------------
# Cross-platform UUID type
# ---------------------------------------------------------------------------
# Uses native PostgreSQL UUID when available, falls back to CHAR(36) on SQLite.

if not IS_SQLITE:
    from sqlalchemy.dialects.postgresql import UUID as PG_UUID, JSONB


class UUIDType(TypeDecorator):
    """Platform-agnostic UUID type: native UUID on PostgreSQL, CHAR(36) on SQLite."""
    impl = String(36)
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(PG_UUID(as_uuid=True))
        return dialect.type_descriptor(String(36))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        if dialect.name == "postgresql":
            return value if isinstance(value, uuid.UUID) else uuid.UUID(str(value))
        return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if isinstance(value, uuid.UUID):
            return value
        return uuid.UUID(str(value))


class JSONBType(TypeDecorator):
    """Platform-agnostic JSONB type: native JSONB on PostgreSQL, JSON on SQLite."""
    impl = JSON
    cache_ok = True

    def load_dialect_impl(self, dialect):
        if dialect.name == "postgresql":
            return dialect.type_descriptor(JSONB)
        return dialect.type_descriptor(JSON)


# ---------------------------------------------------------------------------
# Company — the entity under insolvency / administration
# ---------------------------------------------------------------------------

class CompanyDB(Base):
    __tablename__ = "companies"

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    legal_name = Column(String(255), nullable=False)
    trading_name = Column(String(255), nullable=True)
    abn = Column(String(11), nullable=True, index=True)
    acn = Column(String(9), nullable=True, index=True)

    # SBR engagement fields
    appointment_date = Column(Date, nullable=True)
    practitioner_name = Column(String(255), nullable=True)
    industry = Column(String(100), nullable=True)

    # Financial snapshot
    total_assets = Column(Numeric(15, 2), default=0)
    total_liabilities = Column(Numeric(15, 2), default=0)
    total_creditors = Column(Numeric(15, 2), default=0)

    # Custom glossary for narrative generation (Layer 3 terms)
    custom_glossary = Column(JSONBType(), nullable=True)

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
    assets = relationship("AssetDB", back_populates="company", cascade="all, delete-orphan")
    plan_parameters = relationship("PlanParametersDB", back_populates="company", cascade="all, delete-orphan")

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

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)

    creditor_name = Column(String(255), nullable=False)
    contact_email = Column(String(255), nullable=True)
    contact_phone = Column(String(50), nullable=True)

    amount_claimed = Column(Numeric(15, 2), nullable=False)
    amount_admitted = Column(Numeric(15, 2), nullable=True)
    currency = Column(String(3), default="AUD")

    status = Column(String(20), default="active", nullable=False)
    category = Column(String(50), nullable=True)
    due_date = Column(Date, nullable=True)

    source_invoice_ids = Column(JSONBType(), default=list)
    notes = Column(Text, nullable=True)

    # SBR-specific fields (Prompt 1.2)
    is_related_party = Column(Boolean, default=False)
    is_secured = Column(Boolean, default=False)
    can_vote = Column(Boolean, default=True)
    source = Column(String(20), default="manual")  # 'parsed' or 'manual'

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

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)

    transaction_type = Column(String(30), nullable=False)
    status = Column(String(20), default="authorised", nullable=False)

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
    raw_payload = Column(JSONBType(), nullable=True)

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

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)

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

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    connection_id = Column(
        UUIDType(),
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


# ---------------------------------------------------------------------------
# Asset — individual asset entries for SBR vs Liquidation comparison (1.3)
# ---------------------------------------------------------------------------

class AssetDB(Base):
    __tablename__ = "assets"

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, index=True)
    asset_type = Column(String(50))
    description = Column(Text)
    book_value = Column(Float)
    liquidation_recovery_pct = Column(Float)
    liquidation_value = Column(Float)
    notes = Column(Text, nullable=True)
    source = Column(String(20), default="parsed")

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    company = relationship("CompanyDB", back_populates="assets")

    def __repr__(self) -> str:
        return f"<Asset {self.asset_type} book={self.book_value}>"


# ---------------------------------------------------------------------------
# PlanParameters — configurable SBR plan parameters (1.3)
# ---------------------------------------------------------------------------

class PlanParametersDB(Base):
    __tablename__ = "plan_parameters"

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    company_id = Column(UUIDType(), ForeignKey("companies.id", ondelete="CASCADE"), nullable=False, unique=True, index=True)
    total_contribution = Column(Float)
    practitioner_fee_pct = Column(Float, default=10.0)
    num_initial_payments = Column(Integer, default=2)
    initial_payment_amount = Column(Float, default=0.0)
    num_ongoing_payments = Column(Integer, default=22)
    ongoing_payment_amount = Column(Float, default=0.0)
    est_liquidator_fees = Column(Float, default=50000.0)
    est_legal_fees = Column(Float, default=10000.0)
    est_disbursements = Column(Float, default=5000.0)

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    company = relationship("CompanyDB", back_populates="plan_parameters")

    def __repr__(self) -> str:
        return f"<PlanParameters contribution={self.total_contribution}>"


# ---------------------------------------------------------------------------
# EntityMap — persisted PII entity maps for audit trail (2.1)
# ---------------------------------------------------------------------------

class EntityMapDB(Base):
    __tablename__ = "entity_maps"

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    engagement_id = Column(
        UUIDType(),
        ForeignKey("companies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entity_map = Column(JSONBType(), nullable=False)
    section = Column(String(100), nullable=True)  # which narrative section

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)

    # Relationships
    company = relationship("CompanyDB", backref="entity_maps")

    __table_args__ = (
        Index("ix_entity_maps_engagement_section", "engagement_id", "section"),
    )

    def __repr__(self) -> str:
        return f"<EntityMap engagement={self.engagement_id} section={self.section}>"


# ---------------------------------------------------------------------------
# Narrative — AI-generated Company Offer Statement sections (2.2)
# ---------------------------------------------------------------------------

class NarrativeDB(Base):
    __tablename__ = "narratives"

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    engagement_id = Column(
        UUIDType(),
        ForeignKey("companies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    section = Column(String, nullable=False)  # background, distress_events, expert_advice, plan_summary, viability, comparison_commentary
    content = Column(Text, nullable=False)
    status = Column(String, default="draft")  # draft, reviewed, approved
    metadata_ = Column(JSON, nullable=True)  # source tracking from narrative generator
    entity_map = Column(JSON, nullable=True)  # PII entity map for this section

    # Timestamps
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False)

    # Relationships
    company = relationship("CompanyDB", backref="narratives")

    __table_args__ = (
        Index("ix_narratives_engagement_section", "engagement_id", "section"),
    )

    def __repr__(self) -> str:
        return f"<Narrative engagement={self.engagement_id} section={self.section} status={self.status}>"


# ---------------------------------------------------------------------------
# DocumentOutput — tracks generated .docx files with version history (3.2)
# ---------------------------------------------------------------------------

class DocumentOutputDB(Base):
    __tablename__ = "document_outputs"

    id = Column(UUIDType(), primary_key=True, default=uuid.uuid4)
    engagement_id = Column(
        UUIDType(),
        ForeignKey("companies.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    document_type = Column(String, nullable=False)  # comparison, payment_schedule, company_statement
    version = Column(Integer, default=1)
    filename = Column(String, nullable=False)
    generated_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    generated_by = Column(String, nullable=True)  # placeholder for future auth
    metadata_ = Column(JSON, nullable=True)  # source data hashes, section statuses, etc.

    # Relationships
    company = relationship("CompanyDB", backref="document_outputs")

    __table_args__ = (
        Index("ix_document_outputs_engagement_type", "engagement_id", "document_type"),
    )

    def __repr__(self) -> str:
        return f"<DocumentOutput {self.document_type} v{self.version} engagement={self.engagement_id}>"
