"""Initial schema — companies, creditors, transactions, integration connections, oauth tokens

Revision ID: 0001
Revises: None
Create Date: 2026-02-09

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0001"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # -- Enum types --------------------------------------------------------
    creditor_status = postgresql.ENUM(
        "active", "disputed", "written_off", "paid",
        name="creditor_status", create_type=True,
    )
    transaction_type = postgresql.ENUM(
        "invoice", "credit_note", "payment", "journal", "bank_transaction",
        name="transaction_type", create_type=True,
    )
    transaction_status = postgresql.ENUM(
        "draft", "submitted", "authorised", "paid", "voided",
        name="transaction_status", create_type=True,
    )

    creditor_status.create(op.get_bind(), checkfirst=True)
    transaction_type.create(op.get_bind(), checkfirst=True)
    transaction_status.create(op.get_bind(), checkfirst=True)

    # -- companies ---------------------------------------------------------
    op.create_table(
        "companies",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("legal_name", sa.String(255), nullable=False),
        sa.Column("trading_name", sa.String(255), nullable=True),
        sa.Column("abn", sa.String(11), nullable=True),
        sa.Column("acn", sa.String(9), nullable=True),
        sa.Column("total_assets", sa.Numeric(15, 2), server_default="0"),
        sa.Column("total_liabilities", sa.Numeric(15, 2), server_default="0"),
        sa.Column("total_creditors", sa.Numeric(15, 2), server_default="0"),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column("external_id", sa.String(255), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_companies_abn", "companies", ["abn"])
    op.create_index("ix_companies_acn", "companies", ["acn"])
    op.create_index("ix_companies_source_external", "companies", ["source", "external_id"], unique=True)

    # -- creditors ---------------------------------------------------------
    op.create_table(
        "creditors",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("company_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("creditor_name", sa.String(255), nullable=False),
        sa.Column("contact_email", sa.String(255), nullable=True),
        sa.Column("contact_phone", sa.String(50), nullable=True),
        sa.Column("amount_claimed", sa.Numeric(15, 2), nullable=False),
        sa.Column("amount_admitted", sa.Numeric(15, 2), nullable=True),
        sa.Column("currency", sa.String(3), server_default="AUD"),
        sa.Column("status", creditor_status, server_default="active", nullable=False),
        sa.Column("category", sa.String(50), nullable=True),
        sa.Column("due_date", sa.Date, nullable=True),
        sa.Column("source_invoice_ids", postgresql.JSONB, server_default="[]"),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("as_at_date", sa.Date, nullable=False, server_default=sa.text("CURRENT_DATE")),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_creditors_company_id", "creditors", ["company_id"])
    op.create_index("ix_creditors_company_status", "creditors", ["company_id", "status"])

    # -- transactions ------------------------------------------------------
    op.create_table(
        "transactions",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("company_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("transaction_type", transaction_type, nullable=False),
        sa.Column("status", transaction_status, server_default="authorised", nullable=False),
        sa.Column("reference", sa.String(255), nullable=True),
        sa.Column("description", sa.Text, nullable=True),
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("due_date", sa.Date, nullable=True),
        sa.Column("amount", sa.Numeric(15, 2), nullable=False),
        sa.Column("tax_amount", sa.Numeric(15, 2), server_default="0"),
        sa.Column("currency", sa.String(3), server_default="AUD"),
        sa.Column("account_code", sa.String(50), nullable=True),
        sa.Column("account_name", sa.String(255), nullable=True),
        sa.Column("contact_name", sa.String(255), nullable=True),
        sa.Column("source", sa.String(50), nullable=True),
        sa.Column("external_id", sa.String(255), nullable=True),
        sa.Column("raw_payload", postgresql.JSONB, nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_transactions_company_id", "transactions", ["company_id"])
    op.create_index("ix_transactions_company_date", "transactions", ["company_id", "date"])
    op.create_index("ix_transactions_source_external", "transactions", ["source", "external_id"])
    op.create_index("ix_transactions_type", "transactions", ["company_id", "transaction_type"])

    # -- integration_connections -------------------------------------------
    op.create_table(
        "integration_connections",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("company_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("provider", sa.String(50), nullable=False),
        sa.Column("tenant_id", sa.String(255), nullable=True),
        sa.Column("tenant_name", sa.String(255), nullable=True),
        sa.Column("is_active", sa.Boolean, server_default="true", nullable=False),
        sa.Column("connected_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_intconn_company_id", "integration_connections", ["company_id"])
    op.create_index("ix_intconn_provider_tenant", "integration_connections", ["provider", "tenant_id"], unique=True)

    # -- oauth_tokens ------------------------------------------------------
    op.create_table(
        "oauth_tokens",
        sa.Column("id", postgresql.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("connection_id", postgresql.UUID(as_uuid=True), sa.ForeignKey("integration_connections.id", ondelete="CASCADE"), nullable=False),
        sa.Column("access_token", sa.Text, nullable=False),
        sa.Column("refresh_token", sa.Text, nullable=True),
        sa.Column("token_type", sa.String(50), server_default="Bearer"),
        sa.Column("scope", sa.Text, nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.create_index("ix_oauth_tokens_connection_id", "oauth_tokens", ["connection_id"])


def downgrade() -> None:
    op.drop_table("oauth_tokens")
    op.drop_table("integration_connections")
    op.drop_table("transactions")
    op.drop_table("creditors")
    op.drop_table("companies")

    op.execute("DROP TYPE IF EXISTS transaction_status")
    op.execute("DROP TYPE IF EXISTS transaction_type")
    op.execute("DROP TYPE IF EXISTS creditor_status")
