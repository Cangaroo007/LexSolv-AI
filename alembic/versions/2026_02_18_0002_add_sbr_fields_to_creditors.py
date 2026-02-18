"""add_sbr_fields_to_creditors

Revision ID: 0002
Revises: 0001
Create Date: 2026-02-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0002"
down_revision: Union[str, None] = "0001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Add 'forgiven' to the creditor_status enum
    op.execute("ALTER TYPE creditor_status ADD VALUE IF NOT EXISTS 'forgiven'")

    # Add SBR-specific columns to creditors table
    op.add_column("creditors", sa.Column("is_related_party", sa.Boolean(), server_default="false"))
    op.add_column("creditors", sa.Column("is_secured", sa.Boolean(), server_default="false"))
    op.add_column("creditors", sa.Column("can_vote", sa.Boolean(), server_default="true"))
    op.add_column("creditors", sa.Column("source", sa.String(20), server_default="manual"))


def downgrade() -> None:
    op.drop_column("creditors", "source")
    op.drop_column("creditors", "can_vote")
    op.drop_column("creditors", "is_secured")
    op.drop_column("creditors", "is_related_party")

    # Note: PostgreSQL does not support removing values from an enum type.
    # The 'forgiven' value will remain in creditor_status after downgrade.
