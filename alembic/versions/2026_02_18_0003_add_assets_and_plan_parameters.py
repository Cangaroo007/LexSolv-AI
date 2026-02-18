"""add_assets_and_plan_parameters

Revision ID: 0003
Revises: 0002
Create Date: 2026-02-18

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: Union[str, None] = "0002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Create assets table
    op.create_table(
        "assets",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("company_id", sa.String(), sa.ForeignKey("companies.id"), nullable=True),
        sa.Column("asset_type", sa.String(50), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("book_value", sa.Float(), nullable=True),
        sa.Column("liquidation_recovery_pct", sa.Float(), nullable=True),
        sa.Column("liquidation_value", sa.Float(), nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("source", sa.String(20), server_default="parsed"),
    )

    # Create plan_parameters table
    op.create_table(
        "plan_parameters",
        sa.Column("id", sa.String(), primary_key=True),
        sa.Column("company_id", sa.String(), sa.ForeignKey("companies.id"), unique=True, nullable=True),
        sa.Column("total_contribution", sa.Float(), nullable=True),
        sa.Column("practitioner_fee_pct", sa.Float(), server_default="10.0"),
        sa.Column("num_initial_payments", sa.Integer(), server_default="2"),
        sa.Column("initial_payment_amount", sa.Float(), server_default="0.0"),
        sa.Column("num_ongoing_payments", sa.Integer(), server_default="22"),
        sa.Column("ongoing_payment_amount", sa.Float(), server_default="0.0"),
        sa.Column("est_liquidator_fees", sa.Float(), server_default="50000.0"),
        sa.Column("est_legal_fees", sa.Float(), server_default="10000.0"),
        sa.Column("est_disbursements", sa.Float(), server_default="5000.0"),
    )


def downgrade() -> None:
    op.drop_table("plan_parameters")
    op.drop_table("assets")
