"""Add SBR engagement fields (appointment_date, practitioner_name, industry) to companies

Revision ID: 0007
Revises: 0006
Create Date: 2026-02-19

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = "0007"
down_revision: Union[str, None] = "0006"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column("companies", sa.Column("appointment_date", sa.Date(), nullable=True))
    op.add_column("companies", sa.Column("practitioner_name", sa.String(255), nullable=True))
    op.add_column("companies", sa.Column("industry", sa.String(100), nullable=True))


def downgrade() -> None:
    op.drop_column("companies", "industry")
    op.drop_column("companies", "practitioner_name")
    op.drop_column("companies", "appointment_date")
