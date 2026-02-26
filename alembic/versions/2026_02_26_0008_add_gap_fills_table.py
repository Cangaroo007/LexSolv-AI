"""Add gap_fills table for recording gap resolutions

Revision ID: 0008
Revises: 0007
Create Date: 2026-02-26

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "0008"
down_revision: Union[str, None] = "0007"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Detect dialect to use appropriate UUID and JSONB types
    bind = op.get_bind()
    is_pg = bind.dialect.name == "postgresql"

    if is_pg:
        id_type = postgresql.UUID(as_uuid=True)
        jsonb_type = postgresql.JSONB
    else:
        id_type = sa.String(36)
        jsonb_type = sa.JSON

    op.create_table(
        "gap_fills",
        sa.Column("id", id_type, primary_key=True, server_default=sa.text("gen_random_uuid()") if is_pg else None),
        sa.Column("engagement_id", id_type, sa.ForeignKey("companies.id", ondelete="CASCADE"), nullable=False),
        sa.Column("field_name", sa.String(), nullable=False),
        sa.Column("document_type", sa.String(), nullable=False),
        sa.Column("filled_value", jsonb_type(), nullable=False),
        sa.Column("filled_by", sa.String(), nullable=False),
        sa.Column("filled_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("previous_value", jsonb_type(), nullable=True),
        sa.Column("confidence", sa.Float(), nullable=False, server_default="1.0"),
        sa.Column("notes", sa.String(), nullable=True),
    )

    op.create_index("idx_gap_fills_engagement", "gap_fills", ["engagement_id"])


def downgrade() -> None:
    op.drop_index("idx_gap_fills_engagement", table_name="gap_fills")
    op.drop_table("gap_fills")
