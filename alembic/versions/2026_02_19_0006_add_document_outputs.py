"""add_document_outputs table for tracking generated documents

Revision ID: 0006
Revises: 0005
Create Date: 2026-02-19 00:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID


# revision identifiers, used by Alembic.
revision: str = "0006"
down_revision: Union[str, None] = "0005"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "document_outputs",
        sa.Column("id", UUID(as_uuid=True), primary_key=True),
        sa.Column(
            "engagement_id",
            UUID(as_uuid=True),
            sa.ForeignKey("companies.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("document_type", sa.String, nullable=False),
        sa.Column("version", sa.Integer, default=1),
        sa.Column("filename", sa.String, nullable=False),
        sa.Column(
            "generated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("generated_by", sa.String, nullable=True),
        sa.Column("metadata_", sa.JSON, nullable=True),
    )
    op.create_index(
        "ix_document_outputs_engagement_type",
        "document_outputs",
        ["engagement_id", "document_type"],
    )


def downgrade() -> None:
    op.drop_index("ix_document_outputs_engagement_type", table_name="document_outputs")
    op.drop_table("document_outputs")
