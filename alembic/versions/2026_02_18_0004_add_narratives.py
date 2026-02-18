"""add_narratives

Revision ID: 0004
Revises: 0003
Create Date: 2026-02-18 23:30:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = "0004"
down_revision: Union[str, None] = "0003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "narratives",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("engagement_id", sa.UUID(), nullable=False),
        sa.Column("section", sa.String(), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), server_default="draft", nullable=True),
        sa.Column("metadata_", sa.JSON(), nullable=True),
        sa.Column("entity_map", sa.JSON(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["engagement_id"], ["companies.id"], ondelete="CASCADE"
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_narratives_engagement_id"),
        "narratives",
        ["engagement_id"],
        unique=False,
    )
    op.create_index(
        "ix_narratives_engagement_section",
        "narratives",
        ["engagement_id", "section"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_narratives_engagement_section", table_name="narratives")
    op.drop_index(op.f("ix_narratives_engagement_id"), table_name="narratives")
    op.drop_table("narratives")
