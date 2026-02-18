"""add_entity_maps

Revision ID: 0003
Revises: 51a7d87a3bda
Create Date: 2026-02-18 23:00:00.000000

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision: str = "0003"
down_revision: Union[str, None] = "51a7d87a3bda"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "entity_maps",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("engagement_id", sa.UUID(), nullable=False),
        sa.Column("entity_map", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("section", sa.String(length=100), nullable=True),
        sa.Column(
            "created_at",
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
        op.f("ix_entity_maps_engagement_id"),
        "entity_maps",
        ["engagement_id"],
        unique=False,
    )
    op.create_index(
        "ix_entity_maps_engagement_section",
        "entity_maps",
        ["engagement_id", "section"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_entity_maps_engagement_section", table_name="entity_maps")
    op.drop_index(op.f("ix_entity_maps_engagement_id"), table_name="entity_maps")
    op.drop_table("entity_maps")
