"""Event annotations (deploy/config/incident/maintenance/other) shown over charts

Revision ID: f7a8b9c0d1e2
Revises: e1a3c5f7b9d2
Create Date: 2026-11-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f7a8b9c0d1e2"
down_revision: str | None = "e1a3c5f7b9d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "annotations",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "instance_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("instances.id", ondelete="CASCADE"),
            nullable=True,
        ),
        sa.Column("occurred_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("category", sa.Text(), server_default="other", nullable=False),
        sa.Column(
            "created_by_user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("created_by_username", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "category IN ('deploy', 'config', 'incident', 'maintenance', 'other')",
            name="ck_annotations_category",
        ),
    )
    op.create_index(
        "ix_annotations_instance_occurred_at", "annotations", ["instance_id", "occurred_at"]
    )
    op.create_index("ix_annotations_occurred_at", "annotations", ["occurred_at"])


def downgrade() -> None:
    op.drop_index("ix_annotations_occurred_at", table_name="annotations")
    op.drop_index("ix_annotations_instance_occurred_at", table_name="annotations")
    op.drop_table("annotations")
