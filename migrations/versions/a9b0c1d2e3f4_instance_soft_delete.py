"""Add deleted_at to instances for soft-delete / restore within 30 days

Revision ID: a9b0c1d2e3f4
Revises: f9a2b3c4d5e6
Create Date: 2026-10-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a9b0c1d2e3f4"
down_revision: str | None = "f9a2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("instances", sa.Column("deleted_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_instances_deleted_at", "instances", ["deleted_at"])


def downgrade() -> None:
    op.drop_index("ix_instances_deleted_at", table_name="instances")
    op.drop_column("instances", "deleted_at")
