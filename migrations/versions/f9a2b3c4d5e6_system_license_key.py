"""Add license_key to system_license

Revision ID: f9a2b3c4d5e6
Revises: e3a1b5c9d7f2
Create Date: 2026-10-23
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f9a2b3c4d5e6"
down_revision: str | None = "e3a1b5c9d7f2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "system_license",
        sa.Column("license_key", sa.String(30), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("system_license", "license_key")
