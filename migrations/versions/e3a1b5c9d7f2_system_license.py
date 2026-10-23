"""system_license table

Revision ID: e3a1b5c9d7f2
Revises: b2c4d6e8a0f1
Create Date: 2026-10-20
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "e3a1b5c9d7f2"
down_revision: str | None = "b2c4d6e8a0f1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "system_license",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("plan_id", sa.String(20), nullable=False, server_default="trial"),
        sa.Column(
            "trial_started_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.Column("subscription_expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )
    op.execute("INSERT INTO system_license (plan_id, trial_started_at) VALUES ('trial', now())")


def downgrade() -> None:
    op.drop_table("system_license")
