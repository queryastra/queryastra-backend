"""scheduled_reports_is_recurring — campo is_recurring

Revision ID: b3c5d7e9f1a2
Revises: a2b4c6d8e1f3
Create Date: 2026-10-05 10:00:00.000000

Adiciona `is_recurring` em `scheduled_reports`. Relatórios não recorrentes não têm
`next_run_at` recomputado após a execução — rodam uma única vez na data/hora escolhida.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "b3c5d7e9f1a2"
down_revision = "a2b4c6d8e1f3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "scheduled_reports",
        sa.Column("is_recurring", sa.Boolean(), nullable=False, server_default=sa.text("true")),
    )


def downgrade() -> None:
    op.drop_column("scheduled_reports", "is_recurring")
