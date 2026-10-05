"""missing_index_first_collected_at — rastreia quando o monitoramento de indices ausentes
comecou para uma instancia (independente de encontrar sugestoes)

Revision ID: f1a3b5c7d9e2
Revises: c1d4e7f2a9b6
Create Date: 2026-10-05 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f1a3b5c7d9e2"
down_revision: str | None = "c1d4e7f2a9b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "missing_index_collection_runs",
        sa.Column(
            "first_collected_at",
            sa.TIMESTAMP(timezone=True),
            server_default=sa.text("NOW()"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("missing_index_collection_runs", "first_collected_at")
