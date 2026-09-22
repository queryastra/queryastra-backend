"""missing index collection runs (item 4)

Revision ID: d8e1f4a7c2b6
Revises: c3e5a9f2d8b4
Create Date: 2026-09-22 14:10:00.000000

Rastreia quando cada instancia teve sua ultima checagem de indice ausente e desde quando o
motor monitorado acumula as estatisticas de uso lidas (reinicio do servico/cluster) - ver
docstring de `app.models.missing_index_collection_run.MissingIndexCollectionRun`.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "d8e1f4a7c2b6"
down_revision: str | None = "c3e5a9f2d8b4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "missing_index_collection_runs",
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("collected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("stats_since", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_missing_index_collection_runs_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("instance_id", name="pk_missing_index_collection_runs"),
    )


def downgrade() -> None:
    op.drop_table("missing_index_collection_runs")
