"""add missing_index_suggestions

Revision ID: 028f0bcf4fd6
Revises: ef208ca3271e
Create Date: 2026-09-11 21:00:00.000000

Tabela relacional comum (nao hypertable) — ARCHITECTURE_SPEC.md Q18/Q21,
`Docs/database-schema.md` §3, `Docs/collector-queries.md` §10. Estado mais recente de uma
sugestao de indice ausente por (`instance_id`, `database_name`, `table_name`, `suggestion`),
atualizado via UPSERT por um worker de baixa frequencia
(`app/workers/missing_index_supervisor.py`), separado do `PollingWorker` de alta frequencia.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "028f0bcf4fd6"
down_revision: str | None = "ef208ca3271e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "missing_index_suggestions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("database_name", sa.Text(), nullable=True),
        sa.Column("table_name", sa.Text(), nullable=False),
        sa.Column("suggestion", sa.Text(), nullable=False),
        sa.Column("impact_score", sa.Float(), nullable=True),
        sa.Column("detail", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_missing_index_suggestions_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_missing_index_suggestions"),
        sa.UniqueConstraint(
            "instance_id",
            "database_name",
            "table_name",
            "suggestion",
            name="uq_missing_index_suggestions_identity",
        ),
    )
    op.create_index(
        "ix_missing_index_suggestions_instance_id_last_seen_at",
        "missing_index_suggestions",
        ["instance_id", sa.text("last_seen_at DESC")],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_missing_index_suggestions_instance_id_last_seen_at",
        table_name="missing_index_suggestions",
    )
    op.drop_table("missing_index_suggestions")
