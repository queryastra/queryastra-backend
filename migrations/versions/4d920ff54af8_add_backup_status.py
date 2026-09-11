"""add backup_status

Revision ID: 4d920ff54af8
Revises: 028f0bcf4fd6
Create Date: 2026-09-11 22:30:00.000000

Tabela relacional comum (nao hypertable) — ARCHITECTURE_SPEC.md Q16,
`Docs/database-schema.md` §3, `Docs/collector-queries.md` §6. Estado mais recente de
backup/archiving por (`instance_id`, `database_name`) no SQL Server, ou por `instance_id`
sozinho (Postgres, `database_name` NULL) — atualizado via UPSERT por um worker de baixa
frequencia (`app/workers/backup_status_supervisor.py`), separado do `PollingWorker`.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "4d920ff54af8"
down_revision: str | None = "028f0bcf4fd6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "backup_status",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("database_name", sa.Text(), nullable=True),
        sa.Column("last_backup_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("detail", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("checked_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_backup_status_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_backup_status"),
    )
    # 2 indices unicos parciais (nao 1 UNIQUE simples): NULL nao e tratado como valor repetivel
    # em UNIQUE do Postgres — sem os parciais, cada checagem do Postgres criaria uma linha nova
    # em vez de atualizar a mesma (database_name sempre NULL nesse caso).
    op.create_index(
        "uq_backup_status_instance_database",
        "backup_status",
        ["instance_id", "database_name"],
        unique=True,
        postgresql_where=sa.text("database_name IS NOT NULL"),
    )
    op.create_index(
        "uq_backup_status_instance_only",
        "backup_status",
        ["instance_id"],
        unique=True,
        postgresql_where=sa.text("database_name IS NULL"),
    )


def downgrade() -> None:
    op.drop_index("uq_backup_status_instance_only", table_name="backup_status")
    op.drop_index("uq_backup_status_instance_database", table_name="backup_status")
    op.drop_table("backup_status")
