"""query_plan_snapshots — historico de plano de execucao

Revision ID: f8b2e4a1c3d7
Revises: e209d6a4cc7e
Create Date: 2026-09-18 12:00:00.000000

Item priorizado pelo usuario apos a Fase 2: histórico de plano de execução, pra responder "essa
query, que sempre foi rápida, ficou lenta — o plano mudou?". Tabela relacional INSERT-ONLY (mesmo
padrão de `deadlock_events`): uma linha por plano DISTINTO visto para um par
(`instance_id`, `sql_hash`) durante a varredura periódica do cache de planos
(`app/workers/plan_history_supervisor.py`, só SQL Server) — só grava quando `plan_hash` muda em
relação ao último snapshot conhecido, nunca a cada varredura. Retida até
`settings.problem_events_retention_days`, mesmo worker de expurgo já usado por transação/
bloqueio/deadlock.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "f8b2e4a1c3d7"
down_revision: str | None = "e209d6a4cc7e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "query_plan_snapshots",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("sql_hash", sa.Text(), nullable=False),
        sa.Column("plan_hash", sa.Text(), nullable=False),
        sa.Column("plan_xml_masked", sa.Text(), nullable=False),
        sa.Column("last_execution_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "captured_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_query_plan_snapshots_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_query_plan_snapshots"),
    )
    # Cobre tanto "ultimo plano conhecido pra este par" (insert_if_changed) quanto "historico
    # completo desta query, mais recente primeiro" (endpoint de leitura) numa unica ordenacao.
    op.create_index(
        "ix_query_plan_snapshots_instance_id_sql_hash_captured_at",
        "query_plan_snapshots",
        ["instance_id", "sql_hash", sa.text("captured_at DESC")],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_query_plan_snapshots_instance_id_sql_hash_captured_at",
        table_name="query_plan_snapshots",
    )
    op.drop_table("query_plan_snapshots")
