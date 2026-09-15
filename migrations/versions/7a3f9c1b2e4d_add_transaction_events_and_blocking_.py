"""add transaction_events and blocking_events transaction_id columns

Revision ID: 7a3f9c1b2e4d
Revises: 0607d7592ff6
Create Date: 2026-09-15 12:00:00.000000

Etapa 10, itens 2/3 (rastreamento de transacoes) - ARCHITECTURE_SPEC.md. Tabela relacional
comum (nao hypertable, mesmo padrao de `missing_index_suggestions`) - estado mais recente de uma
transacao ABERTA por (`instance_id`, `transaction_identifier`), atualizado via UPSERT a cada
ciclo do `PollingWorker` de alta frequencia (nao um worker de baixa frequencia separado - uma
transacao orfa precisa ser vista com a mesma cadencia da coleta normal, nao 1x/dia).

`transaction_identifier`: SQL Server usa `sys.dm_tran_session_transactions.transaction_id`
(texto); PostgreSQL nao tem o conceito de transacao nomeada/multi-sessao do SQL Server - usa o
proprio `pid` do backend como identificador (`Docs/collector-queries.md`, secao de transacoes).

`blocker_transaction_id`/`blocked_transaction_id` em `blocking_events`: aditivas/nullable, mesmo
padrao das colunas de enriquecimento da Etapa 9 - permitem que a tela de Bloqueios linke cada
sessao a sua transacao (coluna "Transacao" logo apos "SPID", a pedido explicito do usuario).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "7a3f9c1b2e4d"
down_revision: str | None = "0607d7592ff6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "transaction_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("transaction_identifier", sa.Text(), nullable=False),
        sa.Column("session_id", sa.Text(), nullable=True),
        sa.Column("database_name", sa.Text(), nullable=True),
        sa.Column("transaction_type", sa.Text(), nullable=True),
        sa.Column("transaction_uow", sa.Text(), nullable=True),
        sa.Column("begin_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("state", sa.Text(), nullable=True),
        sa.Column("last_error_code", sa.Integer(), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_transaction_events_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_transaction_events"),
        sa.UniqueConstraint(
            "instance_id",
            "transaction_identifier",
            name="uq_transaction_events_identity",
        ),
    )
    op.create_index(
        "ix_transaction_events_instance_id_last_seen_at",
        "transaction_events",
        ["instance_id", sa.text("last_seen_at DESC")],
    )

    op.add_column("blocking_events", sa.Column("blocker_transaction_id", sa.Text(), nullable=True))
    op.add_column("blocking_events", sa.Column("blocked_transaction_id", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("blocking_events", "blocked_transaction_id")
    op.drop_column("blocking_events", "blocker_transaction_id")
    op.drop_index("ix_transaction_events_instance_id_last_seen_at", table_name="transaction_events")
    op.drop_table("transaction_events")
