"""transaction incident log (closed_at/flag_reason) + blocking_incidents + deadlock_events

Revision ID: c7a1f3d9e5b2
Revises: b4d8e6f1a2c9
Create Date: 2026-09-15 16:30:00.000000

Redesenho pedido pelo usuario apos uso real ao vivo: `transaction_events` deixa de ser um espelho
do estado atual reconciliado a cada ciclo (upsert + delete imediato) e passa a ser um LOG DE
INCIDENTE retido para analise posterior — grava fechamento (`closed_at`), nunca deleta so porque
a transacao fechou (o expurgo por idade fica com
`app/workers/problem_events_retention_worker.py`, nao com o ciclo de coleta). Ver a secao "Etapa
10, redesenho" do plano.

`transaction_events`:
    - `closed_at`: preenchido quando a transacao some do conjunto aberto (nunca mais volta a
      `NULL`).
    - `flag_reason`: tags combinadas (`"bloqueio"`, `"erro"`, `"orfa"`, `"deadlock"`) explicando
      por que essa linha foi gravada — só transações com pelo menos 1 tag chegam a ser
      persistidas (ver `app.collectors.transaction_flagging`).

`blocking_incidents` (NOVA tabela relacional, nao hypertable): `blocking_events` continua existindo
INALTERADA como log bruto insert-only por ciclo (e uma hypertable TimescaleDB particionada por
`time` — nao suporta upsert-in-place por chave de negocio sem incluir `time` na constraint unica,
o que impediria rastrear um `started_at` de verdade). `blocking_incidents` e uma tabela relacional
comum, upsertada a cada ciclo (mesmo padrao de `transaction_events`): uma linha por par
(`instance_id`, `blocker_session_id`, `blocked_session_id`) com `started_at` (primeira vez
observado) e `ended_at` (preenchido quando o par some do batch aberto, nunca deletado — so
expurgado por idade).

`deadlock_events` (novo, Etapa 10 item 7): tabela insert-only (um deadlock e um evento
instantaneo, sem "fechar/abrir") — so SQL Server (`system_health` ring buffer via XML parse),
PostgreSQL nao expoe detalhe de deadlock via SQL.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "c7a1f3d9e5b2"
down_revision: str | None = "b4d8e6f1a2c9"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "transaction_events", sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True)
    )
    op.add_column("transaction_events", sa.Column("flag_reason", sa.Text(), nullable=True))
    op.create_index(
        "ix_transaction_events_instance_id_closed_at",
        "transaction_events",
        ["instance_id", "closed_at"],
    )

    # Enriquecimento aditivo no log bruto (mesmo padrao de blocker_database_name/
    # blocked_database_name) - nao muda a natureza insert-only da hypertable.
    op.add_column(
        "blocking_events",
        sa.Column("blocker_transaction_begin_time", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "blocking_events",
        sa.Column("blocked_transaction_begin_time", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_table(
        "blocking_incidents",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("blocker_session_id", sa.Text(), nullable=False),
        sa.Column("blocked_session_id", sa.Text(), nullable=False),
        sa.Column("blocker_sql_hash", sa.Text(), nullable=True),
        sa.Column("blocked_sql_hash", sa.Text(), nullable=True),
        sa.Column("wait_time_ms", sa.BigInteger(), nullable=False),
        sa.Column("blocker_db_user", sa.Text(), nullable=True),
        sa.Column("blocker_program", sa.Text(), nullable=True),
        sa.Column("blocker_machine", sa.Text(), nullable=True),
        sa.Column("blocker_wait_event_native", sa.Text(), nullable=True),
        sa.Column("blocked_db_user", sa.Text(), nullable=True),
        sa.Column("blocked_program", sa.Text(), nullable=True),
        sa.Column("blocked_machine", sa.Text(), nullable=True),
        sa.Column("blocked_wait_event_native", sa.Text(), nullable=True),
        sa.Column("blocked_wait_resource", sa.Text(), nullable=True),
        sa.Column("blocked_object", sa.Text(), nullable=True),
        sa.Column("blocker_transaction_id", sa.Text(), nullable=True),
        sa.Column("blocked_transaction_id", sa.Text(), nullable=True),
        sa.Column("blocker_database_name", sa.Text(), nullable=True),
        sa.Column("blocked_database_name", sa.Text(), nullable=True),
        sa.Column("blocker_transaction_begin_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("blocked_transaction_begin_time", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("ended_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_blocking_incidents_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_blocking_incidents"),
        sa.UniqueConstraint(
            "instance_id",
            "blocker_session_id",
            "blocked_session_id",
            name="uq_blocking_incidents_identity",
        ),
    )
    op.create_index(
        "ix_blocking_incidents_instance_id_ended_at",
        "blocking_incidents",
        ["instance_id", "ended_at"],
    )

    op.create_table(
        "deadlock_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("time", sa.DateTime(timezone=True), nullable=False),
        sa.Column("victim_session_id", sa.Text(), nullable=True),
        sa.Column("victim_sql_text_masked", sa.Text(), nullable=True),
        sa.Column("survivor_session_id", sa.Text(), nullable=True),
        sa.Column("survivor_sql_text_masked", sa.Text(), nullable=True),
        sa.Column("resource_description", sa.Text(), nullable=True),
        sa.Column("raw_xml_masked", sa.Text(), nullable=True),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_deadlock_events_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_deadlock_events"),
    )
    op.create_index(
        "ix_deadlock_events_instance_id_time",
        "deadlock_events",
        ["instance_id", sa.text("time DESC")],
    )


def downgrade() -> None:
    op.drop_index("ix_deadlock_events_instance_id_time", table_name="deadlock_events")
    op.drop_table("deadlock_events")

    op.drop_index("ix_blocking_incidents_instance_id_ended_at", table_name="blocking_incidents")
    op.drop_table("blocking_incidents")

    op.drop_column("blocking_events", "blocked_transaction_begin_time")
    op.drop_column("blocking_events", "blocker_transaction_begin_time")

    op.drop_index("ix_transaction_events_instance_id_closed_at", table_name="transaction_events")
    op.drop_column("transaction_events", "flag_reason")
    op.drop_column("transaction_events", "closed_at")
