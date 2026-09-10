"""hypertables

Revision ID: 66ae4664300c
Revises: 9cec68412ba8
Create Date: 2026-09-10 11:05:15.870832

Cria as hypertables e continuous aggregates de coleta continua descritos em
`Docs/database-schema.md` §2 (`metric_samples` + `metric_samples_hourly`/`_daily`) e §3
(`query_samples` + `query_stats_hourly`, `blocking_events`, `sql_exclusions`). Requer a
extensao `timescaledb` (ja habilitada no Postgres 18 / TimescaleDB 2.29.2 local de dev).

Cortes de escopo desta migration (ver plano da feature "coleta continua real"):
    - NAO cria `deadlock_events` — deadlock detalhado fica fora de escopo (so
      `pg_stat_database.deadlocks`/contador agregado do lado PostgreSQL, e XE do lado SQL
      Server, nenhum dos dois persistido em tabela dedicada nesta fase).
    - NAO chama `add_continuous_aggregate_policy` para os continuous aggregates
      (`metric_samples_hourly`, `metric_samples_daily`, `query_stats_hourly`) — a spec
      (`Docs/database-schema.md`) nao transcreve o agendamento de refresh automatico destas
      views; refresh manual (`CALL refresh_continuous_aggregate(...)`) ou uma policy futura
      dedicada ficam como TODO.
    - So 3 `add_retention_policy` (as variantes de `metric_samples`, conforme documentado) —
      `query_samples`, `query_stats_hourly` e `blocking_events` ficam sem retention policy
      nesta migration (TODO futuro).
    - `sql_exclusions` e criada so como schema (tabela real via `op.create_table`); a
      aplicacao so faz checagem read-only contra ela em `QuerySamplesRepository` — sem
      endpoint de gestao (CRUD) nesta feature.

DESVIO CONFIRMADO RODANDO (nao assumido): TimescaleDB 2.29.2 neste ambiente (Postgres 18)
recusa `CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous) AS ... ` com a populacao
inicial (`WITH DATA`, o default) dentro do bloco transacional que o Alembic abre por migration
(`asyncpg.exceptions.ActiveSQLTransactionError`). Os 3 continuous aggregates abaixo sao criados
com `WITH NO DATA` para contornar isso — ficam vazios ate o primeiro refresh manual
(`CALL refresh_continuous_aggregate('metric_samples_hourly', NULL, NULL)` etc., fora de uma
transacao) ou ate uma `add_continuous_aggregate_policy` (fora de escopo, TODO acima) popular
via job periodico.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "66ae4664300c"
down_revision: str | None = "9cec68412ba8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("CREATE EXTENSION IF NOT EXISTS timescaledb")

    op.execute("""
        CREATE TABLE metric_samples (
            time        TIMESTAMPTZ NOT NULL,
            target_type TEXT NOT NULL CHECK (target_type IN ('server', 'instance')),
            target_id   UUID NOT NULL,
            metric_name TEXT NOT NULL,
            labels      JSONB NOT NULL DEFAULT '{}',
            value       DOUBLE PRECISION NOT NULL
        )
    """)
    op.execute("SELECT create_hypertable('metric_samples', 'time')")
    op.execute("CREATE INDEX ON metric_samples (target_id, metric_name, time DESC)")

    op.execute("""
        CREATE MATERIALIZED VIEW metric_samples_hourly
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 hour', time) AS bucket,
               target_type, target_id, metric_name,
               avg(value) AS avg_value, max(value) AS max_value, min(value) AS min_value
        FROM metric_samples
        GROUP BY bucket, target_type, target_id, metric_name
        WITH NO DATA
    """)
    op.execute("""
        CREATE MATERIALIZED VIEW metric_samples_daily
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 day', bucket) AS bucket,
               target_type, target_id, metric_name,
               avg(avg_value) AS avg_value, max(max_value) AS max_value, min(min_value) AS min_value
        FROM metric_samples_hourly
        GROUP BY time_bucket('1 day', bucket), target_type, target_id, metric_name
        WITH NO DATA
    """)

    op.execute("SELECT add_retention_policy('metric_samples', INTERVAL '30 days')")
    op.execute("SELECT add_retention_policy('metric_samples_hourly', INTERVAL '90 days')")
    op.execute("SELECT add_retention_policy('metric_samples_daily', INTERVAL '5 years')")

    op.execute("""
        CREATE TABLE query_samples (
            time TIMESTAMPTZ NOT NULL,
            instance_id UUID NOT NULL REFERENCES instances(id),
            sql_hash TEXT NOT NULL,
            sql_name TEXT,
            sql_text_masked TEXT NOT NULL,
            sql_text_encrypted BYTEA,
            wait_type TEXT,
            wait_event_native TEXT,
            wait_time_ms BIGINT,
            duration_ms BIGINT,
            session_id TEXT,
            db_user TEXT,
            program TEXT,
            database_name TEXT,
            machine TEXT
        )
    """)
    op.execute("SELECT create_hypertable('query_samples', 'time')")
    op.execute("CREATE INDEX ON query_samples (instance_id, sql_hash, time DESC)")

    op.execute("""
        CREATE MATERIALIZED VIEW query_stats_hourly
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 hour', time) AS bucket,
               instance_id, sql_hash,
               count(*) AS execution_count,
               sum(wait_time_ms) AS total_wait_time_ms,
               avg(wait_time_ms) AS avg_wait_time_ms
        FROM query_samples
        GROUP BY bucket, instance_id, sql_hash
        WITH NO DATA
    """)

    op.execute("""
        CREATE TABLE blocking_events (
            time TIMESTAMPTZ NOT NULL,
            instance_id UUID NOT NULL REFERENCES instances(id),
            blocker_session_id TEXT NOT NULL,
            blocked_session_id TEXT NOT NULL,
            blocker_sql_hash TEXT,
            blocked_sql_hash TEXT,
            wait_time_ms BIGINT NOT NULL
        )
    """)
    op.execute("SELECT create_hypertable('blocking_events', 'time')")

    op.create_table(
        "sql_exclusions",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("match_type", sa.String(), nullable=False),
        sa.Column("match_value", sa.String(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "match_type IN ('sql_hash', 'program_name', 'db_user')",
            name="ck_sql_exclusions_match_type",
        ),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_sql_exclusions_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_sql_exclusions"),
    )


def downgrade() -> None:
    op.drop_table("sql_exclusions")
    op.execute("DROP TABLE IF EXISTS blocking_events")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS query_stats_hourly CASCADE")
    op.execute("DROP TABLE IF EXISTS query_samples CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS metric_samples_daily CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS metric_samples_hourly CASCADE")
    op.execute("DROP TABLE IF EXISTS metric_samples CASCADE")
