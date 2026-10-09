"""query_stats_snapshot hypertable — T60 (Trending de queries).

Armazena snapshots periodicos de `pg_stat_statements` por instancia PostgreSQL.
Os valores sao ACUMULATIVOS (mesma semantica do pg_stat_statements: contam desde o ultimo
`pg_stat_statements_reset()`). O delta entre snapshots consecutivos e calculado via LAG()
no momento da consulta — isso permite detectar resets (delta negativo → ignora) sem precisar
guardar dois estados no banco.

Intervalo de captura padrao: 1 hora (configuravel via QUERY_STATS_CHECK_INTERVAL_SECONDS).
Retencao: 30 dias (720 linhas/query/instancia/mes — aceitavel para o top-200 por instancia).

Revisao: a7b8c9d0e1f2
Anterior: c3d4e5f6a7b8
"""

from collections.abc import Sequence

from alembic import op

revision: str = "a7b8c9d0e1f2"
down_revision: str | None = "c3d4e5f6a7b8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE query_stats_snapshot (
            time             TIMESTAMPTZ        NOT NULL,
            instance_id      UUID               NOT NULL,
            queryid          BIGINT             NOT NULL,
            query_text       TEXT,
            calls            BIGINT             NOT NULL,
            total_exec_time  DOUBLE PRECISION   NOT NULL,
            total_rows       BIGINT             NOT NULL
        )
    """)
    op.execute("SELECT create_hypertable('query_stats_snapshot', 'time')")
    op.execute(
        "CREATE INDEX ON query_stats_snapshot (instance_id, queryid, time DESC)"
    )
    op.execute(
        "SELECT add_retention_policy('query_stats_snapshot', INTERVAL '30 days')"
    )


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS query_stats_snapshot")
