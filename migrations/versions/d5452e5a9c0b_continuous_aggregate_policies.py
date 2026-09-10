"""continuous_aggregate_policies

Revision ID: d5452e5a9c0b
Revises: 66ae4664300c
Create Date: 2026-09-10 19:43:19.996387

Adiciona `add_continuous_aggregate_policy` para os 3 continuous aggregates criados `WITH NO DATA`
e sem policy de refresh por `migrations/versions/66ae4664300c_hypertables.py`
(`metric_samples_hourly`, `metric_samples_daily`, `query_stats_hourly`) — TODO explicito deixado
naquela migration. So passa a preencher/atualizar essas views para uso futuro; os endpoints de
leitura atuais continuam lendo diretamente das hypertables brutas (`MetricSamplesRepository`,
decisao ja tomada, nao alterada aqui).

Testado de verdade contra `ab_xql_pm_dev` (Postgres 18 / TimescaleDB 2.29.2 local) — diferente da
migration anterior, `add_continuous_aggregate_policy` roda sem problema dentro da transacao que o
Alembic abre por migration (nao e uma populacao inicial de `CREATE MATERIALIZED VIEW`, que foi o
que exigiu `WITH NO DATA` la).
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d5452e5a9c0b"
down_revision: str | None = "66ae4664300c"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        SELECT add_continuous_aggregate_policy('metric_samples_hourly',
            start_offset => INTERVAL '3 hours', end_offset => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour')
    """)
    op.execute("""
        SELECT add_continuous_aggregate_policy('metric_samples_daily',
            start_offset => INTERVAL '3 days', end_offset => INTERVAL '1 day',
            schedule_interval => INTERVAL '1 day')
    """)
    op.execute("""
        SELECT add_continuous_aggregate_policy('query_stats_hourly',
            start_offset => INTERVAL '3 hours', end_offset => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour')
    """)


def downgrade() -> None:
    op.execute("SELECT remove_continuous_aggregate_policy('query_stats_hourly')")
    op.execute("SELECT remove_continuous_aggregate_policy('metric_samples_daily')")
    op.execute("SELECT remove_continuous_aggregate_policy('metric_samples_hourly')")
