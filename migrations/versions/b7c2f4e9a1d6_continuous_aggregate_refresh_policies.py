"""continuous aggregate refresh policies (Estagio B)

Revision ID: b7c2f4e9a1d6
Revises: a1c4e7f92b58
Create Date: 2026-09-22 12:43:25.453746

Etapa 10, item 4 (Estagio B) — liga `metric_samples_hourly`/`metric_samples_daily` com
`add_continuous_aggregate_policy` de verdade (Estagio A, ja shippado, so corrigiu o bug do
`LIMIT` global e o debounce do frontend; a leitura continuava 100% na hypertable bruta, sem
usar os aggregates pra janelas longas). Sem isso, uma janela "Tudo" (`DayFilter`, `ALL_TIME_FROM_
ISO` = ano 2000) numa instancia de poll 1s tenta rankear via window function TODAS as amostras
brutas dos ultimos 30 dias (retencao de `metric_samples`) so pra aplicar o LIMIT por metrica —
caro mesmo com o LIMIT por metric_name corrigido no Estagio A, e simplesmente ERRADO pra
qualquer janela que peca dado mais antigo que a retencao de 30 dias (nao existe mais na
hypertable bruta).

Bug real corrigido nesta migration (achado ao inspecionar o `metric_samples_hourly`/`_daily` ja
existentes): as definicoes originais (`66ae4664300c_hypertables.py`) NAO agrupavam por `labels`
— um continuous aggregate que colapsasse metricas com `labels` diferentes (ex.:
`sqlserver_volume_free_percent` com `labels={"volume": "C:\\"}`/`labels={"volume": "D:\\"}`, ou
`postgresql_replica_replay_lag_seconds` com `labels={"replica": ...}`, ambas adicionadas depois
desta migration original, Fase 2 item 2) misturaria volumes/replicas DIFERENTES numa unica media
sem que ninguem percebesse — silencioso e errado, nao so uma perda de granularidade aceitavel.
Corrigido recriando os 2 continuous aggregates com `labels` no SELECT/GROUP BY.

Motivo de DROP + CREATE (nao ALTER): TimescaleDB nao permite alterar a query de um continuous
aggregate ja existente — a unica forma de mudar o agrupamento e recriar do zero. Os dados
materializados (derivados, nunca fonte de verdade) sao perdidos no DROP e re-populados aqui mesmo
via `CALL refresh_continuous_aggregate(...)` logo em seguida (fora de bloco de transacao — mesma
restricao ja documentada na migration original pra `CREATE MATERIALIZED VIEW ... WITH DATA`,
confirmada agora tambem pra `CALL refresh_continuous_aggregate`:
`ActiveSQLTransactionError: refresh_continuous_aggregate() nao pode ser executado dentro de um
bloco de transacao` — testado ao vivo contra o Postgres/TimescaleDB de dev antes de escrever esta
migration).

`add_continuous_aggregate_policy(..., if_not_exists => true)` deixa a migration idempotente —
necessario porque estas policies exatas (mesmo schedule usado aqui) ja tinham sido adicionadas
manualmente contra o banco de dev nesta sessao de trabalho, fora de qualquer migration (achado ao
investigar o Estagio B: `timescaledb_information.jobs` ja tinha as policies rodando havia dias,
sem nenhum arquivo de migration correspondente — ou seja, um ambiente novo/producao nunca teria
essas policies sem esta migration).
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b7c2f4e9a1d6"
down_revision: str | None = "a1c4e7f92b58"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # CASCADE: metric_samples_daily depende de metric_samples_hourly - precisa cair primeiro.
    op.execute("DROP MATERIALIZED VIEW IF EXISTS metric_samples_daily CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS metric_samples_hourly CASCADE")

    op.execute("""
        CREATE MATERIALIZED VIEW metric_samples_hourly
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 hour', time) AS bucket,
               target_type, target_id, metric_name, labels,
               avg(value) AS avg_value, max(value) AS max_value, min(value) AS min_value
        FROM metric_samples
        GROUP BY bucket, target_type, target_id, metric_name, labels
        WITH NO DATA
    """)
    op.execute("""
        CREATE MATERIALIZED VIEW metric_samples_daily
        WITH (timescaledb.continuous) AS
        SELECT time_bucket('1 day', bucket) AS bucket,
               target_type, target_id, metric_name, labels,
               avg(avg_value) AS avg_value, max(max_value) AS max_value, min(min_value) AS min_value
        FROM metric_samples_hourly
        GROUP BY time_bucket('1 day', bucket), target_type, target_id, metric_name, labels
        WITH NO DATA
    """)

    # Mesmo schedule ja confirmado rodando manualmente no banco de dev (ver docstring acima):
    # end_offset deixa a fatia mais recente (1h/1d) de fora do aggregate de proposito - o
    # aggregate nunca materializa um bucket ainda "incompleto"; o caminho de leitura cobre essa
    # lacuna simplesmente preferindo a hypertable bruta pra qualquer janela curta (ver
    # `MetricSamplesRepository.list_series`).
    op.execute("""
        SELECT add_continuous_aggregate_policy('metric_samples_hourly',
            start_offset => INTERVAL '3 hours',
            end_offset => INTERVAL '1 hour',
            schedule_interval => INTERVAL '1 hour',
            if_not_exists => true)
    """)
    op.execute("""
        SELECT add_continuous_aggregate_policy('metric_samples_daily',
            start_offset => INTERVAL '3 days',
            end_offset => INTERVAL '1 day',
            schedule_interval => INTERVAL '1 day',
            if_not_exists => true)
    """)

    # Retention original (66ae4664300c) ficava presa ao hypertable_id ANTIGO das views que acabei
    # de recriar - precisa ser reaplicada aqui contra o novo hypertable por tras de cada view.
    op.execute("""
        SELECT add_retention_policy('metric_samples_hourly', INTERVAL '90 days',
            if_not_exists => true)
    """)
    op.execute("""
        SELECT add_retention_policy('metric_samples_daily', INTERVAL '5 years',
            if_not_exists => true)
    """)

    # Backfill imediato do historico ja existente em `metric_samples` (sem isso, os 2
    # aggregates ficam vazios ate a policy rodar pela primeira vez, no pior caso 1h/1d depois) -
    # `CALL refresh_continuous_aggregate` nao pode rodar dentro de uma transacao (confirmado ao
    # vivo, ver docstring), entao roda num bloco autocommit dedicado. Hourly primeiro - daily
    # depende dos dados dele.
    with op.get_context().autocommit_block():
        op.execute("CALL refresh_continuous_aggregate('metric_samples_hourly', NULL, NULL)")
        op.execute("CALL refresh_continuous_aggregate('metric_samples_daily', NULL, NULL)")


def downgrade() -> None:
    op.execute("DROP MATERIALIZED VIEW IF EXISTS metric_samples_daily CASCADE")
    op.execute("DROP MATERIALIZED VIEW IF EXISTS metric_samples_hourly CASCADE")
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
    op.execute("SELECT add_retention_policy('metric_samples_hourly', INTERVAL '90 days')")
    op.execute("SELECT add_retention_policy('metric_samples_daily', INTERVAL '5 years')")
