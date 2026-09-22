"""Testes de `MetricSamplesRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import asyncpg
import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.instance import Instance
from app.normalizers.metric_names import COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT
from app.normalizers.models import NormalizedMetricSample
from app.repositories._tables import metric_samples_table
from app.repositories.metric_samples_repository import MetricSamplesRepository


async def _refresh_continuous_aggregate(view_name: str) -> None:
    """`CALL refresh_continuous_aggregate(...)` nao pode rodar dentro de uma transacao
    (confirmado ao vivo contra o Postgres/TimescaleDB de dev) - a sessao ORM do app sempre abre
    uma implicitamente (`autobegin`), entao os testes do Estagio B (`metric_samples_hourly`/
    `_daily`) precisam de uma conexao asyncpg crua, fora do `db_session` de teste."""
    dsn = get_settings().database_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute(f"CALL refresh_continuous_aggregate('{view_name}', NULL, NULL)")
    finally:
        await conn.close()


async def test_insert_batch_persists_rows(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MetricSamplesRepository(db_session)
    samples = [
        NormalizedMetricSample(
            time=datetime.now(UTC),
            target_type="instance",
            target_id=test_instance.id,
            metric_name="test_metric_a",
            value=1.5,
        ),
        NormalizedMetricSample(
            time=datetime.now(UTC),
            target_type="instance",
            target_id=test_instance.id,
            metric_name="test_metric_b",
            value=2.5,
            labels={"foo": "bar"},
        ),
    ]

    await repo.insert_batch(samples)

    result = await db_session.execute(
        sa.select(metric_samples_table).where(metric_samples_table.c.target_id == test_instance.id)
    )
    rows = result.fetchall()
    assert len(rows) == 2
    metric_names = {row.metric_name for row in rows}
    assert metric_names == {"test_metric_a", "test_metric_b"}
    labelled = next(row for row in rows if row.metric_name == "test_metric_b")
    assert labelled.labels == {"foo": "bar"}


async def test_insert_batch_noop_for_empty_list(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MetricSamplesRepository(db_session)
    await repo.insert_batch([])  # nao deve lancar nem tocar o banco


async def test_list_series_filters_by_metric_name_and_window(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MetricSamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_in_window",
                value=1.0,
            ),
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_excluded_by_name",
                value=2.0,
            ),
            NormalizedMetricSample(
                time=now - timedelta(hours=2),
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_in_window",
                value=3.0,
            ),
        ]
    )

    rows = await repo.list_series(
        "instance",
        test_instance.id,
        ["metric_in_window"],
        since=now - timedelta(minutes=5),
        until=now + timedelta(minutes=5),
        limit=100,
    )

    assert len(rows) == 1
    assert rows[0].metric_name == "metric_in_window"


async def test_list_series_applies_limit_per_metric_not_globally(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Regressao: bug real encontrado em producao — um LIMIT global ordenado por
    (metric_name, time) fazia uma metrica gravada a cada ciclo (ex.:
    `collector_cycle_duration_ms`, que ordena antes alfabeticamente) consumir o orcamento
    inteiro numa instancia de poll rapido, deixando outra metrica (ex.: sessoes ativas) sem
    NENHUM ponto. `limit` precisa ser POR `metric_name`, nao um teto compartilhado."""
    repo = MetricSamplesRepository(db_session)
    now = datetime.now(UTC)
    samples = [
        NormalizedMetricSample(
            time=now - timedelta(seconds=i),
            target_type="instance",
            target_id=test_instance.id,
            metric_name=COLLECTOR_CYCLE_DURATION_MS,  # ordena antes de "z_business_metric"
            value=float(i),
        )
        for i in range(5)
    ] + [
        NormalizedMetricSample(
            time=now - timedelta(seconds=i),
            target_type="instance",
            target_id=test_instance.id,
            metric_name="z_business_metric",
            value=float(i),
        )
        for i in range(5)
    ]
    await repo.insert_batch(samples)

    rows = await repo.list_series(
        "instance",
        test_instance.id,
        None,
        since=now - timedelta(minutes=5),
        until=now + timedelta(minutes=5),
        limit=3,  # menor que o total de cada metrica (5) — testa o particionamento de verdade
    )

    by_metric: dict[str, int] = {}
    for row in rows:
        by_metric[row.metric_name] = by_metric.get(row.metric_name, 0) + 1
    assert by_metric == {COLLECTOR_CYCLE_DURATION_MS: 3, "z_business_metric": 3}


async def test_list_series_without_metric_names_filter_returns_none_filtered(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`metric_names=None` devolve todas as metricas do alvo (o `CAST(:metric_names AS text[])
    IS NULL` da query precisa realmente pular o filtro, nao excluir tudo)."""
    repo = MetricSamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_a",
                value=1.0,
            ),
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_b",
                value=2.0,
            ),
        ]
    )

    rows = await repo.list_series(
        "instance",
        test_instance.id,
        None,
        since=now - timedelta(minutes=5),
        until=now + timedelta(minutes=5),
        limit=100,
    )

    assert {row.metric_name for row in rows} == {"metric_a", "metric_b"}
    assert rows[0].value == 1.0


async def test_list_series_medium_window_reads_hourly_aggregate(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Etapa 10, item 4 (Estagio B) - janela > `_RAW_TABLE_MAX_WINDOW` (2 dias) le
    `metric_samples_hourly`, nao a hypertable bruta. Prova disso (nao so inspecao da query): a
    amostra existe na hypertable bruta desde o insert, mas `list_series` so a devolve depois do
    refresh do continuous aggregate - se ainda lesse `metric_samples` diretamente, apareceria
    mesmo sem o refresh."""
    repo = MetricSamplesRepository(db_session)
    sample_time = datetime.now(UTC) - timedelta(days=10)
    await repo.insert_batch(
        [
            NormalizedMetricSample(
                time=sample_time,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_medium_window",
                value=42.0,
            )
        ]
    )

    window_kwargs = {
        "since": sample_time - timedelta(hours=1),
        "until": datetime.now(UTC),
        "limit": 100,
    }
    rows_before_refresh = await repo.list_series(
        "instance", test_instance.id, ["metric_medium_window"], **window_kwargs
    )
    assert rows_before_refresh == []  # ainda nao materializado no aggregate

    await _refresh_continuous_aggregate("metric_samples_hourly")

    rows_after_refresh = await repo.list_series(
        "instance", test_instance.id, ["metric_medium_window"], **window_kwargs
    )
    assert len(rows_after_refresh) == 1
    assert rows_after_refresh[0].value == 42.0


async def test_list_series_long_window_reads_daily_aggregate(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Mesma prova do teste acima, mas pra janela > `_HOURLY_MAX_WINDOW` (45 dias) -
    `metric_samples_daily`, que depende de `metric_samples_hourly` ja estar materializado."""
    repo = MetricSamplesRepository(db_session)
    sample_time = datetime.now(UTC) - timedelta(days=60)
    await repo.insert_batch(
        [
            NormalizedMetricSample(
                time=sample_time,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_long_window",
                value=7.0,
            )
        ]
    )

    window_kwargs = {
        # `metric_samples_daily.bucket` trunca pro INICIO do dia (00:00 UTC) - a folga de 1 dia
        # (nao 1h, como no teste do hourly acima) garante que o bucket do dia caia dentro da
        # janela mesmo quando `sample_time` e mais tarde no dia que a meia-noite.
        "since": sample_time - timedelta(days=1),
        "until": datetime.now(UTC),
        "limit": 100,
    }
    rows_before_refresh = await repo.list_series(
        "instance", test_instance.id, ["metric_long_window"], **window_kwargs
    )
    assert rows_before_refresh == []

    await _refresh_continuous_aggregate("metric_samples_hourly")
    await _refresh_continuous_aggregate("metric_samples_daily")

    rows_after_refresh = await repo.list_series(
        "instance", test_instance.id, ["metric_long_window"], **window_kwargs
    )
    assert len(rows_after_refresh) == 1
    assert rows_after_refresh[0].value == 7.0


async def test_get_latest_value_returns_none_when_no_telemetry(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MetricSamplesRepository(db_session)
    row = await repo.get_latest_value("instance", test_instance.id, COLLECTOR_CYCLE_DURATION_MS)
    assert row is None


async def test_count_distinct_metrics_since_excludes_telemetry_names(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MetricSamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name=COLLECTOR_CYCLE_DURATION_MS,
                value=12.3,
            ),
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name=COLLECTOR_CYCLE_ERROR_COUNT,
                value=0.0,
            ),
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="postgresql_active_session_count",
                value=4.0,
            ),
        ]
    )

    count = await repo.count_distinct_metrics_since(
        "instance",
        test_instance.id,
        since=now - timedelta(minutes=1),
        exclude_metric_names=[COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT],
    )

    assert count == 1
