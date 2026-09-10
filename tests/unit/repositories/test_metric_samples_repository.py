"""Testes de `MetricSamplesRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.metric_names import COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT
from app.normalizers.models import NormalizedMetricSample
from app.repositories._tables import metric_samples_table
from app.repositories.metric_samples_repository import MetricSamplesRepository


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
    assert rows[0].value == 1.0


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
