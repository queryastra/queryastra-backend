"""Testes de `MetricSamplesRepository.insert_batch` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
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
