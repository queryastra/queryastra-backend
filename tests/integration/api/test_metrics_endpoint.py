"""Testes de integracao de `GET /api/v1/metrics/{target_type}/{target_id}` contra o banco real
`ab_xql_pm_dev`. Semeia via `MetricSamplesRepository.insert_batch` (fixture `db_session`) e
chama o endpoint via `client` (fixture de `tests/conftest.py`, ASGI transport contra a app real —
usa a mesma `DATABASE_URL` do `.env`, entao os dados semeados no `db_session` (ja commitados)
sao visiveis para a requisicao do `client`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedMetricSample
from app.repositories.metric_samples_repository import MetricSamplesRepository


async def test_get_metric_series_groups_points_by_metric_name(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
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
                labels={"unit": "count"},
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

    response = await client.get(f"/api/v1/metrics/instance/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    names = {series["metric_name"] for series in body}
    assert names == {"metric_a", "metric_b"}
    series_a = next(s for s in body if s["metric_name"] == "metric_a")
    assert series_a["points"][0]["value"] == 1.0
    assert series_a["points"][0]["labels"] == {"unit": "count"}


async def test_get_metric_series_filters_by_metric_name_query_param(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MetricSamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_wanted",
                value=1.0,
            ),
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_unwanted",
                value=2.0,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/metrics/instance/{test_instance.id}", params={"metric_name": "metric_wanted"}
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["metric_name"] == "metric_wanted"


async def test_get_metric_series_empty_outside_window(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MetricSamplesRepository(db_session)
    old_time = datetime.now(UTC) - timedelta(days=1)
    await repo.insert_batch(
        [
            NormalizedMetricSample(
                time=old_time,
                target_type="instance",
                target_id=test_instance.id,
                metric_name="metric_old",
                value=1.0,
            )
        ]
    )

    response = await client.get(
        f"/api/v1/metrics/instance/{test_instance.id}",
        params={
            "from": (datetime.now(UTC) - timedelta(minutes=5)).isoformat(),
            "to": datetime.now(UTC).isoformat(),
        },
    )

    assert response.status_code == 200
    assert response.json() == []
