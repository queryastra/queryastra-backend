"""Testes de integracao do status derivado de instancia (`GET /api/v1/instances/{id}` e
`GET /api/v1/instances`) contra o banco real `ab_xql_pm_dev`. Semeia `metric_samples`
manualmente via `MetricSamplesRepository.insert_batch` para os 3 cenarios de
`InstanceDerivedStatusService`: healthy, unavailable, unknown."""

from __future__ import annotations

from datetime import UTC, datetime

import sqlalchemy as sa
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alert import Alert, AlertRule
from app.models.instance import Instance
from app.normalizers.metric_names import COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT
from app.normalizers.models import NormalizedMetricSample
from app.repositories.alerts_repository import AlertsRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository


async def test_get_instance_unknown_when_no_telemetry(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/instances/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["health_status"] == "unknown"
    assert body["collection_coverage"] == "unavailable"
    assert body["last_collected_at"] is None
    assert body["capability_errors"] == []


async def test_get_instance_healthy_with_full_coverage(
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
                metric_name=COLLECTOR_CYCLE_DURATION_MS,
                value=15.0,
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
                value=3.0,
            ),
        ]
    )

    response = await client.get(f"/api/v1/instances/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["health_status"] == "healthy"
    assert body["collection_coverage"] == "full"
    assert body["last_collected_at"] is not None


async def test_get_instance_unavailable_when_collector_reported_error(
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
                metric_name=COLLECTOR_CYCLE_DURATION_MS,
                value=8.0,
            ),
            NormalizedMetricSample(
                time=now,
                target_type="instance",
                target_id=test_instance.id,
                metric_name=COLLECTOR_CYCLE_ERROR_COUNT,
                value=1.0,
            ),
        ]
    )

    response = await client.get(f"/api/v1/instances/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert body["health_status"] == "unavailable"
    assert body["collection_coverage"] == "unavailable"


async def test_list_instances_includes_derived_status(
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
                metric_name=COLLECTOR_CYCLE_DURATION_MS,
                value=15.0,
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
                value=3.0,
            ),
        ]
    )

    response = await client.get("/api/v1/instances")

    assert response.status_code == 200
    body = response.json()
    entry = next(i for i in body if i["id"] == str(test_instance.id))
    assert entry["health_status"] == "healthy"
    assert entry["collection_coverage"] == "full"
    assert entry["open_alert_count"] == 0
    assert entry["engine_version"] is None  # test_instance nunca teve um ciclo real de coleta


async def test_list_instances_open_alert_count_reflects_real_alerts(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Regressao: `open_alert_count` era hardcoded em 0 (TODO de quando `GET /alerts` ainda era
    501) — agora reflete `AlertsRepository.count_open_alerts_by_target` de verdade."""
    alerts_repo = AlertsRepository(db_session)
    rule = await alerts_repo.create_alert_rule(
        AlertRule(
            name="regra-teste-open-alert-count",
            target_type="instance",
            metric_name="test_metric",
            comparison="gte",
            threshold_warning=1.0,
        )
    )
    await alerts_repo.create_alert(
        Alert(
            alert_rule_id=rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )

    response = await client.get("/api/v1/instances")

    assert response.status_code == 200
    entry = next(i for i in response.json() if i["id"] == str(test_instance.id))
    assert entry["open_alert_count"] == 1

    await db_session.execute(sa.delete(Alert).where(Alert.alert_rule_id == rule.id))
    await db_session.execute(sa.delete(AlertRule).where(AlertRule.id == rule.id))
    await db_session.commit()
