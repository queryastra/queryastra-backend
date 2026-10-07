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
    assert entry["engine_version_major"] is None


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


async def test_set_instance_active_pauses_and_resumes_monitoring(
    client: AsyncClient, test_instance: Instance
) -> None:
    """Liga/desliga o monitoramento — `PATCH .../active` pausa/retoma sem apagar
    o cadastro, e a instancia continua listada (pausada) em `GET /instances`."""
    assert test_instance.active is True

    pause_response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/active", json={"active": False}
    )
    assert pause_response.status_code == 200
    assert pause_response.json()["active"] is False

    list_response = await client.get("/api/v1/instances")
    entry = next(i for i in list_response.json() if i["id"] == str(test_instance.id))
    assert entry["active"] is False  # continua na lista, so pausada — nao some da UI

    resume_response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/active", json={"active": True}
    )
    assert resume_response.status_code == 200
    assert resume_response.json()["active"] is True


async def test_set_instance_active_404_for_unknown_instance(client: AsyncClient) -> None:
    response = await client.patch(
        "/api/v1/instances/00000000-0000-0000-0000-000000000000/active",
        json={"active": False},
    )
    assert response.status_code == 404


async def test_update_instance_settings_changes_polling_interval(
    client: AsyncClient, test_instance: Instance
) -> None:
    """Tela de Configurações — antes só dava pra definir `polling_interval_seconds` no
    cadastro; agora dá pra editar uma instância já cadastrada."""
    assert test_instance.polling_interval_seconds == 10

    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/settings",
        json={"polling_interval_seconds": 1},
    )
    assert response.status_code == 200
    assert response.json()["polling_interval_seconds"] == 1


async def test_update_instance_settings_rejects_non_positive_interval(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/settings",
        json={"polling_interval_seconds": 0},
    )
    assert response.status_code == 422


async def test_update_instance_settings_404_for_unknown_instance(client: AsyncClient) -> None:
    response = await client.patch(
        "/api/v1/instances/00000000-0000-0000-0000-000000000000/settings",
        json={"polling_interval_seconds": 5},
    )
    assert response.status_code == 404


async def test_update_instance_settings_changes_criticality_only(
    client: AsyncClient, test_instance: Instance
) -> None:
    """Sub-tela "Geral" — manda só `criticality`, sem tocar `polling_interval_seconds`."""
    assert test_instance.criticality == "standard"

    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/settings",
        json={"criticality": "critical"},
    )
    assert response.status_code == 200
    body = response.json()
    assert body["criticality"] == "critical"
    assert body["polling_interval_seconds"] == 10  # não informado, não mudou


async def test_update_instance_settings_rejects_empty_payload(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/settings",
        json={},
    )
    assert response.status_code == 422


async def test_update_instance_min_lock_time_sets_override(
    client: AsyncClient, test_instance: Instance
) -> None:
    """Override ad-hoc pra investigar um caso específico só nesta instância."""
    assert test_instance.min_lock_time_ms is None

    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-lock-time",
        json={"min_lock_time_ms": 500},
    )
    assert response.status_code == 200
    assert response.json()["min_lock_time_ms"] == 500


async def test_update_instance_min_lock_time_null_clears_override(
    client: AsyncClient, test_instance: Instance
) -> None:
    """`null` explícito limpa o override — volta a usar o valor global."""
    await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-lock-time",
        json={"min_lock_time_ms": 500},
    )

    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-lock-time",
        json={"min_lock_time_ms": None},
    )
    assert response.status_code == 200
    assert response.json()["min_lock_time_ms"] is None


async def test_update_instance_min_lock_time_rejects_negative_value(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-lock-time",
        json={"min_lock_time_ms": -1},
    )
    assert response.status_code == 422


async def test_update_instance_min_lock_time_404_for_unknown_instance(
    client: AsyncClient,
) -> None:
    response = await client.patch(
        "/api/v1/instances/00000000-0000-0000-0000-000000000000/min-lock-time",
        json={"min_lock_time_ms": 500},
    )
    assert response.status_code == 404


async def test_set_database_lock_time_override_creates_and_updates(
    client: AsyncClient, test_instance: Instance
) -> None:
    """`PUT` cria na primeira chamada e atualiza (upsert) numa segunda chamada pro mesmo banco."""
    create_response = await client.put(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_lock_time_ms": 500},
    )
    assert create_response.status_code == 200
    assert create_response.json() == {"database_name": "AB_COBRANCA", "min_lock_time_ms": 500}

    update_response = await client.put(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_lock_time_ms": 750},
    )
    assert update_response.status_code == 200
    assert update_response.json()["min_lock_time_ms"] == 750

    list_response = await client.get(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides"
    )
    assert list_response.status_code == 200
    assert list_response.json() == [{"database_name": "AB_COBRANCA", "min_lock_time_ms": 750}]


async def test_clear_database_lock_time_override(
    client: AsyncClient, test_instance: Instance
) -> None:
    await client.put(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_lock_time_ms": 500},
    )

    delete_response = await client.delete(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides/AB_COBRANCA"
    )
    assert delete_response.status_code == 204

    list_response = await client.get(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides"
    )
    assert list_response.json() == []


async def test_clear_database_lock_time_override_404_when_no_override_exists(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.delete(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides/NUNCA_CONFIGURADO"
    )
    assert response.status_code == 404


async def test_reset_all_lock_time_overrides_clears_instance_and_database_overrides(
    client: AsyncClient, test_instance: Instance
) -> None:
    """Desmarcar "Limites ad hoc" na UI aciona este reset em massa — precisa apagar tanto o
    override por instância quanto qualquer override por banco, de TODAS as instâncias."""
    await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-lock-time",
        json={"min_lock_time_ms": 500},
    )
    await client.put(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_lock_time_ms": 300},
    )

    reset_response = await client.delete("/api/v1/instances/lock-time-overrides/reset")
    assert reset_response.status_code == 200
    body = reset_response.json()
    assert body["instances_cleared"] >= 1
    assert body["database_overrides_cleared"] >= 1

    instance_response = await client.get(f"/api/v1/instances/{test_instance.id}")
    assert instance_response.json()["min_lock_time_ms"] is None

    overrides_response = await client.get(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides"
    )
    assert overrides_response.json() == []


async def test_update_instance_min_sleeping_time_sets_override(
    client: AsyncClient, test_instance: Instance
) -> None:
    """Mesmo padrão de `min-lock-time`, para "Transações em Sleeping" (novas tarefas do usuário,
    itens 1/2)."""
    assert test_instance.min_sleeping_time_ms is None

    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-sleeping-time",
        json={"min_sleeping_time_ms": 600000},
    )
    assert response.status_code == 200
    assert response.json()["min_sleeping_time_ms"] == 600000


async def test_update_instance_min_sleeping_time_null_clears_override(
    client: AsyncClient, test_instance: Instance
) -> None:
    await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-sleeping-time",
        json={"min_sleeping_time_ms": 600000},
    )

    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-sleeping-time",
        json={"min_sleeping_time_ms": None},
    )
    assert response.status_code == 200
    assert response.json()["min_sleeping_time_ms"] is None


async def test_update_instance_min_sleeping_time_rejects_negative_value(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-sleeping-time",
        json={"min_sleeping_time_ms": -1},
    )
    assert response.status_code == 422


async def test_set_database_sleeping_time_override_creates_and_updates(
    client: AsyncClient, test_instance: Instance
) -> None:
    create_response = await client.put(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_sleeping_time_ms": 600000},
    )
    assert create_response.status_code == 200
    assert create_response.json() == {
        "database_name": "AB_COBRANCA",
        "min_sleeping_time_ms": 600000,
    }

    update_response = await client.put(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_sleeping_time_ms": 900000},
    )
    assert update_response.status_code == 200
    assert update_response.json()["min_sleeping_time_ms"] == 900000

    list_response = await client.get(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides"
    )
    assert list_response.status_code == 200
    assert list_response.json() == [
        {"database_name": "AB_COBRANCA", "min_sleeping_time_ms": 900000}
    ]


async def test_clear_database_sleeping_time_override(
    client: AsyncClient, test_instance: Instance
) -> None:
    await client.put(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_sleeping_time_ms": 600000},
    )

    delete_response = await client.delete(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides/AB_COBRANCA"
    )
    assert delete_response.status_code == 204

    list_response = await client.get(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides"
    )
    assert list_response.json() == []


async def test_clear_database_sleeping_time_override_404_when_no_override_exists(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.delete(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides/NUNCA_CONFIGURADO"
    )
    assert response.status_code == 404


async def test_reset_all_sleeping_time_overrides_clears_instance_and_database_overrides(
    client: AsyncClient, test_instance: Instance
) -> None:
    await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-sleeping-time",
        json={"min_sleeping_time_ms": 600000},
    )
    await client.put(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_sleeping_time_ms": 300000},
    )

    reset_response = await client.delete("/api/v1/instances/sleeping-time-overrides/reset")
    assert reset_response.status_code == 200
    body = reset_response.json()
    assert body["instances_cleared"] >= 1
    assert body["database_overrides_cleared"] >= 1

    instance_response = await client.get(f"/api/v1/instances/{test_instance.id}")
    assert instance_response.json()["min_sleeping_time_ms"] is None

    overrides_response = await client.get(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides"
    )
    assert overrides_response.json() == []


async def test_ad_hoc_overrides_summary_reports_active_overrides_of_both_parameters(
    client: AsyncClient, test_instance: Instance
) -> None:
    """Pedido explícito do usuário (item 3): saber, de relance, quais instâncias/bancos já têm
    override ativo — tanto de bloqueio quanto de sleeping, sem navegar instância por instância."""
    await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-lock-time",
        json={"min_lock_time_ms": 500},
    )
    await client.put(
        f"/api/v1/instances/{test_instance.id}/database-lock-time-overrides",
        json={"database_name": "AB_COBRANCA", "min_lock_time_ms": 300},
    )
    await client.patch(
        f"/api/v1/instances/{test_instance.id}/min-sleeping-time",
        json={"min_sleeping_time_ms": 600000},
    )
    await client.put(
        f"/api/v1/instances/{test_instance.id}/database-sleeping-time-overrides",
        json={"database_name": "AB_FATURAS", "min_sleeping_time_ms": 900000},
    )

    response = await client.get("/api/v1/instances/ad-hoc-overrides/summary")
    assert response.status_code == 200
    body = response.json()

    blocking_instance_ids = {row["instance_id"] for row in body["blocking"]["instance_overrides"]}
    assert str(test_instance.id) in blocking_instance_ids
    blocking_databases = {
        (row["instance_id"], row["database_name"]) for row in body["blocking"]["database_overrides"]
    }
    assert (str(test_instance.id), "AB_COBRANCA") in blocking_databases

    sleeping_instance_ids = {row["instance_id"] for row in body["sleeping"]["instance_overrides"]}
    assert str(test_instance.id) in sleeping_instance_ids
    sleeping_databases = {
        (row["instance_id"], row["database_name"]) for row in body["sleeping"]["database_overrides"]
    }
    assert (str(test_instance.id), "AB_FATURAS") in sleeping_databases

    # Reset pra nao vazar estado pro proximo teste - reaproveita os 2 endpoints de reset ja
    # cobertos pelos testes acima.
    await client.delete("/api/v1/instances/lock-time-overrides/reset")
    await client.delete("/api/v1/instances/sleeping-time-overrides/reset")
