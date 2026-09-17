"""Testes de integracao dos endpoints de alertas (`/api/v1/alerts/*`) contra o banco real
`ab_xql_pm_dev`. Mesmo padrao de `tests/integration/api/test_metrics_endpoint.py`: semeia via
repositorio/`db_session` e chama o endpoint via `client` (ASGI transport, mesma `DATABASE_URL`).
"""

from __future__ import annotations

from uuid import UUID, uuid4

import sqlalchemy as sa
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alert import Alert, AlertRule
from app.models.instance import Instance
from app.repositories.alerts_repository import AlertsRepository


def _rule_payload(**overrides: object) -> dict:
    base = {
        "name": "regra-de-teste",
        "target_type": "instance",
        "metric_name": "test_metric",
        "comparison": "gte",
        "threshold_warning": 10.0,
        "threshold_critical": 20.0,
    }
    base.update(overrides)
    return base


async def test_create_and_list_alert_rules_includes_inactive_rule(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    response = await client.post("/api/v1/alerts/rules", json=_rule_payload(name="regra-ativa"))
    assert response.status_code == 201
    active_rule_id = response.json()["id"]
    assert response.json()["active"] is True

    repo = AlertsRepository(db_session)
    inactive_rule = await repo.create_alert_rule(
        AlertRule(
            name="regra-inativa",
            target_type="instance",
            metric_name="test_metric",
            comparison="gte",
            threshold_warning=10.0,
            active=False,
        )
    )

    try:
        list_response = await client.get("/api/v1/alerts/rules")
        assert list_response.status_code == 200
        rules_by_id = {r["id"]: r for r in list_response.json()}
        assert active_rule_id in rules_by_id
        assert rules_by_id[active_rule_id]["active"] is True
        assert str(inactive_rule.id) in rules_by_id
        assert rules_by_id[str(inactive_rule.id)]["active"] is False
    finally:
        active_rule = await db_session.get(AlertRule, UUID(active_rule_id))
        assert active_rule is not None
        await db_session.delete(active_rule)
        await db_session.delete(inactive_rule)
        await db_session.commit()


async def test_create_alert_rule_rejects_metric_rule_without_threshold(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/alerts/rules",
        json=_rule_payload(threshold_warning=None, threshold_critical=None),
    )
    assert response.status_code == 422


async def test_list_alerts_filters_by_state_and_resolves_names(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    open_alert = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )
    await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="critical",
            state="closed",
        )
    )

    response = await client.get("/api/v1/alerts", params={"state": "open"})
    assert response.status_code == 200
    body = response.json()
    ids = {a["id"] for a in body}
    assert str(open_alert.id) in ids
    only_open = next(a for a in body if a["id"] == str(open_alert.id))
    assert only_open["alert_name"] == alert_rule.name
    assert only_open["target_name"] == test_instance.name
    assert all(a["state"] == "open" for a in body)


async def test_acknowledge_alert_succeeds_then_conflicts_on_second_call(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    created = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )

    response = await client.post(f"/api/v1/alerts/{created.id}/acknowledge")
    assert response.status_code == 200
    assert response.json()["state"] == "acknowledged"

    conflict_response = await client.post(f"/api/v1/alerts/{created.id}/acknowledge")
    assert conflict_response.status_code == 409


async def test_acknowledge_alert_returns_404_when_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/api/v1/alerts/{uuid4()}/acknowledge")
    assert response.status_code == 404


async def test_create_list_and_delete_alert_silence(
    client: AsyncClient, test_instance: Instance, alert_rule: AlertRule
) -> None:
    create_response = await client.post(
        "/api/v1/alerts/silences",
        json={
            "target_type": "instance",
            "target_id": str(test_instance.id),
            "alert_rule_id": str(alert_rule.id),
            "reason": "manutencao planejada",
        },
    )
    assert create_response.status_code == 201
    silence_id = create_response.json()["id"]

    list_response = await client.get(
        "/api/v1/alerts/silences", params={"target_id": str(test_instance.id)}
    )
    assert list_response.status_code == 200
    assert {s["id"] for s in list_response.json()} == {silence_id}

    delete_response = await client.delete(f"/api/v1/alerts/silences/{silence_id}")
    assert delete_response.status_code == 204

    list_after_delete = await client.get(
        "/api/v1/alerts/silences", params={"target_id": str(test_instance.id)}
    )
    assert list_after_delete.json() == []


async def test_delete_alert_silence_returns_404_when_not_found(client: AsyncClient) -> None:
    response = await client.delete(f"/api/v1/alerts/silences/{uuid4()}")
    assert response.status_code == 404


async def test_create_alert_rule_rejects_custom_sql_with_blocked_keyword(
    client: AsyncClient,
) -> None:
    response = await client.post(
        "/api/v1/alerts/rules",
        json=_rule_payload(
            metric_name=None,
            rule_type="custom_sql",
            custom_sql="DELETE FROM t",
            comparison="gt",
            threshold_warning=1.0,
            threshold_critical=None,
        ),
    )
    assert response.status_code == 422


async def test_create_alert_rule_with_escalation_levels(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    response = await client.post(
        "/api/v1/alerts/rules",
        json=_rule_payload(
            name="regra-com-escalonamento",
            escalation_levels=[{"level_number": 2, "wait_minutes": 30}],
        ),
    )
    assert response.status_code == 201
    body = response.json()
    assert body["rule_type"] == "threshold"
    assert body["reset_mode"] == "auto_when_false"
    assert len(body["escalation_levels"]) == 1
    assert body["escalation_levels"][0]["level_number"] == 2
    assert body["escalation_levels"][0]["wait_minutes"] == 30

    rule = await db_session.get(AlertRule, UUID(body["id"]))
    assert rule is not None
    await db_session.delete(rule)
    await db_session.commit()


async def test_close_alert_succeeds_then_conflicts_on_second_call(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    created = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )

    response = await client.post(f"/api/v1/alerts/{created.id}/close")
    assert response.status_code == 200
    assert response.json()["state"] == "closed"

    conflict_response = await client.post(f"/api/v1/alerts/{created.id}/close")
    assert conflict_response.status_code == 409


async def test_close_alert_returns_404_when_not_found(client: AsyncClient) -> None:
    response = await client.post(f"/api/v1/alerts/{uuid4()}/close")
    assert response.status_code == 404


async def test_delete_alert_rule_removes_rule_and_dependent_alerts(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = AlertsRepository(db_session)
    rule = await repo.create_alert_rule(
        AlertRule(
            name="regra-para-excluir",
            target_type="instance",
            metric_name="test_metric",
            comparison="gte",
            threshold_warning=10.0,
        )
    )
    alert = await repo.create_alert(
        Alert(
            alert_rule_id=rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )

    response = await client.delete(f"/api/v1/alerts/rules/{rule.id}")
    assert response.status_code == 204

    # `rule`/`alert` ja estao no identity map desta sessao (criados via `repo` acima) - uma
    # query SQL direta (nao `.get()`) evita devolver o objeto em memoria em vez de reconsultar
    # o banco real apos a exclusao (feita por uma sessao diferente, a do client).
    remaining_rules = await db_session.execute(sa.select(AlertRule).where(AlertRule.id == rule.id))
    assert remaining_rules.scalar_one_or_none() is None
    remaining_alerts = await db_session.execute(sa.select(Alert).where(Alert.id == alert.id))
    assert remaining_alerts.scalar_one_or_none() is None


async def test_delete_alert_rule_returns_404_when_not_found(client: AsyncClient) -> None:
    response = await client.delete(f"/api/v1/alerts/rules/{uuid4()}")
    assert response.status_code == 404


async def test_delete_alert_removes_record_not_just_state(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    created = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )

    response = await client.delete(f"/api/v1/alerts/{created.id}")
    assert response.status_code == 204

    remaining = await db_session.execute(sa.select(Alert).where(Alert.id == created.id))
    assert remaining.scalar_one_or_none() is None


async def test_delete_alert_returns_404_when_not_found(client: AsyncClient) -> None:
    response = await client.delete(f"/api/v1/alerts/{uuid4()}")
    assert response.status_code == 404


async def test_list_alerts_filters_by_target_id(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance, alert_rule: AlertRule
) -> None:
    repo = AlertsRepository(db_session)
    other_instance_id = uuid4()
    matching = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=test_instance.id,
            severity="warning",
        )
    )
    other = await repo.create_alert(
        Alert(
            alert_rule_id=alert_rule.id,
            target_type="instance",
            target_id=other_instance_id,
            severity="warning",
        )
    )

    response = await client.get("/api/v1/alerts", params={"target_id": str(test_instance.id)})
    assert response.status_code == 200
    ids = {a["id"] for a in response.json()}
    assert str(matching.id) in ids
    assert str(other.id) not in ids


async def test_get_alert_rule_by_id_returns_rule_with_escalation_levels(
    client: AsyncClient,
) -> None:
    create_response = await client.post(
        "/api/v1/alerts/rules",
        json=_rule_payload(
            name="regra-para-buscar",
            escalation_levels=[{"level_number": 2, "wait_minutes": 15}],
        ),
    )
    assert create_response.status_code == 201
    rule_id = create_response.json()["id"]

    response = await client.get(f"/api/v1/alerts/rules/{rule_id}")
    assert response.status_code == 200
    body = response.json()
    assert body["name"] == "regra-para-buscar"
    assert len(body["escalation_levels"]) == 1
    assert body["escalation_levels"][0]["level_number"] == 2

    delete_response = await client.delete(f"/api/v1/alerts/rules/{rule_id}")
    assert delete_response.status_code == 204


async def test_get_alert_rule_returns_404_when_not_found(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/alerts/rules/{uuid4()}")
    assert response.status_code == 404


async def test_update_alert_rule_replaces_fields_and_escalation_levels(
    client: AsyncClient,
) -> None:
    create_response = await client.post(
        "/api/v1/alerts/rules",
        json=_rule_payload(
            name="regra-para-editar",
            threshold_warning=10.0,
            threshold_critical=20.0,
            escalation_levels=[{"level_number": 2, "wait_minutes": 15}],
        ),
    )
    assert create_response.status_code == 201
    rule_id = create_response.json()["id"]

    update_response = await client.patch(
        f"/api/v1/alerts/rules/{rule_id}",
        json=_rule_payload(
            name="regra-editada",
            threshold_warning=30.0,
            threshold_critical=40.0,
            escalation_levels=[{"level_number": 2, "wait_minutes": 45}],
        ),
    )
    assert update_response.status_code == 200
    body = update_response.json()
    assert body["name"] == "regra-editada"
    assert body["threshold_warning"] == 30.0
    assert body["threshold_critical"] == 40.0
    assert len(body["escalation_levels"]) == 1
    assert body["escalation_levels"][0]["wait_minutes"] == 45

    delete_response = await client.delete(f"/api/v1/alerts/rules/{rule_id}")
    assert delete_response.status_code == 204


async def test_update_alert_rule_returns_404_when_not_found(client: AsyncClient) -> None:
    response = await client.patch(f"/api/v1/alerts/rules/{uuid4()}", json=_rule_payload())
    assert response.status_code == 404
