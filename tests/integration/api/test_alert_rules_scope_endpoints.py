"""Integracao (banco real): escopo por instancia, modo de avaliacao e canais por regra em
`/api/v1/alerts/rules`, e o catalogo de metricas derivadas."""

from __future__ import annotations

from uuid import UUID, uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.alert import AlertRule
from app.models.instance import Instance
from app.normalizers.metric_names import PG_BUFFER_HIT_RATIO_PERCENT


def _payload(**overrides: object) -> dict:
    base = {
        "name": f"regra-escopo-{uuid4().hex[:6]}",
        "target_type": "instance",
        "metric_name": "postgresql_deadlocks_total",
        "comparison": "gte",
        "threshold_warning": 1.0,
        "threshold_critical": 5.0,
    }
    base.update(overrides)
    return base


async def test_scoped_rate_rule_with_channels_roundtrip_and_filter(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    resp = await client.post(
        "/api/v1/alerts/rules",
        json=_payload(
            instance_id=str(test_instance.id),
            evaluation_mode="rate_per_minute",
            notify_emails=[" dba@example.com ", ""],
            webhook_url="https://hooks.example.com/x",
        ),
    )
    assert resp.status_code == 201, resp.text
    body = resp.json()
    rule_id = body["id"]
    global_resp = await client.post("/api/v1/alerts/rules", json=_payload())
    global_id = global_resp.json()["id"]
    try:
        assert body["instance_id"] == str(test_instance.id)
        assert body["evaluation_mode"] == "rate_per_minute"
        assert body["notify_emails"] == ["dba@example.com"]
        assert body["webhook_url"] == "https://hooks.example.com/x"
        assert body["notify_team_id"] is None

        for_instance = await client.get(
            "/api/v1/alerts/rules", params={"instance_id": str(test_instance.id)}
        )
        ids = {r["id"] for r in for_instance.json()}
        assert {rule_id, global_id} <= ids

        for_other = await client.get("/api/v1/alerts/rules", params={"instance_id": str(uuid4())})
        assert rule_id not in {r["id"] for r in for_other.json()}

        scoped_only = await client.get("/api/v1/alerts/rules", params={"scope": "instance"})
        assert global_id not in {r["id"] for r in scoped_only.json()}
    finally:
        for raw in (rule_id, global_id):
            rule = await db_session.get(AlertRule, UUID(raw))
            if rule is not None:
                await db_session.delete(rule)
        await db_session.commit()


async def test_rule_validation_errors(client: AsyncClient, db_session: AsyncSession) -> None:
    # `db_session` (mesmo sem uso direto) descarta o pool ao final — sem isso conexoes asyncpg
    # presas a este event loop quebram o proximo teste ("Event loop is closed").
    del db_session
    assert (
        await client.post("/api/v1/alerts/rules", json=_payload(instance_id=str(uuid4())))
    ).status_code == 422
    assert (
        await client.post("/api/v1/alerts/rules", json=_payload(notify_team_id=999_999_999))
    ).status_code == 422
    assert (
        await client.post("/api/v1/alerts/rules", json=_payload(notify_emails=["not-an-email"]))
    ).status_code == 422
    assert (
        await client.post("/api/v1/alerts/rules", json=_payload(webhook_url="ftp://x"))
    ).status_code == 422
    assert (
        await client.post(
            "/api/v1/alerts/rules",
            json=_payload(rule_type="anomaly", evaluation_mode="rate_per_minute"),
        )
    ).status_code == 422


async def test_metric_catalog_lists_derived_metrics(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    del db_session
    resp = await client.get("/api/v1/alerts/metric-catalog")
    assert resp.status_code == 200
    assert PG_BUFFER_HIT_RATIO_PERCENT in resp.json()["derived_metrics"]
