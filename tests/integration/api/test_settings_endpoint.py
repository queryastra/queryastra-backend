"""Teste de integracao de `GET/PATCH /api/v1/settings` contra o banco real `ab_xql_pm_dev` —
tela "Configuracoes -> Geral" no frontend."""

from __future__ import annotations

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.repositories.system_settings_repository import SystemSettingsRepository


async def test_get_settings_returns_singleton_row(client: AsyncClient) -> None:
    response = await client.get("/api/v1/settings")

    assert response.status_code == 200
    body = response.json()
    assert "blocking_min_duration_ms" in body
    assert isinstance(body["blocking_min_duration_ms"], int)


async def test_patch_settings_updates_and_persists_the_value(
    client: AsyncClient, db_session: AsyncSession
) -> None:
    repo = SystemSettingsRepository(db_session)
    try:
        response = await client.patch("/api/v1/settings", json={"blocking_min_duration_ms": 3000})

        assert response.status_code == 200
        assert response.json()["blocking_min_duration_ms"] == 3000

        follow_up = await client.get("/api/v1/settings")
        assert follow_up.json()["blocking_min_duration_ms"] == 3000
    finally:
        await repo.update_blocking_min_duration_ms(0)


async def test_patch_settings_rejects_negative_value(client: AsyncClient) -> None:
    response = await client.patch("/api/v1/settings", json={"blocking_min_duration_ms": -1})

    assert response.status_code == 422
