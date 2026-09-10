"""Testa que `GET /api/v1/health` responde 200 de verdade — smoke test do backend.

`Docs/specs/02-backend-contract.md`: este e o unico endpoint deste scaffold com logica real
(nenhuma dependencia de banco), usado para validar que a app sobe corretamente.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient

pytestmark = pytest.mark.asyncio


async def test_health_returns_200(client: AsyncClient) -> None:
    response = await client.get("/api/v1/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}
