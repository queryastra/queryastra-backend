"""Fixtures compartilhadas de teste.

Nenhuma fixture aqui depende de banco real — os testes deste scaffold sao unitarios (masking,
taxonomia, contrato de coletor) ou de integracao de API que nao tocam persistencia
(`GET /api/v1/health`, contrato de router).
"""

from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from app.main import app


@pytest.fixture
async def client() -> AsyncClient:
    """Cliente HTTP assincrono contra a app FastAPI, sem subir servidor real (ASGI transport)."""
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac
