"""Agregador de sub-routers sob `/api/v1` — `Docs/specs/02-backend-contract.md`.

"Contratos publicos devem ter schema versionado e teste de compatibilidade." Este router e o
unico lugar que registra os sub-routers; `tests/integration/api/test_v1_contract_snapshot.py`
valida os paths expostos aqui contra o esperado.
"""

from __future__ import annotations

from fastapi import APIRouter

from app.api.v1 import alerts, hosts, instances, metrics, queries, waits_blocking, websocket

api_v1_router = APIRouter()

api_v1_router.include_router(instances.router, prefix="/instances", tags=["instances"])
api_v1_router.include_router(hosts.router, prefix="/hosts", tags=["hosts"])
api_v1_router.include_router(metrics.router, prefix="/metrics", tags=["metrics"])
api_v1_router.include_router(queries.router, prefix="/queries", tags=["queries"])
api_v1_router.include_router(
    waits_blocking.router, prefix="/waits-blocking", tags=["waits-blocking"]
)
api_v1_router.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
api_v1_router.include_router(websocket.router, tags=["websocket"])

# Endpoint de health-check do backend em si (nao confundir com health-check de instancia
# monitorada, que fica em `instances.router`). Este e o unico endpoint deste scaffold que
# responde de verdade (200 OK), servindo de smoke test — Docs/specs/02-backend-contract.md.
health_router = APIRouter()


@health_router.get("/health")
async def health() -> dict[str, str]:
    """Smoke test do backend: sempre 200 OK quando o processo esta de pe."""
    return {"status": "ok"}


api_v1_router.include_router(health_router)
