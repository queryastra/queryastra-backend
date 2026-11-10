"""Agregador de sub-routers sob `/api/v1` — `Docs/specs/02-backend-contract.md`.

"Contratos publicos devem ter schema versionado e teste de compatibilidade." Este router e o
unico lugar que registra os sub-routers; `tests/integration/api/test_v1_contract_snapshot.py`
valida os paths expostos aqui contra o esperado.
"""

from __future__ import annotations

from fastapi import APIRouter

from app.api.v1 import (
    ai_deep_analysis,
    ai_keys,
    alerts,
    anomalies,
    audit_log,
    auth,
    backup_status,
    capacity,
    db_maintenance,
    deadlocks,
    health_stats,
    hosts,
    instances,
    license,
    metrics,
    missing_index,
    queries,
    reports,
    settings,
    status,
    topology,
    transactions,
    update,
    vacuum,
    waits_blocking,
    websocket,
)

api_v1_router = APIRouter()

api_v1_router.include_router(auth.router, prefix="/auth", tags=["auth"])
api_v1_router.include_router(license.router, prefix="/license", tags=["license"])
api_v1_router.include_router(audit_log.router, prefix="/audit-log", tags=["audit-log"])
api_v1_router.include_router(instances.router, prefix="/instances", tags=["instances"])
api_v1_router.include_router(hosts.router, prefix="/hosts", tags=["hosts"])
api_v1_router.include_router(metrics.router, prefix="/metrics", tags=["metrics"])
api_v1_router.include_router(queries.router, prefix="/queries", tags=["queries"])
api_v1_router.include_router(
    waits_blocking.router, prefix="/waits-blocking", tags=["waits-blocking"]
)
api_v1_router.include_router(alerts.router, prefix="/alerts", tags=["alerts"])
api_v1_router.include_router(
    missing_index.router, prefix="/missing-index-suggestions", tags=["missing-index"]
)
api_v1_router.include_router(backup_status.router, prefix="/backup-status", tags=["backup-status"])
api_v1_router.include_router(transactions.router, prefix="/transactions", tags=["transactions"])
api_v1_router.include_router(deadlocks.router, prefix="/deadlocks", tags=["deadlocks"])
api_v1_router.include_router(settings.router, prefix="/settings", tags=["settings"])
api_v1_router.include_router(ai_keys.router, prefix="/ai-keys", tags=["ai-keys"])
api_v1_router.include_router(topology.router, prefix="/topology", tags=["topology"])
api_v1_router.include_router(capacity.router, prefix="/capacity", tags=["capacity"])
api_v1_router.include_router(reports.router, prefix="/reports", tags=["reports"])
api_v1_router.include_router(status.router, prefix="/status", tags=["status"])
api_v1_router.include_router(update.router, prefix="/update", tags=["update"])
api_v1_router.include_router(vacuum.router, prefix="/instances", tags=["vacuum"])
api_v1_router.include_router(health_stats.router, prefix="/instances", tags=["health-stats"])
api_v1_router.include_router(
    db_maintenance.router, prefix="/instances", tags=["db-maintenance"]
)
api_v1_router.include_router(ai_deep_analysis.router, prefix="/instances", tags=["ai-deep-analysis"])
api_v1_router.include_router(anomalies.instance_router, prefix="/instances", tags=["anomalies"])
api_v1_router.include_router(anomalies.router, prefix="/anomalies", tags=["anomalies"])
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
