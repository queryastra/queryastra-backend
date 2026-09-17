"""Esqueleto de teste de contrato — `Docs/specs/02-backend-contract.md`:

"Contratos publicos devem ter schema versionado e teste de compatibilidade. Toda alteracao de
endpoint exige atualizar a spec e o frontend consumidor."

Valida que a app expoe exatamente os paths REST esperados sob `/api/v1` (via schema OpenAPI,
robusto a mudanca de detalhe interno de roteamento) mais o endpoint WebSocket de live update.
Quando um endpoint novo for adicionado ou um path mudar, este teste deve ser atualizado junto
com `Docs/specs/02-backend-contract.md` e o frontend consumidor — nao e so um detalhe de
implementacao.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app

EXPECTED_REST_PATHS = {
    "/api/v1/health",
    "/api/v1/instances",
    "/api/v1/instances/{instance_id}",
    "/api/v1/instances/{instance_id}/active",
    "/api/v1/instances/{instance_id}/settings",
    "/api/v1/instances/test-connection",
    "/api/v1/instances/{instance_id}/health-check",
    "/api/v1/hosts/{hostname}/metrics",
    "/api/v1/hosts",
    "/api/v1/metrics/{target_type}/{target_id}",
    "/api/v1/queries/active/{instance_id}",
    "/api/v1/queries/top-sql/{instance_id}",
    "/api/v1/queries/top-sql-by-wait-type/{instance_id}",
    "/api/v1/queries/databases/{instance_id}",
    "/api/v1/queries/detail/{instance_id}/{sql_hash}",
    "/api/v1/queries/detail/{instance_id}/{sql_hash}/stats",
    "/api/v1/queries/detail/{instance_id}/{sql_hash}/sql-insight",
    "/api/v1/queries/detail/{instance_id}/{sql_hash}/execution-plan",
    "/api/v1/queries/databases-by-day/{instance_id}",
    "/api/v1/queries/top-sql-by-day/{instance_id}",
    "/api/v1/queries/databases-by-hour/{instance_id}",
    "/api/v1/queries/top-sql-by-hour/{instance_id}",
    "/api/v1/queries/wait-types/{instance_id}",
    "/api/v1/queries/wait-types-by-day/{instance_id}",
    "/api/v1/queries/wait-types-by-hour/{instance_id}",
    "/api/v1/queries/users/{instance_id}",
    "/api/v1/queries/users-by-day/{instance_id}",
    "/api/v1/queries/users-by-hour/{instance_id}",
    "/api/v1/waits-blocking/blocking/{instance_id}",
    "/api/v1/waits-blocking/blocking/{instance_id}/incidents",
    "/api/v1/waits-blocking/blocking/{instance_id}/session/{session_id}",
    "/api/v1/waits-blocking/waits/{instance_id}",
    "/api/v1/alerts/rules",
    "/api/v1/alerts",
    "/api/v1/alerts/{alert_id}/acknowledge",
    "/api/v1/alerts/{alert_id}/close",
    "/api/v1/alerts/silences",
    "/api/v1/alerts/silences/{silence_id}",
    "/api/v1/missing-index-suggestions/{instance_id}",
    "/api/v1/backup-status/{instance_id}",
    "/api/v1/transactions/{instance_id}",
    "/api/v1/transactions/{instance_id}/{transaction_identifier}",
    "/api/v1/deadlocks/{instance_id}",
    "/api/v1/deadlocks/{instance_id}/{deadlock_id}",
}

EXPECTED_WEBSOCKET_PATH = "/api/v1/ws/live"


def test_openapi_schema_exposes_expected_rest_paths() -> None:
    schema = app.openapi()
    assert set(schema["paths"].keys()) == EXPECTED_REST_PATHS


def test_websocket_route_is_registered() -> None:
    # Rotas WebSocket nao aparecem no schema OpenAPI, e a versao instalada do FastAPI resolve
    # inclusao de router de forma preguicosa (introspeccao estatica de `app.router.routes` nao
    # reflete rotas incluidas via `include_router`) — a forma robusta de confirmar que o path
    # esta registrado e abrir a conexao de verdade contra o `TestClient`.
    #
    # `/ws/live` nao ecoa mais (`app/api/v1/websocket.py`): assina `app.core.events.event_bus` e
    # retransmite qualquer evento publicado la. Nao publicamos daqui: o `TestClient` roda o app
    # numa thread/loop propria (portal do `anyio`), diferente da thread deste teste sincrono —
    # chamar `event_bus.publish` (uma `asyncio.Queue` de outro loop) de fora dessa thread nao e
    # thread-safe. Abrir e fechar a conexao ja e prova suficiente de que a rota esta registrada
    # e aceita o handshake; o broadcast de verdade (mesma thread/loop do worker) e coberto pelo
    # smoke test manual do backend.
    with TestClient(app).websocket_connect(EXPECTED_WEBSOCKET_PATH):
        pass
