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
    "/api/v1/instances/test-connection",
    "/api/v1/instances/{instance_id}/health-check",
    "/api/v1/metrics/{target_type}/{target_id}",
    "/api/v1/queries/active/{instance_id}",
    "/api/v1/queries/top-sql/{instance_id}",
    "/api/v1/waits-blocking/blocking/{instance_id}",
    "/api/v1/waits-blocking/waits/{instance_id}",
    "/api/v1/alerts/rules",
    "/api/v1/alerts",
    "/api/v1/alerts/{alert_id}/acknowledge",
    "/api/v1/alerts/silences",
    "/api/v1/alerts/silences/{silence_id}",
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
    with TestClient(app).websocket_connect(EXPECTED_WEBSOCKET_PATH) as websocket:
        websocket.send_text("hello")
        assert websocket.receive_text() == "pong:hello"
