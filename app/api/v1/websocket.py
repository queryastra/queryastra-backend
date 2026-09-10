"""Canal de live update — `Docs/specs/02-backend-contract.md`, ARCHITECTURE_SPEC.md Q4.

Neste scaffold o endpoint aceita a conexao WebSocket e responde a um ping com um echo simples,
sem nenhum broadcast real de metrica/alerta ainda. Serve para validar o transporte
(handshake, keep-alive) antes de conectar ao pipeline de coleta/alertas de verdade.
"""

from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.websocket("/ws/live")
async def live_updates(websocket: WebSocket) -> None:
    """Aceita a conexao e ecoa qualquer mensagem recebida com o prefixo `pong:`.

    TODO (nao implementado neste scaffold): assinar o worker de polling/alertas e fazer
    broadcast real de `NormalizedMetricSample`/`Alert` para os clientes conectados.
    """
    await websocket.accept()
    try:
        while True:
            message = await websocket.receive_text()
            await websocket.send_text(f"pong:{message}")
    except WebSocketDisconnect:
        logger.info("Cliente WebSocket desconectado de /ws/live")
