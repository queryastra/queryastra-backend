"""Canal de live update — `Docs/specs/02-backend-contract.md`, ARCHITECTURE_SPEC.md Q4.

`/ws/live` assina o `app.core.events.event_bus` (singleton in-process) e faz broadcast de
qualquer evento publicado nele a todos os clientes conectados — hoje, eventos de alerta
publicados por `app.workers.alerting_worker` (criacao/atualizacao de `Alert`) e eventos de ciclo
de coleta (`instance_collected`) publicados por `app.workers.polling_worker` ao final de cada
`_run_cycle()`.
"""

from __future__ import annotations

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

from app.core.events import event_bus
from app.core.logging import get_logger

logger = get_logger(__name__)

router = APIRouter()


@router.websocket("/ws/live")
async def live_updates(websocket: WebSocket) -> None:
    """Aceita a conexao e retransmite todo evento publicado no `event_bus` como JSON."""
    await websocket.accept()
    queue = event_bus.subscribe()
    try:
        while True:
            event = await queue.get()
            await websocket.send_json(event)
    except WebSocketDisconnect:
        logger.info("Cliente WebSocket desconectado de /ws/live")
    finally:
        event_bus.unsubscribe(queue)
