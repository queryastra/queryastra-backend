"""Canal de live update — `Docs/specs/02-backend-contract.md`, ARCHITECTURE_SPEC.md Q4.

`/ws/live` assina o `app.core.events.event_bus` (singleton in-process) e retransmite os eventos
publicados nele — alertas (`app.workers.alerting_worker`), ciclos de coleta
(`instance_collected`, `app.workers.polling_worker`) e notificacoes in-app
(`notification_created`, `app.services.notification_service`).

Autenticacao: navegadores nao conseguem enviar header `Authorization` no handshake WebSocket,
entao o JWT vem em `?token=`. Token ausente/invalido/expirado, ou usuario inexistente/inativo,
fecha a conexao com 1008 (policy violation) antes de qualquer evento.

Filtro por usuario (`event_visible_to`):
- `notification_created` so vai para o `user_id` do evento;
- eventos com `instance_id` so vao para quem tem acesso a instancia (admin, concessao direta ou
  via time) — o conjunto acessivel e recarregado a cada `_ACCESS_REFRESH_SECONDS`, para que uma
  concessao/revogacao valha sem reconectar;
- demais eventos (sem instancia) vao para todo usuario autenticado.
"""

from __future__ import annotations

import asyncio
import time
from uuid import UUID

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, status
from jose import JWTError

from app.api.deps import CurrentUser
from app.core.events import event_bus
from app.core.logging import get_logger
from app.db.session import async_session_factory
from app.repositories.user_instance_access_repository import UserInstanceAccessRepository
from app.repositories.users_repository import UsersRepository
from app.services.auth_service import decode_token

logger = get_logger(__name__)

router = APIRouter()

_ACCESS_REFRESH_SECONDS = 30.0

async def load_active_user(user_id: int) -> CurrentUser | None:
    """Usuario ATIVO atual do banco (papel do banco, nao o do token — rebaixar um admin vale ja na
    reconexao). Funcao de modulo para os testes poderem substitui-la (monkeypatch)."""
    async with async_session_factory() as session:
        user = await UsersRepository(session).get_by_id(user_id)
    if user is None or not user.is_active:
        return None
    return CurrentUser(user_id=user.id, username=user.username, role=user.role)


async def load_accessible_instance_ids(user_id: int) -> set[UUID]:
    async with async_session_factory() as session:
        return await UserInstanceAccessRepository(session).get_instance_ids_for_user(user_id)


def event_visible_to(
    event: dict, user: CurrentUser, accessible_instance_ids: set[UUID] | None
) -> bool:
    """`accessible_instance_ids` so e consultado para nao-admins (admins enxergam tudo); `None`
    para um nao-admin nega eventos de instancia (fail-closed)."""
    if event.get("type") == "notification_created":
        return event.get("user_id") == user.user_id
    instance_id_raw = event.get("instance_id")
    if not instance_id_raw or user.is_admin:
        return True
    if accessible_instance_ids is None:
        return False
    try:
        instance_id = UUID(str(instance_id_raw))
    except ValueError:
        return False
    return instance_id in accessible_instance_ids


def _user_from_token(token: str | None) -> CurrentUser | None:
    if not token:
        return None
    try:
        payload = decode_token(token)
        return CurrentUser(
            user_id=int(payload["sub"]), username=payload["username"], role=payload["role"]
        )
    except (JWTError, KeyError, TypeError, ValueError):
        return None


class _AccessCache:
    def __init__(self, user: CurrentUser) -> None:
        self._user = user
        self._ids: set[UUID] | None = None
        self._loaded_at = 0.0

    async def get(self) -> set[UUID] | None:
        if self._user.is_admin:
            return None
        now = time.monotonic()
        if self._ids is None or now - self._loaded_at >= _ACCESS_REFRESH_SECONDS:
            self._ids = await load_accessible_instance_ids(self._user.user_id)
            self._loaded_at = now
        return self._ids


@router.websocket("/ws/live")
async def live_updates(websocket: WebSocket) -> None:
    """Autentica via `?token=`, depois retransmite os eventos visiveis ao usuario como JSON."""
    user = _user_from_token(websocket.query_params.get("token"))
    if user is None:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return
    active_user = await load_active_user(user.user_id)
    if active_user is None:
        await websocket.close(code=status.WS_1008_POLICY_VIOLATION)
        return

    await websocket.accept()
    access = _AccessCache(active_user)
    queue = event_bus.subscribe()

    async def forward_events() -> None:
        while True:
            event = await queue.get()
            needs_access = (
                event.get("type") != "notification_created" and bool(event.get("instance_id"))
            )
            accessible = await access.get() if needs_access else None
            if event_visible_to(event, active_user, accessible):
                await websocket.send_json(event)

    async def wait_for_disconnect() -> None:
        # Sem ler o socket, o handler nunca ve o close do cliente nem o do shutdown do uvicorn
        # (que espera os WebSockets terminarem) — o reload/parada travaria com uma aba aberta.
        while (await websocket.receive())["type"] != "websocket.disconnect":
            pass

    tasks = {asyncio.create_task(forward_events()), asyncio.create_task(wait_for_disconnect())}
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        for task in done:
            exc = task.exception()
            if exc is not None and not isinstance(exc, WebSocketDisconnect):
                raise exc
        logger.info("Cliente WebSocket desconectado de /ws/live")
    finally:
        for task in tasks:
            task.cancel()
        event_bus.unsubscribe(queue)
