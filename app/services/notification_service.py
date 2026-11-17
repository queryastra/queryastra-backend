"""Notificacoes in-app: assinante do `event_bus` que transforma eventos de alerta em linhas de
`notifications` (uma por usuario com acesso a instancia) e publica `notification_created`.

Regras (`should_notify`):
- `alert_created` (alerta NOVO, warning ou critical) sempre notifica;
- `alert_updated` notifica so quando o alerta passa a/esta em `critical` por mudanca de nivel
  (`level_changed`) ou por um nivel de escalonamento (`escalated`) — lembretes, recuperacoes e
  repeticoes nao geram notificacao in-app (ja chegam pelos canais e-mail/webhook).

Destinatarios: `UserInstanceAccessRepository.list_user_ids_with_access` (admins ativos +
concessao direta + times). O evento `notification_created` carrega `user_id` — o `/ws/live` so o
entrega a esse usuario.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.events import EventBus, event_bus
from app.core.logging import get_logger
from app.models.notification import Notification
from app.repositories.instances_repository import InstancesRepository
from app.repositories.notifications_repository import NotificationsRepository
from app.repositories.user_instance_access_repository import UserInstanceAccessRepository

logger = get_logger(__name__)

ALERTS_LINK = "/alerts"
_ESCALATION_KINDS = frozenset({"level_changed", "escalated"})


def should_notify(event: dict) -> bool:
    if not event.get("instance_id"):
        return False
    severity = event.get("severity")
    if event.get("type") == "alert_created":
        return severity in ("warning", "critical")
    if event.get("type") == "alert_updated":
        return severity == "critical" and event.get("kind") in _ESCALATION_KINDS
    return False


def build_notification_content(event: dict, instance_name: str | None) -> tuple[str, str, str]:
    """`(kind, title, body)` — texto neutro (o frontend traduz pelo `kind`/`severity`; o titulo
    leva o nome da regra e da instancia, que nao sao traduziveis)."""
    rule_name = event.get("rule_name") or "Alerta"
    target = instance_name or event.get("instance_id") or ""
    severity = str(event.get("severity") or "warning")
    if event.get("type") == "alert_created":
        kind = "alert_created"
        body = f"Novo alerta {severity.upper()} em {target}."
    else:
        kind = "alert_escalated"
        body = f"Alerta escalado para {severity.upper()} em {target}."
    title = f"{rule_name} — {target}" if target else rule_name
    return kind, title, body


def serialize_notification(notification: Notification) -> dict:
    return {
        "id": notification.id,
        "kind": notification.kind,
        "severity": notification.severity,
        "title": notification.title,
        "body": notification.body,
        "link": notification.link,
        "instance_id": str(notification.instance_id) if notification.instance_id else None,
        "alert_id": str(notification.alert_id) if notification.alert_id else None,
        "created_at": _iso(notification.created_at),
        "read_at": _iso(notification.read_at),
    }


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value is not None else None


class NotificationService:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        bus: EventBus = event_bus,
        access_repository_factory: Callable[
            [AsyncSession], UserInstanceAccessRepository
        ] = UserInstanceAccessRepository,
        notifications_repository_factory: Callable[
            [AsyncSession], NotificationsRepository
        ] = NotificationsRepository,
        instances_repository_factory: Callable[
            [AsyncSession], InstancesRepository
        ] = InstancesRepository,
    ) -> None:
        self._session_factory = session_factory
        self._bus = bus
        self._access_repository_factory = access_repository_factory
        self._notifications_repository_factory = notifications_repository_factory
        self._instances_repository_factory = instances_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        queue = self._bus.subscribe()
        logger.info("NotificationService iniciado")
        try:
            while not self._stop_event.is_set():
                get_task = asyncio.ensure_future(queue.get())
                stop_task = asyncio.ensure_future(self._stop_event.wait())
                done, pending = await asyncio.wait(
                    {get_task, stop_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for task in pending:
                    task.cancel()
                if get_task not in done:
                    break
                try:
                    await self.handle_event(get_task.result())
                except Exception:  # noqa: BLE001 - um evento ruim nunca derruba o assinante
                    logger.exception("Falha ao gerar notificacoes para evento")
        finally:
            self._bus.unsubscribe(queue)
            logger.info("NotificationService finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def handle_event(self, event: dict) -> list[Notification]:
        if not should_notify(event):
            return []
        instance_id = UUID(str(event["instance_id"]))
        alert_id_raw = event.get("alert_id")
        async with self._session_factory() as session:
            user_ids = await self._access_repository_factory(session).list_user_ids_with_access(
                instance_id
            )
            if not user_ids:
                return []
            instance = await self._instances_repository_factory(session).get_by_id(instance_id)
            kind, title, body = build_notification_content(
                event, instance.name if instance is not None else None
            )
            rows = [
                Notification(
                    user_id=user_id,
                    kind=kind,
                    severity=str(event.get("severity")),
                    title=title,
                    body=body,
                    link=ALERTS_LINK,
                    instance_id=instance_id,
                    alert_id=UUID(str(alert_id_raw)) if alert_id_raw else None,
                )
                for user_id in sorted(user_ids)
            ]
            created = await self._notifications_repository_factory(session).create_many(rows)
        for notification in created:
            self._bus.publish(
                {
                    "type": "notification_created",
                    "user_id": notification.user_id,
                    "notification": serialize_notification(notification),
                }
            )
        return created
