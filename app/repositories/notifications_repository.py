"""Repositorio de `notifications` (notificacoes in-app por usuario)."""

from __future__ import annotations

from datetime import datetime

import sqlalchemy as sa
from sqlalchemy import select

from app.models.notification import Notification
from app.repositories.base import BaseRepository


class NotificationsRepository(BaseRepository):
    async def create_many(self, notifications: list[Notification]) -> list[Notification]:
        """Insere e commita; devolve as linhas com `id`/`created_at` preenchidos."""
        if not notifications:
            return []
        self._session.add_all(notifications)
        await self._session.commit()
        for notification in notifications:
            await self._session.refresh(notification)
        return notifications

    async def list_for_user(
        self, user_id: int, *, unread_only: bool, offset: int, limit: int
    ) -> tuple[list[Notification], int]:
        """Pagina mais recentes primeiro; devolve `(itens, total)` do filtro."""
        conditions = [Notification.user_id == user_id]
        if unread_only:
            conditions.append(Notification.read_at.is_(None))
        total = (
            await self._session.execute(select(sa.func.count()).where(*conditions))
        ).scalar_one()
        rows = await self._session.execute(
            select(Notification)
            .where(*conditions)
            .order_by(Notification.created_at.desc(), Notification.id.desc())
            .offset(offset)
            .limit(limit)
        )
        return list(rows.scalars().all()), int(total)

    async def count_unread(self, user_id: int) -> int:
        result = await self._session.execute(
            select(sa.func.count()).where(
                Notification.user_id == user_id, Notification.read_at.is_(None)
            )
        )
        return int(result.scalar_one())

    async def mark_read(
        self, user_id: int, notification_id: int, now: datetime
    ) -> Notification | None:
        """Marca uma notificacao do PROPRIO usuario como lida (idempotente). `None` se nao existe
        ou pertence a outro usuario."""
        notification = await self._session.get(Notification, notification_id)
        if notification is None or notification.user_id != user_id:
            return None
        if notification.read_at is None:
            notification.read_at = now
            await self._session.commit()
            await self._session.refresh(notification)
        return notification

    async def mark_all_read(self, user_id: int, now: datetime) -> int:
        result = await self._session.execute(
            sa.update(Notification)
            .where(Notification.user_id == user_id, Notification.read_at.is_(None))
            .values(read_at=now)
            .returning(Notification.id)
        )
        updated = len(result.all())
        await self._session.commit()
        return updated

    async def delete_older_than(self, cutoff: datetime) -> int:
        result = await self._session.execute(
            sa.delete(Notification)
            .where(Notification.created_at < cutoff)
            .returning(Notification.id)
        )
        deleted = len(result.all())
        await self._session.commit()
        return deleted
