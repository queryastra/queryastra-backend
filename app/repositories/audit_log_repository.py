"""Repositório de entradas de auditoria — leitura paginada e inserção."""

from __future__ import annotations

from sqlalchemy import func, select

from app.models.audit_log import AuditLog
from app.repositories.base import BaseRepository


class AuditLogRepository(BaseRepository):
    async def list_paginated(
        self,
        *,
        limit: int = 50,
        offset: int = 0,
        action_filter: str | None = None,
        actor_filter: str | None = None,
    ) -> tuple[list[AuditLog], int]:
        q = select(AuditLog)
        if action_filter:
            q = q.where(AuditLog.action.ilike(f"%{action_filter}%"))
        if actor_filter:
            q = q.where(AuditLog.actor_username.ilike(f"%{actor_filter}%"))
        total: int = (
            await self._session.execute(select(func.count()).select_from(q.subquery()))
        ).scalar_one()
        rows = list(
            (
                await self._session.execute(
                    q.order_by(AuditLog.occurred_at.desc()).offset(offset).limit(limit)
                )
            )
            .scalars()
            .all()
        )
        return rows, total
