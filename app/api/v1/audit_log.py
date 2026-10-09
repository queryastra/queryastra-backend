"""GET /api/v1/audit-log — log de auditoria do sistema (admin only, paginado, filtrável)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_db_session, require_admin
from app.repositories.audit_log_repository import AuditLogRepository

router = APIRouter()


class AuditEntryResponse(BaseModel):
    id: int
    occurred_at: datetime
    actor_username: str
    action: str
    target_type: str | None
    target_id: str | None
    target_name: str | None
    ip_address: str | None
    detail: dict | None


class AuditLogPage(BaseModel):
    items: list[AuditEntryResponse]
    total: int
    offset: int
    limit: int


@router.get("", response_model=AuditLogPage)
async def list_audit_log(
    _: Annotated[CurrentUser, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=500)] = 50,
    action: Annotated[str | None, Query()] = None,
    actor: Annotated[str | None, Query()] = None,
) -> AuditLogPage:
    """Lista entradas do log de auditoria, mais recentes primeiro. Requer papel admin."""
    repo = AuditLogRepository(session)
    rows, total = await repo.list_paginated(
        limit=limit, offset=offset, action_filter=action, actor_filter=actor
    )
    return AuditLogPage(
        items=[
            AuditEntryResponse(
                id=r.id,
                occurred_at=r.occurred_at,
                actor_username=r.actor_username,
                action=r.action,
                target_type=r.target_type,
                target_id=r.target_id,
                target_name=r.target_name,
                ip_address=r.ip_address,
                detail=r.detail,
            )
            for r in rows
        ],
        total=total,
        offset=offset,
        limit=limit,
    )
