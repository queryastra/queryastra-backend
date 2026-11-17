"""Notificacoes in-app do usuario autenticado (sino do TopNav).

Todos os endpoints operam SO sobre as notificacoes do proprio usuario (`user_id` do JWT) —
nao ha visao administrativa das notificacoes de terceiros.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_db_session
from app.models.notification import Notification
from app.repositories.notifications_repository import NotificationsRepository

router = APIRouter()


class NotificationResponse(BaseModel):
    id: int
    kind: str
    severity: str
    title: str
    body: str | None
    link: str | None
    instance_id: UUID | None
    alert_id: UUID | None
    created_at: datetime
    read_at: datetime | None


class NotificationListResponse(BaseModel):
    items: list[NotificationResponse]
    total: int
    unread_count: int


class UnreadCountResponse(BaseModel):
    unread_count: int


class MarkAllReadResponse(BaseModel):
    updated: int


def get_notifications_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> NotificationsRepository:
    return NotificationsRepository(session)


def _to_response(n: Notification) -> NotificationResponse:
    return NotificationResponse(
        id=n.id,
        kind=n.kind,
        severity=n.severity,
        title=n.title,
        body=n.body,
        link=n.link,
        instance_id=n.instance_id,
        alert_id=n.alert_id,
        created_at=n.created_at,
        read_at=n.read_at,
    )


@router.get("", response_model=NotificationListResponse)
async def list_notifications(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    repository: Annotated[NotificationsRepository, Depends(get_notifications_repository)],
    unread_only: bool = False,
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=100)] = 20,
) -> NotificationListResponse:
    items, total = await repository.list_for_user(
        current_user.user_id, unread_only=unread_only, offset=offset, limit=limit
    )
    unread = await repository.count_unread(current_user.user_id)
    return NotificationListResponse(
        items=[_to_response(n) for n in items], total=total, unread_count=unread
    )


@router.get("/unread-count", response_model=UnreadCountResponse)
async def unread_count(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    repository: Annotated[NotificationsRepository, Depends(get_notifications_repository)],
) -> UnreadCountResponse:
    return UnreadCountResponse(unread_count=await repository.count_unread(current_user.user_id))


@router.post("/read-all", response_model=MarkAllReadResponse)
async def mark_all_read(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    repository: Annotated[NotificationsRepository, Depends(get_notifications_repository)],
) -> MarkAllReadResponse:
    updated = await repository.mark_all_read(current_user.user_id, datetime.now(UTC))
    return MarkAllReadResponse(updated=updated)


@router.post("/{notification_id}/read", response_model=NotificationResponse)
async def mark_read(
    notification_id: int,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    repository: Annotated[NotificationsRepository, Depends(get_notifications_repository)],
) -> NotificationResponse:
    notification = await repository.mark_read(
        current_user.user_id, notification_id, datetime.now(UTC)
    )
    if notification is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Notificacao nao encontrada."
        )
    return _to_response(notification)
