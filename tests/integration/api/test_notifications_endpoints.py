"""Integracao (banco real): endpoints de notificacoes in-app — sempre escopados ao usuario atual."""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user
from app.main import app
from app.models.notification import Notification
from app.models.user import User
from app.repositories.notifications_repository import NotificationsRepository
from app.services.auth_service import hash_password


async def _make_user(db_session: AsyncSession) -> User:
    user = User(
        username=f"notif-{uuid4().hex[:10]}",
        hashed_password=hash_password("x"),
        role="viewer",
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    return user


@pytest.fixture
async def two_users(db_session: AsyncSession) -> AsyncIterator[tuple[User, User]]:
    first, second = await _make_user(db_session), await _make_user(db_session)
    yield first, second
    # notifications cascateiam com o usuario (ondelete CASCADE)
    await db_session.execute(sa.delete(User).where(User.id.in_([first.id, second.id])))
    await db_session.commit()


def _notification(user_id: int, title: str, severity: str = "warning") -> Notification:
    return Notification(
        user_id=user_id, kind="alert_created", severity=severity, title=title, link="/alerts"
    )


async def test_list_count_and_mark_read_are_scoped_to_current_user(
    client: AsyncClient, db_session: AsyncSession, two_users: tuple[User, User]
) -> None:
    me, other = two_users
    repo = NotificationsRepository(db_session)
    mine = await repo.create_many(
        [_notification(me.id, "a"), _notification(me.id, "b", "critical")]
    )
    theirs = await repo.create_many([_notification(other.id, "c")])
    app.dependency_overrides[get_current_user] = lambda: CurrentUser(me.id, me.username, "viewer")

    data = (await client.get("/api/v1/notifications")).json()
    assert data["total"] == 2
    assert data["unread_count"] == 2
    assert {item["title"] for item in data["items"]} == {"a", "b"}

    assert (await client.get("/api/v1/notifications/unread-count")).json() == {"unread_count": 2}

    resp = await client.post(f"/api/v1/notifications/{mine[0].id}/read")
    assert resp.status_code == 200
    assert resp.json()["read_at"] is not None
    # nao consegue marcar a notificacao de outro usuario
    assert (await client.post(f"/api/v1/notifications/{theirs[0].id}/read")).status_code == 404

    unread = (await client.get("/api/v1/notifications", params={"unread_only": True})).json()
    assert [item["title"] for item in unread["items"]] == ["b"]

    resp = await client.post("/api/v1/notifications/read-all")
    assert resp.json() == {"updated": 1}
    assert (await client.get("/api/v1/notifications/unread-count")).json() == {"unread_count": 0}

    page = (await client.get("/api/v1/notifications", params={"limit": 1, "offset": 1})).json()
    assert page["total"] == 2
    assert len(page["items"]) == 1


async def test_retention_deletes_only_old_rows(
    db_session: AsyncSession, two_users: tuple[User, User]
) -> None:
    me, _ = two_users
    repo = NotificationsRepository(db_session)
    old, recent = await repo.create_many([_notification(me.id, "old"), _notification(me.id, "new")])
    old.created_at = datetime.now(UTC) - timedelta(days=45)
    await db_session.commit()

    await repo.delete_older_than(datetime.now(UTC) - timedelta(days=30))

    remaining = (
        await db_session.execute(
            sa.select(Notification.title).where(Notification.user_id == me.id)
        )
    ).scalars().all()
    assert remaining == ["new"]
    assert recent.id is not None
