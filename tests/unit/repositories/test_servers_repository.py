"""Testes de `ServersRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.server import Server
from app.repositories.servers_repository import ServersRepository


async def test_get_or_create_by_hostname_does_not_duplicate(db_session: AsyncSession) -> None:
    repo = ServersRepository(db_session)
    hostname = f"test-host-{uuid4()}"

    try:
        first = await repo.get_or_create_by_hostname(hostname, "linux")
        second = await repo.get_or_create_by_hostname(hostname, "linux")

        assert first.id == second.id
        assert first.hostname == hostname
        assert first.os_type == "linux"
        assert first.active is True

        rows = (
            (await db_session.execute(sa.select(Server).where(Server.hostname == hostname)))
            .scalars()
            .all()
        )
        assert len(rows) == 1
    finally:
        await db_session.execute(sa.delete(Server).where(Server.hostname == hostname))
        await db_session.commit()


async def test_get_by_hostname_returns_none_when_absent(db_session: AsyncSession) -> None:
    repo = ServersRepository(db_session)
    assert await repo.get_by_hostname(f"nao-existe-{uuid4()}") is None


async def test_list_active_includes_created_server(db_session: AsyncSession) -> None:
    repo = ServersRepository(db_session)
    hostname = f"test-host-{uuid4()}"

    try:
        created = await repo.get_or_create_by_hostname(hostname, "windows")

        active = await repo.list_active()

        assert any(server.id == created.id for server in active)
    finally:
        await db_session.execute(sa.delete(Server).where(Server.hostname == hostname))
        await db_session.commit()
