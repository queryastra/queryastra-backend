"""Integracao (banco real): times, resolucao de acesso por time e e-mail de usuario.

O `conftest.py` deste pacote injeta um admin stub em `get_current_user`; testes que precisam agir
como viewer trocam o override localmente (`_act_as`).
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from uuid import uuid4

import pytest
import sqlalchemy as sa
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user
from app.main import app
from app.models.instance import Instance
from app.models.team import Team
from app.models.user import User
from app.repositories.user_instance_access_repository import UserInstanceAccessRepository
from app.services.auth_service import hash_password
from tests.integration.api.conftest import _stub_admin


@pytest.fixture
async def viewer_user(db_session: AsyncSession) -> AsyncIterator[User]:
    user = User(
        username=f"viewer-{uuid4().hex[:10]}",
        hashed_password=hash_password("irrelevant-pw"),
        role="viewer",
        is_active=True,
    )
    db_session.add(user)
    await db_session.commit()
    await db_session.refresh(user)
    yield user
    await db_session.execute(sa.delete(User).where(User.id == user.id))
    await db_session.commit()


@pytest.fixture
async def cleanup_teams(db_session: AsyncSession) -> AsyncIterator[list[int]]:
    created: list[int] = []
    yield created
    if created:
        await db_session.execute(sa.delete(Team).where(Team.id.in_(created)))
        await db_session.commit()


def _act_as(user: User) -> None:
    app.dependency_overrides[get_current_user] = lambda: CurrentUser(
        user_id=user.id, username=user.username, role=user.role
    )


async def test_team_grants_resolve_instance_access(
    client: AsyncClient,
    db_session: AsyncSession,
    viewer_user: User,
    test_instance: Instance,
    cleanup_teams: list[int],
) -> None:
    access = UserInstanceAccessRepository(db_session)
    resp = await client.post(
        "/api/v1/auth/teams", json={"name": f"dba-{uuid4().hex[:8]}", "description": "DBAs"}
    )
    assert resp.status_code == 201, resp.text
    team_id = resp.json()["id"]
    cleanup_teams.append(team_id)

    resp = await client.post(f"/api/v1/auth/teams/{team_id}/members/{viewer_user.id}")
    assert resp.status_code == 200
    assert resp.json()["member_ids"] == [viewer_user.id]
    assert not await access.has_access(viewer_user.id, test_instance.id)

    resp = await client.post(f"/api/v1/auth/teams/{team_id}/instances/{test_instance.id}")
    assert resp.status_code == 200
    assert resp.json()["instance_ids"] == [str(test_instance.id)]

    assert await access.has_access(viewer_user.id, test_instance.id)
    assert test_instance.id in await access.get_instance_ids_for_user(viewer_user.id)
    assert viewer_user.id in await access.list_user_ids_with_access(test_instance.id)
    # direto continua so com concessoes diretas (tela de usuarios)
    assert await access.list_granted_instances(viewer_user.id) == []

    users = (await client.get("/api/v1/auth/users")).json()
    viewer_row = next(u for u in users if u["id"] == viewer_user.id)
    assert [t["id"] for t in viewer_row["teams"]] == [team_id]

    # Como viewer: a instancia aparece em GET /instances e endpoints com check_instance_access
    _act_as(viewer_user)
    listed = (await client.get("/api/v1/instances")).json()
    assert str(test_instance.id) in [i["id"] for i in listed]
    resp = await client.get(f"/api/v1/backup-status/{test_instance.id}")
    assert resp.status_code != 403

    # Remover o membro revoga o acesso
    app.dependency_overrides[get_current_user] = _stub_admin
    resp = await client.delete(f"/api/v1/auth/teams/{team_id}/members/{viewer_user.id}")
    assert resp.status_code == 204
    assert not await access.has_access(viewer_user.id, test_instance.id)

    resp = await client.delete(f"/api/v1/auth/teams/{team_id}/instances/{test_instance.id}")
    assert resp.status_code == 204
    resp = await client.delete(f"/api/v1/auth/teams/{team_id}")
    assert resp.status_code == 204
    cleanup_teams.remove(team_id)


async def test_viewer_without_grant_is_denied(
    client: AsyncClient, viewer_user: User, test_instance: Instance
) -> None:
    _act_as(viewer_user)
    resp = await client.get(f"/api/v1/backup-status/{test_instance.id}")
    assert resp.status_code == 403


async def test_duplicate_team_name_conflicts(
    client: AsyncClient, cleanup_teams: list[int]
) -> None:
    name = f"team-{uuid4().hex[:8]}"
    first = await client.post("/api/v1/auth/teams", json={"name": name})
    assert first.status_code == 201
    cleanup_teams.append(first.json()["id"])
    second = await client.post("/api/v1/auth/teams", json={"name": name.upper()})
    assert second.status_code == 409


async def test_teams_endpoints_require_admin(client: AsyncClient, viewer_user: User) -> None:
    _act_as(viewer_user)
    assert (await client.get("/api/v1/auth/teams")).status_code == 403
    assert (await client.post("/api/v1/auth/teams", json={"name": "x"})).status_code == 403


async def test_user_email_admin_patch_and_self_profile(
    client: AsyncClient, db_session: AsyncSession, viewer_user: User
) -> None:
    email = f"{uuid4().hex[:8]}@example.com"
    resp = await client.patch(f"/api/v1/auth/users/{viewer_user.id}", json={"email": email})
    assert resp.status_code == 200, resp.text
    assert resp.json()["email"] == email

    resp = await client.patch(f"/api/v1/auth/users/{viewer_user.id}", json={"email": "invalid"})
    assert resp.status_code == 422

    other = User(
        username=f"other-{uuid4().hex[:8]}",
        hashed_password=hash_password("x"),
        role="viewer",
        is_active=True,
    )
    db_session.add(other)
    await db_session.commit()
    try:
        resp = await client.patch(f"/api/v1/auth/users/{other.id}", json={"email": email.upper()})
        assert resp.status_code == 409

        _act_as(viewer_user)
        new_email = f"{uuid4().hex[:8]}@example.com"
        resp = await client.put("/api/v1/auth/me/profile", json={"email": new_email})
        assert resp.status_code == 200
        assert resp.json()["email"] == new_email
        me = (await client.get("/api/v1/auth/me")).json()
        assert me["email"] == new_email

        resp = await client.put("/api/v1/auth/me/profile", json={"email": ""})
        assert resp.status_code == 200
        assert resp.json()["email"] is None
    finally:
        await db_session.execute(sa.delete(User).where(User.id == other.id))
        await db_session.commit()
