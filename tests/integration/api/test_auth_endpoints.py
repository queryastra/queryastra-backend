"""Testes de integracao dos endpoints de autenticacao (/api/v1/auth/*) contra o banco real.

Usa o banco `ab_xql_pm_dev` (mesma `DATABASE_URL` de `.env`). O usuario admin seed e criado
pelo lifespan do backend — estes testes o reutilizam diretamente.

IMPORTANTE: este modulo usa autenticacao REAL (sem o override do conftest.py). A fixture
`no_auth_override` abaixo remove o override antes de cada teste deste modulo, permitindo que
os endpoints validem tokens de verdade.
"""

from __future__ import annotations

import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.config import get_settings
from app.main import app
from app.repositories.users_repository import UsersRepository
from app.services.auth_service import create_access_token, hash_password

settings = get_settings()


@pytest.fixture(autouse=True)
def no_auth_override() -> None:
    """Remove o override de auth do conftest para que estes testes usem autenticacao real."""
    app.dependency_overrides.pop(get_current_user, None)
    yield
    # conftest.override_auth cuidara da limpeza ao final do teste


@pytest.fixture(autouse=True)
async def ensure_admin_user(db_session: AsyncSession) -> None:
    """Garante que o usuario admin seed existe no banco antes dos testes de login."""
    repo = UsersRepository(db_session)
    user = await repo.get_by_username(settings.admin_username)
    if user is None:
        hashed = hash_password(settings.admin_password)
        await repo.create(settings.admin_username, hashed, "admin")
        await db_session.commit()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _admin_token() -> str:
    """Cria um token JWT valido com papel admin (sem bater no banco)."""
    return create_access_token(user_id=1, username=settings.admin_username, role="admin")


def _viewer_token() -> str:
    """Cria um token JWT valido com papel viewer (sem bater no banco)."""
    return create_access_token(user_id=999, username="viewer_test", role="viewer")


def _auth_headers(token: str) -> dict:
    return {"Authorization": f"Bearer {token}"}


# ---------------------------------------------------------------------------
# Testes
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_login_success(client: AsyncClient) -> None:
    """POST /auth/login com credenciais do seed → 200 com access_token."""
    resp = await client.post(
        "/api/v1/auth/login",
        json={"username": settings.admin_username, "password": settings.admin_password},
    )
    assert resp.status_code == 200
    data = resp.json()
    assert "access_token" in data
    assert data["token_type"] == "bearer"
    assert data["role"] == "admin"
    assert data["username"] == settings.admin_username


@pytest.mark.asyncio
async def test_login_wrong_password(client: AsyncClient) -> None:
    """POST /auth/login com senha errada → 401."""
    resp = await client.post(
        "/api/v1/auth/login",
        json={"username": settings.admin_username, "password": "senha_errada"},
    )
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_me_authenticated(client: AsyncClient) -> None:
    """GET /auth/me com Bearer token valido → 200 com username/role."""
    token = _admin_token()
    resp = await client.get("/api/v1/auth/me", headers=_auth_headers(token))
    assert resp.status_code == 200
    data = resp.json()
    assert data["username"] == settings.admin_username
    assert data["role"] == "admin"


@pytest.mark.asyncio
async def test_me_unauthenticated(client: AsyncClient) -> None:
    """GET /auth/me sem token → 401."""
    resp = await client.get("/api/v1/auth/me")
    assert resp.status_code == 401


@pytest.mark.asyncio
async def test_admin_can_list_users(client: AsyncClient) -> None:
    """GET /auth/users com token admin → 200 com lista de usuarios."""
    token = _admin_token()
    resp = await client.get("/api/v1/auth/users", headers=_auth_headers(token))
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    # deve conter pelo menos o admin seed
    usernames = [u["username"] for u in data]
    assert settings.admin_username in usernames


@pytest.mark.asyncio
async def test_viewer_cannot_access_settings_mutation(client: AsyncClient) -> None:
    """PATCH /settings com token viewer → 403."""
    token = _viewer_token()
    resp = await client.patch(
        "/api/v1/settings",
        json={"blocking_min_duration_ms": 1000},
        headers=_auth_headers(token),
    )
    assert resp.status_code == 403
