"""Endpoints de autenticacao e gestao de usuarios (RBAC).

- POST /auth/login — login com username/password, retorna JWT
- GET  /auth/me    — dados do usuario autenticado
- GET  /auth/users — lista todos os usuarios (admin only)
- POST /auth/users — cria novo usuario (admin only)
- PATCH /auth/users/{user_id} — muda role ou desativa (admin only)
- GET  /auth/users/{user_id}/instances — lista instâncias autorizadas (admin only)
- POST /auth/users/{user_id}/instances/{instance_id} — concede acesso (admin only)
- DELETE /auth/users/{user_id}/instances/{instance_id} — revoga acesso (admin only)
- POST /auth/users/{user_id}/reset-token — gera link de redefinicao de senha (admin only)
- PUT  /auth/users/{user_id}/password — troca senha diretamente (admin only, sem senha atual)
- POST /auth/reset-password — redefine senha via token (publico)
- PUT  /auth/me/password — troca senha do usuario autenticado
"""

from __future__ import annotations

import hashlib
import secrets
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_db_session, require_admin
from app.repositories.password_reset_tokens_repository import PasswordResetTokensRepository
from app.repositories.user_instance_access_repository import UserInstanceAccessRepository
from app.repositories.users_repository import UsersRepository
from app.services.auth_service import create_access_token, hash_password, verify_password

_PASSWORD_RESET_TOKEN_TTL_HOURS = 24

router = APIRouter()

_VALID_ROLES = ("admin", "operator", "viewer")


# ---------------------------------------------------------------------------
# Schemas de request/response
# ---------------------------------------------------------------------------


class LoginRequest(BaseModel):
    username: str
    password: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    role: str
    username: str


class MeResponse(BaseModel):
    user_id: int
    username: str
    role: str


class UserResponse(BaseModel):
    id: int
    username: str
    role: str
    is_active: bool
    created_at: datetime


class UserCreateRequest(BaseModel):
    username: str = Field(min_length=1, max_length=64)
    password: str = Field(min_length=1)
    role: str = Field(pattern="^(admin|operator|viewer)$")


class UserPatchRequest(BaseModel):
    """Atualiza role e/ou desativa o usuario. Pelo menos um campo e obrigatorio."""

    role: str | None = Field(default=None, pattern="^(admin|operator|viewer)$")
    is_active: bool | None = None


# ---------------------------------------------------------------------------
# Dependency providers
# ---------------------------------------------------------------------------


def get_users_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> UsersRepository:
    return UsersRepository(session)


def get_user_instance_access_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> UserInstanceAccessRepository:
    return UserInstanceAccessRepository(session)


def get_password_reset_tokens_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> PasswordResetTokensRepository:
    return PasswordResetTokensRepository(session)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post("/login", response_model=LoginResponse)
async def login(
    payload: LoginRequest,
    repository: Annotated[UsersRepository, Depends(get_users_repository)],
) -> LoginResponse:
    """Login com username/password. Retorna JWT Bearer token."""
    user = await repository.get_by_username(payload.username)
    password_ok = (
        user is not None
        and user.is_active
        and verify_password(payload.password, user.hashed_password)
    )
    if not password_ok or user is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais invalidas.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    token = create_access_token(user.id, user.username, user.role)
    return LoginResponse(access_token=token, role=user.role, username=user.username)


@router.get("/me", response_model=MeResponse)
async def me(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
) -> MeResponse:
    """Dados do usuario autenticado."""
    return MeResponse(
        user_id=current_user.user_id,
        username=current_user.username,
        role=current_user.role,
    )


@router.get("/users", response_model=list[UserResponse])
async def list_users(
    _: Annotated[CurrentUser, Depends(require_admin)],
    repository: Annotated[UsersRepository, Depends(get_users_repository)],
) -> list[UserResponse]:
    """Lista todos os usuarios (sem hashed_password). Requer papel admin."""
    users = await repository.list_all()
    return [
        UserResponse(
            id=u.id,
            username=u.username,
            role=u.role,
            is_active=u.is_active,
            created_at=u.created_at,
        )
        for u in users
    ]


@router.post("/users", response_model=UserResponse, status_code=status.HTTP_201_CREATED)
async def create_user(
    payload: UserCreateRequest,
    _: Annotated[CurrentUser, Depends(require_admin)],
    repository: Annotated[UsersRepository, Depends(get_users_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> UserResponse:
    """Cria um novo usuario. Requer papel admin."""
    existing = await repository.get_by_username(payload.username)
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Username '{payload.username}' ja esta em uso.",
        )
    hashed = hash_password(payload.password)
    user = await repository.create(payload.username, hashed, payload.role)
    await session.commit()
    return UserResponse(
        id=user.id,
        username=user.username,
        role=user.role,
        is_active=user.is_active,
        created_at=user.created_at,
    )


@router.patch("/users/{user_id}", response_model=UserResponse)
async def patch_user(
    user_id: int,
    payload: UserPatchRequest,
    _: Annotated[CurrentUser, Depends(require_admin)],
    repository: Annotated[UsersRepository, Depends(get_users_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> UserResponse:
    """Muda o papel e/ou desativa um usuario. Requer papel admin."""
    if payload.role is None and payload.is_active is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Informe ao menos um campo para atualizar (role ou is_active).",
        )
    user = await repository.get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao encontrado.")
    if payload.role is not None:
        user = await repository.update_role(user_id, payload.role)
        assert user is not None
    if payload.is_active is False:
        user = await repository.deactivate(user_id)
        assert user is not None
    elif payload.is_active is True:
        user = await repository.reactivate(user_id)
        assert user is not None
    await session.commit()
    return UserResponse(
        id=user.id,
        username=user.username,
        role=user.role,
        is_active=user.is_active,
        created_at=user.created_at,
    )


@router.delete("/users/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_user(
    user_id: int,
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    repository: Annotated[UsersRepository, Depends(get_users_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    """Remove permanentemente um usuario. Requer papel admin. Admin nao pode excluir a si mesmo."""
    if user_id == current_admin.user_id:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Nao e possivel excluir o proprio usuario.",
        )
    found = await repository.delete(user_id)
    if not found:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao encontrado.")
    await session.commit()


class InstanceAccessResponse(BaseModel):
    instance_id: str


class ResetTokenRequest(BaseModel):
    pass  # sem campos — o user_id vem da URL


class ResetTokenResponse(BaseModel):
    reset_url: str
    expires_at: datetime


class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str = Field(min_length=8, description="Minimo de 8 caracteres.")


class AdminChangePasswordRequest(BaseModel):
    new_password: str = Field(min_length=8, description="Minimo de 8 caracteres.")


class ChangePasswordRequest(BaseModel):
    current_password: str
    new_password: str = Field(min_length=8, description="Minimo de 8 caracteres.")


@router.get("/users/{user_id}/instances", response_model=list[InstanceAccessResponse])
async def list_user_instances(
    user_id: int,
    _: Annotated[CurrentUser, Depends(require_admin)],
    users_repo: Annotated[UsersRepository, Depends(get_users_repository)],
    access_repo: Annotated[
        UserInstanceAccessRepository, Depends(get_user_instance_access_repository)
    ],
) -> list[InstanceAccessResponse]:
    """Lista instâncias autorizadas para um usuário. Requer papel admin."""
    user = await users_repo.get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao encontrado.")
    ids = await access_repo.list_granted_instances(user_id)
    return [InstanceAccessResponse(instance_id=str(iid)) for iid in ids]


@router.post(
    "/users/{user_id}/instances/{instance_id}",
    response_model=InstanceAccessResponse,
    status_code=status.HTTP_201_CREATED,
)
async def grant_instance_access(
    user_id: int,
    instance_id: UUID,
    _: Annotated[CurrentUser, Depends(require_admin)],
    users_repo: Annotated[UsersRepository, Depends(get_users_repository)],
    access_repo: Annotated[
        UserInstanceAccessRepository, Depends(get_user_instance_access_repository)
    ],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstanceAccessResponse:
    """Concede acesso a uma instância para um usuário. Requer papel admin."""
    user = await users_repo.get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao encontrado.")
    await access_repo.grant(user_id, instance_id)
    await session.commit()
    return InstanceAccessResponse(instance_id=str(instance_id))


@router.delete("/users/{user_id}/instances/{instance_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_instance_access(
    user_id: int,
    instance_id: UUID,
    _: Annotated[CurrentUser, Depends(require_admin)],
    access_repo: Annotated[
        UserInstanceAccessRepository, Depends(get_user_instance_access_repository)
    ],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    """Revoga acesso a uma instância de um usuário. Requer papel admin."""
    removed = await access_repo.revoke(user_id, instance_id)
    await session.commit()
    if not removed:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Acesso nao encontrado para este usuario/instancia.",
        )


# ---------------------------------------------------------------------------
# Redefinicao e troca de senha
# ---------------------------------------------------------------------------


@router.post("/users/{user_id}/reset-token", response_model=ResetTokenResponse)
async def generate_reset_token(
    user_id: int,
    _: Annotated[CurrentUser, Depends(require_admin)],
    users_repo: Annotated[UsersRepository, Depends(get_users_repository)],
    tokens_repo: Annotated[
        PasswordResetTokensRepository, Depends(get_password_reset_tokens_repository)
    ],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> ResetTokenResponse:
    """Gera link de redefinicao de senha para um usuario. Requer papel admin.

    O token bruto e exibido UMA vez e nunca armazenado — apenas seu SHA-256.
    O admin deve copiar o link e enviar manualmente ao usuario.
    """
    user = await users_repo.get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao encontrado.")

    raw_token = secrets.token_urlsafe(32)
    token_hash = hashlib.sha256(raw_token.encode()).hexdigest()
    expires_at = datetime.now(tz=UTC) + timedelta(hours=_PASSWORD_RESET_TOKEN_TTL_HOURS)

    await tokens_repo.create(user_id=user_id, token_hash=token_hash, expires_at=expires_at)
    await session.commit()

    return ResetTokenResponse(
        reset_url=f"/reset-password?token={raw_token}",
        expires_at=expires_at,
    )


@router.put("/users/{user_id}/password", status_code=status.HTTP_200_OK)
async def admin_change_user_password(
    user_id: int,
    payload: AdminChangePasswordRequest,
    _: Annotated[CurrentUser, Depends(require_admin)],
    users_repo: Annotated[UsersRepository, Depends(get_users_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> dict[str, bool]:
    """Troca a senha de um usuario diretamente, sem exigir a senha atual. Requer papel admin."""
    user = await users_repo.get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao encontrado.")

    user.hashed_password = hash_password(payload.new_password)
    await session.commit()

    return {"ok": True}


@router.post("/reset-password", status_code=status.HTTP_200_OK)
async def reset_password(
    payload: ResetPasswordRequest,
    users_repo: Annotated[UsersRepository, Depends(get_users_repository)],
    tokens_repo: Annotated[
        PasswordResetTokensRepository, Depends(get_password_reset_tokens_repository)
    ],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> dict[str, str]:
    """Redefine a senha usando um token de redefinicao valido. Endpoint publico (sem autenticacao).

    Retorna 400 se o token for invalido, expirado ou ja utilizado.
    """
    token_hash = hashlib.sha256(payload.token.encode()).hexdigest()
    token_row = await tokens_repo.get_valid_by_hash(token_hash)
    if token_row is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token invalido, expirado ou ja utilizado.",
        )

    user = await users_repo.get_by_id(token_row.user_id)
    if user is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Token invalido, expirado ou ja utilizado.",
        )

    user.hashed_password = hash_password(payload.new_password)
    await session.flush()
    await tokens_repo.mark_used(token_row.id)
    await session.commit()

    return {"detail": "Senha redefinida com sucesso."}


@router.put("/me/password", status_code=status.HTTP_200_OK)
async def change_my_password(
    payload: ChangePasswordRequest,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    users_repo: Annotated[UsersRepository, Depends(get_users_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> dict[str, str]:
    """Troca a senha do usuario autenticado. Requer a senha atual para confirmacao."""
    user = await users_repo.get_by_id(current_user.user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao encontrado.")

    if not verify_password(payload.current_password, user.hashed_password):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Senha atual incorreta.",
        )

    user.hashed_password = hash_password(payload.new_password)
    await session.commit()

    return {"detail": "Senha alterada com sucesso."}
