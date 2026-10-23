"""Dependencies comuns da API — `Docs/specs/02-backend-contract.md` (camada `api`).

`get_db_session` reexporta a sessao assincrona (`app/db/session.py`) como dependency FastAPI.
`PaginationParams` padroniza paginacao entre os endpoints de listagem (instances, metrics,
queries, alerts) — nenhum endpoint deve inventar seu proprio esquema de `page`/`limit`.
`get_current_user`/`require_admin`/`require_operator` — dependencias de autorizacao RBAC.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import Depends, HTTPException, Query, Security, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import JWTError
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session
from app.repositories.system_license_repository import SystemLicenseRepository
from app.services.auth_service import decode_token
from app.services.plan_service import PlanConfig, PlanService

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency FastAPI que entrega uma `AsyncSession` por request."""
    async for session in get_session():
        yield session


async def get_plan_service(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> PlanService:
    """Dependency FastAPI: resolve o plano efetivo do sistema a partir de `system_license`."""
    repo = SystemLicenseRepository(session)
    license_row = await repo.get_or_create()
    await session.commit()
    return PlanService(
        trial_started_at=license_row.trial_started_at,
        plan_id=license_row.plan_id,
        subscription_expires_at=license_row.subscription_expires_at,
    )


class PaginationParams(BaseModel):
    """Parametros de paginacao comuns a endpoints de listagem."""

    offset: int = Field(default=0, ge=0)
    limit: int = Field(default=DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE)


def pagination_params(
    offset: Annotated[int, Query(ge=0)] = 0,
    limit: Annotated[int, Query(ge=1, le=MAX_PAGE_SIZE)] = DEFAULT_PAGE_SIZE,
) -> PaginationParams:
    """Dependency FastAPI que monta `PaginationParams` a partir de query params."""
    return PaginationParams(offset=offset, limit=limit)


def resolve_time_window(
    since: datetime | None, until: datetime | None, default_lookback: timedelta
) -> tuple[datetime, datetime]:
    """Resolve a janela `[since, until]` de um endpoint de leitura.

    Sem periodo informado, olha `default_lookback` para tras a partir de agora (UTC).
    """
    now = datetime.now(UTC)
    return (since or now - default_lookback), (until or now)


# ---------------------------------------------------------------------------
# RBAC — autenticacao JWT
# ---------------------------------------------------------------------------

_bearer = HTTPBearer(auto_error=False)


class CurrentUser:
    """Usuario autenticado extraido do JWT — injetado pelas dependencias de auth."""

    def __init__(self, user_id: int, username: str, role: str) -> None:
        self.user_id = user_id
        self.username = username
        self.role = role

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"

    @property
    def is_operator(self) -> bool:
        return self.role in ("admin", "operator")


async def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Security(_bearer),  # noqa: B008
) -> CurrentUser:
    """Dependencia FastAPI: extrai e valida o Bearer JWT. Lanca 401 se ausente ou invalido."""
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Credenciais de autenticacao nao informadas.",
            headers={"WWW-Authenticate": "Bearer"},
        )
    try:
        payload = decode_token(credentials.credentials)
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token invalido ou expirado.",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc
    return CurrentUser(
        user_id=int(payload["sub"]),
        username=payload["username"],
        role=payload["role"],
    )


async def require_admin(
    current_user: CurrentUser = Depends(get_current_user),  # noqa: B008
) -> CurrentUser:
    """Dependencia FastAPI: exige papel 'admin'. Lanca 403 caso contrario."""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acesso restrito a administradores.",
        )
    return current_user


async def require_operator(
    current_user: CurrentUser = Depends(get_current_user),  # noqa: B008
) -> CurrentUser:
    """Dependencia FastAPI: exige papel 'operator' ou superior. Lanca 403 caso contrario."""
    if not current_user.is_operator:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acesso restrito a operadores ou administradores.",
        )
    return current_user


async def check_instance_access(
    instance_id: UUID,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> None:
    """Dependência FastAPI: verifica acesso a uma instância específica.

    Admins passam sem verificação. Para operator/viewer, exige registro em
    `user_instance_access`. Lança 403 se o acesso não foi concedido.
    """
    if current_user.is_admin:
        return
    from app.repositories.user_instance_access_repository import (  # noqa: PLC0415
        UserInstanceAccessRepository,
    )

    repo = UserInstanceAccessRepository(session)
    has = await repo.has_access(current_user.user_id, instance_id)
    if not has:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Acesso nao autorizado a esta instancia.",
        )
