"""Dependencies comuns da API — `Docs/specs/02-backend-contract.md` (camada `api`).

`get_db_session` reexporta a sessao assincrona (`app/db/session.py`) como dependency FastAPI.
`PaginationParams` padroniza paginacao entre os endpoints de listagem (instances, metrics,
queries, alerts) — nenhum endpoint deve inventar seu proprio esquema de `page`/`limit`.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import Query
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.db.session import get_session

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 500


async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    """Dependency FastAPI que entrega uma `AsyncSession` por request."""
    async for session in get_session():
        yield session


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
