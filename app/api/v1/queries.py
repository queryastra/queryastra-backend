"""Endpoints de queries ativas / top SQL.

Destino de dados: hypertable bruta `query_samples` (`Docs/database-schema.md` §3), consultada
diretamente — o continuous aggregate `query_stats_hourly` foi criado `WITH NO DATA` e sem
policy de refresh, esta vazio e nunca atualizado, portanto nunca e usado aqui (a agregacao de
"Top SQL" e feita na propria query contra `query_samples`). Texto de SQL retornado a UI e sempre
`sql_text_masked` (`Docs/rules/03-sql-privacy.md`) — o texto original cifrado so e acessivel pelo
fluxo de "revelar" auditado da Fase 2 (RBAC), fora de escopo deste MVP.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session, resolve_time_window
from app.core.config import get_settings
from app.repositories.query_samples_repository import QuerySamplesRepository

router = APIRouter()


class QuerySampleResponse(BaseModel):
    """Amostra de query ativa, com texto ja mascarado."""

    time: datetime
    instance_id: UUID
    sql_hash: str
    sql_name: str | None
    sql_text_masked: str
    wait_type: str | None
    wait_event_native: str | None
    wait_time_ms: int | None
    duration_ms: int | None
    session_id: str | None
    db_user: str | None
    program: str | None
    database_name: str | None
    machine: str | None


class TopSqlEntryResponse(BaseModel):
    """Linha agregada de "Top SQL", calculada em tempo real sobre `query_samples`."""

    instance_id: UUID
    sql_hash: str
    sql_name: str | None
    sql_text_masked: str
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float
    predominant_wait_type: str | None


# --- Dependency providers -------------------------------------------------------------------


def get_query_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> QuerySamplesRepository:
    return QuerySamplesRepository(session)


# --- Endpoints -------------------------------------------------------------------------------


@router.get("/active/{instance_id}", response_model=list[QuerySampleResponse])
async def list_active_queries(
    instance_id: UUID,
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[QuerySampleResponse]:
    """Lista amostras de query de uma instancia no periodo (sem `from`/`to`, usa a janela
    padrao `API_READ_DEFAULT_LOOKBACK_SECONDS`)."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.list_active(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    # `query_samples_table` ja inclui `instance_id` no select — NAO passar `instance_id=` aqui
    # tambem, colidiria com **row._mapping (TypeError de keyword duplicado).
    return [QuerySampleResponse(**row._mapping) for row in rows]


@router.get("/top-sql/{instance_id}", response_model=list[TopSqlEntryResponse])
async def top_sql(
    instance_id: UUID,
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[TopSqlEntryResponse]:
    """Top SQL agregado de uma instancia, calculado em tempo real sobre `query_samples`
    (GROUP BY sql_hash). PostgreSQL: nao depende de `pg_stat_statements` (agrega o que o
    collector ja normalizou de `pg_stat_activity`). SQL Server: idem, via `sql_hash` normalizado
    pelo collector."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_sql(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    # Aqui a query de `top_sql` nao seleciona `instance_id` — sem colisao ao passar explicito.
    return [TopSqlEntryResponse(instance_id=instance_id, **row._mapping) for row in rows]
