"""Endpoints de queries ativas / top SQL / drill-down hierarquico por banco de dados.

Destino de dados: hypertable bruta `query_samples` (`Docs/database-schema.md` §3), consultada
diretamente — o continuous aggregate `query_stats_hourly` foi criado `WITH NO DATA` e sem
policy de refresh, esta vazio e nunca atualizado, portanto nunca e usado aqui (a agregacao de
"Top SQL" e feita na propria query contra `query_samples`). Texto de SQL retornado a UI e sempre
`sql_text_masked` (`Docs/rules/03-sql-privacy.md`) — o texto original cifrado so e acessivel pelo
fluxo de "revelar" auditado da Fase 2 (RBAC), fora de escopo deste MVP.

Drill-down hierarquico (Instancia -> Banco -> Query): `GET /databases/{instance_id}` (nivel 1,
agrega por `database_name`), `GET /top-sql/{instance_id}?database_name=X` (nivel 2, mesmo
agregado por `sql_hash` de sempre, agora filtravel por banco) e `GET /detail/{instance_id}/
{sql_hash}` (nivel 3/folha, amostras brutas daquele hash).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
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
    database_name: str | None
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float
    predominant_wait_type: str | None


class DatabaseSummaryResponse(BaseModel):
    """Linha agregada por banco de dados — nivel 1 do drill-down hierarquico (Instancia ->
    Banco -> Query)."""

    database_name: str
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float


class QuerySampleDetailResponse(BaseModel):
    """Uma amostra bruta de execucao, dentro do detalhe de uma query (nivel 3/folha)."""

    time: datetime
    duration_ms: int | None
    wait_type: str | None
    wait_event_native: str | None
    wait_time_ms: int | None
    session_id: str | None
    db_user: str | None
    program: str | None
    machine: str | None


class QueryDetailResponse(BaseModel):
    """Detalhe de um `sql_hash` especifico — nivel 3 (folha) do drill-down hierarquico."""

    instance_id: UUID
    sql_hash: str
    sql_name: str | None
    sql_text_masked: str
    database_name: str | None
    samples: list[QuerySampleDetailResponse]


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
    database_name: Annotated[str | None, Query()] = None,
) -> list[TopSqlEntryResponse]:
    """Top SQL agregado de uma instancia, calculado em tempo real sobre `query_samples`
    (GROUP BY sql_hash). PostgreSQL: nao depende de `pg_stat_statements` (agrega o que o
    collector ja normalizou de `pg_stat_activity`). SQL Server: idem, via `sql_hash` normalizado
    pelo collector.

    `database_name` (nivel 2 do drill-down hierarquico: Instancia -> Banco -> Query) restringe a
    agregacao a um unico banco; sem o parametro, mantem o comportamento historico de agregar
    todos os bancos juntos sob o mesmo `sql_hash`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_sql(
        instance_id,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
        database_name=database_name,
    )
    # Aqui a query de `top_sql` nao seleciona `instance_id` — sem colisao ao passar explicito.
    return [TopSqlEntryResponse(instance_id=instance_id, **row._mapping) for row in rows]


@router.get("/databases/{instance_id}", response_model=list[DatabaseSummaryResponse])
async def list_databases(
    instance_id: UUID,
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[DatabaseSummaryResponse]:
    """Nivel 1 do drill-down hierarquico (Instancia -> Banco -> Query): agrega `query_samples`
    por `database_name` (GROUP BY database_name), calculado em tempo real. Amostras sem
    `database_name` sao agrupadas sob a label 'desconhecido', nunca descartadas."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_sql_by_database(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    return [DatabaseSummaryResponse(**row._mapping) for row in rows]


@router.get("/detail/{instance_id}/{sql_hash}", response_model=QueryDetailResponse)
async def get_query_detail(
    instance_id: UUID,
    sql_hash: str,
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> QueryDetailResponse:
    """Nivel 3 (folha) do drill-down hierarquico: amostras brutas de um `sql_hash` especifico
    dentro de uma instancia, no periodo. 404 quando nao ha nenhuma amostra desse hash no
    periodo."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.query_detail(
        instance_id, sql_hash, resolved_since, resolved_until, settings.api_read_max_rows
    )
    if not rows:
        raise HTTPException(
            status_code=404, detail="Nenhuma amostra encontrada para essa query no periodo."
        )
    first = rows[0]
    return QueryDetailResponse(
        instance_id=instance_id,
        sql_hash=sql_hash,
        sql_name=first.sql_name,
        sql_text_masked=first.sql_text_masked,
        database_name=first.database_name,
        samples=[
            QuerySampleDetailResponse(
                **{
                    k: v
                    for k, v in r._mapping.items()
                    if k in QuerySampleDetailResponse.model_fields
                }
            )
            for r in rows
        ],
    )
