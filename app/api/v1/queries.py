"""Endpoints de queries ativas / top SQL — stub.

Destino de dados: hypertable `query_samples` + continuous aggregate `query_stats_hourly`
(`Docs/database-schema.md` §3). Texto de SQL retornado a UI e sempre `sql_text_masked`
(`Docs/rules/03-sql-privacy.md`) — o texto original cifrado so e acessivel pelo fluxo de
"revelar" auditado da Fase 2 (RBAC), fora de escopo deste MVP.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

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


class TopSqlEntryResponse(BaseModel):
    """Linha agregada de "Top SQL" (equivalente a `query_stats_hourly`)."""

    instance_id: UUID
    sql_hash: str
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float


@router.get("/active/{instance_id}", response_model=list[QuerySampleResponse])
async def list_active_queries(instance_id: UUID) -> list[QuerySampleResponse]:
    """Lista queries ativas do ultimo poll de uma instancia. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Listagem de queries ativas ainda nao implementada.",
    )


@router.get("/top-sql/{instance_id}", response_model=list[TopSqlEntryResponse])
async def top_sql(instance_id: UUID) -> list[TopSqlEntryResponse]:
    """Top SQL agregado de uma instancia. Ainda nao implementado.

    PostgreSQL: requer `pg_stat_statements` habilitada (degrada graciosamente quando ausente,
    ver Docs/collector-queries.md — "Prontos para agir" #1). SQL Server: via `query_hash`, sem
    extensao.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Top SQL ainda nao implementado.",
    )
