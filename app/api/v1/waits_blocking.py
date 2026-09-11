"""Endpoints de waits e bloqueios.

Bloqueio: hypertable bruta `blocking_events` (`Docs/database-schema.md` §3), alimentado por
`pg_blocking_pids()` (PostgreSQL) ou `sys.dm_exec_requests.blocking_session_id` + CTE recursiva
(SQL Server) — `Docs/collector-queries.md` §2. Wait: hypertable bruta `query_samples`, agregada
por categoria canonica (`app.normalizers.wait_taxonomy.WaitCategory`) em tempo real — nenhum
endpoint aqui consulta os continuous aggregates (`query_stats_hourly`), vazios e nunca
atualizados.

Quebra de contrato intencional: `GET /blocking/{instance_id}` deixa de devolver uma lista plana
de `blocking_events` e passa a devolver uma arvore recursiva (`BlockingTreeNodeResponse`) — cada
sessao bloqueadora raiz com seus bloqueados aninhados em `children`, refletindo cadeias de
bloqueio de mais de um nivel a partir dos eventos diretos gravados.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel, Field
from sqlalchemy import Row
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session, resolve_time_window
from app.core.config import get_settings
from app.repositories.blocking_events_repository import BlockingEventsRepository
from app.repositories.query_samples_repository import QuerySamplesRepository

router = APIRouter()


class BlockingTreeNodeResponse(BaseModel):
    """Um no da arvore de bloqueio: uma sessao (bloqueadora ou bloqueada) e seus bloqueados
    diretos aninhados em `children`."""

    session_id: str
    sql_hash: str | None
    sql_text_masked: str | None
    wait_time_ms: int
    db_user: str | None
    children: list[BlockingTreeNodeResponse] = Field(default_factory=list)


BlockingTreeNodeResponse.model_rebuild()  # necessario para tipo recursivo com forward ref


class WaitSummaryEntryResponse(BaseModel):
    """Resumo de wait por categoria canonica para uma instancia num periodo."""

    wait_type: str  # WaitCategory
    total_wait_time_ms: int


# --- Dependency providers -------------------------------------------------------------------


def get_blocking_events_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> BlockingEventsRepository:
    return BlockingEventsRepository(session)


def get_query_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> QuerySamplesRepository:
    return QuerySamplesRepository(session)


# --- Montagem da arvore ------------------------------------------------------------------------


def _build_blocking_tree(
    events: list[Row], enrichment: dict[str, Row]
) -> list[BlockingTreeNodeResponse]:
    """Monta a arvore de bloqueio a partir de eventos diretos (nivel unico por linha de
    `blocking_events`) — encadeia varios niveis quando um bloqueador tambem aparece como
    bloqueado em outra linha do mesmo snapshot."""
    children_by_blocker: dict[str, list[Row]] = defaultdict(list)
    blocked_ids: set[str] = set()
    blocker_sql_hash_by_session: dict[str, str | None] = {}
    for e in events:
        children_by_blocker[e.blocker_session_id].append(e)
        blocked_ids.add(e.blocked_session_id)
        blocker_sql_hash_by_session[e.blocker_session_id] = e.blocker_sql_hash

    def node_for(
        session_id: str, sql_hash: str | None, wait_time_ms: int
    ) -> BlockingTreeNodeResponse:
        info = enrichment.get(session_id)
        return BlockingTreeNodeResponse(
            session_id=session_id,
            sql_hash=sql_hash,
            sql_text_masked=info.sql_text_masked if info else None,
            wait_time_ms=wait_time_ms,
            db_user=info.db_user if info else None,
            children=[
                node_for(c.blocked_session_id, c.blocked_sql_hash, c.wait_time_ms)
                for c in children_by_blocker.get(session_id, [])
            ],
        )

    roots = [sid for sid in children_by_blocker if sid not in blocked_ids]
    return [node_for(r, blocker_sql_hash_by_session[r], 0) for r in roots]


# --- Endpoints -------------------------------------------------------------------------------


@router.get("/blocking/{instance_id}", response_model=list[BlockingTreeNodeResponse])
async def list_blocking_events(
    instance_id: UUID,
    blocking_repo: Annotated[BlockingEventsRepository, Depends(get_blocking_events_repository)],
    query_repo: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BlockingTreeNodeResponse]:
    """Arvore de bloqueio do ultimo ciclo que registrou bloqueio dentro da janela (nao uma
    janela deslizante inteira — bloqueios ja resolvidos nao aparecem junto dos atuais). `to`
    (opcional, aditivo) permite delimitar um dia calendario especifico no passado (`DayFilter`
    do frontend); sem ele, `resolve_time_window` resolve pra "agora", o que na pratica nunca
    exclui nada (nenhum evento e gravado com `time` no futuro)."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    events = await blocking_repo.list_latest_snapshot(
        instance_id, resolved_since, settings.api_read_max_rows, until=resolved_until
    )
    session_ids = {e.blocker_session_id for e in events} | {e.blocked_session_id for e in events}
    enrichment = await query_repo.latest_by_session(instance_id, list(session_ids), resolved_since)
    return _build_blocking_tree(events, enrichment)


@router.get("/waits/{instance_id}", response_model=list[WaitSummaryEntryResponse])
async def list_wait_summary(
    instance_id: UUID,
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[WaitSummaryEntryResponse]:
    """Resumo de wait por categoria canonica de uma instancia, agregado em tempo real sobre
    `query_samples`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_summary(instance_id, resolved_since, resolved_until)
    return [WaitSummaryEntryResponse(**row._mapping) for row in rows]
