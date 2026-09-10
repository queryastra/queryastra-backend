"""Endpoints de waits e bloqueios — stub.

Bloqueio: `blocking_events` (`Docs/database-schema.md` §3), alimentado por
`pg_blocking_pids()` (PostgreSQL) ou `sys.dm_exec_requests.blocking_session_id` +
CTE recursiva (SQL Server) — `Docs/collector-queries.md` §2. Wait: categoria canonica
(`app.normalizers.wait_taxonomy.WaitCategory`) + `wait_event_native` preservado.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

router = APIRouter()


class BlockingEventResponse(BaseModel):
    """Evento de bloqueio — `blocking_events`."""

    time: datetime
    instance_id: UUID
    blocker_session_id: str
    blocked_session_id: str
    blocker_sql_hash: str | None
    blocked_sql_hash: str | None
    wait_time_ms: int


class WaitSummaryEntryResponse(BaseModel):
    """Resumo de wait por categoria canonica para uma instancia num periodo."""

    wait_type: str  # WaitCategory
    total_wait_time_ms: int


@router.get("/blocking/{instance_id}", response_model=list[BlockingEventResponse])
async def list_blocking_events(instance_id: UUID) -> list[BlockingEventResponse]:
    """Lista eventos de bloqueio recentes de uma instancia. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Listagem de bloqueios ainda nao implementada.",
    )


@router.get("/waits/{instance_id}", response_model=list[WaitSummaryEntryResponse])
async def list_wait_summary(instance_id: UUID) -> list[WaitSummaryEntryResponse]:
    """Resumo de wait por categoria canonica de uma instancia. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Resumo de waits ainda nao implementado.",
    )
