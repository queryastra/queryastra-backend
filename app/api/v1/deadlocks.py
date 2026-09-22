"""Endpoint de leitura de deadlocks — Etapa 10, item 7.

Le direto da tabela relacional `deadlock_events` (insert-only, atualizada pelo
`DeadlockSupervisor` de baixa frequencia — `app/workers/deadlock_supervisor.py`). So SQL Server
tem deadlock detalhado — PostgreSQL sempre devolve lista vazia (nunca escreve nesta tabela, ver
`app.collectors.postgresql.adapter.PostgreSQLCollector.collect_deadlock_events`); o frontend
decide a mensagem honesta ("PostgreSQL não expõe detalhe de deadlock via SQL") a partir de
`instance.dbType`, sem precisar de um campo `available` aqui.

`detail` (grafo completo — todos os processos e recursos, ver `app.models.deadlock_event.
DeadlockEvent`) vem tanto na listagem quanto no detalhe — a pedido explicito do usuario, pra
abrir as queries/procedures/objetos envolvidos de cada sessao (bloqueadora e vitima) sem uma
segunda chamada.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.core.config import get_settings
from app.repositories.deadlock_events_repository import DeadlockEventsRepository

router = APIRouter()


class DeadlockEventResponse(BaseModel):
    id: UUID
    instance_id: UUID
    time: datetime
    database_name: str | None
    victim_session_id: str | None
    victim_sql_text_masked: str | None
    survivor_session_id: str | None
    survivor_sql_text_masked: str | None
    resource_description: str | None
    raw_xml_masked: str | None
    detail: dict[str, Any] | None
    # Deteccao instantanea (PollingWorker, contador sqlserver_deadlocks_total) - True enquanto so
    # sabemos QUE um deadlock aconteceu (demais campos ainda None); False assim que
    # DeadlockEventsRepository reconcilia com o relatorio completo do system_health.
    pending: bool

    model_config = {"from_attributes": True}


def get_deadlock_events_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> DeadlockEventsRepository:
    return DeadlockEventsRepository(session)


@router.get("/{instance_id}", response_model=list[DeadlockEventResponse])
async def list_deadlocks(
    instance_id: UUID,
    repository: Annotated[DeadlockEventsRepository, Depends(get_deadlock_events_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    database_name: Annotated[str | None, Query()] = None,
) -> list[DeadlockEventResponse]:
    """Deadlocks retidos de uma instância — mais recentes primeiro. `from`/`to`/`database_name`
    são opcionais (sem eles, todo o histórico retido)."""
    settings = get_settings()
    rows = await repository.list_for_instance(
        instance_id,
        settings.api_read_max_rows,
        since=since,
        until=until,
        database_name=database_name,
    )
    return [DeadlockEventResponse.model_validate(row) for row in rows]


@router.get("/{instance_id}/{deadlock_id}", response_model=DeadlockEventResponse)
async def get_deadlock(
    instance_id: UUID,
    deadlock_id: UUID,
    repository: Annotated[DeadlockEventsRepository, Depends(get_deadlock_events_repository)],
) -> DeadlockEventResponse:
    """Detalhe de um deadlock específico — grafo completo (`detail`) para abrir queries/
    procedures/objetos de cada sessão envolvida."""
    row = await repository.get_by_id(instance_id, deadlock_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Deadlock não encontrado.")
    return DeadlockEventResponse.model_validate(row)
