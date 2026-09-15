"""Endpoint de leitura de deadlocks — Etapa 10, item 7.

Le direto da tabela relacional `deadlock_events` (insert-only, atualizada pelo
`DeadlockSupervisor` de baixa frequencia — `app/workers/deadlock_supervisor.py`), sem janela de
tempo (nao e serie temporal bruta). So SQL Server tem deadlock detalhado — PostgreSQL sempre
devolve lista vazia (nunca escreve nesta tabela, ver
`app.collectors.postgresql.adapter.PostgreSQLCollector.collect_deadlock_events`); o frontend
decide a mensagem honesta ("PostgreSQL não expõe detalhe de deadlock via SQL") a partir de
`instance.dbType`, sem precisar de um campo `available` aqui.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
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
    victim_session_id: str | None
    victim_sql_text_masked: str | None
    survivor_session_id: str | None
    survivor_sql_text_masked: str | None
    resource_description: str | None
    raw_xml_masked: str | None

    model_config = {"from_attributes": True}


def get_deadlock_events_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> DeadlockEventsRepository:
    return DeadlockEventsRepository(session)


@router.get("/{instance_id}", response_model=list[DeadlockEventResponse])
async def list_deadlocks(
    instance_id: UUID,
    repository: Annotated[DeadlockEventsRepository, Depends(get_deadlock_events_repository)],
) -> list[DeadlockEventResponse]:
    """Deadlocks retidos de uma instância — mais recentes primeiro."""
    settings = get_settings()
    rows = await repository.list_for_instance(instance_id, settings.api_read_max_rows)
    return [DeadlockEventResponse.model_validate(row) for row in rows]
