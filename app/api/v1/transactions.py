"""Endpoints de rastreamento de transacoes — Etapa 10, itens 2/3 (redesenhado).

Le direto da tabela relacional `transaction_events` — um LOG DE INCIDENTE retido para analise
posterior (so transacoes "dignas de nota": bloqueio, erro, orfa ou deadlock — ver
`app.collectors.transaction_flagging`), nao mais um espelho de toda transacao aberta. Uma
transacao ja fechada continua acessivel (`closed_at` preenchido) ate a janela de retencao
(`settings.problem_events_retention_days`) expirar — nunca 404 so por ter fechado.

`is_orphan_candidate` (so quando ainda aberta): `begin_time` mais antigo que
`settings.transaction_orphan_threshold_minutes` — destaque/ordenacao, nunca fecha a transacao de
verdade nem afeta o dado em si.

`related_blocking_*` (a pedido explicito do usuario): correlaciona cada transacao com um
eventual incidente de bloqueio envolvendo a mesma sessao (`BlockingIncidentsRepository`) — o mais
recente, quando ha mais de um. Liga pra `/instances/{id}/blocking/{sessionId}`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.core.config import get_settings
from app.models.blocking_incident import BlockingIncident
from app.models.transaction_event import TransactionEvent
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository
from app.repositories.transaction_events_repository import TransactionEventsRepository

router = APIRouter()


class TransactionEventResponse(BaseModel):
    id: UUID
    instance_id: UUID
    transaction_identifier: str
    session_id: str | None
    database_name: str | None
    transaction_type: str | None
    transaction_uow: str | None
    begin_time: datetime | None
    state: str | None
    last_error_code: int | None
    first_seen_at: datetime
    last_seen_at: datetime
    closed_at: datetime | None
    flag_reason: str | None
    is_orphan_candidate: bool
    related_blocking_session_id: str | None
    related_blocking_role: str | None
    related_blocking_ended_at: datetime | None

    model_config = {"from_attributes": True}


def get_transaction_events_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TransactionEventsRepository:
    return TransactionEventsRepository(session)


def get_blocking_incidents_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> BlockingIncidentsRepository:
    return BlockingIncidentsRepository(session)


def _is_orphan_candidate(
    begin_time: datetime | None, closed_at: datetime | None, threshold_minutes: int
) -> bool:
    if begin_time is None or closed_at is not None:
        return False
    now = datetime.now(UTC)
    age_minutes = (now - begin_time).total_seconds() / 60
    return age_minutes >= threshold_minutes


def _most_recent_incident_for_session(
    session_id: str, incidents: list[BlockingIncident]
) -> BlockingIncident | None:
    matching = [
        i
        for i in incidents
        if i.blocker_session_id == session_id or i.blocked_session_id == session_id
    ]
    if not matching:
        return None
    return max(matching, key=lambda i: i.last_seen_at)


def _to_response(
    row: TransactionEvent,
    threshold_minutes: int,
    incidents: list[BlockingIncident],
) -> TransactionEventResponse:
    related_session_id: str | None = None
    related_role: str | None = None
    related_ended_at: datetime | None = None
    if row.session_id is not None:
        incident = _most_recent_incident_for_session(row.session_id, incidents)
        if incident is not None:
            if incident.blocker_session_id == row.session_id:
                related_session_id = incident.blocked_session_id
                related_role = "bloqueadora"
            else:
                related_session_id = incident.blocker_session_id
                related_role = "bloqueada"
            related_ended_at = incident.ended_at

    return TransactionEventResponse(
        id=row.id,
        instance_id=row.instance_id,
        transaction_identifier=row.transaction_identifier,
        session_id=row.session_id,
        database_name=row.database_name,
        transaction_type=row.transaction_type,
        transaction_uow=row.transaction_uow,
        begin_time=row.begin_time,
        state=row.state,
        last_error_code=row.last_error_code,
        first_seen_at=row.first_seen_at,
        last_seen_at=row.last_seen_at,
        closed_at=row.closed_at,
        flag_reason=row.flag_reason,
        is_orphan_candidate=_is_orphan_candidate(row.begin_time, row.closed_at, threshold_minutes),
        related_blocking_session_id=related_session_id,
        related_blocking_role=related_role,
        related_blocking_ended_at=related_ended_at,
    )


@router.get("/{instance_id}", response_model=list[TransactionEventResponse])
async def list_transactions(
    instance_id: UUID,
    repository: Annotated[TransactionEventsRepository, Depends(get_transaction_events_repository)],
    blocking_repository: Annotated[
        BlockingIncidentsRepository, Depends(get_blocking_incidents_repository)
    ],
    open_only: Annotated[bool, Query()] = True,
    database_name: Annotated[str | None, Query()] = None,
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[TransactionEventResponse]:
    """Transações da instância — por padrão só as ainda abertas (mais antigas primeiro, mais
    prováveis de serem órfãs/esquecidas); `open_only=false` inclui as já encerradas e retidas
    (mais recentes primeiro — feed de histórico para investigação). `from`/`to` (opcionais, só
    fazem sentido com `open_only=false`) filtram por sobreposição com o período."""
    settings = get_settings()
    rows = await repository.list_for_instance(
        instance_id,
        settings.api_read_max_rows,
        open_only=open_only,
        database_name=database_name,
        since=since,
        until=until,
    )
    session_ids = [row.session_id for row in rows if row.session_id is not None]
    incidents = await blocking_repository.find_involving_sessions(
        instance_id, session_ids, settings.api_read_max_rows
    )
    return [
        _to_response(row, settings.transaction_orphan_threshold_minutes, incidents) for row in rows
    ]


@router.get("/{instance_id}/{transaction_identifier}", response_model=TransactionEventResponse)
async def get_transaction(
    instance_id: UUID,
    transaction_identifier: str,
    repository: Annotated[TransactionEventsRepository, Depends(get_transaction_events_repository)],
    blocking_repository: Annotated[
        BlockingIncidentsRepository, Depends(get_blocking_incidents_repository)
    ],
) -> TransactionEventResponse:
    """Detalhe de uma transação específica — continua resolvendo mesmo depois de encerrada
    (`closed_at` preenchido), até a janela de retenção expirar; 404 só quando nunca existiu ou já
    foi expurgada."""
    settings = get_settings()
    row = await repository.get_by_identifier(instance_id, transaction_identifier)
    if row is None:
        raise HTTPException(
            status_code=404,
            detail=(
                "Transação não encontrada — nunca existiu, ou já foi expurgada após o período "
                "de retenção."
            ),
        )
    incidents = (
        await blocking_repository.find_involving_sessions(
            instance_id, [row.session_id], settings.api_read_max_rows
        )
        if row.session_id is not None
        else []
    )
    return _to_response(row, settings.transaction_orphan_threshold_minutes, incidents)
