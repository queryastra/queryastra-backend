"""Endpoints on-demand de saude de vacuum/autovacuum e idle-in-transaction (Fase 5, T54+T56).

Nao ha persistencia — os dados sao consultados diretamente no servidor monitorado a cada
requisicao. Apropriado para dados de saude em tempo real que mudam continuamente.

T54: vacuum/autovacuum (PostgreSQL only) — retorna lista vazia para outros engines.
T56: idle-in-transaction (PostgreSQL + SQL Server) — retorna lista vazia para outros engines.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import check_instance_access, get_db_session
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.logging import get_logger
from app.repositories.instances_repository import InstancesRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

router = APIRouter()


class VacuumTableStatsResponse(BaseModel):
    schema_name: str
    table_name: str
    n_dead_tup: int
    n_live_tup: int
    dead_tup_pct: float
    last_vacuum: datetime | None
    last_autovacuum: datetime | None
    last_analyze: datetime | None
    last_autoanalyze: datetime | None
    vacuum_count: int
    autovacuum_count: int
    table_size_bytes: int
    xid_age: int


class AutovacuumWorkerResponse(BaseModel):
    pid: int
    datname: str
    query: str
    running_for_seconds: int


class VacuumStatsResponse(BaseModel):
    tables: list[VacuumTableStatsResponse]
    workers: list[AutovacuumWorkerResponse]


class IdleInTransactionSessionResponse(BaseModel):
    session_id: int
    username: str | None
    application_name: str | None
    client_info: str | None
    idle_seconds: int
    open_transaction_count: int | None
    last_query: str | None
    state: str | None


async def _build_collector(
    instance_id: UUID,
    session: AsyncSession,
    credential_resolver: CredentialResolver,
):
    instance = await InstancesRepository(session).get_by_id(instance_id)
    if instance is None or not instance.active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instancia nao encontrada ou inativa.",
        )

    username: str | None = None
    secret_value: str | None = None
    if instance.auth_method == "encrypted_fallback":
        if instance.encrypted_credential_ref is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Referencia de credencial ausente na instancia.",
            )
        try:
            resolved = await credential_resolver.resolve(
                CredentialReference(instance.encrypted_credential_ref)
            )
            username, secret_value = resolved.username, resolved.secret_value
        except CredentialResolutionError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Falha ao resolver credencial da instancia.",
            ) from exc

    connection = ConnectionParams(
        host=instance.host,
        port=instance.port,
        auth_method=instance.auth_method,
        username=username,
        secret_value=secret_value,
    )
    return create_collector(instance.db_type, connection=connection, instance_id=instance.id)


@router.get("/{instance_id}/vacuum", response_model=VacuumStatsResponse)
async def get_vacuum_stats(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> VacuumStatsResponse:
    """Estatisticas de vacuum/autovacuum por tabela + workers em execucao (PostgreSQL only).

    Consulta on-demand — dados sempre frescos. Retorna listas vazias para engines sem suporte."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        tables, workers = await collector.collect_vacuum_stats()
    except Exception as exc:
        logger.exception("Erro ao coletar vacuum stats para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar vacuum stats na instancia.",
        ) from exc
    finally:
        await collector.close()

    return VacuumStatsResponse(
        tables=[VacuumTableStatsResponse(**t.model_dump()) for t in tables],
        workers=[AutovacuumWorkerResponse(**w.model_dump()) for w in workers],
    )


@router.get("/{instance_id}/idle-in-transaction", response_model=list[IdleInTransactionSessionResponse])
async def get_idle_in_transaction(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[IdleInTransactionSessionResponse]:
    """Sessoes com transacao aberta mas sem request ativa (PostgreSQL + SQL Server).

    Consulta on-demand — dados sempre frescos. Retorna lista vazia para engines sem suporte."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        sessions = await collector.collect_idle_in_transaction()
    except Exception as exc:
        logger.exception("Erro ao coletar idle-in-transaction para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar sessoes idle in transaction na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [IdleInTransactionSessionResponse(**s.model_dump()) for s in sessions]
