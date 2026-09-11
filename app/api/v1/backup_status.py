"""Endpoint de leitura de estado de backup/archiving — ARCHITECTURE_SPEC.md Q16.

Le direto da tabela relacional `backup_status` (estado mais recente por instancia/banco,
atualizado pelo `BackupStatusSupervisor` de baixa frequencia —
`app/workers/backup_status_supervisor.py`), sem janela de tempo (nao e serie temporal bruta como
`query_samples`/`metric_samples`)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.repositories.backup_status_repository import BackupStatusRepository

router = APIRouter()


class BackupStatusResponse(BaseModel):
    """Estado de backup/archiving — `database_name=None` representa a instancia inteira
    (PostgreSQL, `pg_stat_archiver` e cluster-wide) em vez de um banco especifico (SQL Server,
    `msdb.dbo.backupset`). Ver docstring de `NormalizedBackupStatus` para a diferenca de
    semantica entre engines (Postgres nao tem catalogo nativo de backup completo)."""

    id: UUID
    instance_id: UUID
    database_name: str | None
    last_backup_at: datetime | None
    detail: dict | None
    checked_at: datetime

    model_config = {"from_attributes": True}


def get_backup_status_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> BackupStatusRepository:
    return BackupStatusRepository(session)


@router.get("/{instance_id}", response_model=list[BackupStatusResponse])
async def list_backup_status(
    instance_id: UUID,
    repository: Annotated[BackupStatusRepository, Depends(get_backup_status_repository)],
) -> list[BackupStatusResponse]:
    """Estado de backup/archiving de uma instancia — estado mais recente, nao historico bruto."""
    rows = await repository.list_for_instance(instance_id)
    return [BackupStatusResponse.model_validate(row) for row in rows]
