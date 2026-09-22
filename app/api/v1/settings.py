"""Endpoint de preferências globais editáveis — tela "Configurações → Geral" no frontend.

Lê/grava a linha singleton de `system_settings` (ver `app.models.system_settings`). Sem
restrição de acesso hoje (será limitada por RBAC quando essa feature existir, Fase 4). Uma
mudança aqui vale a partir do PRÓXIMO ciclo de poll de cada instância — `PollingWorker` relê o
valor a cada ciclo (não cacheado como `app.core.config.Settings`), sem exigir restart do backend.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.models.system_settings import SystemSettings
from app.repositories.system_settings_repository import SystemSettingsRepository

router = APIRouter()


class SystemSettingsResponse(BaseModel):
    blocking_min_duration_ms: int
    sleeping_min_duration_ms: int


class SystemSettingsUpdateRequest(BaseModel):
    """Atualização PARCIAL — só o(s) campo(s) presentes (não `None`) são gravados. Necessário
    porque cada campo tem seu próprio auto-save independente na UI (Configurações → Geral); um
    payload que exigisse os 2 campos sempre poderia sobrescrever o outro com um valor obsoleto."""

    blocking_min_duration_ms: int | None = Field(default=None, ge=0)
    sleeping_min_duration_ms: int | None = Field(default=None, ge=0)


def get_system_settings_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> SystemSettingsRepository:
    return SystemSettingsRepository(session)


def _to_response(row: SystemSettings) -> SystemSettingsResponse:
    return SystemSettingsResponse(
        blocking_min_duration_ms=row.blocking_min_duration_ms,
        sleeping_min_duration_ms=row.sleeping_min_duration_ms,
    )


@router.get("", response_model=SystemSettingsResponse)
async def get_system_settings(
    repository: Annotated[SystemSettingsRepository, Depends(get_system_settings_repository)],
) -> SystemSettingsResponse:
    row = await repository.get()
    return _to_response(row)


@router.patch("", response_model=SystemSettingsResponse)
async def update_system_settings(
    payload: SystemSettingsUpdateRequest,
    repository: Annotated[SystemSettingsRepository, Depends(get_system_settings_repository)],
) -> SystemSettingsResponse:
    row = await repository.get()
    if payload.blocking_min_duration_ms is not None:
        row = await repository.update_blocking_min_duration_ms(payload.blocking_min_duration_ms)
    if payload.sleeping_min_duration_ms is not None:
        row = await repository.update_sleeping_min_duration_ms(payload.sleeping_min_duration_ms)
    return _to_response(row)
