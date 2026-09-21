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
from app.repositories.system_settings_repository import SystemSettingsRepository

router = APIRouter()


class SystemSettingsResponse(BaseModel):
    blocking_min_duration_ms: int


class SystemSettingsUpdateRequest(BaseModel):
    blocking_min_duration_ms: int = Field(ge=0)


def get_system_settings_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> SystemSettingsRepository:
    return SystemSettingsRepository(session)


@router.get("", response_model=SystemSettingsResponse)
async def get_system_settings(
    repository: Annotated[SystemSettingsRepository, Depends(get_system_settings_repository)],
) -> SystemSettingsResponse:
    row = await repository.get()
    return SystemSettingsResponse(blocking_min_duration_ms=row.blocking_min_duration_ms)


@router.patch("", response_model=SystemSettingsResponse)
async def update_system_settings(
    payload: SystemSettingsUpdateRequest,
    repository: Annotated[SystemSettingsRepository, Depends(get_system_settings_repository)],
) -> SystemSettingsResponse:
    row = await repository.update_blocking_min_duration_ms(payload.blocking_min_duration_ms)
    return SystemSettingsResponse(blocking_min_duration_ms=row.blocking_min_duration_ms)
