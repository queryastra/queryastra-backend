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

from app.api.deps import CurrentUser, get_current_user, get_db_session, require_admin
from app.models.system_settings import SystemSettings
from app.repositories.system_settings_repository import SystemSettingsRepository

router = APIRouter()


class SystemSettingsResponse(BaseModel):
    blocking_min_duration_ms: int
    sleeping_min_duration_ms: int
    smtp_host: str | None
    smtp_port: int
    smtp_username: str | None
    smtp_from_address: str | None
    smtp_configured: bool


class SystemSettingsUpdateRequest(BaseModel):
    """Atualização PARCIAL — só o(s) campo(s) presentes (não `None`) são gravados."""

    blocking_min_duration_ms: int | None = Field(default=None, ge=0)
    sleeping_min_duration_ms: int | None = Field(default=None, ge=0)
    # SMTP — enviado em bloco quando o card SMTP for salvo
    smtp_host: str | None = Field(default=None)
    smtp_port: int | None = Field(default=None, ge=1, le=65535)
    smtp_username: str | None = Field(default=None)
    smtp_password: str | None = Field(default=None)
    smtp_from_address: str | None = Field(default=None)
    smtp_save: bool = Field(default=False)


def get_system_settings_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> SystemSettingsRepository:
    return SystemSettingsRepository(session)


def _to_response(row: SystemSettings) -> SystemSettingsResponse:
    return SystemSettingsResponse(
        blocking_min_duration_ms=row.blocking_min_duration_ms,
        sleeping_min_duration_ms=row.sleeping_min_duration_ms,
        smtp_host=row.smtp_host,
        smtp_port=row.smtp_port,
        smtp_username=row.smtp_username,
        smtp_from_address=row.smtp_from_address,
        smtp_configured=bool(row.smtp_password_encrypted),
    )


@router.get("", response_model=SystemSettingsResponse)
async def get_system_settings(
    repository: Annotated[SystemSettingsRepository, Depends(get_system_settings_repository)],
    _: Annotated[CurrentUser, Depends(get_current_user)],
) -> SystemSettingsResponse:
    row = await repository.get()
    return _to_response(row)


@router.patch("", response_model=SystemSettingsResponse)
async def update_system_settings(
    payload: SystemSettingsUpdateRequest,
    repository: Annotated[SystemSettingsRepository, Depends(get_system_settings_repository)],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> SystemSettingsResponse:
    row = await repository.get()
    if payload.blocking_min_duration_ms is not None:
        row = await repository.update_blocking_min_duration_ms(payload.blocking_min_duration_ms)
    if payload.sleeping_min_duration_ms is not None:
        row = await repository.update_sleeping_min_duration_ms(payload.sleeping_min_duration_ms)
    if payload.smtp_save:
        row = await repository.update_smtp_settings(
            host=payload.smtp_host,
            port=payload.smtp_port if payload.smtp_port is not None else 587,
            username=payload.smtp_username,
            raw_password=payload.smtp_password,
            from_address=payload.smtp_from_address,
        )
    return _to_response(row)
