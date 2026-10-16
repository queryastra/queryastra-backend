"""Endpoint de preferências globais editáveis — tela "Configurações → Geral" no frontend.

Lê/grava a linha singleton de `system_settings` (ver `app.models.system_settings`). Sem
restrição de acesso hoje (será limitada por RBAC quando essa feature existir, Fase 4). Uma
mudança aqui vale a partir do PRÓXIMO ciclo de poll de cada instância — `PollingWorker` relê o
valor a cada ciclo (não cacheado como `app.core.config.Settings`), sem exigir restart do backend.
"""

from __future__ import annotations

import asyncio
import smtplib
from email.message import EmailMessage
from typing import Annotated

from fastapi import APIRouter, Depends
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_db_session, require_admin
from app.core.config import get_settings
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
    ai_provider: str | None
    ai_base_url: str | None
    ai_model: str | None
    ai_configured: bool


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
    # IA (BYOK) — enviado em bloco quando o card de IA for salvo
    ai_provider: str | None = Field(default=None)
    ai_api_key: str | None = Field(default=None)
    ai_base_url: str | None = Field(default=None)
    ai_model: str | None = Field(default=None)
    ai_save: bool = Field(default=False)


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
        ai_provider=row.ai_provider,
        ai_base_url=row.ai_base_url,
        ai_model=row.ai_model,
        ai_configured=bool(row.ai_api_key_encrypted),
    )


@router.get("", response_model=SystemSettingsResponse)
async def get_system_settings(
    repository: Annotated[SystemSettingsRepository, Depends(get_system_settings_repository)],
    _: Annotated[CurrentUser, Depends(get_current_user)],
) -> SystemSettingsResponse:
    row = await repository.get()
    return _to_response(row)


class TestSmtpRequest(BaseModel):
    to: str


class TestSmtpResponse(BaseModel):
    ok: bool
    error: str | None = None


def _send_test_email_sync(
    host: str,
    port: int,
    username: str | None,
    password: str | None,
    sender: str,
    to: str,
) -> None:
    msg = EmailMessage()
    msg["Subject"] = "[QueryAstra] Teste de configuração SMTP"
    msg["From"] = sender
    msg["To"] = to
    msg.set_content(
        "Este é um e-mail de teste enviado pelo QueryAstra para verificar as configurações SMTP.\n\n"
        "Se você recebeu esta mensagem, as configurações estão corretas."
    )
    use_ssl = port == 465
    smtp_class = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with smtp_class(host, port, timeout=10) as smtp:
        if not use_ssl:
            smtp.ehlo()
            smtp.starttls()
        if username and password:
            smtp.login(username, password)
        smtp.send_message(msg)


@router.post("/test-smtp", response_model=TestSmtpResponse)
async def test_smtp(
    payload: TestSmtpRequest,
    repository: Annotated[SystemSettingsRepository, Depends(get_system_settings_repository)],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> TestSmtpResponse:
    row = await repository.get()
    env = get_settings()

    host = row.smtp_host or env.alert_smtp_host
    if not host:
        return TestSmtpResponse(ok=False, error="SMTP não configurado — defina o servidor (host) primeiro.")

    port = row.smtp_port or env.alert_smtp_port
    username = row.smtp_username or env.alert_smtp_username
    password = repository.get_smtp_password_plain(row) or env.alert_smtp_password
    sender = row.smtp_from_address or env.alert_smtp_from or "noreply@queryastra.local"

    try:
        await asyncio.to_thread(
            _send_test_email_sync,
            host=host,
            port=port,
            username=username,
            password=password,
            sender=sender,
            to=payload.to,
        )
        return TestSmtpResponse(ok=True)
    except Exception as exc:
        return TestSmtpResponse(ok=False, error=str(exc))


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
    if payload.ai_save:
        row = await repository.update_ai_settings(
            provider=payload.ai_provider,
            raw_api_key=payload.ai_api_key,
            base_url=payload.ai_base_url,
            model=payload.ai_model,
        )
    return _to_response(row)
