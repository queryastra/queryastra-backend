"""Endpoint de preferências globais editáveis — tela "Configurações → Geral" no frontend.

Lê/grava a linha singleton de `system_settings` (ver `app.models.system_settings`). Sem
restrição de acesso hoje (será limitada por RBAC quando essa feature existir, Fase 4). Uma
mudança aqui vale a partir do PRÓXIMO ciclo de poll de cada instância — `PollingWorker` relê o
valor a cada ciclo (não cacheado como `app.core.config.Settings`), sem exigir restart do backend.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_db_session, require_admin
from app.models.system_settings import SystemSettings
from app.repositories.system_settings_repository import SystemSettingsRepository
from app.repositories.users_repository import UsersRepository
from app.services.audit_service import record_audit
from app.services.auth_service import verify_password
from app.services.email_service import (
    EmailNotConfiguredError,
    OutgoingEmail,
    resolve_email_config,
    send_email,
)

router = APIRouter()


class SystemSettingsResponse(BaseModel):
    blocking_min_duration_ms: int
    sleeping_min_duration_ms: int
    smtp_host: str | None
    smtp_port: int
    smtp_username: str | None
    smtp_from_address: str | None
    smtp_configured: bool
    email_provider: str
    email_from_name: str | None
    brevo_configured: bool
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
    # Provedor de e-mail — enviado em bloco quando o card "Envio de e-mail" for salvo.
    # brevo_api_key: None preserva a chave armazenada; "" remove.
    email_provider: str | None = Field(default=None, pattern="^(smtp|brevo)$")
    brevo_api_key: str | None = Field(default=None)
    email_from_name: str | None = Field(default=None, max_length=200)
    # Remetente (vale para os dois provedores; e o mesmo `smtp_from_address`). None preserva.
    email_from_address: str | None = Field(default=None, max_length=320)
    email_provider_save: bool = Field(default=False)
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
        email_provider=row.email_provider or "smtp",
        email_from_name=row.email_from_name,
        brevo_configured=bool(row.brevo_api_key_encrypted),
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
    provider: str | None = None


@router.post("/test-smtp", response_model=TestSmtpResponse)
async def test_smtp(
    payload: TestSmtpRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> TestSmtpResponse:
    """Envia um e-mail de teste pelo provedor configurado (SMTP ou Brevo). O nome do endpoint
    foi mantido por compatibilidade com o frontend; vale para os dois provedores."""
    config = await resolve_email_config(session)
    email = OutgoingEmail(
        recipients=[payload.to.strip()],
        subject="[QueryAstra] Teste de configuração de e-mail",
        text_body=(
            "Este é um e-mail de teste enviado pelo QueryAstra para verificar as configurações "
            f"de envio (provedor: {config.provider}).\n\n"
            "Se você recebeu esta mensagem, as configurações estão corretas."
        ),
    )
    try:
        await send_email(config, email)
        return TestSmtpResponse(ok=True, provider=config.provider)
    except EmailNotConfiguredError as exc:
        return TestSmtpResponse(ok=False, error=str(exc), provider=config.provider)
    except Exception as exc:  # noqa: BLE001 - erro devolvido ao admin, nunca o segredo
        return TestSmtpResponse(ok=False, error=str(exc), provider=config.provider)


class DeleteAccountRequest(BaseModel):
    password: str


@router.delete("/account", status_code=status.HTTP_204_NO_CONTENT)
async def delete_own_account(
    payload: DeleteAccountRequest,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> Response:
    """Remove a conta do usuário autenticado. Requer confirmação de senha.

    Bloqueia se o usuário for o único admin ativo (evita lock-out do sistema).
    """
    users_repo = UsersRepository(session)
    user = await users_repo.get_by_id(current_user.user_id)
    if user is None or not verify_password(payload.password, user.hashed_password):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Senha incorreta.")

    if user.role == "admin":
        all_users = await users_repo.list_all()
        active_admins = [
            u for u in all_users if u.role == "admin" and u.is_active and u.id != user.id
        ]
        if not active_admins:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Não é possível remover a conta: você é o único administrador ativo.",
            )

    await record_audit(session, actor=user.username, action="account.deleted")
    await users_repo.delete(current_user.user_id)
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


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
    if payload.email_provider_save:
        row = await repository.update_email_provider_settings(
            provider=payload.email_provider or "smtp",
            raw_brevo_api_key=payload.brevo_api_key,
            from_name=(payload.email_from_name or "").strip() or None,
            from_address=(
                payload.email_from_address.strip()
                if payload.email_from_address is not None
                else None
            ),
        )
    if payload.ai_save:
        row = await repository.update_ai_settings(
            provider=payload.ai_provider,
            raw_api_key=payload.ai_api_key,
            base_url=payload.ai_base_url,
            model=payload.ai_model,
        )
    return _to_response(row)
