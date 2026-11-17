"""Servico compartilhado de envio de e-mail — usado por alertas, relatorios agendados, aviso de
expiracao de trial e pelo "enviar e-mail de teste" da tela de configuracoes.

Dois provedores, escolhidos em `system_settings.email_provider` (tela Configuracoes → Geral):

- `smtp` (default): `system_settings.smtp_*` primeiro; campo vazio cai no equivalente
  `ALERT_SMTP_*` do `.env` (mesma regra que `report_worker` ja aplicava). Porta 465 = SSL
  implicito; demais = STARTTLS.
- `brevo`: API transacional (`POST https://api.brevo.com/v3/smtp/email`, header `api-key`) via
  `httpx`. A chave fica cifrada (Fernet) em `system_settings.brevo_api_key_encrypted`.

Segredos (senha SMTP, chave Brevo) NUNCA sao logados — `EmailTransportConfig.__repr__` os oculta e
as mensagens de erro do Brevo so carregam o status HTTP e a mensagem devolvida pela API.
"""

from __future__ import annotations

import asyncio
import base64
import smtplib
from dataclasses import dataclass, field
from email.message import EmailMessage
from typing import Literal

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.config import Settings, get_settings
from app.core.logging import get_logger
from app.repositories.system_settings_repository import SystemSettingsRepository

logger = get_logger(__name__)

BREVO_API_URL = "https://api.brevo.com/v3/smtp/email"
DEFAULT_SENDER = "noreply@queryastra.local"

EmailProvider = Literal["smtp", "brevo"]


class EmailNotConfiguredError(RuntimeError):
    """Provedor escolhido sem os campos minimos (host SMTP / chave Brevo)."""


class EmailDeliveryError(RuntimeError):
    """Falha de entrega reportada pelo provedor."""


@dataclass(frozen=True, slots=True)
class EmailAttachment:
    filename: str
    content: bytes
    mime_type: str = "application/octet-stream"


@dataclass(frozen=True, slots=True)
class OutgoingEmail:
    recipients: list[str]
    subject: str
    text_body: str
    html_body: str | None = None
    attachments: list[EmailAttachment] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class EmailTransportConfig:
    """Configuracao resolvida (banco + fallback `.env`) do provedor de e-mail."""

    provider: EmailProvider
    sender_address: str
    sender_name: str | None = None
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = field(default=None, repr=False)
    brevo_api_key: str | None = field(default=None, repr=False)

    @property
    def is_configured(self) -> bool:
        if self.provider == "brevo":
            return bool(self.brevo_api_key)
        return bool(self.smtp_host)


def parse_recipients(raw: str | None) -> list[str]:
    """`"a@x.com, b@y.com"` -> `["a@x.com", "b@y.com"]` (formato de ALERT_EMAIL_TO)."""
    return [address.strip() for address in (raw or "").split(",") if address.strip()]


def dedupe_recipients(*groups: list[str] | None) -> list[str]:
    """Une listas de destinatarios preservando a ordem e ignorando duplicatas (case-insensitive)."""
    seen: set[str] = set()
    result: list[str] = []
    for group in groups:
        for address in group or []:
            cleaned = address.strip()
            key = cleaned.lower()
            if cleaned and key not in seen:
                seen.add(key)
                result.append(cleaned)
    return result


async def resolve_email_config(
    session: AsyncSession, env: Settings | None = None
) -> EmailTransportConfig:
    """Le `system_settings` e aplica o fallback do `.env` campo a campo."""
    env = env or get_settings()
    repo = SystemSettingsRepository(session)
    row = await repo.get()
    provider: EmailProvider = "brevo" if row.email_provider == "brevo" else "smtp"
    sender = row.smtp_from_address or env.alert_smtp_from or DEFAULT_SENDER
    if provider == "brevo":
        return EmailTransportConfig(
            provider="brevo",
            sender_address=sender,
            sender_name=row.email_from_name or "QueryAstra",
            brevo_api_key=repo.get_brevo_api_key_plain(row),
        )
    return EmailTransportConfig(
        provider="smtp",
        sender_address=sender,
        sender_name=row.email_from_name,
        smtp_host=row.smtp_host or env.alert_smtp_host,
        smtp_port=row.smtp_port or env.alert_smtp_port,
        smtp_username=row.smtp_username or env.alert_smtp_username,
        smtp_password=repo.get_smtp_password_plain(row) or env.alert_smtp_password,
    )


async def load_email_config(
    session_factory: async_sessionmaker[AsyncSession],
) -> EmailTransportConfig:
    async with session_factory() as session:
        return await resolve_email_config(session)


def _format_sender(config: EmailTransportConfig) -> str:
    if config.sender_name:
        return f"{config.sender_name} <{config.sender_address}>"
    return config.sender_address


def build_mime_message(config: EmailTransportConfig, email: OutgoingEmail) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = email.subject
    message["From"] = _format_sender(config)
    message["To"] = ", ".join(email.recipients)
    message.set_content(email.text_body)
    if email.html_body:
        message.add_alternative(email.html_body, subtype="html")
    for attachment in email.attachments:
        maintype, _, subtype = attachment.mime_type.partition("/")
        message.add_attachment(
            attachment.content,
            maintype=maintype or "application",
            subtype=subtype or "octet-stream",
            filename=attachment.filename,
        )
    return message


def _send_smtp_sync(config: EmailTransportConfig, email: OutgoingEmail) -> None:
    assert config.smtp_host is not None
    message = build_mime_message(config, email)
    use_ssl = config.smtp_port == 465
    smtp_class = smtplib.SMTP_SSL if use_ssl else smtplib.SMTP
    with smtp_class(config.smtp_host, config.smtp_port, timeout=10.0) as smtp:
        if not use_ssl:
            smtp.ehlo()
            smtp.starttls()
        if config.smtp_username and config.smtp_password:
            smtp.login(config.smtp_username, config.smtp_password)
        smtp.send_message(message, from_addr=config.sender_address, to_addrs=email.recipients)


def build_brevo_payload(config: EmailTransportConfig, email: OutgoingEmail) -> dict:
    sender: dict[str, str] = {"email": config.sender_address}
    if config.sender_name:
        sender["name"] = config.sender_name
    payload: dict = {
        "sender": sender,
        "to": [{"email": address} for address in email.recipients],
        "subject": email.subject,
        "textContent": email.text_body,
    }
    if email.html_body:
        payload["htmlContent"] = email.html_body
    if email.attachments:
        payload["attachment"] = [
            {"name": a.filename, "content": base64.b64encode(a.content).decode("ascii")}
            for a in email.attachments
        ]
    return payload


async def _send_brevo(
    config: EmailTransportConfig,
    email: OutgoingEmail,
    http_client: httpx.AsyncClient | None = None,
) -> None:
    assert config.brevo_api_key is not None
    headers = {
        "api-key": config.brevo_api_key,
        "accept": "application/json",
        "content-type": "application/json",
    }
    payload = build_brevo_payload(config, email)
    if http_client is not None:
        response = await http_client.post(BREVO_API_URL, json=payload, headers=headers)
    else:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(BREVO_API_URL, json=payload, headers=headers)
    if response.status_code >= 400:
        detail = ""
        try:
            body = response.json()
            if isinstance(body, dict):
                detail = str(body.get("message") or body.get("code") or "")
        except ValueError:
            detail = ""
        raise EmailDeliveryError(
            f"Brevo respondeu HTTP {response.status_code}" + (f": {detail}" if detail else "")
        )


async def send_email(
    config: EmailTransportConfig,
    email: OutgoingEmail,
    *,
    http_client: httpx.AsyncClient | None = None,
) -> None:
    """Envia `email` pelo provedor de `config`. Levanta `EmailNotConfiguredError` se faltar o
    minimo do provedor, `EmailDeliveryError`/excecoes de rede/SMTP em falha de entrega."""
    if not email.recipients:
        raise ValueError("Nenhum destinatario informado.")
    if not config.is_configured:
        raise EmailNotConfiguredError(
            "Chave da API Brevo nao configurada."
            if config.provider == "brevo"
            else "Servidor SMTP (host) nao configurado."
        )
    if config.provider == "brevo":
        await _send_brevo(config, email, http_client)
    else:
        await asyncio.to_thread(_send_smtp_sync, config, email)
    logger.info(
        "E-mail enviado via %s para %d destinatario(s): %s",
        config.provider,
        len(email.recipients),
        email.subject,
    )
