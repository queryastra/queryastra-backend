"""Testes de `app.services.email_service` — selecao de provedor, payload Brevo (httpx mockado via
`MockTransport`), SMTP (smtplib mockado) e resolucao de configuracao banco + `.env`."""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import httpx
import pytest

from app.services import email_service
from app.services.email_service import (
    BREVO_API_URL,
    EmailAttachment,
    EmailDeliveryError,
    EmailNotConfiguredError,
    EmailTransportConfig,
    OutgoingEmail,
    dedupe_recipients,
    parse_recipients,
    resolve_email_config,
    send_email,
)

_EMAIL = OutgoingEmail(
    recipients=["dba@example.com"],
    subject="Assunto",
    text_body="corpo",
    attachments=[EmailAttachment("r.pdf", b"%PDF", "application/pdf")],
)


async def test_brevo_provider_posts_transactional_api_with_api_key_header() -> None:
    captured: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["api_key"] = request.headers.get("api-key")
        captured["body"] = json.loads(request.content)
        return httpx.Response(201, json={"messageId": "<x>"})

    config = EmailTransportConfig(
        provider="brevo",
        sender_address="noreply@example.com",
        sender_name="QueryAstra",
        brevo_api_key="secret-key",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        await send_email(config, _EMAIL, http_client=client)

    assert captured["url"] == BREVO_API_URL
    assert captured["api_key"] == "secret-key"
    body = captured["body"]
    assert body["sender"] == {"email": "noreply@example.com", "name": "QueryAstra"}
    assert body["to"] == [{"email": "dba@example.com"}]
    assert body["subject"] == "Assunto"
    assert body["attachment"][0]["name"] == "r.pdf"


async def test_brevo_error_raises_without_leaking_api_key() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(401, json={"code": "unauthorized", "message": "Key not found"})

    config = EmailTransportConfig(
        provider="brevo", sender_address="a@example.com", brevo_api_key="super-secret"
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(EmailDeliveryError) as exc_info:
            await send_email(config, _EMAIL, http_client=client)
    assert "401" in str(exc_info.value)
    assert "super-secret" not in str(exc_info.value)
    assert "super-secret" not in repr(config)


async def test_unconfigured_provider_raises_not_configured() -> None:
    with pytest.raises(EmailNotConfiguredError):
        await send_email(EmailTransportConfig(provider="brevo", sender_address="a@x.com"), _EMAIL)
    with pytest.raises(EmailNotConfiguredError):
        await send_email(EmailTransportConfig(provider="smtp", sender_address="a@x.com"), _EMAIL)


async def test_smtp_provider_uses_starttls_and_login(monkeypatch: pytest.MonkeyPatch) -> None:
    events: list[tuple] = []

    class _FakeSMTP:
        def __init__(self, host: str, port: int, timeout: float) -> None:
            events.append(("connect", host, port))

        def __enter__(self) -> _FakeSMTP:
            return self

        def __exit__(self, *exc: object) -> None:
            return None

        def ehlo(self) -> None:
            events.append(("ehlo",))

        def starttls(self) -> None:
            events.append(("starttls",))

        def login(self, user: str, password: str) -> None:
            events.append(("login", user))

        def send_message(self, message: Any, from_addr: str, to_addrs: list[str]) -> None:
            events.append(("send", from_addr, tuple(to_addrs), message["Subject"]))

    monkeypatch.setattr(email_service.smtplib, "SMTP", _FakeSMTP)
    config = EmailTransportConfig(
        provider="smtp",
        sender_address="alerts@example.com",
        smtp_host="smtp.example.com",
        smtp_port=587,
        smtp_username="user",
        smtp_password="pw",
    )
    await send_email(config, _EMAIL)

    assert events[0] == ("connect", "smtp.example.com", 587)
    assert ("starttls",) in events
    assert ("login", "user") in events
    assert events[-1] == ("send", "alerts@example.com", ("dba@example.com",), "Assunto")


class _FakeSettingsRepo:
    def __init__(self, row: SimpleNamespace) -> None:
        self._row = row

    async def get(self) -> SimpleNamespace:
        return self._row

    def get_smtp_password_plain(self, row: SimpleNamespace) -> str | None:
        return row.smtp_password

    def get_brevo_api_key_plain(self, row: SimpleNamespace) -> str | None:
        return row.brevo_key


def _row(**overrides: Any) -> SimpleNamespace:
    base = dict(
        email_provider="smtp",
        smtp_host=None,
        smtp_port=587,
        smtp_username=None,
        smtp_password=None,
        smtp_from_address=None,
        email_from_name=None,
        brevo_key=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _env(**overrides: Any) -> Any:
    base = dict(
        alert_smtp_host="env-smtp.example.com",
        alert_smtp_port=2525,
        alert_smtp_username="env-user",
        alert_smtp_password="env-pw",
        alert_smtp_from="env@example.com",
    )
    base.update(overrides)
    return SimpleNamespace(**base)


async def test_resolve_smtp_prefers_db_and_falls_back_to_env(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    row = _row(smtp_host="db-smtp.example.com", smtp_username=None)
    monkeypatch.setattr(
        email_service, "SystemSettingsRepository", lambda _s: _FakeSettingsRepo(row)
    )

    config = await resolve_email_config(object(), _env())  # type: ignore[arg-type]

    assert config.provider == "smtp"
    assert config.smtp_host == "db-smtp.example.com"
    assert config.smtp_username == "env-user"
    assert config.smtp_password == "env-pw"
    assert config.sender_address == "env@example.com"


async def test_resolve_brevo_provider(monkeypatch: pytest.MonkeyPatch) -> None:
    row = _row(email_provider="brevo", brevo_key="k", smtp_from_address="from@example.com")
    monkeypatch.setattr(
        email_service, "SystemSettingsRepository", lambda _s: _FakeSettingsRepo(row)
    )

    config = await resolve_email_config(object(), _env())  # type: ignore[arg-type]

    assert config.provider == "brevo"
    assert config.brevo_api_key == "k"
    assert config.sender_address == "from@example.com"
    assert config.is_configured


def test_recipient_helpers() -> None:
    assert parse_recipients(" a@x.com, ,b@x.com ") == ["a@x.com", "b@x.com"]
    assert dedupe_recipients(["a@x.com"], ["A@x.com", "c@x.com"], None) == ["a@x.com", "c@x.com"]
