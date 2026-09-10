"""Worker de avaliacao de alertas.

Por ciclo: le `alert_rules` ativas (`app.repositories.alerts_repository`), avalia contra a
amostra mais recente de cada instancia ativa (threshold simples: 1 metrica + comparacao +
threshold_warning/critical — CORTE DE ESCOPO: sem anomaly detection, sem os 5 metodos de
agregacao), aplica supressao por estado (nao reabre um `Alert` ja `open` para a mesma condicao)
e `alert_silences` (silenciamento proativo, independente do estado do alerta), publica um evento
no `app.core.events.event_bus` (push via `/ws/live`) e dispara `AlertNotification` via webhook e/ou
e-mail (em paralelo, cada canal independente — a falha de um nunca impede o outro) respeitando o
lembrete por tempo em alerta critico nao reconhecido.

CORTES DE ESCOPO (ver tambem docstring do modulo de alertas):
- So threshold simples, dois canais de notificacao (webhook + e-mail), toda mudanca de
  severidade notifica em ambos os canais habilitados.
- Sem escalonamento multi-canal (ex.: e-mail so depois de N minutos sem ack), so lembrete por
  tempo pro critico nao reconhecido.
- Sem blackout recorrente semanal (`instance_maintenance_windows`) aqui — so `alert_silences`
  pontual. TODO: cruzar com `InstanceMaintenanceWindow.suppress_polling`-like flag de alerta se
  o modelo ganhar esse campo no futuro.
- Avaliacao so cobre `target_type='instance'` (regras com `target_type='server'` sao ignoradas
  nesta etapa — TODO: coleta de host ja existe (`app/api/v1/hosts.py`), mas a avaliacao de
  alertas contra metricas de servidor fica para uma proxima etapa).
"""

from __future__ import annotations

import asyncio
import operator
import smtplib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from email.message import EmailMessage
from typing import Literal

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.events import event_bus
from app.core.logging import get_logger
from app.models.alert import Alert, AlertNotification, AlertRule
from app.models.instance import Instance
from app.repositories.alerts_repository import AlertsRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository

logger = get_logger(__name__)

_COMPARATORS: dict[str, Callable[[float, float], bool]] = {
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
}

_AlertsRepositoryFactory = Callable[[AsyncSession], AlertsRepository]
_MetricSamplesRepositoryFactory = Callable[[AsyncSession], MetricSamplesRepository]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_NotificationSender = Callable[[str, dict], Awaitable[None]]

NotificationKind = Literal["triggered", "level_changed", "reminder", "recovered"]


@dataclass(frozen=True, slots=True)
class EmailSettings:
    """Configuracao resolvida do canal de e-mail (`app.core.config.Settings.alert_smtp_*`).

    So construida pelo caller (`app/main.py`) quando `alert_smtp_host` esta preenchido — a
    ausencia de `EmailSettings` (`None` em `AlertingWorker`) e o sinal de "canal desabilitado",
    mesmo padrao ja usado por `webhook_url=None`.
    """

    host: str
    port: int
    username: str | None
    password: str | None
    sender: str
    recipients: list[str]


_EmailSender = Callable[[dict, EmailSettings], Awaitable[None]]


def _classify_severity(
    value: float, comparison: str, threshold_warning: float | None, threshold_critical: float | None
) -> Literal["warning", "critical"] | None:
    cmp = _COMPARATORS[comparison]
    if threshold_critical is not None and cmp(value, threshold_critical):
        return "critical"
    if threshold_warning is not None and cmp(value, threshold_warning):
        return "warning"
    return None


async def _default_webhook_sender(url: str, payload: dict) -> None:
    async with httpx.AsyncClient(timeout=5.0) as client:
        response = await client.post(url, json=payload)
        response.raise_for_status()


def _build_email_message(payload: dict, settings: EmailSettings) -> EmailMessage:
    message = EmailMessage()
    message["Subject"] = f"[{payload['severity'].upper()}] {payload['rule_name']}"
    message["From"] = settings.sender
    message["To"] = ", ".join(settings.recipients)
    body = "\n".join(f"{key}: {value}" for key, value in payload.items())
    message.set_content(body)
    return message


def _send_email_sync(payload: dict, settings: EmailSettings) -> None:
    """Envio sincrono via `smtplib` — chamado atraves de `asyncio.to_thread` pelo sender async
    default, ja que `smtplib` nao tem API assincrona nativa."""
    message = _build_email_message(payload, settings)
    with smtplib.SMTP(settings.host, settings.port, timeout=5.0) as smtp:
        smtp.starttls()
        if settings.username and settings.password:
            smtp.login(settings.username, settings.password)
        smtp.send_message(message)


async def _default_email_sender(payload: dict, settings: EmailSettings) -> None:
    await asyncio.to_thread(_send_email_sync, payload, settings)


class AlertingWorker:
    """Avalia alert_rules de metrica simples contra a amostra mais recente de instancias
    ativas. So target_type='instance' nesta etapa - coleta de host ('server') nao existe."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        interval_seconds: int,
        critical_reminder_interval_seconds: int,
        *,
        webhook_url: str | None = None,
        email_settings: EmailSettings | None = None,
        alerts_repository_factory: _AlertsRepositoryFactory = AlertsRepository,
        metric_samples_repository_factory: (
            _MetricSamplesRepositoryFactory
        ) = MetricSamplesRepository,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        notification_sender: _NotificationSender = _default_webhook_sender,
        email_sender: _EmailSender = _default_email_sender,
    ) -> None:
        self._session_factory = session_factory
        self._interval_seconds = interval_seconds
        self._critical_reminder_interval_seconds = critical_reminder_interval_seconds
        self._webhook_url = webhook_url
        self._email_settings = email_settings
        self._alerts_repository_factory = alerts_repository_factory
        self._metric_samples_repository_factory = metric_samples_repository_factory
        self._instances_repository_factory = instances_repository_factory
        self._notification_sender = notification_sender
        self._email_sender = email_sender
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("AlertingWorker iniciado interval=%ss", self._interval_seconds)
        while not self._stop_event.is_set():
            try:
                await self._run_cycle()
            except Exception:  # noqa: BLE001 - ciclo nunca derruba o worker
                logger.exception("Erro inesperado em AlertingWorker._run_cycle")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass
        logger.info("AlertingWorker finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _run_cycle(self) -> None:
        async with self._session_factory() as session:
            alerts_repo = self._alerts_repository_factory(session)
            metric_repo = self._metric_samples_repository_factory(session)
            instances_repo = self._instances_repository_factory(session)

            rules = await alerts_repo.list_active_rules()
            simple_rules = [
                r for r in rules if r.target_type == "instance" and r.metric_name and r.comparison
            ]
            if not simple_rules:
                return

            instances = await instances_repo.list_active()

            for rule in simple_rules:
                for instance in instances:
                    try:
                        await self._evaluate(alerts_repo, metric_repo, rule, instance)
                    except Exception:  # noqa: BLE001 - 1 instancia/regra nunca derruba o ciclo
                        logger.exception(
                            "Erro ao avaliar alert_rule_id=%s instance_id=%s", rule.id, instance.id
                        )
                        await session.rollback()

    async def _evaluate(
        self,
        alerts_repo: AlertsRepository,
        metric_repo: MetricSamplesRepository,
        rule: AlertRule,
        instance: Instance,
    ) -> None:
        assert rule.metric_name is not None
        assert rule.comparison is not None
        row = await metric_repo.get_latest_value("instance", instance.id, rule.metric_name)
        if row is None:
            return

        severity = _classify_severity(
            row.value, rule.comparison, rule.threshold_warning, rule.threshold_critical
        )
        open_alert = await alerts_repo.get_open_alert(rule.id, instance.id)
        now = datetime.now(UTC)

        if severity is None:
            if open_alert is not None:
                open_alert.state = "closed"
                open_alert.resolved_at = now
                await alerts_repo.update_alert(open_alert)
                self._publish_alert_event(open_alert, kind="recovered")
                await self._send_notification(
                    alerts_repo, open_alert, rule, kind="recovered", value=row.value
                )
            return

        if open_alert is None:
            if await alerts_repo.get_active_silence("instance", instance.id, rule.id) is not None:
                return
            created = await alerts_repo.create_alert(
                Alert(
                    alert_rule_id=rule.id,
                    target_type="instance",
                    target_id=instance.id,
                    severity=severity,
                )
            )
            self._publish_alert_event(created, kind="triggered")
            await self._send_notification(
                alerts_repo, created, rule, kind="triggered", value=row.value
            )
            return

        if open_alert.severity == severity:
            due_for_reminder = (
                severity == "critical"
                and open_alert.state == "open"
                and (
                    open_alert.last_reminder_at is None
                    or (now - open_alert.last_reminder_at).total_seconds()
                    >= self._critical_reminder_interval_seconds
                )
            )
            if due_for_reminder:
                open_alert.last_reminder_at = now
                await alerts_repo.update_alert(open_alert)
                self._publish_alert_event(open_alert, kind="reminder")
                await self._send_notification(
                    alerts_repo, open_alert, rule, kind="reminder", value=row.value
                )
            return

        # severidade mudou - reabre visibilidade se estava reconhecido (escalada nao deve
        # ficar escondida atras de um ack antigo, sem isso nunca mais recebe lembrete).
        open_alert.severity = severity
        open_alert.last_reminder_at = None
        if open_alert.state == "acknowledged":
            open_alert.state = "open"
            open_alert.acknowledged_at = None
        await alerts_repo.update_alert(open_alert)
        self._publish_alert_event(open_alert, kind="level_changed")
        await self._send_notification(
            alerts_repo, open_alert, rule, kind="level_changed", value=row.value
        )

    def _publish_alert_event(self, alert: Alert, *, kind: NotificationKind) -> None:
        """Publica no `event_bus` (push via `/ws/live`) sempre que um `Alert` e criado ou
        atualizado — independente de silenciamento/falha de envio dos canais de notificacao, que
        sao responsabilidade de `_send_notification`."""
        event_type = "alert_created" if kind == "triggered" else "alert_updated"
        event_bus.publish({"type": event_type, "alert_id": str(alert.id), "kind": kind})

    async def _send_notification(
        self,
        alerts_repo: AlertsRepository,
        alert: Alert,
        rule: AlertRule,
        *,
        kind: NotificationKind,
        value: float,
    ) -> None:
        # Silence suprime notificacao mesmo de alerta ja aberto (nao so na criacao) - o
        # model documenta silence como "independente do estado de qualquer alerta ja disparado".
        active_silence = await alerts_repo.get_active_silence(
            alert.target_type, alert.target_id, rule.id
        )
        if active_silence is not None:
            return

        payload = {
            "kind": kind,
            "alert_id": str(alert.id),
            "alert_rule_id": str(rule.id),
            "rule_name": rule.name,
            "target_type": alert.target_type,
            "target_id": str(alert.target_id),
            "metric_name": rule.metric_name,
            "value": value,
            "severity": alert.severity,
            "triggered_at": alert.triggered_at.isoformat(),
        }

        # Webhook e e-mail sao independentes: disparados em paralelo, a falha (ou ausencia de
        # configuracao) de um nunca impede o outro. So grava `AlertNotification` para o canal que
        # realmente foi enviado com sucesso (mesmo padrao ja usado pelo webhook antes deste
        # canal e-mail existir).
        channel_results = await asyncio.gather(
            self._try_send_webhook(payload),
            self._try_send_email(payload),
        )
        sent_channels = [channel for channel, sent in channel_results if sent]

        if not any(channel is not None for channel, _sent in channel_results):
            logger.info(
                "Notificacao de alerta (nenhum canal habilitado) kind=%s alert_id=%s",
                kind,
                alert.id,
            )

        for channel in sent_channels:
            await alerts_repo.create_alert_notification(
                AlertNotification(alert_id=alert.id, channel=channel, payload=payload)
            )

    async def _try_send_webhook(self, payload: dict) -> tuple[str | None, bool]:
        """`(canal, enviado)` — canal `None` quando o webhook esta desabilitado (sem URL)."""
        if self._webhook_url is None:
            return (None, False)
        try:
            await self._notification_sender(self._webhook_url, payload)
        except Exception:  # noqa: BLE001 - falha de notificacao nunca derruba a avaliacao
            logger.exception("Falha ao enviar webhook de alerta alert_id=%s", payload["alert_id"])
            return ("webhook", False)
        return ("webhook", True)

    async def _try_send_email(self, payload: dict) -> tuple[str | None, bool]:
        """`(canal, enviado)` — canal `None` quando o e-mail esta desabilitado (sem SMTP host)."""
        if self._email_settings is None:
            return (None, False)
        try:
            await self._email_sender(payload, self._email_settings)
        except Exception:  # noqa: BLE001 - falha de notificacao nunca derruba a avaliacao
            logger.exception("Falha ao enviar e-mail de alerta alert_id=%s", payload["alert_id"])
            return ("email", False)
        return ("email", True)
