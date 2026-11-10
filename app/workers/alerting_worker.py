"""Worker de avaliacao de alertas.

Por ciclo: le `alert_rules` ativas (`app.repositories.alerts_repository`), avalia cada uma contra
o alvo (hoje so `target_type='instance'`) conforme seu `rule_type` (Fase 2,
`Docs/product-vision-and-roadmap.md`):

- `threshold` (default, preserva 100% do comportamento anterior a Fase 2): 1 metrica +
  comparacao + threshold_warning/critical, contra a amostra mais recente de `metric_samples`.
  `freespace`/`replication_lag` (item 2) NAO sao um `rule_type` proprio — sao so um `metric_name`
  novo, coletado por `ExtendedHealthMetricsSupervisor` e avaliado aqui como `threshold` comum.
- `anomaly` (item 1): baseline estatistico simples (media/desvio padrao dos ultimos
  `baseline_window_days`, default 7) — severidade por desvio (2 sigma=warning, 3 sigma=critical),
  sem alerta se `sample_count < 20` (baseline instavel demais pra confiar, nao inventa alerta).
- `custom_sql` (item 2): executa `AlertRule.custom_sql` (ja validado por
  `app.services.custom_sql_guard` no cadastro) contra a propria instancia via
  `Collector.run_scalar_query`, avalia o valor escalar como `threshold` comum.
- `blocking_recurrence` (item 4): conta incidentes em `blocking_incidents` dentro de
  `recurrence_window_days`, avalia a CONTAGEM como `threshold` comum (`threshold_warning`/
  `_critical` viram contagem de incidentes, nao valor de metrica).
- `plan_compile_timeout` (item 5): NAO avaliado aqui — alimentado por
  `app.workers.plan_compile_timeout_supervisor.PlanCompileTimeoutSupervisor`, que cria/atualiza
  `Alert` diretamente a partir do parsing do plano XML.

Alem da classificacao de severidade, cada avaliacao passa por 3 mecanismos transversais (Fase 2,
itens 3/4), aplicados a QUALQUER `rule_type` que produza uma severidade:

- **`reset_mode`** (item 3, 4 dos 5 modos do SAM/Orion — "condicao customizada" fora de escopo):
  `auto_when_false` (default, comportamento anterior — fecha quando a condicao para de ser
  verdadeira), `auto_after_duration` (fecha sozinho apos `reset_after_seconds` mesmo que a
  condicao continue verdadeira — o proximo ciclo abre um alerta NOVO se ainda verdadeiro),
  `no_reset_every_poll` (nunca fecha sozinho por condicao falsa nem por duracao — notifica a
  cada ciclo enquanto verdadeiro, sem nunca criar um 2o `Alert` pro mesmo par regra/alvo),
  `no_reset_manual` (nunca fecha sozinho — so via `POST /alerts/{id}/close`).
- **Escalonamento em niveis** (item 3): cada nivel 2+ (`AlertsRepository.list_escalation_levels`)
  dispara notificacao propria se o `Alert` continuar `open` (nao reconhecido) por
  `wait_minutes` desde `triggered_at` - reconhecer um alerta congela o escalonamento no nivel
  atual (mesmo racional do lembrete critico ja existente).
- **Recorrencia** (item 4): ao criar um `Alert` NOVO (nao reabertura), se a regra tiver
  `recurrence_threshold_count`/`recurrence_window_days` configurados, conta quantos `Alert`
  desse par (regra, alvo) ja dispararam na janela — se atingir o limite, forca
  `severity="critical"` e marca a descricao, independente da severidade normal da regra. Nao
  aplicado a `rule_type="blocking_recurrence"` (a propria regra ja E uma medida de recorrencia).

CORTES DE ESCOPO (ver tambem docstring do modulo de alertas / roadmap):
- Modo de reset "condicao customizada" (5o do SAM/Orion) fora de escopo.
- Escalonamento so usa os 2 canais ja existentes (webhook + e-mail), com destinatarios de
  e-mail por nivel opcionais (sem canal novo, ex. SMS).
- Sem blackout recorrente semanal (`instance_maintenance_windows`) aqui — so `alert_silences`
  pontual.
- Avaliacao so cobre `target_type='instance'` (regras com `target_type='server'` sao ignoradas).
"""

from __future__ import annotations

import asyncio
import operator
import smtplib
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from email.message import EmailMessage
from typing import Literal

import httpx
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.collectors.base import Collector
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.events import event_bus
from app.core.logging import get_logger
from app.models.alert import Alert, AlertNotification, AlertRule
from app.models.instance import Instance
from app.repositories.alerts_repository import AlertsRepository
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

_COMPARATORS: dict[str, Callable[[float, float], bool]] = {
    "gt": operator.gt,
    "gte": operator.ge,
    "lt": operator.lt,
    "lte": operator.le,
}

# Item 1 (anomaly): abaixo desse numero de amostras, o baseline e instavel demais para confiar -
# nenhum alerta e emitido (nao "insuficiente", nao inventa um alerta a partir de pouco dado).
_ANOMALY_MIN_SAMPLE_COUNT = 20
_ANOMALY_WARNING_SIGMA = 2.0
_ANOMALY_CRITICAL_SIGMA = 3.0

_AlertsRepositoryFactory = Callable[[AsyncSession], AlertsRepository]
_MetricSamplesRepositoryFactory = Callable[[AsyncSession], MetricSamplesRepository]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_BlockingIncidentsRepositoryFactory = Callable[[AsyncSession], BlockingIncidentsRepository]
_NotificationSender = Callable[[str, dict], Awaitable[None]]
_CollectorFactory = Callable[..., Collector]

NotificationKind = Literal[
    "triggered",
    "level_changed",
    "reminder",
    "recovered",
    "repeated",
    "escalated",
    "recurrence_escalated",
]


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


def _classify_anomaly_severity(
    value: float, mean: float, stddev: float
) -> Literal["warning", "critical"] | None:
    if stddev <= 0:
        return None
    deviation = abs(value - mean) / stddev
    if deviation >= _ANOMALY_CRITICAL_SIGMA:
        return "critical"
    if deviation >= _ANOMALY_WARNING_SIGMA:
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
    """Avalia `alert_rules` ativas contra `target_type='instance'` — dispatch por `rule_type`,
    ver docstring do modulo para o desenho completo de cada tipo e dos 3 mecanismos
    transversais (reset_mode/escalonamento/recorrencia)."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        interval_seconds: int,
        critical_reminder_interval_seconds: int,
        *,
        webhook_url: str | None = None,
        email_settings: EmailSettings | None = None,
        custom_sql_timeout_seconds: float = 5.0,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        alerts_repository_factory: _AlertsRepositoryFactory = AlertsRepository,
        metric_samples_repository_factory: (
            _MetricSamplesRepositoryFactory
        ) = MetricSamplesRepository,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        blocking_incidents_repository_factory: (
            _BlockingIncidentsRepositoryFactory
        ) = BlockingIncidentsRepository,
        notification_sender: _NotificationSender = _default_webhook_sender,
        email_sender: _EmailSender = _default_email_sender,
    ) -> None:
        self._session_factory = session_factory
        self._interval_seconds = interval_seconds
        self._critical_reminder_interval_seconds = critical_reminder_interval_seconds
        self._webhook_url = webhook_url
        self._email_settings = email_settings
        self._custom_sql_timeout_seconds = custom_sql_timeout_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._alerts_repository_factory = alerts_repository_factory
        self._metric_samples_repository_factory = metric_samples_repository_factory
        self._instances_repository_factory = instances_repository_factory
        self._blocking_incidents_repository_factory = blocking_incidents_repository_factory
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
            blocking_repo = self._blocking_incidents_repository_factory(session)

            rules = await alerts_repo.list_active_rules()
            instance_rules = [r for r in rules if r.target_type == "instance"]
            if not instance_rules:
                return

            instances = await instances_repo.list_active()

            for rule in instance_rules:
                for instance in instances:
                    try:
                        await self._evaluate_rule(
                            alerts_repo, metric_repo, blocking_repo, rule, instance
                        )
                    except Exception:  # noqa: BLE001 - 1 instancia/regra nunca derruba o ciclo
                        logger.exception(
                            "Erro ao avaliar alert_rule_id=%s instance_id=%s", rule.id, instance.id
                        )
                        await session.rollback()

    async def _evaluate_rule(
        self,
        alerts_repo: AlertsRepository,
        metric_repo: MetricSamplesRepository,
        blocking_repo: BlockingIncidentsRepository,
        rule: AlertRule,
        instance: Instance,
    ) -> None:
        rule_type = rule.rule_type or "threshold"

        if rule_type == "threshold":
            if not rule.metric_name or not rule.comparison:
                return
            row = await metric_repo.get_latest_value("instance", instance.id, rule.metric_name)
            if row is None:
                return
            value = float(row.value)
            severity = _classify_severity(
                value, rule.comparison, rule.threshold_warning, rule.threshold_critical
            )

        elif rule_type == "anomaly":
            if not rule.metric_name:
                return
            window_days = rule.baseline_window_days or 7
            since = datetime.now(UTC) - timedelta(days=window_days)
            stats = await metric_repo.get_baseline_stats(
                "instance", instance.id, rule.metric_name, since
            )
            if (
                stats is None
                or stats.sample_count < _ANOMALY_MIN_SAMPLE_COUNT
                or stats.stddev is None
            ):
                return
            latest = await metric_repo.get_latest_value("instance", instance.id, rule.metric_name)
            if latest is None:
                return
            value = float(latest.value)
            severity = _classify_anomaly_severity(value, float(stats.mean), float(stats.stddev))

        elif rule_type == "custom_sql":
            if not rule.custom_sql or not rule.comparison:
                return
            value_or_none = await self._run_custom_sql(instance, rule.custom_sql)
            if value_or_none is None:
                return
            value = value_or_none
            severity = _classify_severity(
                value, rule.comparison, rule.threshold_warning, rule.threshold_critical
            )

        elif rule_type == "blocking_recurrence":
            if not rule.comparison or rule.recurrence_window_days is None:
                return
            since = datetime.now(UTC) - timedelta(days=rule.recurrence_window_days)
            count = await blocking_repo.count_incidents_since(instance.id, since)
            value = float(count)
            severity = _classify_severity(
                value, rule.comparison, rule.threshold_warning, rule.threshold_critical
            )

        else:
            # `plan_compile_timeout` (e qualquer rule_type futuro desconhecido) nao e avaliado
            # aqui - alimentado por um worker/supervisor proprio.
            return

        await self._apply_severity_result(alerts_repo, rule, instance, severity, value)

    async def _run_custom_sql(self, instance: Instance, sql: str) -> float | None:
        """Fase 2, item 2 (`rule_type="custom_sql"`) — `sql` ja passou por
        `app.services.custom_sql_guard` no cadastro da regra (nunca validado de novo aqui, so
        executado). Qualquer falha (credencial, conexao, timeout, valor nao numerico) devolve
        `None` — a avaliacao simplesmente pula este ciclo, nunca derruba o worker."""
        username: str | None = None
        secret_value: str | None = None
        if instance.auth_method == "encrypted_fallback":
            if instance.encrypted_credential_ref is None:
                logger.error(
                    "encrypted_credential_ref ausente para instance_id=%s "
                    "(auth_method=encrypted_fallback) - custom_sql pulado",
                    instance.id,
                )
                return None
            try:
                resolved = await self._credential_resolver.resolve(
                    CredentialReference(instance.encrypted_credential_ref)
                )
            except CredentialResolutionError:
                logger.exception(
                    "Falha ao resolver credencial para custom_sql instance_id=%s", instance.id
                )
                return None
            username, secret_value = resolved.username, resolved.secret_value

        connection = ConnectionParams(
            host=instance.host,
            port=instance.port,
            auth_method=instance.auth_method,
            username=username,
            secret_value=secret_value,
        )
        collector = self._create_collector(
            instance.db_type, connection=connection, instance_id=instance.id
        )
        try:
            return await collector.run_scalar_query(sql, self._custom_sql_timeout_seconds)
        except Exception:  # noqa: BLE001 - falha de execucao nunca derruba a avaliacao
            logger.exception("Falha ao executar custom_sql instance_id=%s", instance.id)
            return None
        finally:
            await collector.close()

    async def _apply_severity_result(
        self,
        alerts_repo: AlertsRepository,
        rule: AlertRule,
        instance: Instance,
        severity: Literal["warning", "critical"] | None,
        value: float,
    ) -> None:
        now = datetime.now(UTC)
        reset_mode = rule.reset_mode or "auto_when_false"
        open_alert = await alerts_repo.get_open_alert(rule.id, instance.id)

        # Item 3, modo "auto_after_duration": fecha um alerta que ja passou do tempo configurado,
        # independente da severidade atual - se a condicao continuar verdadeira, o restante desta
        # avaliacao abre um alerta NOVO (tratado como reset "por duracao", nao "recuperado").
        if (
            open_alert is not None
            and reset_mode == "auto_after_duration"
            and rule.reset_after_seconds is not None
            and (now - open_alert.triggered_at).total_seconds() >= rule.reset_after_seconds
        ):
            open_alert.state = "closed"
            open_alert.resolved_at = now
            await alerts_repo.update_alert(open_alert)
            self._publish_alert_event(open_alert, kind="recovered")
            await self._send_notification(
                alerts_repo, open_alert, rule, kind="recovered", value=value
            )
            open_alert = None

        if severity is None:
            if open_alert is not None and reset_mode == "auto_when_false":
                open_alert.state = "closed"
                open_alert.resolved_at = now
                await alerts_repo.update_alert(open_alert)
                self._publish_alert_event(open_alert, kind="recovered")
                await self._send_notification(
                    alerts_repo, open_alert, rule, kind="recovered", value=value
                )
            # `no_reset_every_poll`/`no_reset_manual`: nunca fecha so por severidade voltar a
            # None - fica aberto ate a proxima vez que a condicao for verdadeira (every_poll) ou
            # ate fechamento manual (manual).
            return

        if open_alert is None:
            created = Alert(
                alert_rule_id=rule.id,
                target_type="instance",
                target_id=instance.id,
                severity=severity,
            )
            kind: NotificationKind = "triggered"
            if (
                rule.rule_type != "blocking_recurrence"
                and rule.recurrence_threshold_count is not None
                and rule.recurrence_window_days is not None
            ):
                since = now - timedelta(days=rule.recurrence_window_days)
                recent_count = await alerts_repo.count_recent_alerts(rule.id, instance.id, since)
                if recent_count >= rule.recurrence_threshold_count - 1:
                    created.severity = "critical"
                    created.description = (
                        f"Recorrencia: {recent_count + 1}x em {rule.recurrence_window_days} dia(s)"
                    )
                    kind = "recurrence_escalated"
            if await alerts_repo.get_active_silence("instance", instance.id, rule.id) is not None:
                # Silence suprime so a NOTIFICACAO, mas ainda nao ha alerta criado neste ramo -
                # aqui o comportamento historico e nao criar o Alert quando ja silenciado no
                # momento da criacao (evita ruido de um alerta que nunca sera notificado).
                return
            created = await alerts_repo.create_alert(created)
            self._publish_alert_event(created, kind=kind)
            await self._send_notification(alerts_repo, created, rule, kind=kind, value=value)
            await self._check_escalation(alerts_repo, rule, created, value, now)
            return

        if reset_mode == "no_reset_every_poll":
            open_alert.severity = severity
            await alerts_repo.update_alert(open_alert)
            await self._send_notification(
                alerts_repo, open_alert, rule, kind="repeated", value=value
            )
            await self._check_escalation(alerts_repo, rule, open_alert, value, now)
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
                    alerts_repo, open_alert, rule, kind="reminder", value=value
                )
            await self._check_escalation(alerts_repo, rule, open_alert, value, now)
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
            alerts_repo, open_alert, rule, kind="level_changed", value=value
        )
        await self._check_escalation(alerts_repo, rule, open_alert, value, now)

    async def _check_escalation(
        self,
        alerts_repo: AlertsRepository,
        rule: AlertRule,
        alert: Alert,
        value: float,
        now: datetime,
    ) -> None:
        """Item 3 (escalonamento em niveis) — so avanca enquanto `alert.state == "open"`
        (reconhecer congela o escalonamento no nivel atual, mesmo racional do lembrete critico).
        Cada nivel so dispara uma vez (`last_escalation_level` e o "ate onde ja escalou")."""
        if alert.state != "open":
            return
        levels = await alerts_repo.list_escalation_levels(rule.id)
        if not levels:
            return
        elapsed_seconds = (now - alert.triggered_at).total_seconds()
        for level in sorted(levels, key=lambda lv: lv.level_number):
            if level.level_number <= alert.last_escalation_level:
                continue
            if elapsed_seconds < level.wait_minutes * 60:
                continue
            alert.last_escalation_level = level.level_number
            await alerts_repo.update_alert(alert)
            self._publish_alert_event(alert, kind="escalated")
            await self._send_notification(
                alerts_repo,
                alert,
                rule,
                kind="escalated",
                value=value,
                override_email_recipients=level.email_recipients,
            )

    async def send_notification(
        self,
        alerts_repo: AlertsRepository,
        alert: Alert,
        rule: AlertRule,
        *,
        kind: NotificationKind,
        value: float,
    ) -> None:
        """Canais de notificacao (webhook/e-mail, respeitando silenciamento) para alertas
        criados por outros workers — ex.: `AnomalyDetectionWorker` (rule_type
        `seasonal_anomaly`, que este worker nao avalia)."""
        await self._send_notification(alerts_repo, alert, rule, kind=kind, value=value)

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
        override_email_recipients: list[str] | None = None,
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
            "rule_type": rule.rule_type,
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
            self._try_send_email(payload, override_email_recipients),
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

    async def _try_send_email(
        self, payload: dict, override_recipients: list[str] | None
    ) -> tuple[str | None, bool]:
        """`(canal, enviado)` — canal `None` quando o e-mail esta desabilitado (sem SMTP host).
        `override_recipients` (item 3, escalonamento por nivel) substitui a lista de
        destinatarios global so para este envio, sem persistir a mudanca."""
        if self._email_settings is None:
            return (None, False)
        settings = self._email_settings
        if override_recipients:
            settings = replace(settings, recipients=override_recipients)
        try:
            await self._email_sender(payload, settings)
        except Exception:  # noqa: BLE001 - falha de notificacao nunca derruba a avaliacao
            logger.exception("Falha ao enviar e-mail de alerta alert_id=%s", payload["alert_id"])
            return ("email", False)
        return ("email", True)
