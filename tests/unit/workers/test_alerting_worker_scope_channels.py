"""Testes de escopo por instancia, avaliacao por taxa (`rate_per_minute`), resolucao de canais por
regra (e-mails/time/webhook) e envio de e-mail via provedor configurado — `AlertingWorker`.

Reusa os fakes de `test_alerting_worker` (sessao/repositorios em memoria, sem banco/rede).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

from app.models.alert import AlertRule
from app.services.email_service import EmailTransportConfig, OutgoingEmail
from app.workers.alerting_worker import (
    AlertingWorker,
    compute_rate_per_minute,
    resolve_channels,
)
from tests.unit.workers.test_alerting_worker import (
    _alerts_repository_factory,
    _AlertsStore,
    _FakeMetricRow,
    _FakeNotificationSender,
    _FakeSessionFactory,
    _instances_repository_factory,
    _make_instance,
    _make_rule,
)


@dataclass
class _Point:
    time: datetime
    value: float


class _FakeMetricRepo:
    def __init__(
        self,
        latest: dict[tuple[UUID, str], float],
        points: dict[tuple[UUID, str], list[_Point]] | None = None,
    ) -> None:
        self._latest = latest
        self._points = points or {}

    async def get_latest_value(
        self, target_type: str, target_id: UUID, metric_name: str
    ) -> _FakeMetricRow | None:
        value = self._latest.get((target_id, metric_name))
        return None if value is None else _FakeMetricRow(value)

    async def get_latest_two_points(
        self, target_type: str, target_id: UUID, metric_name: str
    ) -> list[_Point]:
        return self._points.get((target_id, metric_name), [])


class _FakeEmailSender:
    def __init__(self) -> None:
        self.calls: list[tuple[EmailTransportConfig, OutgoingEmail]] = []

    async def __call__(self, config: EmailTransportConfig, email: OutgoingEmail) -> None:
        self.calls.append((config, email))


def _worker(
    *,
    store: _AlertsStore,
    metric_repo: _FakeMetricRepo,
    instances: list,
    sender: _FakeNotificationSender | None = None,
    webhook_url: str | None = None,
    email_config: EmailTransportConfig | None = None,
    email_sender: _FakeEmailSender | None = None,
    default_recipients: list[str] | None = None,
    team_emails: dict[int, list[str]] | None = None,
) -> AlertingWorker:
    async def loader() -> EmailTransportConfig | None:
        return email_config

    async def team_resolver(team_id: int) -> list[str]:
        return (team_emails or {}).get(team_id, [])

    return AlertingWorker(
        _FakeSessionFactory(),
        60,
        900,
        webhook_url=webhook_url,
        email_config_loader=loader if email_config is not None else None,
        default_email_recipients=default_recipients or [],
        team_emails_resolver=team_resolver,
        alerts_repository_factory=_alerts_repository_factory(store),
        metric_samples_repository_factory=lambda _session: metric_repo,
        instances_repository_factory=_instances_repository_factory(instances),
        notification_sender=sender or _FakeNotificationSender(),
        email_sender=email_sender or _FakeEmailSender(),
    )


_SMTP = EmailTransportConfig(
    provider="smtp", sender_address="alerts@example.com", smtp_host="smtp.example.com"
)


# --- escopo por instancia ---------------------------------------------------------------------


async def test_scoped_rule_only_evaluates_its_own_instance() -> None:
    scoped_instance = _make_instance(name="a")
    other_instance = _make_instance(name="b")
    rule = _make_rule()
    rule.instance_id = scoped_instance.id
    store = _AlertsStore(rules=[rule])
    metric_repo = _FakeMetricRepo(
        {(scoped_instance.id, "test_metric"): 25.0, (other_instance.id, "test_metric"): 25.0}
    )
    worker = _worker(
        store=store, metric_repo=metric_repo, instances=[scoped_instance, other_instance]
    )

    await worker._run_cycle()

    assert [a.target_id for a in store.alerts.values()] == [scoped_instance.id]


async def test_global_rule_evaluates_every_instance() -> None:
    instances = [_make_instance(name="a"), _make_instance(name="b")]
    rule = _make_rule()
    store = _AlertsStore(rules=[rule])
    metric_repo = _FakeMetricRepo({(i.id, "test_metric"): 25.0 for i in instances})
    worker = _worker(store=store, metric_repo=metric_repo, instances=instances)

    await worker._run_cycle()

    assert {a.target_id for a in store.alerts.values()} == {i.id for i in instances}


# --- avaliacao por taxa -----------------------------------------------------------------------


def test_compute_rate_per_minute_uses_delta_over_elapsed_minutes() -> None:
    now = datetime.now(UTC)
    points = [_Point(now, 130.0), _Point(now - timedelta(seconds=30), 100.0)]
    assert compute_rate_per_minute(points) == 60.0


def test_compute_rate_per_minute_skips_counter_reset_and_missing_sample() -> None:
    now = datetime.now(UTC)
    reset = [_Point(now, 5.0), _Point(now - timedelta(minutes=1), 100.0)]
    assert compute_rate_per_minute(reset) is None
    assert compute_rate_per_minute([_Point(now, 5.0)]) is None
    assert compute_rate_per_minute([]) is None
    assert compute_rate_per_minute([_Point(now, 5.0), _Point(now, 1.0)]) is None


async def test_rate_rule_alerts_on_rate_not_on_raw_counter_value() -> None:
    instance = _make_instance()
    rule = _make_rule(threshold_warning=10.0, threshold_critical=50.0)
    rule.evaluation_mode = "rate_per_minute"
    now = datetime.now(UTC)
    store = _AlertsStore(rules=[rule])
    # valor cru (1_000_000) estouraria qualquer threshold; a taxa e 12/min -> warning
    metric_repo = _FakeMetricRepo(
        {(instance.id, "test_metric"): 1_000_000.0},
        {
            (instance.id, "test_metric"): [
                _Point(now, 1_000_012.0),
                _Point(now - timedelta(minutes=1), 1_000_000.0),
            ]
        },
    )
    worker = _worker(store=store, metric_repo=metric_repo, instances=[instance])

    await worker._run_cycle()

    assert [a.severity for a in store.alerts.values()] == ["warning"]


async def test_rate_rule_skips_cycle_on_counter_reset_without_closing_open_alert() -> None:
    from tests.unit.workers.test_alerting_worker import _make_alert

    instance = _make_instance()
    rule = _make_rule()
    rule.evaluation_mode = "rate_per_minute"
    existing = _make_alert(rule=rule, instance=instance, severity="warning", state="open")
    store = _AlertsStore(rules=[rule], alerts={existing.id: existing})
    now = datetime.now(UTC)
    metric_repo = _FakeMetricRepo(
        {},
        {
            (instance.id, "test_metric"): [
                _Point(now, 3.0),
                _Point(now - timedelta(minutes=1), 900.0),
            ]
        },
    )
    worker = _worker(store=store, metric_repo=metric_repo, instances=[instance])

    await worker._run_cycle()

    assert store.alerts[existing.id].state == "open"


# --- resolucao de canais ----------------------------------------------------------------------


def _rule(**fields: object) -> AlertRule:
    rule = _make_rule()
    for key, value in fields.items():
        setattr(rule, key, value)
    return rule


def test_resolve_channels_rule_emails_plus_team_dedup_and_rule_webhook() -> None:
    rule = _rule(notify_emails=["dba@x.com", "OPS@x.com"], webhook_url="https://hook.rule/abc")
    channels = resolve_channels(
        rule,
        global_webhook_url="https://hook.global",
        global_email_recipients=["global@x.com"],
        team_emails=["ops@x.com", "lead@x.com"],
    )
    assert channels.webhook_url == "https://hook.rule/abc"
    assert channels.email_recipients == ["dba@x.com", "OPS@x.com", "lead@x.com"]


def test_resolve_channels_falls_back_to_globals() -> None:
    channels = resolve_channels(
        _rule(notify_emails=None, webhook_url=None),
        global_webhook_url="https://hook.global",
        global_email_recipients=["global@x.com"],
    )
    assert channels.webhook_url == "https://hook.global"
    assert channels.email_recipients == ["global@x.com"]


def test_resolve_channels_escalation_override_replaces_recipients() -> None:
    channels = resolve_channels(
        _rule(notify_emails=["dba@x.com"]),
        global_webhook_url=None,
        global_email_recipients=["global@x.com"],
        team_emails=["team@x.com"],
        override_email_recipients=["boss@x.com"],
    )
    assert channels.webhook_url is None
    assert channels.email_recipients == ["boss@x.com"]


async def test_notification_uses_rule_webhook_and_team_emails_via_provider() -> None:
    instance = _make_instance()
    rule = _make_rule()
    rule.webhook_url = "https://hook.rule/x"
    rule.notify_emails = ["dba@x.com"]
    rule.notify_team_id = 7
    store = _AlertsStore(rules=[rule])
    sender = _FakeNotificationSender()
    email_sender = _FakeEmailSender()
    worker = _worker(
        store=store,
        metric_repo=_FakeMetricRepo({(instance.id, "test_metric"): 25.0}),
        instances=[instance],
        sender=sender,
        webhook_url="https://hook.global",
        email_config=_SMTP,
        email_sender=email_sender,
        default_recipients=["global@x.com"],
        team_emails={7: ["member@x.com"]},
    )

    await worker._run_cycle()

    assert [url for url, _payload in sender.calls] == ["https://hook.rule/x"]
    assert len(email_sender.calls) == 1
    config, email = email_sender.calls[0]
    assert config.provider == "smtp"
    assert email.recipients == ["dba@x.com", "member@x.com"]
    assert email.subject.startswith("[CRITICAL]")
    assert sorted(n.channel for n in store.notifications) == ["email", "webhook"]


async def test_email_channel_disabled_when_provider_not_configured() -> None:
    instance = _make_instance()
    rule = _make_rule()
    store = _AlertsStore(rules=[rule])
    email_sender = _FakeEmailSender()
    worker = _worker(
        store=store,
        metric_repo=_FakeMetricRepo({(instance.id, "test_metric"): 25.0}),
        instances=[instance],
        email_config=EmailTransportConfig(provider="brevo", sender_address="a@x.com"),
        email_sender=email_sender,
        default_recipients=["global@x.com"],
    )

    await worker._run_cycle()

    assert email_sender.calls == []
    assert store.notifications == []
