"""Testes de `AlertingWorker` — `app/workers/alerting_worker.py`.

Usa fakes de `session_factory` e dos 3 repositorios (injetados via seams do construtor) e um
`notification_sender` fake — sem banco real, sem `AsyncSession` real, sem I/O de rede. `Instance`/
`AlertRule`/`Alert`/`AlertSilence` sao instanciados direto (models SQLAlchemy comuns, nunca
persistidos), mesmo padrao de `tests/unit/workers/test_polling_supervisor.py`.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from app.models.alert import Alert, AlertNotification, AlertRule, AlertSilence
from app.models.instance import Instance
from app.workers.alerting_worker import AlertingWorker


def _make_instance(*, name: str = "instancia-teste") -> Instance:
    instance = Instance(
        id=uuid4(),
        name=name,
        db_type="postgresql",
        host="127.0.0.1",
        port=5432,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
        active=True,
    )
    instance.maintenance_windows = []
    return instance


def _make_rule(
    *,
    comparison: str = "gte",
    threshold_warning: float | None = 10.0,
    threshold_critical: float | None = 20.0,
) -> AlertRule:
    return AlertRule(
        id=uuid4(),
        name="regra-teste",
        target_type="instance",
        metric_name="test_metric",
        comparison=comparison,
        threshold_warning=threshold_warning,
        threshold_critical=threshold_critical,
        active=True,
    )


def _make_alert(
    *,
    rule: AlertRule,
    instance: Instance,
    severity: str = "warning",
    state: str = "open",
    last_reminder_at: datetime | None = None,
) -> Alert:
    return Alert(
        id=uuid4(),
        alert_rule_id=rule.id,
        target_type="instance",
        target_id=instance.id,
        severity=severity,
        state=state,
        triggered_at=datetime.now(UTC),
        acknowledged_at=datetime.now(UTC) if state == "acknowledged" else None,
        last_reminder_at=last_reminder_at,
    )


@dataclass
class _FakeMetricRow:
    value: float


class _FakeSession:
    def __init__(self) -> None:
        self.rollback_calls = 0

    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def rollback(self) -> None:
        self.rollback_calls += 1


class _FakeSessionFactory:
    def __init__(self) -> None:
        self.sessions: list[_FakeSession] = []

    def __call__(self) -> _FakeSession:
        session = _FakeSession()
        self.sessions.append(session)
        return session


@dataclass
class _AlertsStore:
    rules: list[AlertRule] = field(default_factory=list)
    alerts: dict[UUID, Alert] = field(default_factory=dict)
    silences: list[AlertSilence] = field(default_factory=list)
    notifications: list[AlertNotification] = field(default_factory=list)


class _FakeAlertsRepository:
    def __init__(self, session: object, store: _AlertsStore) -> None:
        del session
        self._store = store

    async def list_active_rules(self) -> list[AlertRule]:
        return [r for r in self._store.rules if r.active]

    async def get_open_alert(self, alert_rule_id: UUID, target_id: UUID) -> Alert | None:
        candidates = [
            a
            for a in self._store.alerts.values()
            if a.alert_rule_id == alert_rule_id and a.target_id == target_id and a.state != "closed"
        ]
        if not candidates:
            return None
        return sorted(candidates, key=lambda a: a.triggered_at, reverse=True)[0]

    async def create_alert(self, alert: Alert) -> Alert:
        alert.id = alert.id or uuid4()
        alert.triggered_at = alert.triggered_at or datetime.now(UTC)
        alert.state = "open"
        self._store.alerts[alert.id] = alert
        return alert

    async def update_alert(self, alert: Alert) -> Alert:
        self._store.alerts[alert.id] = alert
        return alert

    async def get_active_silence(
        self, target_type: str, target_id: UUID, alert_rule_id: UUID
    ) -> AlertSilence | None:
        now = datetime.now(UTC)
        candidates = [
            s
            for s in self._store.silences
            if s.target_type == target_type
            and s.target_id == target_id
            and (s.alert_rule_id is None or s.alert_rule_id == alert_rule_id)
            and (s.silenced_until is None or s.silenced_until > now)
        ]
        return candidates[0] if candidates else None

    async def create_alert_notification(self, notification: AlertNotification) -> AlertNotification:
        notification.id = uuid4()
        self._store.notifications.append(notification)
        return notification

    async def count_recent_alerts(self, rule_id: UUID, target_id: UUID, since: datetime) -> int:
        return sum(
            1
            for a in self._store.alerts.values()
            if a.alert_rule_id == rule_id and a.target_id == target_id and a.triggered_at >= since
        )

    async def list_escalation_levels(self, rule_id: UUID) -> list:
        return []


def _alerts_repository_factory(store: _AlertsStore):  # noqa: ANN201 - fake factory
    def factory(session: object) -> _FakeAlertsRepository:
        return _FakeAlertsRepository(session, store)

    return factory


class _FakeMetricSamplesRepository:
    def __init__(self, session: object, values: dict[tuple[str, UUID, str], float]) -> None:
        del session
        self._values = values

    async def get_latest_value(
        self, target_type: str, target_id: UUID, metric_name: str
    ) -> _FakeMetricRow | None:
        key = (target_type, target_id, metric_name)
        if key not in self._values:
            return None
        return _FakeMetricRow(self._values[key])


def _metric_samples_repository_factory(values: dict[tuple[str, UUID, str], float]):  # noqa: ANN201
    def factory(session: object) -> _FakeMetricSamplesRepository:
        return _FakeMetricSamplesRepository(session, values)

    return factory


class _FakeInstancesRepository:
    def __init__(self, session: object, instances: list[Instance]) -> None:
        del session
        self._instances = instances

    async def list_active(self) -> list[Instance]:
        return list(self._instances)


def _instances_repository_factory(instances: list[Instance]):  # noqa: ANN201
    def factory(session: object) -> _FakeInstancesRepository:
        return _FakeInstancesRepository(session, instances)

    return factory


class _FakeNotificationSender:
    def __init__(self, *, fail: bool = False) -> None:
        self.calls: list[tuple[str, dict]] = []
        self._fail = fail

    async def __call__(self, url: str, payload: dict) -> None:
        self.calls.append((url, payload))
        if self._fail:
            raise RuntimeError("falha simulada de envio")


def _make_worker(
    *,
    store: _AlertsStore,
    values: dict[tuple[str, UUID, str], float],
    instances: list[Instance],
    sender: _FakeNotificationSender,
    webhook_url: str | None = "http://example.invalid/webhook",
    critical_reminder_interval_seconds: int = 900,
) -> AlertingWorker:
    return AlertingWorker(
        _FakeSessionFactory(),
        60,
        critical_reminder_interval_seconds,
        webhook_url=webhook_url,
        alerts_repository_factory=_alerts_repository_factory(store),
        metric_samples_repository_factory=_metric_samples_repository_factory(values),
        instances_repository_factory=_instances_repository_factory(instances),
        notification_sender=sender,
    )


async def test_creates_new_alert_when_threshold_exceeded() -> None:
    instance = _make_instance()
    rule = _make_rule()
    store = _AlertsStore(rules=[rule])
    values = {("instance", instance.id, "test_metric"): 25.0}
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    assert len(store.alerts) == 1
    created = next(iter(store.alerts.values()))
    assert created.severity == "critical"
    assert created.target_id == instance.id
    assert len(sender.calls) == 1
    assert sender.calls[0][1]["kind"] == "triggered"
    assert len(store.notifications) == 1


async def test_does_not_duplicate_alert_when_already_acknowledged() -> None:
    instance = _make_instance()
    rule = _make_rule()
    existing = _make_alert(rule=rule, instance=instance, severity="warning", state="acknowledged")
    store = _AlertsStore(rules=[rule], alerts={existing.id: existing})
    values = {("instance", instance.id, "test_metric"): 12.0}  # ainda warning, mesma severidade
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    assert len(store.alerts) == 1
    assert store.alerts[existing.id].state == "acknowledged"
    assert sender.calls == []


async def test_closes_alert_and_notifies_recovered_when_back_to_normal() -> None:
    instance = _make_instance()
    rule = _make_rule()
    existing = _make_alert(rule=rule, instance=instance, severity="warning", state="open")
    store = _AlertsStore(rules=[rule], alerts={existing.id: existing})
    values = {("instance", instance.id, "test_metric"): 1.0}  # abaixo de qualquer threshold
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    updated = store.alerts[existing.id]
    assert updated.state == "closed"
    assert updated.resolved_at is not None
    assert sender.calls[0][1]["kind"] == "recovered"


async def test_severity_change_notifies_and_reopens_acknowledged_alert() -> None:
    instance = _make_instance()
    rule = _make_rule()
    existing = _make_alert(rule=rule, instance=instance, severity="warning", state="acknowledged")
    store = _AlertsStore(rules=[rule], alerts={existing.id: existing})
    values = {("instance", instance.id, "test_metric"): 25.0}  # agora critico
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    updated = store.alerts[existing.id]
    assert updated.severity == "critical"
    assert updated.state == "open"
    assert updated.acknowledged_at is None
    assert sender.calls[0][1]["kind"] == "level_changed"


async def test_critical_reminder_respects_interval() -> None:
    instance = _make_instance()
    rule = _make_rule()
    now = datetime.now(UTC)
    existing = _make_alert(rule=rule, instance=instance, severity="critical", state="open")
    existing.last_reminder_at = now - timedelta(seconds=10)
    store = _AlertsStore(rules=[rule], alerts={existing.id: existing})
    values = {("instance", instance.id, "test_metric"): 25.0}  # continua critico
    sender = _FakeNotificationSender()
    worker = _make_worker(
        store=store,
        values=values,
        instances=[instance],
        sender=sender,
        critical_reminder_interval_seconds=900,
    )

    await worker._run_cycle()  # 10s desde o ultimo lembrete, intervalo de 900s - nao deve mandar

    assert sender.calls == []
    assert store.alerts[existing.id].last_reminder_at == existing.last_reminder_at

    store.alerts[existing.id].last_reminder_at = now - timedelta(seconds=1000)
    await worker._run_cycle()  # agora passou do intervalo

    assert len(sender.calls) == 1
    assert sender.calls[0][1]["kind"] == "reminder"


async def test_silence_created_before_evaluation_suppresses_alert_creation() -> None:
    instance = _make_instance()
    rule = _make_rule()
    silence = AlertSilence(
        id=uuid4(), target_type="instance", target_id=instance.id, alert_rule_id=None
    )
    store = _AlertsStore(rules=[rule], silences=[silence])
    values = {("instance", instance.id, "test_metric"): 25.0}
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    assert store.alerts == {}
    assert sender.calls == []


async def test_silence_created_after_open_suppresses_notification_but_updates_alert() -> None:
    instance = _make_instance()
    rule = _make_rule()
    existing = _make_alert(rule=rule, instance=instance, severity="warning", state="acknowledged")
    silence = AlertSilence(
        id=uuid4(), target_type="instance", target_id=instance.id, alert_rule_id=None
    )
    store = _AlertsStore(rules=[rule], alerts={existing.id: existing}, silences=[silence])
    values = {("instance", instance.id, "test_metric"): 25.0}  # severidade muda p/ critical
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    updated = store.alerts[existing.id]
    assert updated.severity == "critical"
    assert updated.state == "open"
    assert sender.calls == []
    assert store.notifications == []


async def test_error_evaluating_one_instance_does_not_affect_another() -> None:
    good_instance = _make_instance(name="ok")
    bad_instance = _make_instance(name="quebrada")
    rule = _make_rule()
    store = _AlertsStore(rules=[rule])
    values = {("instance", good_instance.id, "test_metric"): 25.0}  # bad_instance sem valor -> None
    sender = _FakeNotificationSender()

    class _RaisingMetricSamplesRepository:
        def __init__(self, session: object) -> None:
            del session

        async def get_latest_value(self, target_type: str, target_id: UUID, metric_name: str):
            if target_id == bad_instance.id:
                raise RuntimeError("falha simulada de leitura")
            return _FakeMetricRow(values[(target_type, target_id, metric_name)])

    worker = AlertingWorker(
        _FakeSessionFactory(),
        60,
        900,
        webhook_url="http://example.invalid/webhook",
        alerts_repository_factory=_alerts_repository_factory(store),
        metric_samples_repository_factory=lambda session: _RaisingMetricSamplesRepository(session),
        instances_repository_factory=_instances_repository_factory([bad_instance, good_instance]),
        notification_sender=sender,
    )

    await worker._run_cycle()  # nao deve lancar

    assert len(store.alerts) == 1
    created = next(iter(store.alerts.values()))
    assert created.target_id == good_instance.id
