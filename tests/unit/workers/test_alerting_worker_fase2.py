"""Testes de `AlertingWorker` — Fase 2 (rule_type anomaly/custom_sql/blocking_recurrence,
reset_mode, escalonamento em niveis, recorrencia). Complementa
`tests/unit/workers/test_alerting_worker.py` (comportamento `rule_type="threshold"` original,
inalterado). Mesmos fakes de sessao/repositorio via seams do construtor, sem banco real.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID, uuid4

from app.models.alert import Alert, AlertEscalationLevel, AlertNotification, AlertRule, AlertSilence
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


def _make_rule(**kwargs: object) -> AlertRule:
    defaults: dict[str, object] = dict(
        id=uuid4(),
        name="regra-teste",
        target_type="instance",
        metric_name="test_metric",
        comparison="gte",
        threshold_warning=10.0,
        threshold_critical=20.0,
        active=True,
        rule_type="threshold",
        reset_mode="auto_when_false",
    )
    defaults.update(kwargs)
    return AlertRule(**defaults)  # type: ignore[arg-type]


@dataclass
class _FakeMetricRow:
    value: float


@dataclass
class _FakeBaselineRow:
    mean: float
    stddev: float
    sample_count: int


class _FakeSession:
    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def rollback(self) -> None:
        pass


class _FakeSessionFactory:
    def __call__(self) -> _FakeSession:
        return _FakeSession()


@dataclass
class _AlertsStore:
    rules: list[AlertRule] = field(default_factory=list)
    alerts: dict[UUID, Alert] = field(default_factory=dict)
    silences: list[AlertSilence] = field(default_factory=list)
    notifications: list[AlertNotification] = field(default_factory=list)
    escalation_levels: dict[UUID, list[AlertEscalationLevel]] = field(default_factory=dict)


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

    async def list_escalation_levels(self, rule_id: UUID) -> list[AlertEscalationLevel]:
        return list(self._store.escalation_levels.get(rule_id, []))


def _alerts_repository_factory(store: _AlertsStore):  # noqa: ANN201
    def factory(session: object) -> _FakeAlertsRepository:
        return _FakeAlertsRepository(session, store)

    return factory


class _FakeMetricSamplesRepository:
    def __init__(
        self,
        session: object,
        values: dict[tuple[str, UUID, str], float],
        baselines: dict[tuple[str, UUID, str], _FakeBaselineRow] | None = None,
    ) -> None:
        del session
        self._values = values
        self._baselines = baselines or {}

    async def get_latest_value(self, target_type: str, target_id: UUID, metric_name: str):
        key = (target_type, target_id, metric_name)
        if key not in self._values:
            return None
        return _FakeMetricRow(self._values[key])

    async def get_baseline_stats(self, target_type: str, target_id: UUID, metric_name: str, since):
        del since
        return self._baselines.get((target_type, target_id, metric_name))


def _metric_samples_repository_factory(
    values: dict[tuple[str, UUID, str], float],
    baselines: dict[tuple[str, UUID, str], _FakeBaselineRow] | None = None,
):  # noqa: ANN201
    def factory(session: object) -> _FakeMetricSamplesRepository:
        return _FakeMetricSamplesRepository(session, values, baselines)

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


class _FakeBlockingIncidentsRepository:
    def __init__(self, session: object, counts: dict[UUID, int]) -> None:
        del session
        self._counts = counts

    async def count_incidents_since(self, instance_id: UUID, since: datetime) -> int:
        del since
        return self._counts.get(instance_id, 0)


def _blocking_incidents_repository_factory(counts: dict[UUID, int]):  # noqa: ANN201
    def factory(session: object) -> _FakeBlockingIncidentsRepository:
        return _FakeBlockingIncidentsRepository(session, counts)

    return factory


class _FakeNotificationSender:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict]] = []

    async def __call__(self, url: str, payload: dict) -> None:
        self.calls.append((url, payload))


class _FakeCollector:
    def __init__(self, scalar_value: float | None) -> None:
        self._scalar_value = scalar_value
        self.closed = False

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        del sql, timeout_seconds
        return self._scalar_value

    async def close(self) -> None:
        self.closed = True


def _make_worker(
    *,
    store: _AlertsStore,
    values: dict[tuple[str, UUID, str], float] | None = None,
    baselines: dict[tuple[str, UUID, str], _FakeBaselineRow] | None = None,
    instances: list[Instance],
    sender: _FakeNotificationSender,
    blocking_counts: dict[UUID, int] | None = None,
    collector_factory=None,
    critical_reminder_interval_seconds: int = 900,
) -> AlertingWorker:
    kwargs: dict[str, object] = dict(
        webhook_url="http://example.invalid/webhook",
        alerts_repository_factory=_alerts_repository_factory(store),
        metric_samples_repository_factory=_metric_samples_repository_factory(
            values or {}, baselines
        ),
        instances_repository_factory=_instances_repository_factory(instances),
        blocking_incidents_repository_factory=_blocking_incidents_repository_factory(
            blocking_counts or {}
        ),
        notification_sender=sender,
    )
    if collector_factory is not None:
        kwargs["collector_factory"] = collector_factory
    return AlertingWorker(
        _FakeSessionFactory(),
        60,
        critical_reminder_interval_seconds,
        **kwargs,  # type: ignore[arg-type]
    )


async def test_anomaly_rule_triggers_on_large_deviation() -> None:
    instance = _make_instance()
    rule = _make_rule(rule_type="anomaly", metric_name="test_metric", baseline_window_days=7)
    store = _AlertsStore(rules=[rule])
    values = {("instance", instance.id, "test_metric"): 100.0}
    baselines = {
        ("instance", instance.id, "test_metric"): _FakeBaselineRow(
            mean=10.0, stddev=2.0, sample_count=50
        )
    }
    sender = _FakeNotificationSender()
    worker = _make_worker(
        store=store, values=values, baselines=baselines, instances=[instance], sender=sender
    )

    await worker._run_cycle()

    assert len(store.alerts) == 1
    created = next(iter(store.alerts.values()))
    assert created.severity == "critical"  # (100-10)/2 = 45 sigma >> 3


async def test_anomaly_rule_skips_when_sample_count_too_low() -> None:
    instance = _make_instance()
    rule = _make_rule(rule_type="anomaly", metric_name="test_metric", baseline_window_days=7)
    store = _AlertsStore(rules=[rule])
    values = {("instance", instance.id, "test_metric"): 100.0}
    baselines = {
        ("instance", instance.id, "test_metric"): _FakeBaselineRow(
            mean=10.0, stddev=2.0, sample_count=5
        )
    }
    sender = _FakeNotificationSender()
    worker = _make_worker(
        store=store, values=values, baselines=baselines, instances=[instance], sender=sender
    )

    await worker._run_cycle()

    assert store.alerts == {}


async def test_custom_sql_rule_evaluates_scalar_from_collector() -> None:
    instance = _make_instance()
    rule = _make_rule(
        rule_type="custom_sql",
        metric_name=None,
        custom_sql="SELECT COUNT(*) FROM pix_eventos WHERE status = 'pendente'",
        comparison="gt",
        threshold_warning=5.0,
        threshold_critical=50.0,
    )
    store = _AlertsStore(rules=[rule])
    sender = _FakeNotificationSender()

    def collector_factory(db_type, connection, instance_id=None):
        del db_type, connection, instance_id
        return _FakeCollector(scalar_value=10.0)

    worker = _make_worker(
        store=store, instances=[instance], sender=sender, collector_factory=collector_factory
    )

    await worker._run_cycle()

    assert len(store.alerts) == 1
    assert next(iter(store.alerts.values())).severity == "warning"


async def test_custom_sql_rule_skips_when_collector_returns_none() -> None:
    instance = _make_instance()
    rule = _make_rule(
        rule_type="custom_sql",
        metric_name=None,
        custom_sql="SELECT COUNT(*) FROM t",
        comparison="gt",
        threshold_warning=5.0,
    )
    store = _AlertsStore(rules=[rule])
    sender = _FakeNotificationSender()

    def collector_factory(db_type, connection, instance_id=None):
        del db_type, connection, instance_id
        return _FakeCollector(scalar_value=None)

    worker = _make_worker(
        store=store, instances=[instance], sender=sender, collector_factory=collector_factory
    )

    await worker._run_cycle()

    assert store.alerts == {}


async def test_blocking_recurrence_rule_counts_incidents_as_value() -> None:
    instance = _make_instance()
    rule = _make_rule(
        rule_type="blocking_recurrence",
        metric_name=None,
        comparison="gte",
        threshold_warning=3.0,
        threshold_critical=10.0,
        recurrence_window_days=1,
    )
    store = _AlertsStore(rules=[rule])
    sender = _FakeNotificationSender()
    worker = _make_worker(
        store=store,
        instances=[instance],
        sender=sender,
        blocking_counts={instance.id: 5},
    )

    await worker._run_cycle()

    assert len(store.alerts) == 1
    assert next(iter(store.alerts.values())).severity == "warning"


async def test_reset_mode_auto_after_duration_closes_and_reopens_when_still_true() -> None:
    instance = _make_instance()
    rule = _make_rule(reset_mode="auto_after_duration", reset_after_seconds=60)
    now = datetime.now(UTC)
    existing = Alert(
        id=uuid4(),
        alert_rule_id=rule.id,
        target_type="instance",
        target_id=instance.id,
        severity="warning",
        state="open",
        triggered_at=now - timedelta(seconds=120),
    )
    store = _AlertsStore(rules=[rule], alerts={existing.id: existing})
    values = {("instance", instance.id, "test_metric"): 25.0}  # continua critico
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    # o alerta antigo foi fechado (por duracao) e um NOVO foi criado (condicao ainda verdadeira)
    assert len(store.alerts) == 2
    old = store.alerts[existing.id]
    assert old.state == "closed"
    new_alerts = [a for a in store.alerts.values() if a.id != existing.id]
    assert len(new_alerts) == 1
    assert new_alerts[0].state == "open"


async def test_reset_mode_no_reset_every_poll_never_closes_and_notifies_every_cycle() -> None:
    instance = _make_instance()
    rule = _make_rule(reset_mode="no_reset_every_poll")
    store = _AlertsStore(rules=[rule])
    values = {("instance", instance.id, "test_metric"): 25.0}
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()
    assert len(store.alerts) == 1
    first_id = next(iter(store.alerts))

    await worker._run_cycle()
    await worker._run_cycle()

    assert len(store.alerts) == 1  # nunca cria um 2o Alert
    assert store.alerts[first_id].state == "open"
    assert len(sender.calls) == 3  # notifica em toda chamada (triggered + 2x repeated)
    assert sender.calls[1][1]["kind"] == "repeated"
    assert sender.calls[2][1]["kind"] == "repeated"


async def test_reset_mode_no_reset_manual_stays_open_when_condition_becomes_false() -> None:
    instance = _make_instance()
    rule = _make_rule(reset_mode="no_reset_manual")
    existing = Alert(
        id=uuid4(),
        alert_rule_id=rule.id,
        target_type="instance",
        target_id=instance.id,
        severity="warning",
        state="open",
        triggered_at=datetime.now(UTC),
    )
    store = _AlertsStore(rules=[rule], alerts={existing.id: existing})
    values = {("instance", instance.id, "test_metric"): 1.0}  # abaixo de qualquer threshold
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    assert store.alerts[existing.id].state == "open"  # nao fechou sozinho
    assert sender.calls == []


async def test_escalation_level_notifies_after_wait_minutes_elapsed() -> None:
    instance = _make_instance()
    rule = _make_rule()
    existing = Alert(
        id=uuid4(),
        alert_rule_id=rule.id,
        target_type="instance",
        target_id=instance.id,
        severity="warning",
        state="open",
        triggered_at=datetime.now(UTC) - timedelta(minutes=10),
        last_escalation_level=0,
    )
    store = _AlertsStore(
        rules=[rule],
        alerts={existing.id: existing},
        escalation_levels={
            rule.id: [
                AlertEscalationLevel(
                    id=uuid4(), alert_rule_id=rule.id, level_number=2, wait_minutes=5
                )
            ]
        },
    )
    values = {("instance", instance.id, "test_metric"): 12.0}  # continua warning (mesma severidade)
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    assert store.alerts[existing.id].last_escalation_level == 2
    assert any(call[1]["kind"] == "escalated" for call in sender.calls)


async def test_escalation_does_not_advance_once_acknowledged() -> None:
    instance = _make_instance()
    rule = _make_rule()
    existing = Alert(
        id=uuid4(),
        alert_rule_id=rule.id,
        target_type="instance",
        target_id=instance.id,
        severity="warning",
        state="acknowledged",
        triggered_at=datetime.now(UTC) - timedelta(minutes=10),
        last_escalation_level=0,
    )
    store = _AlertsStore(
        rules=[rule],
        alerts={existing.id: existing},
        escalation_levels={
            rule.id: [
                AlertEscalationLevel(
                    id=uuid4(), alert_rule_id=rule.id, level_number=2, wait_minutes=5
                )
            ]
        },
    )
    values = {("instance", instance.id, "test_metric"): 12.0}
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    assert store.alerts[existing.id].last_escalation_level == 0
    assert sender.calls == []


async def test_recurrence_forces_critical_and_tags_description_on_new_alert() -> None:
    instance = _make_instance()
    rule = _make_rule(recurrence_threshold_count=3, recurrence_window_days=1)
    now = datetime.now(UTC)
    prior_1 = Alert(
        id=uuid4(),
        alert_rule_id=rule.id,
        target_type="instance",
        target_id=instance.id,
        severity="warning",
        state="closed",
        triggered_at=now - timedelta(hours=1),
    )
    prior_2 = Alert(
        id=uuid4(),
        alert_rule_id=rule.id,
        target_type="instance",
        target_id=instance.id,
        severity="warning",
        state="closed",
        triggered_at=now - timedelta(minutes=30),
    )
    store = _AlertsStore(rules=[rule], alerts={prior_1.id: prior_1, prior_2.id: prior_2})
    values = {("instance", instance.id, "test_metric"): 12.0}  # normalmente warning
    sender = _FakeNotificationSender()
    worker = _make_worker(store=store, values=values, instances=[instance], sender=sender)

    await worker._run_cycle()

    new_alerts = [a for a in store.alerts.values() if a.id not in (prior_1.id, prior_2.id)]
    assert len(new_alerts) == 1
    created = new_alerts[0]
    assert created.severity == "critical"
    assert created.description is not None and "Recorrencia" in created.description
    assert sender.calls[-1][1]["kind"] == "recurrence_escalated"


async def test_recurrence_not_applied_to_blocking_recurrence_rule_type() -> None:
    """blocking_recurrence ja E uma medida de recorrencia - nao aplica a escalada de item 4
    por cima (mesmo se a regra tivesse os campos configurados por engano)."""
    instance = _make_instance()
    rule = _make_rule(
        rule_type="blocking_recurrence",
        metric_name=None,
        comparison="gte",
        threshold_warning=3.0,
        recurrence_window_days=1,
        recurrence_threshold_count=1,  # configurado, mas nao deve ser aplicado
    )
    store = _AlertsStore(rules=[rule])
    sender = _FakeNotificationSender()
    worker = _make_worker(
        store=store, instances=[instance], sender=sender, blocking_counts={instance.id: 5}
    )

    await worker._run_cycle()

    created = next(iter(store.alerts.values()))
    assert created.severity == "warning"  # nao forcado para critical
    assert created.description is None
