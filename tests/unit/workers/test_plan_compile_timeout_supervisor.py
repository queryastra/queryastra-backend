"""Testes de `PlanCompileTimeoutSupervisor` — `app/workers/plan_compile_timeout_supervisor.py`.

Mesmo padrao de fakes de `tests/unit/workers/test_backup_status_supervisor.py`.
"""

from __future__ import annotations

from uuid import uuid4

from app.models.alert import Alert, AlertRule
from app.models.instance import Instance
from app.normalizers.models import NormalizedPlanCompileTimeoutFinding
from app.workers.plan_compile_timeout_supervisor import PlanCompileTimeoutSupervisor


def _make_instance(*, db_type: str = "sqlserver", active: bool = True) -> Instance:
    instance = Instance(
        id=uuid4(),
        name="instancia-teste",
        db_type=db_type,
        host="127.0.0.1",
        port=1433,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
        active=active,
    )
    instance.maintenance_windows = []
    return instance


class _FakeSession:
    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _FakeSessionFactory:
    def __call__(self) -> _FakeSession:
        return _FakeSession()


class _FakeInstancesRepository:
    def __init__(self, session: object, registry: dict) -> None:  # noqa: ANN001
        del session
        self._registry = registry

    async def get_by_id(self, instance_id):  # noqa: ANN001, ANN201
        return self._registry.get(instance_id)

    async def list_active(self) -> list[Instance]:
        return [i for i in self._registry.values() if i.active]


def _instances_repository_factory(registry: dict):  # noqa: ANN201
    def factory(session: object) -> _FakeInstancesRepository:
        return _FakeInstancesRepository(session, registry)

    return factory


class _FakeAlertsRepository:
    rules: list[AlertRule] = []
    alerts: list[Alert] = []

    def __init__(self, session: object) -> None:
        del session

    async def get_rule_by_name(self, name: str) -> AlertRule | None:
        return next((r for r in type(self).rules if r.name == name), None)

    async def create_alert_rule(self, rule: AlertRule) -> AlertRule:
        rule.id = rule.id or uuid4()
        type(self).rules.append(rule)
        return rule

    async def list_open_alerts(self, alert_rule_id, target_id) -> list[Alert]:  # noqa: ANN001
        return [
            a
            for a in type(self).alerts
            if a.alert_rule_id == alert_rule_id and a.target_id == target_id and a.state != "closed"
        ]

    async def create_alert(self, alert: Alert) -> Alert:
        alert.id = alert.id or uuid4()
        alert.state = "open"
        type(self).alerts.append(alert)
        return alert


def _reset_fake_alerts_repository() -> None:
    _FakeAlertsRepository.rules = []
    _FakeAlertsRepository.alerts = []


class _FakeCollector:
    _findings_by_instance: dict = {}
    calls_by_instance: dict = {}
    close_calls: list = []

    def __init__(self, db_type, connection, instance_id) -> None:  # noqa: ANN001
        self._instance_id = instance_id

    async def scan_plan_compile_timeouts(self) -> list[NormalizedPlanCompileTimeoutFinding]:
        type(self).calls_by_instance[self._instance_id] = (
            type(self).calls_by_instance.get(self._instance_id, 0) + 1
        )
        return type(self)._findings_by_instance.get(self._instance_id, [])

    async def close(self) -> None:
        type(self).close_calls.append(self._instance_id)


def _reset_fake_collector() -> None:
    _FakeCollector._findings_by_instance = {}
    _FakeCollector.calls_by_instance = {}
    _FakeCollector.close_calls = []


def _make_supervisor(registry: dict, *, check_interval_seconds: int = 86400):
    return PlanCompileTimeoutSupervisor(
        _FakeSessionFactory(),
        check_interval_seconds,
        instances_repository_factory=_instances_repository_factory(registry),
        alerts_repository_factory=_FakeAlertsRepository,
        collector_factory=_FakeCollector,
    )


async def test_skips_postgresql_instance_without_connecting() -> None:
    _reset_fake_collector()
    _reset_fake_alerts_repository()
    instance = _make_instance(db_type="postgresql")
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance


async def test_creates_rule_and_alert_for_new_finding() -> None:
    _reset_fake_collector()
    _reset_fake_alerts_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    finding = NormalizedPlanCompileTimeoutFinding(
        sql_hash="abc123", reason="TimeOutReached", statement_text_masked="SELECT * FROM t"
    )
    _FakeCollector._findings_by_instance = {instance.id: [finding]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert len(_FakeAlertsRepository.rules) == 1
    rule = _FakeAlertsRepository.rules[0]
    assert rule.rule_type == "plan_compile_timeout"
    assert instance.name in rule.name
    assert len(_FakeAlertsRepository.alerts) == 1
    alert = _FakeAlertsRepository.alerts[0]
    assert alert.target_id == instance.id
    assert "abc123" in (alert.description or "")


async def test_does_not_duplicate_alert_for_same_sql_hash_across_cycles() -> None:
    _reset_fake_collector()
    _reset_fake_alerts_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    finding = NormalizedPlanCompileTimeoutFinding(
        sql_hash="abc123", reason="TimeOutReached", statement_text_masked="SELECT * FROM t"
    )
    _FakeCollector._findings_by_instance = {instance.id: [finding]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)
    await supervisor._run_one_instance(instance.id)

    assert len(_FakeAlertsRepository.rules) == 1  # reusa a regra ja criada
    assert len(_FakeAlertsRepository.alerts) == 1  # nao duplica o alerta


async def test_creates_separate_alert_for_different_sql_hash() -> None:
    _reset_fake_collector()
    _reset_fake_alerts_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    finding_1 = NormalizedPlanCompileTimeoutFinding(
        sql_hash="hash-1", reason="TimeOutReached", statement_text_masked="SELECT 1"
    )
    finding_2 = NormalizedPlanCompileTimeoutFinding(
        sql_hash="hash-2", reason="MemoryLimitExceeded", statement_text_masked="SELECT 2"
    )
    _FakeCollector._findings_by_instance = {instance.id: [finding_1]}
    supervisor = _make_supervisor(registry)
    await supervisor._run_one_instance(instance.id)

    _FakeCollector._findings_by_instance = {instance.id: [finding_1, finding_2]}
    await supervisor._run_one_instance(instance.id)

    assert len(_FakeAlertsRepository.alerts) == 2


async def test_run_one_instance_skips_inactive_instance() -> None:
    _reset_fake_collector()
    _reset_fake_alerts_repository()
    instance = _make_instance(active=False)
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance
