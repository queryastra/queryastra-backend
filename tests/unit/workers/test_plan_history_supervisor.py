"""Testes de `PlanHistorySupervisor` — `app/workers/plan_history_supervisor.py`.

Mesmo padrao de fakes de `tests/unit/workers/test_plan_compile_timeout_supervisor.py`.
"""

from __future__ import annotations

from uuid import uuid4

from app.models.instance import Instance
from app.normalizers.models import NormalizedPlanSnapshot
from app.workers.plan_history_supervisor import PlanHistorySupervisor


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


class _FakeQueryPlanSnapshotsRepository:
    inserted: list[NormalizedPlanSnapshot] = []
    insert_calls: int = 0

    def __init__(self, session: object) -> None:
        del session

    async def insert_if_changed(
        self, snapshots: list[NormalizedPlanSnapshot]
    ) -> list[NormalizedPlanSnapshot]:
        type(self).insert_calls += 1
        type(self).inserted.extend(snapshots)
        return snapshots


def _reset_fake_repository() -> None:
    _FakeQueryPlanSnapshotsRepository.inserted = []
    _FakeQueryPlanSnapshotsRepository.insert_calls = 0


class _FakeCollector:
    _snapshots_by_instance: dict = {}
    calls_by_instance: dict = {}
    close_calls: list = []

    def __init__(self, db_type, connection, instance_id) -> None:  # noqa: ANN001
        self._instance_id = instance_id

    async def scan_plan_history(self) -> list[NormalizedPlanSnapshot]:
        type(self).calls_by_instance[self._instance_id] = (
            type(self).calls_by_instance.get(self._instance_id, 0) + 1
        )
        return type(self)._snapshots_by_instance.get(self._instance_id, [])

    async def close(self) -> None:
        type(self).close_calls.append(self._instance_id)


def _reset_fake_collector() -> None:
    _FakeCollector._snapshots_by_instance = {}
    _FakeCollector.calls_by_instance = {}
    _FakeCollector.close_calls = []


def _make_supervisor(registry: dict, *, check_interval_seconds: int = 86400):
    return PlanHistorySupervisor(
        _FakeSessionFactory(),
        check_interval_seconds,
        instances_repository_factory=_instances_repository_factory(registry),
        query_plan_snapshots_repository_factory=_FakeQueryPlanSnapshotsRepository,
        collector_factory=_FakeCollector,
    )


async def test_skips_postgresql_instance_without_connecting() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance(db_type="postgresql")
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance


async def test_run_one_instance_skips_inactive_instance() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance(active=False)
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance


async def test_forwards_scanned_snapshots_to_repository() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    snapshot = NormalizedPlanSnapshot(
        instance_id=instance.id,
        sql_hash="hash-1",
        plan_hash="plan-hash-1",
        plan_xml_masked="<ShowPlanXML>1</ShowPlanXML>",
        last_execution_time=None,
    )
    _FakeCollector._snapshots_by_instance = {instance.id: [snapshot]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert _FakeQueryPlanSnapshotsRepository.insert_calls == 1
    assert _FakeQueryPlanSnapshotsRepository.inserted == [snapshot]
    assert _FakeCollector.close_calls == [instance.id]


async def test_skips_repository_call_when_scan_returns_nothing() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert _FakeQueryPlanSnapshotsRepository.insert_calls == 0


async def test_error_in_one_instance_does_not_affect_another() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    good_instance = _make_instance()
    registry = {good_instance.id: good_instance}
    snapshot = NormalizedPlanSnapshot(
        instance_id=good_instance.id,
        sql_hash="hash-1",
        plan_hash="plan-hash-1",
        plan_xml_masked="<ShowPlanXML>1</ShowPlanXML>",
        last_execution_time=None,
    )
    _FakeCollector._snapshots_by_instance = {good_instance.id: [snapshot]}
    supervisor = _make_supervisor(registry)

    # Instancia inexistente no registry (get_by_id devolve None) - varredura pulada sem erro
    # propagar e sem afetar a instancia "boa" chamada depois.
    await supervisor._run_one_instance(uuid4())
    await supervisor._run_one_instance(good_instance.id)

    assert _FakeQueryPlanSnapshotsRepository.insert_calls == 1
