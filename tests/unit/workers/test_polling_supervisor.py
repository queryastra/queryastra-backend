"""Testes de `PollingSupervisor` — `app/workers/polling_supervisor.py`.

Usa fakes de `session_factory`, `credential_resolver`, `collector_factory` e dos 4
repositorios (injetados via seams do construtor) — sem banco real, sem `AsyncSession` real.
`Instance` e instanciado direto (model SQLAlchemy comum, nunca persistido) e mutado em memoria
entre chamadas de `_reconcile()` para simular mudanca de estado (ativo/inativo, intervalo).
"""

from __future__ import annotations

import asyncio
from uuid import uuid4

from app.collectors.base import CollectionResult
from app.models.instance import Instance
from app.security.credential_reference import CredentialResolutionError, ResolvedCredential
from app.workers.polling_supervisor import PollingSupervisor


def _make_instance(
    *,
    active: bool = True,
    polling_interval_seconds: int = 10,
    auth_method: str = "integrated",
    encrypted_credential_ref: str | None = None,
    db_type: str = "postgresql",
) -> Instance:
    instance = Instance(
        id=uuid4(),
        name="instancia-teste",
        db_type=db_type,
        host="127.0.0.1",
        port=5432,
        criticality="standard",
        polling_interval_seconds=polling_interval_seconds,
        auth_method=auth_method,
        encrypted_credential_ref=encrypted_credential_ref,
        active=active,
    )
    # Instance transiente (nunca persistida) - acessar `.maintenance_windows` sem setar
    # explicitamente dispararia lazy-load sincrono incompativel com AsyncSession. Mesma tecnica
    # de tests/unit/workers/test_polling_worker.py.
    instance.maintenance_windows = []
    return instance


class _FakeSession:
    async def __aenter__(self) -> _FakeSession:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None


class _FakeSessionFactory:
    """Fake de `async_sessionmaker` - so abre/fecha um context manager, nunca usado de verdade."""

    def __call__(self) -> _FakeSession:
        return _FakeSession()


class _FakeInstancesRepository:
    """Compartilha uma lista mutavel de instancias entre chamadas (`registry`), como um "banco"."""

    def __init__(self, session: object, registry: dict) -> None:  # noqa: ANN001 - fake
        del session
        self._registry = registry

    async def get_by_id(self, instance_id):  # noqa: ANN001, ANN201 - fake
        return self._registry.get(instance_id)

    async def list_active(self) -> list[Instance]:
        return [i for i in self._registry.values() if i.active]


def _instances_repository_factory(registry: dict):  # noqa: ANN201 - fake factory
    def factory(session: object) -> _FakeInstancesRepository:
        return _FakeInstancesRepository(session, registry)

    return factory


class _FakeMetricSamplesRepository:
    def __init__(self, session: object) -> None:
        del session
        self.batches: list = []

    async def insert_batch(self, samples: list) -> None:  # noqa: ANN201 - fake
        self.batches.append(samples)


class _FakeQuerySamplesRepository:
    def __init__(self, session: object) -> None:
        del session
        self.batches: list = []

    async def insert_batch(self, samples: list) -> None:
        self.batches.append(samples)


class _FakeBlockingEventsRepository:
    def __init__(self, session: object) -> None:
        del session
        self.batches: list = []

    async def insert_batch(self, events: list) -> None:
        self.batches.append(events)


class _FakeTransactionEventsRepository:
    def __init__(self, session: object) -> None:
        del session
        self.calls: list = []

    async def upsert_batch(
        self,
        instance_id: object,
        events: list,
        open_transaction_identifiers: object,
        observed_at: object,
    ) -> None:
        self.calls.append((instance_id, events, open_transaction_identifiers, observed_at))


class _FakeBlockingIncidentsRepository:
    def __init__(self, session: object) -> None:
        del session
        self.calls: list = []

    async def upsert_batch(self, instance_id: object, events: list, observed_at: object) -> None:
        self.calls.append((instance_id, events, observed_at))


def _build_result(instance_id) -> CollectionResult:  # noqa: ANN001 - fake
    from datetime import UTC, datetime

    return CollectionResult(
        instance_id=instance_id,
        collected_at=datetime.now(UTC),
        duration_ms=5,
        metric_samples=[],
        query_samples=[],
        blocking_events=[],
    )


class _FakeCollector:
    """Coleta com sucesso por padrao; `calls_by_instance` registra chamadas globais."""

    _fail_for: set = set()
    calls_by_instance: dict = {}
    close_calls: list = []

    def __init__(self, db_type, connection, instance_id) -> None:  # noqa: ANN001 - fake
        self._instance_id = instance_id

    async def check_capabilities(self):  # noqa: ANN201 - fake
        return []

    async def collect(self) -> CollectionResult:
        type(self).calls_by_instance[self._instance_id] = (
            type(self).calls_by_instance.get(self._instance_id, 0) + 1
        )
        return _build_result(self._instance_id)

    async def close(self) -> None:
        type(self).close_calls.append(self._instance_id)


def _reset_fake_collector() -> None:
    _FakeCollector._fail_for = set()
    _FakeCollector.calls_by_instance = {}
    _FakeCollector.close_calls = []


def _collector_factory_raising_for(instance_ids: set):  # noqa: ANN201 - fake factory
    def factory(db_type, connection, instance_id=None):  # noqa: ANN001 - fake
        if instance_id in instance_ids:
            raise RuntimeError("falha simulada ao construir collector")
        return _FakeCollector(db_type, connection, instance_id)

    return factory


def _make_supervisor(
    registry: dict,
    *,
    credential_resolver=None,  # noqa: ANN001 - fake, tipo inferido pelo uso
    collector_factory=None,  # noqa: ANN001
    discovery_interval_seconds: int = 3600,
) -> PollingSupervisor:
    kwargs: dict = {}
    if credential_resolver is not None:
        kwargs["credential_resolver"] = credential_resolver
    return PollingSupervisor(
        _FakeSessionFactory(),
        discovery_interval_seconds,
        instances_repository_factory=_instances_repository_factory(registry),
        metric_samples_repository_factory=_FakeMetricSamplesRepository,
        query_samples_repository_factory=_FakeQuerySamplesRepository,
        blocking_events_repository_factory=_FakeBlockingEventsRepository,
        transaction_events_repository_factory=_FakeTransactionEventsRepository,
        blocking_incidents_repository_factory=_FakeBlockingIncidentsRepository,
        collector_factory=collector_factory or _FakeCollector,
        **kwargs,
    )


async def test_reconcile_starts_task_for_new_active_instance() -> None:
    _reset_fake_collector()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._reconcile()

    assert instance.id in supervisor._instance_tasks
    handle = supervisor._instance_tasks[instance.id]
    assert handle.interval_seconds == instance.polling_interval_seconds
    assert not handle.task.done()

    supervisor.stop()
    await supervisor._stop_all_instance_tasks()


async def test_reconcile_stops_task_when_instance_becomes_inactive() -> None:
    _reset_fake_collector()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._reconcile()
    assert instance.id in supervisor._instance_tasks

    instance.active = False
    await supervisor._reconcile()

    assert instance.id not in supervisor._instance_tasks


async def test_reconcile_stops_task_when_instance_is_removed() -> None:
    _reset_fake_collector()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._reconcile()
    assert instance.id in supervisor._instance_tasks

    del registry[instance.id]
    await supervisor._reconcile()

    assert instance.id not in supervisor._instance_tasks


async def test_reconcile_restarts_task_when_interval_changes() -> None:
    _reset_fake_collector()
    instance = _make_instance(polling_interval_seconds=10)
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._reconcile()
    old_task = supervisor._instance_tasks[instance.id].task

    instance.polling_interval_seconds = 5
    await supervisor._reconcile()

    new_handle = supervisor._instance_tasks[instance.id]
    assert new_handle.interval_seconds == 5
    assert old_task.done()
    assert new_handle.task is not old_task

    supervisor.stop()
    await supervisor._stop_all_instance_tasks()


async def test_reconcile_restarts_task_that_finished_unexpectedly() -> None:
    _reset_fake_collector()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._reconcile()
    handle = supervisor._instance_tasks[instance.id]
    # Simula a task tendo terminado sozinha (defesa extra) - forca stop_event e espera a task
    # atual (que so termina no proximo timeout) ser substituida por uma nova finalizada.
    handle.stop_event.set()
    await handle.task  # deixa a task real terminar de verdade

    await supervisor._reconcile()

    new_handle = supervisor._instance_tasks[instance.id]
    assert new_handle.task is not handle.task
    assert not new_handle.task.done()

    supervisor.stop()
    await supervisor._stop_all_instance_tasks()


async def test_run_one_cycle_does_not_raise_when_instance_no_longer_exists() -> None:
    _reset_fake_collector()
    registry: dict = {}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_cycle(uuid4())  # nao deve lancar


async def test_run_one_cycle_skips_when_encrypted_fallback_without_ref() -> None:
    _reset_fake_collector()
    instance = _make_instance(auth_method="encrypted_fallback", encrypted_credential_ref=None)
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_cycle(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance


async def test_run_one_cycle_skips_when_credential_resolution_fails() -> None:
    _reset_fake_collector()
    instance = _make_instance(
        auth_method="encrypted_fallback", encrypted_credential_ref="ref-opaco"
    )
    registry = {instance.id: instance}

    class _FailingResolver:
        async def resolve(self, reference):  # noqa: ANN001, ANN201 - fake
            raise CredentialResolutionError("referencia invalida")

    supervisor = _make_supervisor(registry, credential_resolver=_FailingResolver())

    await supervisor._run_one_cycle(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance


async def test_run_one_cycle_resolves_credential_and_collects_when_encrypted_fallback() -> None:
    _reset_fake_collector()
    instance = _make_instance(
        auth_method="encrypted_fallback", encrypted_credential_ref="ref-opaco"
    )
    registry = {instance.id: instance}

    class _WorkingResolver:
        async def resolve(self, reference):  # noqa: ANN001, ANN201 - fake
            return ResolvedCredential(username="user", secret_value="secret")

    supervisor = _make_supervisor(registry, credential_resolver=_WorkingResolver())

    await supervisor._run_one_cycle(instance.id)

    assert _FakeCollector.calls_by_instance.get(instance.id) == 1
    assert instance.id in _FakeCollector.close_calls


async def test_collector_construction_error_for_one_instance_does_not_affect_another() -> None:
    _reset_fake_collector()
    instance_a = _make_instance()
    instance_b = _make_instance()
    registry = {instance_a.id: instance_a, instance_b.id: instance_b}

    collector_factory = _collector_factory_raising_for({instance_a.id})
    supervisor = _make_supervisor(registry, collector_factory=collector_factory)

    await supervisor._run_one_cycle(instance_a.id)  # nao deve lancar, so loga e retorna
    await supervisor._run_one_cycle(instance_b.id)

    assert instance_a.id not in _FakeCollector.calls_by_instance
    assert _FakeCollector.calls_by_instance.get(instance_b.id) == 1


async def test_run_and_stop_clears_all_instance_tasks() -> None:
    _reset_fake_collector()
    instance = _make_instance(polling_interval_seconds=3600)
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry, discovery_interval_seconds=3600)

    run_task = asyncio.create_task(supervisor.run())
    await asyncio.sleep(0.05)  # deixa reconcile + 1 ciclo de instancia rodar

    assert instance.id in supervisor._instance_tasks

    supervisor.stop()
    await asyncio.wait_for(run_task, timeout=2)

    assert supervisor._instance_tasks == {}
