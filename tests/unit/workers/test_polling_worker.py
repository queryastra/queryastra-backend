"""Testes de `PollingWorker` — `app/workers/polling_worker.py`.

Usa fakes de `Collector` e dos 4 repositorios (gravam chamadas em listas, sem banco real) —
`_run_cycle()` e chamado diretamente para nao depender de `interval_seconds`.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, replace
from datetime import UTC, datetime, time
from uuid import uuid4

from app.collectors.base import CollectionResult
from app.core.events import event_bus
from app.models.instance import Instance, InstanceMaintenanceWindow
from app.normalizers.metric_names import COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT
from app.normalizers.models import (
    NormalizedBlockingEvent,
    NormalizedMetricSample,
    NormalizedQuerySample,
)
from app.workers.polling_worker import PollingWorker


class _FakeCollector:
    def __init__(
        self, result: CollectionResult | None = None, exc: Exception | None = None
    ) -> None:
        self._result = result
        self._exc = exc
        self.calls = 0

    async def check_capabilities(self):  # noqa: ANN201 - fake, assinatura simples
        return []

    async def collect(self) -> CollectionResult:
        self.calls += 1
        if self._exc is not None:
            raise self._exc
        assert self._result is not None
        return self._result

    async def close(self) -> None:
        return None


class _FakeMetricSamplesRepository:
    def __init__(self) -> None:
        self.batches: list[list[NormalizedMetricSample]] = []

    async def insert_batch(self, samples: list[NormalizedMetricSample]) -> None:
        self.batches.append(samples)


class _FakeQuerySamplesRepository:
    def __init__(self) -> None:
        self.batches: list[list[NormalizedQuerySample]] = []

    async def insert_batch(self, samples: list[NormalizedQuerySample]) -> None:
        self.batches.append(samples)


class _FakeBlockingEventsRepository:
    def __init__(self) -> None:
        self.batches: list[list[NormalizedBlockingEvent]] = []

    async def insert_batch(self, events: list[NormalizedBlockingEvent]) -> None:
        self.batches.append(events)


@dataclass
class _FakeSystemSettingsRow:
    blocking_min_duration_ms: int


class _FakeSystemSettingsRepository:
    def __init__(self, blocking_min_duration_ms: int) -> None:
        self._row = _FakeSystemSettingsRow(blocking_min_duration_ms)
        self.get_calls = 0

    async def get(self) -> _FakeSystemSettingsRow:
        self.get_calls += 1
        return self._row


class _FakeInstancesRepository:
    def __init__(self, instance: Instance | None) -> None:
        self._instance = instance
        self.engine_version_calls: list[tuple] = []

    async def get_by_id(self, instance_id):  # noqa: ANN001, ANN201 - fake, assinatura simples
        return self._instance

    async def list_active(self) -> list[Instance]:
        return [self._instance] if self._instance is not None else []

    async def create(self, instance: Instance) -> Instance:
        return instance

    async def update_engine_version(
        self, instance_id, engine_version, engine_version_major
    ) -> None:  # noqa: ANN001 - fake, assinatura simples
        self.engine_version_calls.append((instance_id, engine_version, engine_version_major))


def _build_result(  # noqa: ANN001 - fake, assinatura simples
    instance_id, *, engine_version: str | None = None, engine_version_major: int | None = None
) -> CollectionResult:
    collected_at = datetime.now(UTC)
    return CollectionResult(
        instance_id=instance_id,
        collected_at=collected_at,
        duration_ms=5,
        engine_version=engine_version,
        engine_version_major=engine_version_major,
        metric_samples=[
            NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name="test_metric",
                value=1.0,
            )
        ],
        query_samples=[
            NormalizedQuerySample(
                time=collected_at,
                instance_id=instance_id,
                engine="postgresql",
                sql_hash="hash",
                sql_text_masked="SELECT ?",
            )
        ],
        blocking_events=[
            NormalizedBlockingEvent(
                time=collected_at,
                instance_id=instance_id,
                blocker_session_id="1",
                blocked_session_id="2",
                wait_time_ms=10,
            )
        ],
    )


async def test_run_cycle_persists_samples_and_records_success_telemetry() -> None:
    instance_id = uuid4()
    result = _build_result(instance_id)
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=None)

    worker = PollingWorker(
        instance_id, collector, 10, metric_repo, query_repo, blocking_repo, instances_repo
    )
    await worker._run_cycle()

    assert collector.calls == 1
    assert query_repo.batches == [result.query_samples]
    assert blocking_repo.batches == [result.blocking_events]
    # metric_repo.insert_batch e chamado 2x: amostras da coleta + telemetria de ciclo.
    assert len(metric_repo.batches) == 2
    assert metric_repo.batches[0] == result.metric_samples

    telemetry_batch = metric_repo.batches[1]
    names = {s.metric_name for s in telemetry_batch}
    assert names == {COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT}
    error_sample = next(s for s in telemetry_batch if s.metric_name == COLLECTOR_CYCLE_ERROR_COUNT)
    assert error_sample.value == 0.0
    duration_sample = next(
        s for s in telemetry_batch if s.metric_name == COLLECTOR_CYCLE_DURATION_MS
    )
    assert duration_sample.value >= 0.0
    # Sem engine_version no CollectionResult (default None) -> nunca persiste (guard em
    # _run_cycle checando `is not None` nos dois campos).
    assert instances_repo.engine_version_calls == []


async def test_run_cycle_persists_detected_engine_version() -> None:
    """Regressao: a versao que o coletor ja detecta a cada ciclo (pra decidir variante de query,
    ex.: pg_stat_checkpointer vs pg_stat_bgwriter) passa a ser persistida de volta em
    `instances.engine_version*` via o `instances_repository` ja injetado — sem mudar o construtor
    de `PollingWorker` (mesmo padrao da publicacao de evento em `event_bus`)."""
    instance_id = uuid4()
    result = _build_result(instance_id, engine_version="17.0", engine_version_major=17)
    collector = _FakeCollector(result=result)
    instances_repo = _FakeInstancesRepository(instance=None)

    worker = PollingWorker(
        instance_id,
        collector,
        10,
        _FakeMetricSamplesRepository(),
        _FakeQuerySamplesRepository(),
        _FakeBlockingEventsRepository(),
        instances_repo,
    )
    await worker._run_cycle()

    assert instances_repo.engine_version_calls == [(instance_id, "17.0", 17)]


async def test_run_cycle_skips_entirely_during_active_maintenance_window() -> None:
    instance_id = uuid4()
    now = datetime.now(UTC)
    schema_day_of_week = (now.weekday() + 1) % 7  # 0 = domingo, ver docstring do worker

    instance = Instance(
        id=instance_id,
        name="instancia-em-manutencao",
        db_type="postgresql",
        host="127.0.0.1",
        port=5432,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
    )
    instance.maintenance_windows = [
        InstanceMaintenanceWindow(
            instance_id=instance_id,
            day_of_week=schema_day_of_week,
            start_time=time(0, 0, 0),
            end_time=time(23, 59, 59),
            suppress_polling=True,
        )
    ]

    collector = _FakeCollector(result=_build_result(instance_id))
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=instance)

    worker = PollingWorker(
        instance_id, collector, 10, metric_repo, query_repo, blocking_repo, instances_repo
    )
    await worker._run_cycle()

    assert collector.calls == 0
    assert metric_repo.batches == []
    assert query_repo.batches == []
    assert blocking_repo.batches == []


async def test_run_cycle_records_error_telemetry_when_collect_raises() -> None:
    instance_id = uuid4()
    collector = _FakeCollector(exc=RuntimeError("falha de rede simulada"))
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=None)

    worker = PollingWorker(
        instance_id, collector, 10, metric_repo, query_repo, blocking_repo, instances_repo
    )
    await worker._run_cycle()  # nunca deve propagar a excecao

    assert query_repo.batches == []
    assert blocking_repo.batches == []
    # so a telemetria e gravada (amostras da coleta nunca chegam a existir).
    assert len(metric_repo.batches) == 1
    telemetry_batch = metric_repo.batches[0]
    error_sample = next(s for s in telemetry_batch if s.metric_name == COLLECTOR_CYCLE_ERROR_COUNT)
    assert error_sample.value == 1.0


async def test_run_cycle_publishes_instance_collected_event() -> None:
    """`_run_cycle()` publica um evento `instance_collected` no `event_bus` real (singleton do
    modulo) — push em tempo real via `/ws/live`. Usa `event_bus.subscribe()` (mesmo singleton
    usado por `app/api/v1/websocket.py`), sem fake: e o objeto real do processo."""
    instance_id = uuid4()
    result = _build_result(instance_id)
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=None)

    worker = PollingWorker(
        instance_id, collector, 10, metric_repo, query_repo, blocking_repo, instances_repo
    )

    queue = event_bus.subscribe()
    try:
        await worker._run_cycle()
        event = await asyncio.wait_for(queue.get(), timeout=1)
    finally:
        event_bus.unsubscribe(queue)

    assert event["type"] == "instance_collected"
    assert event["instance_id"] == str(instance_id)
    assert event["error_count"] == 0
    assert event["duration_ms"] >= 0


async def test_run_cycle_does_not_publish_event_during_maintenance_window() -> None:
    instance_id = uuid4()
    now = datetime.now(UTC)
    schema_day_of_week = (now.weekday() + 1) % 7

    instance = Instance(
        id=instance_id,
        name="instancia-em-manutencao-evento",
        db_type="postgresql",
        host="127.0.0.1",
        port=5432,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
    )
    instance.maintenance_windows = [
        InstanceMaintenanceWindow(
            instance_id=instance_id,
            day_of_week=schema_day_of_week,
            start_time=time(0, 0, 0),
            end_time=time(23, 59, 59),
            suppress_polling=True,
        )
    ]

    collector = _FakeCollector(result=_build_result(instance_id))
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=instance)

    worker = PollingWorker(
        instance_id, collector, 10, metric_repo, query_repo, blocking_repo, instances_repo
    )

    queue = event_bus.subscribe()
    try:
        await worker._run_cycle()
        assert queue.empty()
    finally:
        event_bus.unsubscribe(queue)


async def test_run_stops_promptly_after_stop_is_called() -> None:
    instance_id = uuid4()
    collector = _FakeCollector(result=_build_result(instance_id))
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=None)

    worker = PollingWorker(
        instance_id, collector, 3600, metric_repo, query_repo, blocking_repo, instances_repo
    )

    run_task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.05)  # deixa o primeiro ciclo rodar
    worker.stop()
    await asyncio.wait_for(run_task, timeout=2)

    assert collector.calls == 1


async def test_run_cycle_filters_blocking_events_below_min_duration_setting() -> None:
    """`_build_result` produz um bloqueio com `wait_time_ms=10` — um limite de 50ms deve
    descartar esse evento antes de persistir (item priorizado pelo usuario: "Tempo minimo para
    captura de Bloqueios (Locks)")."""
    instance_id = uuid4()
    result = _build_result(instance_id)
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=None)
    settings_repo = _FakeSystemSettingsRepository(blocking_min_duration_ms=50)

    worker = PollingWorker(
        instance_id,
        collector,
        10,
        metric_repo,
        query_repo,
        blocking_repo,
        instances_repo,
        system_settings_repository=settings_repo,
    )
    await worker._run_cycle()

    assert blocking_repo.batches == [[]]
    assert settings_repo.get_calls == 1


async def test_run_cycle_keeps_blocking_events_at_or_above_min_duration_setting() -> None:
    instance_id = uuid4()
    result = _build_result(instance_id)  # wait_time_ms=10
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=None)
    settings_repo = _FakeSystemSettingsRepository(blocking_min_duration_ms=10)

    worker = PollingWorker(
        instance_id,
        collector,
        10,
        metric_repo,
        query_repo,
        blocking_repo,
        instances_repo,
        system_settings_repository=settings_repo,
    )
    await worker._run_cycle()

    assert blocking_repo.batches == [result.blocking_events]


async def test_run_cycle_skips_settings_lookup_when_repository_not_injected() -> None:
    """Sem `system_settings_repository` (default None) — comportamento IDENTICO a antes desta
    feature, nenhum bloqueio e filtrado (todos os testes acima desta linha no arquivo ja cobrem
    isso, mas este e explicito sobre o motivo)."""
    instance_id = uuid4()
    result = _build_result(instance_id)
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instances_repo = _FakeInstancesRepository(instance=None)

    worker = PollingWorker(
        instance_id, collector, 10, metric_repo, query_repo, blocking_repo, instances_repo
    )
    await worker._run_cycle()

    assert blocking_repo.batches == [result.blocking_events]


class _FakeDatabaseOverride:
    def __init__(self, database_name: str, min_lock_time_ms: int) -> None:
        self.database_name = database_name
        self.min_lock_time_ms = min_lock_time_ms


class _FakeInstanceDatabaseLockOverridesRepository:
    def __init__(self, overrides: list[_FakeDatabaseOverride]) -> None:
        self._overrides = overrides
        self.list_calls = 0

    async def list_for_instance(self, instance_id):  # noqa: ANN001, ANN201 - fake
        self.list_calls += 1
        return self._overrides


def _make_instance_with_override(instance_id, min_lock_time_ms: int | None) -> Instance:  # noqa: ANN001
    instance = Instance(
        id=instance_id,
        name="instancia-com-override",
        db_type="postgresql",
        host="127.0.0.1",
        port=5432,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
        min_lock_time_ms=min_lock_time_ms,
    )
    instance.maintenance_windows = []
    return instance


async def test_run_cycle_prefers_instance_override_over_global_setting() -> None:
    """Override ad-hoc (`instance.min_lock_time_ms=5`) vence o valor global (500 no fake) —
    resultado: o bloqueio com `wait_time_ms=10` (de `_build_result`) passa (10 >= 5)."""
    instance_id = uuid4()
    result = _build_result(instance_id)
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instance = _make_instance_with_override(instance_id, min_lock_time_ms=5)
    instances_repo = _FakeInstancesRepository(instance=instance)
    settings_repo = _FakeSystemSettingsRepository(blocking_min_duration_ms=500)

    worker = PollingWorker(
        instance_id,
        collector,
        10,
        metric_repo,
        query_repo,
        blocking_repo,
        instances_repo,
        system_settings_repository=settings_repo,
    )
    await worker._run_cycle()

    assert blocking_repo.batches == [result.blocking_events]
    # O global nunca foi consultado - o override por instancia ja resolveu o limite.
    assert settings_repo.get_calls == 0


async def test_run_cycle_falls_back_to_global_setting_when_override_is_null() -> None:
    """`instance.min_lock_time_ms=None` (sem override) cai pro valor global (50) - o bloqueio
    com `wait_time_ms=10` e descartado (10 < 50)."""
    instance_id = uuid4()
    result = _build_result(instance_id)
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instance = _make_instance_with_override(instance_id, min_lock_time_ms=None)
    instances_repo = _FakeInstancesRepository(instance=instance)
    settings_repo = _FakeSystemSettingsRepository(blocking_min_duration_ms=50)

    worker = PollingWorker(
        instance_id,
        collector,
        10,
        metric_repo,
        query_repo,
        blocking_repo,
        instances_repo,
        system_settings_repository=settings_repo,
    )
    await worker._run_cycle()

    assert blocking_repo.batches == [[]]
    assert settings_repo.get_calls == 1


async def test_run_cycle_prefers_database_override_over_instance_override() -> None:
    """Override ad-hoc por BANCO (`AB_COBRANCA`, limite 5) vence tanto o override de instância
    (500) quanto o valor global — o evento bloqueado nesse banco (`wait_time_ms=10`) passa."""
    instance_id = uuid4()
    result = _build_result(instance_id)
    result = replace(
        result,
        blocking_events=[
            result.blocking_events[0].model_copy(update={"blocked_database_name": "AB_COBRANCA"})
        ],
    )
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instance = _make_instance_with_override(instance_id, min_lock_time_ms=500)
    instances_repo = _FakeInstancesRepository(instance=instance)
    database_overrides_repo = _FakeInstanceDatabaseLockOverridesRepository(
        [_FakeDatabaseOverride("AB_COBRANCA", 5)]
    )

    worker = PollingWorker(
        instance_id,
        collector,
        10,
        metric_repo,
        query_repo,
        blocking_repo,
        instances_repo,
        instance_database_lock_overrides_repository=database_overrides_repo,
    )
    await worker._run_cycle()

    assert blocking_repo.batches == [result.blocking_events]
    assert database_overrides_repo.list_calls == 1


async def test_run_cycle_database_override_does_not_affect_other_databases() -> None:
    """O override ad-hoc por banco (`AB_COBRANCA`, limite 5) só vale pra eventos DAQUELE banco —
    um evento de outro banco (`OUTRO_BANCO`) continua caindo pro override de instância (500),
    que descarta o evento (`wait_time_ms=10` < 500)."""
    instance_id = uuid4()
    result = _build_result(instance_id)
    result = replace(
        result,
        blocking_events=[
            result.blocking_events[0].model_copy(update={"blocked_database_name": "OUTRO_BANCO"})
        ],
    )
    collector = _FakeCollector(result=result)
    metric_repo = _FakeMetricSamplesRepository()
    query_repo = _FakeQuerySamplesRepository()
    blocking_repo = _FakeBlockingEventsRepository()
    instance = _make_instance_with_override(instance_id, min_lock_time_ms=500)
    instances_repo = _FakeInstancesRepository(instance=instance)
    database_overrides_repo = _FakeInstanceDatabaseLockOverridesRepository(
        [_FakeDatabaseOverride("AB_COBRANCA", 5)]
    )

    worker = PollingWorker(
        instance_id,
        collector,
        10,
        metric_repo,
        query_repo,
        blocking_repo,
        instances_repo,
        instance_database_lock_overrides_repository=database_overrides_repo,
    )
    await worker._run_cycle()

    assert blocking_repo.batches == [[]]
