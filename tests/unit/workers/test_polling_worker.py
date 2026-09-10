"""Testes de `PollingWorker` — `app/workers/polling_worker.py`.

Usa fakes de `Collector` e dos 4 repositorios (gravam chamadas em listas, sem banco real) —
`_run_cycle()` e chamado diretamente para nao depender de `interval_seconds`.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, time
from uuid import uuid4

from app.collectors.base import CollectionResult
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


class _FakeInstancesRepository:
    def __init__(self, instance: Instance | None) -> None:
        self._instance = instance

    async def get_by_id(self, instance_id):  # noqa: ANN001, ANN201 - fake, assinatura simples
        return self._instance

    async def list_active(self) -> list[Instance]:
        return [self._instance] if self._instance is not None else []

    async def create(self, instance: Instance) -> Instance:
        return instance


def _build_result(instance_id) -> CollectionResult:  # noqa: ANN001 - fake, assinatura simples
    collected_at = datetime.now(UTC)
    return CollectionResult(
        instance_id=instance_id,
        collected_at=collected_at,
        duration_ms=5,
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
