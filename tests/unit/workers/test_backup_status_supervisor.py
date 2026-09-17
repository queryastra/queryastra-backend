"""Testes de `BackupStatusSupervisor` — `app/workers/backup_status_supervisor.py`.

Mesmo padrao de fakes de `tests/unit/workers/test_missing_index_supervisor.py` — sem banco real,
sem `AsyncSession` real, `Instance` instanciado direto e mutado em memoria.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from uuid import uuid4

from app.models.instance import Instance
from app.normalizers.metric_names import PG_BACKUP_DAYS_SINCE_LAST, SQLSERVER_BACKUP_DAYS_SINCE_LAST
from app.normalizers.models import NormalizedBackupStatus
from app.security.credential_reference import CredentialResolutionError, ResolvedCredential
from app.workers.backup_status_supervisor import BackupStatusSupervisor


def _make_instance(
    *,
    active: bool = True,
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
        polling_interval_seconds=10,
        auth_method=auth_method,
        encrypted_credential_ref=encrypted_credential_ref,
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


class _FakeBackupStatusRepository:
    """Compartilha `upserts` globalmente entre instancias (classe, nao instancia) — mesma
    convencao de `_FakeMissingIndexSuggestionsRepository`."""

    upserts: list = []

    def __init__(self, session: object) -> None:
        del session

    async def upsert_batch(self, rows: list, checked_at) -> None:  # noqa: ANN001
        type(self).upserts.append((rows, checked_at))


def _reset_fake_repository() -> None:
    _FakeBackupStatusRepository.upserts = []


class _FakeMetricSamplesRepository:
    """Compartilha `inserted` globalmente entre instancias (classe, nao instancia) — mesma
    convencao de `_FakeBackupStatusRepository`."""

    inserted: list = []

    def __init__(self, session: object) -> None:
        del session

    async def insert_batch(self, samples: list) -> None:  # noqa: ANN001
        type(self).inserted.append(samples)


def _reset_fake_metric_samples_repository() -> None:
    _FakeMetricSamplesRepository.inserted = []


class _FakeCollector:
    """Coleta com sucesso por padrao; `calls_by_instance`/`close_calls` registram chamadas."""

    _rows_by_instance: dict = {}
    calls_by_instance: dict = {}
    close_calls: list = []

    def __init__(self, db_type, connection, instance_id) -> None:  # noqa: ANN001 - fake
        self._instance_id = instance_id

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        type(self).calls_by_instance[self._instance_id] = (
            type(self).calls_by_instance.get(self._instance_id, 0) + 1
        )
        return type(self)._rows_by_instance.get(self._instance_id, [])

    async def close(self) -> None:
        type(self).close_calls.append(self._instance_id)


def _reset_fake_collector() -> None:
    _FakeCollector._rows_by_instance = {}
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
    check_interval_seconds: int = 3600,
) -> BackupStatusSupervisor:
    kwargs: dict = {}
    if credential_resolver is not None:
        kwargs["credential_resolver"] = credential_resolver
    return BackupStatusSupervisor(
        _FakeSessionFactory(),
        check_interval_seconds,
        instances_repository_factory=_instances_repository_factory(registry),
        backup_status_repository_factory=_FakeBackupStatusRepository,
        metric_samples_repository_factory=_FakeMetricSamplesRepository,
        collector_factory=collector_factory or _FakeCollector,
        **kwargs,
    )


async def test_run_all_instances_collects_for_every_active_instance() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance_a = _make_instance()
    instance_b = _make_instance()
    registry = {instance_a.id: instance_a, instance_b.id: instance_b}
    supervisor = _make_supervisor(registry)

    await supervisor._run_all_instances()

    assert _FakeCollector.calls_by_instance.get(instance_a.id) == 1
    assert _FakeCollector.calls_by_instance.get(instance_b.id) == 1
    assert instance_a.id in _FakeCollector.close_calls
    assert instance_b.id in _FakeCollector.close_calls


async def test_run_all_instances_skips_inactive_instance() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    active_instance = _make_instance(active=True)
    inactive_instance = _make_instance(active=False)
    registry = {active_instance.id: active_instance, inactive_instance.id: inactive_instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_all_instances()

    assert active_instance.id in _FakeCollector.calls_by_instance
    assert inactive_instance.id not in _FakeCollector.calls_by_instance


async def test_run_one_instance_upserts_rows_when_present() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    row = NormalizedBackupStatus(
        instance_id=instance.id,
        database_name=None,
        last_backup_at=None,
        detail={"archived_count": 1},
    )
    _FakeCollector._rows_by_instance = {instance.id: [row]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert len(_FakeBackupStatusRepository.upserts) == 1
    upserted_rows, checked_at = _FakeBackupStatusRepository.upserts[0]
    assert upserted_rows == [row]
    assert isinstance(checked_at, datetime)
    assert checked_at.tzinfo is not None


async def test_run_one_instance_writes_sentinel_metric_when_never_backed_up() -> None:
    """Fase 2, item 2 (backup atrasado como alerta) — `last_backup_at=None` vira o valor
    sentinela bem alto (3650 dias), nao `None`/ausencia de metrica."""
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance(db_type="postgresql")
    registry = {instance.id: instance}
    row = NormalizedBackupStatus(
        instance_id=instance.id, database_name=None, last_backup_at=None, detail={}
    )
    _FakeCollector._rows_by_instance = {instance.id: [row]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert len(_FakeMetricSamplesRepository.inserted) == 1
    samples = _FakeMetricSamplesRepository.inserted[0]
    assert len(samples) == 1
    assert samples[0].metric_name == PG_BACKUP_DAYS_SINCE_LAST
    assert samples[0].value == 3650.0
    assert samples[0].target_id == instance.id


async def test_run_one_instance_handles_naive_datetime_from_sqlserver() -> None:
    """`pymssql` devolve `backup_finish_date` como datetime NAIVE (sem timezone) - confirmado
    ao vivo contra Autklinux20, tratado como UTC (mesma convencao do resto do pipeline)."""
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance(db_type="sqlserver")
    registry = {instance.id: instance}
    naive_last_backup = datetime.now(UTC).replace(tzinfo=None) - timedelta(days=5)
    row = NormalizedBackupStatus(
        instance_id=instance.id,
        database_name="AB_COBRANCA",
        last_backup_at=naive_last_backup,
        detail={},
    )
    _FakeCollector._rows_by_instance = {instance.id: [row]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)  # nao deve levantar TypeError

    samples = _FakeMetricSamplesRepository.inserted[0]
    assert 4.9 < samples[0].value < 5.1


async def test_run_one_instance_writes_real_days_since_backup_for_sqlserver() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance(db_type="sqlserver")
    registry = {instance.id: instance}
    now = datetime.now(UTC)
    row = NormalizedBackupStatus(
        instance_id=instance.id,
        database_name="AB_COBRANCA",
        last_backup_at=now - timedelta(days=3),
        detail={},
    )
    _FakeCollector._rows_by_instance = {instance.id: [row]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    samples = _FakeMetricSamplesRepository.inserted[0]
    assert samples[0].metric_name == SQLSERVER_BACKUP_DAYS_SINCE_LAST
    assert 2.9 < samples[0].value < 3.1


async def test_run_one_instance_metric_uses_worst_case_across_databases() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance(db_type="sqlserver")
    registry = {instance.id: instance}
    now = datetime.now(UTC)
    rows = [
        NormalizedBackupStatus(
            instance_id=instance.id,
            database_name="banco_em_dia",
            last_backup_at=now - timedelta(hours=2),
            detail={},
        ),
        NormalizedBackupStatus(
            instance_id=instance.id,
            database_name="banco_atrasado",
            last_backup_at=now - timedelta(days=10),
            detail={},
        ),
    ]
    _FakeCollector._rows_by_instance = {instance.id: rows}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    samples = _FakeMetricSamplesRepository.inserted[0]
    assert 9.9 < samples[0].value < 10.1  # pior caso (10 dias), nao o melhor (2h)


async def test_run_one_instance_skips_upsert_when_no_rows() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert _FakeBackupStatusRepository.upserts == []
    assert instance.id in _FakeCollector.close_calls


async def test_run_one_instance_does_not_raise_when_instance_no_longer_exists() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    registry: dict = {}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(uuid4())  # nao deve lancar


async def test_run_one_instance_skips_when_encrypted_fallback_without_ref() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance(auth_method="encrypted_fallback", encrypted_credential_ref=None)
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance


async def test_run_one_instance_skips_when_credential_resolution_fails() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance(
        auth_method="encrypted_fallback", encrypted_credential_ref="ref-opaco"
    )
    registry = {instance.id: instance}

    class _FailingResolver:
        async def resolve(self, reference):  # noqa: ANN001, ANN201 - fake
            raise CredentialResolutionError("referencia invalida")

    supervisor = _make_supervisor(registry, credential_resolver=_FailingResolver())

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance


async def test_run_one_instance_resolves_credential_when_encrypted_fallback() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance(
        auth_method="encrypted_fallback", encrypted_credential_ref="ref-opaco"
    )
    registry = {instance.id: instance}

    class _WorkingResolver:
        async def resolve(self, reference):  # noqa: ANN001, ANN201 - fake
            return ResolvedCredential(username="user", secret_value="secret")

    supervisor = _make_supervisor(registry, credential_resolver=_WorkingResolver())

    await supervisor._run_one_instance(instance.id)

    assert _FakeCollector.calls_by_instance.get(instance.id) == 1
    assert instance.id in _FakeCollector.close_calls


async def test_collector_construction_error_for_one_instance_does_not_affect_another() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance_a = _make_instance()
    instance_b = _make_instance()
    registry = {instance_a.id: instance_a, instance_b.id: instance_b}

    collector_factory = _collector_factory_raising_for({instance_a.id})
    supervisor = _make_supervisor(registry, collector_factory=collector_factory)

    await supervisor._run_one_instance(instance_a.id)  # nao deve lancar, so loga e retorna
    await supervisor._run_one_instance(instance_b.id)

    assert instance_a.id not in _FakeCollector.calls_by_instance
    assert _FakeCollector.calls_by_instance.get(instance_b.id) == 1


async def test_run_and_stop_completes_cleanly() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_metric_samples_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry, check_interval_seconds=3600)

    run_task = asyncio.create_task(supervisor.run())
    await asyncio.sleep(0.05)  # deixa 1 ciclo completo rodar

    assert instance.id in _FakeCollector.calls_by_instance

    supervisor.stop()
    await asyncio.wait_for(run_task, timeout=2)
