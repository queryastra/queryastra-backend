"""Testes de `TransactionErrorSupervisor` — `app/workers/transaction_error_supervisor.py`.

Mesmo padrao de fakes de `tests/unit/workers/test_backup_status_supervisor.py` - sem banco real,
sem `AsyncSession` real, `Instance` instanciado direto e mutado em memoria.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

from app.models.instance import Instance
from app.normalizers.models import NormalizedConfirmedTransactionError
from app.security.credential_reference import CredentialResolutionError, ResolvedCredential
from app.workers.transaction_error_supervisor import TransactionErrorSupervisor


def _make_instance(
    *,
    active: bool = True,
    auth_method: str = "integrated",
    encrypted_credential_ref: str | None = None,
    db_type: str = "sqlserver",
) -> Instance:
    instance = Instance(
        id=uuid4(),
        name="instancia-teste",
        db_type=db_type,
        host="127.0.0.1",
        port=1433,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method=auth_method,
        encrypted_credential_ref=encrypted_credential_ref,
        active=active,
    )
    instance.maintenance_windows = []
    return instance


def _make_error(
    instance_id, session_id: str, error_number: int = 547, severity: int = 16, time=None
) -> NormalizedConfirmedTransactionError:  # noqa: ANN001 - fake, assinatura simples
    return NormalizedConfirmedTransactionError(
        instance_id=instance_id,
        session_id=session_id,
        error_number=error_number,
        severity=severity,
        message_masked="The INSERT statement conflicted with the CHECK constraint.",
        time=time or datetime.now(UTC),
    )


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


class _FakeTransactionEventsRepository:
    """Compartilha `tag_calls` globalmente entre instancias (classe, nao instancia) - mesma
    convencao de `_FakeBackupStatusRepository`."""

    tag_calls: list = []
    tag_return_value: int = 1

    def __init__(self, session: object) -> None:
        del session

    async def tag_confirmed_errors_by_session(self, instance_id, errors_by_session: dict) -> int:  # noqa: ANN001
        type(self).tag_calls.append((instance_id, errors_by_session))
        return type(self).tag_return_value


def _reset_fake_repository() -> None:
    _FakeTransactionEventsRepository.tag_calls = []
    _FakeTransactionEventsRepository.tag_return_value = 1


class _FakeCollector:
    """Sessao "pronta" e sem erros por padrao; `bootstrap_by_instance`/`errors_by_instance`
    configuraveis por teste, `close_calls` registra chamadas."""

    bootstrap_by_instance: dict = {}
    bootstrap_calls: list = []
    errors_by_instance: dict = {}
    collect_calls: list = []
    close_calls: list = []

    def __init__(self, db_type, connection, instance_id) -> None:  # noqa: ANN001 - fake
        self._instance_id = instance_id

    async def ensure_transaction_error_capture(self) -> bool:
        type(self).bootstrap_calls.append(self._instance_id)
        return type(self).bootstrap_by_instance.get(self._instance_id, True)

    async def collect_transaction_error_events(
        self,
    ) -> list[NormalizedConfirmedTransactionError]:
        type(self).collect_calls.append(self._instance_id)
        return type(self).errors_by_instance.get(self._instance_id, [])

    async def close(self) -> None:
        type(self).close_calls.append(self._instance_id)


def _reset_fake_collector() -> None:
    _FakeCollector.bootstrap_by_instance = {}
    _FakeCollector.bootstrap_calls = []
    _FakeCollector.errors_by_instance = {}
    _FakeCollector.collect_calls = []
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
    check_interval_seconds: int = 120,
) -> TransactionErrorSupervisor:
    kwargs: dict = {}
    if credential_resolver is not None:
        kwargs["credential_resolver"] = credential_resolver
    return TransactionErrorSupervisor(
        _FakeSessionFactory(),
        check_interval_seconds,
        instances_repository_factory=_instances_repository_factory(registry),
        transaction_events_repository_factory=_FakeTransactionEventsRepository,
        collector_factory=collector_factory or _FakeCollector,
        **kwargs,
    )


async def test_run_all_instances_collects_for_every_active_instance() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance_a = _make_instance()
    instance_b = _make_instance()
    registry = {instance_a.id: instance_a, instance_b.id: instance_b}
    supervisor = _make_supervisor(registry)

    await supervisor._run_all_instances()

    assert instance_a.id in _FakeCollector.collect_calls
    assert instance_b.id in _FakeCollector.collect_calls
    assert instance_a.id in _FakeCollector.close_calls
    assert instance_b.id in _FakeCollector.close_calls


async def test_run_all_instances_skips_inactive_instance() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    active_instance = _make_instance(active=True)
    inactive_instance = _make_instance(active=False)
    registry = {active_instance.id: active_instance, inactive_instance.id: inactive_instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_all_instances()

    assert active_instance.id in _FakeCollector.collect_calls
    assert inactive_instance.id not in _FakeCollector.collect_calls


async def test_run_one_instance_tags_transactions_when_errors_present() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    error = _make_error(instance.id, "62")
    _FakeCollector.errors_by_instance = {instance.id: [error]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert len(_FakeTransactionEventsRepository.tag_calls) == 1
    tagged_instance_id, errors_by_session = _FakeTransactionEventsRepository.tag_calls[0]
    assert tagged_instance_id == instance.id
    assert errors_by_session == {"62": (547, error.message_masked)}


async def test_run_one_instance_picks_most_severe_error_per_session() -> None:
    """Regressao: 2 erros da MESMA sessao no mesmo ciclo - mantem so o mais severo, nao os 2
    nem o mais antigo."""
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    less_severe = _make_error(instance.id, "62", error_number=8152, severity=16)
    more_severe = _make_error(instance.id, "62", error_number=547, severity=20)
    _FakeCollector.errors_by_instance = {instance.id: [less_severe, more_severe]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    _, errors_by_session = _FakeTransactionEventsRepository.tag_calls[0]
    assert errors_by_session == {"62": (547, more_severe.message_masked)}


async def test_run_one_instance_skips_read_when_session_bootstrap_fails() -> None:
    """Falha ao criar/iniciar a sessao Extended Events (ex.: sem ALTER ANY EVENT SESSION) -
    nunca tenta ler, nunca marca a instancia como erro."""
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    _FakeCollector.bootstrap_by_instance = {instance.id: False}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)  # nao deve lancar

    assert instance.id in _FakeCollector.bootstrap_calls
    assert instance.id not in _FakeCollector.collect_calls
    assert _FakeTransactionEventsRepository.tag_calls == []
    assert instance.id in _FakeCollector.close_calls


async def test_run_one_instance_skips_tagging_when_no_errors() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert _FakeTransactionEventsRepository.tag_calls == []
    assert instance.id in _FakeCollector.close_calls


async def test_run_one_instance_does_not_raise_when_instance_no_longer_exists() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    registry: dict = {}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(uuid4())  # nao deve lancar


async def test_run_one_instance_skips_when_encrypted_fallback_without_ref() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance(auth_method="encrypted_fallback", encrypted_credential_ref=None)
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.collect_calls


async def test_run_one_instance_skips_when_credential_resolution_fails() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance(
        auth_method="encrypted_fallback", encrypted_credential_ref="ref-opaco"
    )
    registry = {instance.id: instance}

    class _FailingResolver:
        async def resolve(self, reference):  # noqa: ANN001, ANN201 - fake
            raise CredentialResolutionError("referencia invalida")

    supervisor = _make_supervisor(registry, credential_resolver=_FailingResolver())

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.collect_calls


async def test_run_one_instance_resolves_credential_when_encrypted_fallback() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance(
        auth_method="encrypted_fallback", encrypted_credential_ref="ref-opaco"
    )
    registry = {instance.id: instance}

    class _WorkingResolver:
        async def resolve(self, reference):  # noqa: ANN001, ANN201 - fake
            return ResolvedCredential(username="user", secret_value="secret")

    supervisor = _make_supervisor(registry, credential_resolver=_WorkingResolver())

    await supervisor._run_one_instance(instance.id)

    assert instance.id in _FakeCollector.collect_calls
    assert instance.id in _FakeCollector.close_calls


async def test_collector_construction_error_for_one_instance_does_not_affect_another() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance_a = _make_instance()
    instance_b = _make_instance()
    registry = {instance_a.id: instance_a, instance_b.id: instance_b}

    collector_factory = _collector_factory_raising_for({instance_a.id})
    supervisor = _make_supervisor(registry, collector_factory=collector_factory)

    await supervisor._run_one_instance(instance_a.id)  # nao deve lancar, so loga e retorna
    await supervisor._run_one_instance(instance_b.id)

    assert instance_a.id not in _FakeCollector.collect_calls
    assert instance_b.id in _FakeCollector.collect_calls


async def test_run_and_stop_completes_cleanly() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry, check_interval_seconds=3600)

    run_task = asyncio.create_task(supervisor.run())
    await asyncio.sleep(0.05)  # deixa 1 ciclo completo rodar

    assert instance.id in _FakeCollector.collect_calls

    supervisor.stop()
    await asyncio.wait_for(run_task, timeout=2)
