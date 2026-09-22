"""Testes de `MissingIndexSupervisor` — `app/workers/missing_index_supervisor.py`.

Mesmo padrao de fakes de `tests/unit/workers/test_polling_supervisor.py` — sem banco real, sem
`AsyncSession` real, `Instance` instanciado direto e mutado em memoria.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import uuid4

from app.models.instance import Instance
from app.normalizers.models import NormalizedMissingIndexSuggestion
from app.security.credential_reference import CredentialResolutionError, ResolvedCredential
from app.workers.missing_index_supervisor import MissingIndexSupervisor


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


class _FakeMissingIndexSuggestionsRepository:
    """Compartilha `upserts` globalmente entre instancias (classe, nao instancia) — mesma
    convencao de `_FakeCollector.calls_by_instance` em test_polling_supervisor.py."""

    upserts: list = []

    def __init__(self, session: object) -> None:
        del session

    async def upsert_batch(self, suggestions: list, observed_at) -> None:  # noqa: ANN001
        type(self).upserts.append((suggestions, observed_at))


def _reset_fake_repository() -> None:
    _FakeMissingIndexSuggestionsRepository.upserts = []


class _FakeMissingIndexCollectionRunsRepository:
    """Compartilha `upserts` globalmente entre instancias (classe, nao instancia) — mesma
    convencao de `_FakeMissingIndexSuggestionsRepository`."""

    upserts: list = []

    def __init__(self, session: object) -> None:
        del session

    async def upsert(self, instance_id, collected_at, stats_since):  # noqa: ANN001
        type(self).upserts.append((instance_id, collected_at, stats_since))


def _reset_fake_collection_runs_repository() -> None:
    _FakeMissingIndexCollectionRunsRepository.upserts = []


class _FakeCollector:
    """Coleta com sucesso por padrao; `calls_by_instance`/`close_calls` registram chamadas."""

    _suggestions_by_instance: dict = {}
    _engine_start_time_by_instance: dict = {}
    calls_by_instance: dict = {}
    close_calls: list = []

    def __init__(self, db_type, connection, instance_id) -> None:  # noqa: ANN001 - fake
        self._instance_id = instance_id

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        type(self).calls_by_instance[self._instance_id] = (
            type(self).calls_by_instance.get(self._instance_id, 0) + 1
        )
        return type(self)._suggestions_by_instance.get(self._instance_id, [])

    async def get_engine_start_time(self) -> datetime | None:
        return type(self)._engine_start_time_by_instance.get(self._instance_id)

    async def close(self) -> None:
        type(self).close_calls.append(self._instance_id)


def _reset_fake_collector() -> None:
    _FakeCollector._suggestions_by_instance = {}
    _FakeCollector._engine_start_time_by_instance = {}
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
) -> MissingIndexSupervisor:
    kwargs: dict = {}
    if credential_resolver is not None:
        kwargs["credential_resolver"] = credential_resolver
    return MissingIndexSupervisor(
        _FakeSessionFactory(),
        check_interval_seconds,
        instances_repository_factory=_instances_repository_factory(registry),
        missing_index_suggestions_repository_factory=_FakeMissingIndexSuggestionsRepository,
        missing_index_collection_runs_repository_factory=(
            _FakeMissingIndexCollectionRunsRepository
        ),
        collector_factory=collector_factory or _FakeCollector,
        **kwargs,
    )


async def test_run_all_instances_collects_for_every_active_instance() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_collection_runs_repository()
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
    _reset_fake_collection_runs_repository()
    active_instance = _make_instance(active=True)
    inactive_instance = _make_instance(active=False)
    registry = {active_instance.id: active_instance, inactive_instance.id: inactive_instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_all_instances()

    assert active_instance.id in _FakeCollector.calls_by_instance
    assert inactive_instance.id not in _FakeCollector.calls_by_instance


async def test_run_one_instance_upserts_suggestions_when_present() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_collection_runs_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    suggestion = NormalizedMissingIndexSuggestion(
        instance_id=instance.id,
        database_name="db_a",
        table_name="public.pedidos",
        suggestion="candidato a indice",
        impact_score=10.0,
    )
    _FakeCollector._suggestions_by_instance = {instance.id: [suggestion]}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert len(_FakeMissingIndexSuggestionsRepository.upserts) == 1
    upserted_suggestions, observed_at = _FakeMissingIndexSuggestionsRepository.upserts[0]
    assert upserted_suggestions == [suggestion]
    assert isinstance(observed_at, datetime)
    assert observed_at.tzinfo is not None


async def test_run_one_instance_skips_upsert_when_no_suggestions() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_collection_runs_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert _FakeMissingIndexSuggestionsRepository.upserts == []
    assert instance.id in _FakeCollector.close_calls


async def test_run_one_instance_records_collection_run_even_without_suggestions() -> None:
    """Item 4 (pedido explicito do usuario): sem isso, nao ha como distinguir "nunca coletou" de
    "coletou e nao achou nenhuma sugestao" - o registro de execucao precisa ser gravado mesmo
    quando `suggestions` vem vazio."""
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_collection_runs_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert len(_FakeMissingIndexCollectionRunsRepository.upserts) == 1
    recorded_instance_id, collected_at, stats_since = (
        _FakeMissingIndexCollectionRunsRepository.upserts[0]
    )
    assert recorded_instance_id == instance.id
    assert isinstance(collected_at, datetime)
    assert collected_at.tzinfo is not None
    assert stats_since is None  # _FakeCollector nao configurado com engine_start_time


async def test_run_one_instance_records_engine_start_time_from_collector() -> None:
    """`stats_since` (desde quando o motor acumula as estatisticas de uso, reinicio do
    servico/cluster) e' uma pergunta DIFERENTE de `collected_at` (quando o worker rodou) - ver
    docstring de `MissingIndexCollectionRun`."""
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_collection_runs_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    engine_start_time = datetime(2025, 1, 1, tzinfo=UTC)
    _FakeCollector._engine_start_time_by_instance = {instance.id: engine_start_time}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    _, _, stats_since = _FakeMissingIndexCollectionRunsRepository.upserts[0]
    assert stats_since == engine_start_time


async def test_run_one_instance_does_not_raise_when_instance_no_longer_exists() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_collection_runs_repository()
    registry: dict = {}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(uuid4())  # nao deve lancar


async def test_run_one_instance_skips_when_encrypted_fallback_without_ref() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_collection_runs_repository()
    instance = _make_instance(auth_method="encrypted_fallback", encrypted_credential_ref=None)
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry)

    await supervisor._run_one_instance(instance.id)

    assert instance.id not in _FakeCollector.calls_by_instance


async def test_run_one_instance_skips_when_credential_resolution_fails() -> None:
    _reset_fake_collector()
    _reset_fake_repository()
    _reset_fake_collection_runs_repository()
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
    _reset_fake_collection_runs_repository()
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
    _reset_fake_collection_runs_repository()
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
    _reset_fake_collection_runs_repository()
    instance = _make_instance()
    registry = {instance.id: instance}
    supervisor = _make_supervisor(registry, check_interval_seconds=3600)

    run_task = asyncio.create_task(supervisor.run())
    await asyncio.sleep(0.05)  # deixa 1 ciclo completo rodar

    assert instance.id in _FakeCollector.calls_by_instance

    supervisor.stop()
    await asyncio.wait_for(run_task, timeout=2)
