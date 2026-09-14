"""Testes de `InstancesRepository` — `app/repositories/instances_repository.py`."""

from __future__ import annotations

from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.repositories.instances_repository import InstancesRepository


async def test_update_engine_version_persists_detected_version(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`PollingWorker._run_cycle()` chama isto apos um `collect()` bem-sucedido, com a versao
    que o coletor ja detecta a cada ciclo (`CollectionResult.engine_version*`) — antes desta
    feature, essa deteccao nunca era persistida de volta em `instances` (corte de escopo da
    Etapa 3)."""
    repo = InstancesRepository(db_session)
    assert test_instance.engine_version is None
    assert test_instance.engine_version_major is None

    await repo.update_engine_version(test_instance.id, "17.0", 17)

    updated = await repo.get_by_id(test_instance.id)
    assert updated is not None
    assert updated.engine_version == "17.0"
    assert updated.engine_version_major == 17


async def test_set_active_toggles_flag_and_persists(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Equivalente ao botao ON/OFF do SolarWinds — pausa/retoma monitoramento sem apagar o
    cadastro. `PollingSupervisor`/`MissingIndexSupervisor`/`BackupStatusSupervisor`/
    `AlertingWorker` ja re-leem `instances.active` via `list_active()`, entao so persistir o
    flag e suficiente pra eles pararem/retomarem na proxima reconciliacao."""
    repo = InstancesRepository(db_session)
    assert test_instance.active is True

    paused = await repo.set_active(test_instance.id, False)
    assert paused is not None
    assert paused.active is False

    reloaded = await repo.get_by_id(test_instance.id)
    assert reloaded is not None
    assert reloaded.active is False

    resumed = await repo.set_active(test_instance.id, True)
    assert resumed is not None
    assert resumed.active is True


async def test_set_active_returns_none_for_unknown_instance(db_session: AsyncSession) -> None:
    repo = InstancesRepository(db_session)
    result = await repo.set_active(uuid4(), False)
    assert result is None


async def test_list_all_includes_inactive_instances(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Regressao critica: `list_all()` (usada por `GET /instances`) precisa continuar
    devolvendo uma instancia pausada — diferente de `list_active()` (usada pelos
    workers/supervisors), que deve mesmo ignora-la. Sem isso, pausar uma instancia a faria
    sumir da tela sem nenhum jeito de reativar pela UI."""
    repo = InstancesRepository(db_session)
    await repo.set_active(test_instance.id, False)

    all_instances = await repo.list_all()
    assert any(i.id == test_instance.id for i in all_instances)

    active_instances = await repo.list_active()
    assert all(i.id != test_instance.id for i in active_instances)
