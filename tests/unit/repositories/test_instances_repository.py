"""Testes de `InstancesRepository` — `app/repositories/instances_repository.py`."""

from __future__ import annotations

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
