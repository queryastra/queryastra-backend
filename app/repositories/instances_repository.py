"""Repositorio de `Instance`/`InstanceMaintenanceWindow`.

Ver `app/models/instance.py` para os campos, `Docs/database-schema.md` §1 para o contrato de
persistencia.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import selectinload

from app.models.instance import Instance
from app.repositories.base import BaseRepository


class InstancesRepository(BaseRepository):
    """Acesso a dados de instancias monitoradas."""

    async def get_by_id(self, instance_id: UUID) -> Instance | None:
        # `selectinload` carrega `maintenance_windows` na mesma ida ao banco — acessar essa
        # relacao depois de fechar o `await` (ex.: `PollingWorker._is_within_maintenance_window`)
        # dispararia um lazy-load sincrono incompativel com `AsyncSession`
        # (`sqlalchemy.exc.MissingGreenlet`), confirmado rodando o smoke test real desta feature.
        return await self._session.get(
            Instance, instance_id, options=[selectinload(Instance.maintenance_windows)]
        )

    async def list_active(self) -> list[Instance]:
        result = await self._session.execute(select(Instance).where(Instance.active.is_(True)))
        return list(result.scalars().all())

    async def create(self, instance: Instance) -> Instance:
        self._session.add(instance)
        await self._session.commit()
        await self._session.refresh(instance)
        return instance
