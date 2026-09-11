"""Repositorio de `Instance`/`InstanceMaintenanceWindow`.

Ver `app/models/instance.py` para os campos, `Docs/database-schema.md` §1 para o contrato de
persistencia.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select, update
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

    async def update_engine_version(
        self, instance_id: UUID, engine_version: str, engine_version_major: int
    ) -> None:
        """Grava a versao do motor detectada no ciclo de coleta mais recente (`PollingWorker`,
        via `CollectionResult.engine_version*`) — o coletor ja detecta a versao a cada ciclo
        (pra decidir qual variante de query usar, ex.: `pg_stat_checkpointer` vs
        `pg_stat_bgwriter`), mas ate aqui isso nunca era persistido de volta em `instances`
        (corte de escopo da Etapa 3, fechado agora). UPDATE simples, sem `commit()` proprio
        (chamado dentro do ciclo do `PollingWorker`, que ja gerencia a sessao/commit dos
        demais inserts do mesmo ciclo)."""
        await self._session.execute(
            update(Instance)
            .where(Instance.id == instance_id)
            .values(engine_version=engine_version, engine_version_major=engine_version_major)
        )
        await self._session.commit()
