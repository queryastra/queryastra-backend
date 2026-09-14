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

    async def list_all(self) -> list[Instance]:
        """Todas as instancias cadastradas, ativas ou nao — usada pela listagem da UI
        (`GET /instances`), diferente de `list_active()` (usada pelos workers/supervisors, que
        so devem agir sobre instancias ativas). Sem isso, pausar o monitoramento de uma
        instancia (`set_active(..., False)`) a faria sumir da tela sem jeito de reativar."""
        result = await self._session.execute(select(Instance).order_by(Instance.name))
        return list(result.scalars().all())

    async def set_active(self, instance_id: UUID, active: bool) -> Instance | None:
        """Liga/desliga o monitoramento de uma instancia (equivalente ao botao ON/OFF da coluna
        Monitor do SolarWinds) sem apagar o cadastro. `PollingSupervisor`/`MissingIndexSupervisor`/
        `BackupStatusSupervisor`/`AlertingWorker` ja respeitam `instances.active` via
        `list_active()` — nenhuma mudanca de worker necessaria, so persistir o flag."""
        instance = await self.get_by_id(instance_id)
        if instance is None:
            return None
        instance.active = active
        await self._session.commit()
        await self._session.refresh(instance)
        return instance

    async def update_settings(
        self,
        instance_id: UUID,
        *,
        polling_interval_seconds: int | None = None,
        criticality: str | None = None,
    ) -> Instance | None:
        """Tela de Configurações (`PATCH /instances/{id}/settings`) — atualiza só os campos
        informados (`None` = não mexe nesse campo), sem restrição de acesso hoje (será limitada
        por RBAC quando essa feature existir, Fase 2). Cobre por enquanto:

        - `polling_interval_seconds` (sub-tela "Intervalo de Coleta") — antes só era possível
          definir isso uma vez, no cadastro (`POST /instances`), sem jeito de editar depois.
          `PollingSupervisor` reconcilia instâncias a cada `supervisor_discovery_interval_seconds`
          (default 15s), então o novo intervalo passa a valer sozinho na próxima reconciliação,
          sem restart do backend.
        - `criticality` (sub-tela "Geral") — mesmo gap: só era definível no cadastro até agora.

        Novos campos editáveis entram aqui como mais um parâmetro opcional, sem precisar de um
        novo endpoint/rota."""
        instance = await self.get_by_id(instance_id)
        if instance is None:
            return None
        if polling_interval_seconds is not None:
            instance.polling_interval_seconds = polling_interval_seconds
        if criticality is not None:
            instance.criticality = criticality
        await self._session.commit()
        await self._session.refresh(instance)
        return instance

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
