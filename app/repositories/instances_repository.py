"""Repositorio de `Instance`/`InstanceMaintenanceWindow`.

Ver `app/models/instance.py` para os campos, `Docs/database-schema.md` §1 para o contrato de
persistencia.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import func, select, update
from sqlalchemy.orm import selectinload

from app.models.instance import Instance
from app.repositories.base import BaseRepository

_SOFT_DELETE_GRACE_DAYS = 30


class InstancesRepository(BaseRepository):
    """Acesso a dados de instancias monitoradas."""

    async def get_by_id(self, instance_id: UUID) -> Instance | None:
        # `selectinload` carrega `maintenance_windows` na mesma ida ao banco — acessar essa
        # relacao depois de fechar o `await` (ex.: `PollingWorker._is_within_maintenance_window`)
        # dispararia um lazy-load sincrono incompativel com `AsyncSession`
        # (`sqlalchemy.exc.MissingGreenlet`), confirmado rodando o smoke test real desta feature.
        result = await self._session.execute(
            select(Instance)
            .where(Instance.id == instance_id, Instance.deleted_at.is_(None))
            .options(selectinload(Instance.maintenance_windows))
        )
        return result.scalar_one_or_none()

    async def count(self) -> int:
        """Conta instancias nao-deletadas (ativas e inativas) — usado para checar limite do plano."""
        result = await self._session.execute(
            select(func.count()).select_from(Instance).where(Instance.deleted_at.is_(None))
        )
        return result.scalar_one()

    async def list_active(self) -> list[Instance]:
        result = await self._session.execute(
            select(Instance).where(Instance.active.is_(True), Instance.deleted_at.is_(None))
        )
        return list(result.scalars().all())

    async def list_all(self) -> list[Instance]:
        """Todas as instancias cadastradas (nao-deletadas), ativas ou nao — usada pela listagem
        da UI (`GET /instances`), diferente de `list_active()` (usada pelos workers/supervisors,
        que so devem agir sobre instancias ativas). Sem isso, pausar o monitoramento de uma
        instancia (`set_active(..., False)`) a faria sumir da tela sem jeito de reativar."""
        result = await self._session.execute(
            select(Instance).where(Instance.deleted_at.is_(None)).order_by(Instance.name)
        )
        return list(result.scalars().all())

    async def set_active(self, instance_id: UUID, active: bool) -> Instance | None:
        """Liga/desliga o monitoramento de uma instancia sem apagar o cadastro.
        `PollingSupervisor`/`MissingIndexSupervisor`/`BackupStatusSupervisor`/`AlertingWorker` ja
        respeitam `instances.active` via `list_active()` — nenhuma mudanca de worker necessaria,
        so persistir o flag."""
        instance = await self.get_by_id(instance_id)
        if instance is None:
            return None
        instance.active = active
        await self._session.commit()
        await self._session.refresh(instance)
        return instance

    async def delete(self, instance_id: UUID) -> bool:
        """Soft-delete: marca deleted_at, para a coleta e mantém o UUID por 30 dias.
        Permite restaurar a instância (e ver o histórico) se recadastrada no período de carência.
        Hard-delete real é feito por `purge_expired_soft_deleted()`."""
        instance = await self.get_by_id(instance_id)
        if instance is None:
            return False
        instance.deleted_at = datetime.now(UTC)
        instance.active = False
        await self._session.commit()
        return True

    async def find_soft_deleted(self, host: str, port: int, db_type: str) -> Instance | None:
        """Busca instância soft-deletada com mesmo host+porta+engine dentro do período de carência.
        Usada pelo fluxo de recadastro para restaurar o UUID original e recuperar o histórico."""
        cutoff = datetime.now(UTC) - timedelta(days=_SOFT_DELETE_GRACE_DAYS)
        result = await self._session.execute(
            select(Instance)
            .where(
                Instance.host == host,
                Instance.port == port,
                Instance.db_type == db_type,
                Instance.deleted_at.is_not(None),
                Instance.deleted_at >= cutoff,
            )
            .options(selectinload(Instance.maintenance_windows))
            .order_by(Instance.deleted_at.desc())
            .limit(1)
        )
        return result.scalar_one_or_none()

    async def restore(
        self,
        instance: Instance,
        *,
        name: str,
        criticality: str,
        polling_interval_seconds: int,
        encrypted_credential_ref: str | None,
        cloud_provider: str | None,
    ) -> Instance:
        """Restaura instância soft-deletada: limpa deleted_at, reativa coleta e atualiza campos
        mutáveis com os valores do novo cadastro (nome, credencial, intervalo, criticidade)."""
        instance.deleted_at = None
        instance.active = True
        instance.name = name
        instance.criticality = criticality
        instance.polling_interval_seconds = polling_interval_seconds
        instance.encrypted_credential_ref = encrypted_credential_ref
        instance.cloud_provider = cloud_provider
        instance.updated_at = datetime.now(UTC).replace(tzinfo=None)
        await self._session.commit()
        await self._session.refresh(instance)
        return instance

    async def purge_expired_soft_deleted(self) -> int:
        """Hard-delete de instâncias cujo período de carência (30 dias) expirou.
        Usa DELETE direto (não ORM) para evitar lazy-load de relacionamentos em AsyncSession.
        O FK ondelete=CASCADE na tabela instance_maintenance_windows cuida do cascade no banco."""
        cutoff = datetime.now(UTC) - timedelta(days=_SOFT_DELETE_GRACE_DAYS)
        result = await self._session.execute(
            sa.delete(Instance).where(
                Instance.deleted_at.is_not(None),
                Instance.deleted_at < cutoff,
            )
        )
        await self._session.commit()
        return cast(sa.CursorResult, result).rowcount or 0

    async def update_settings(
        self,
        instance_id: UUID,
        *,
        name: str | None = None,
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
        if name is not None:
            instance.name = name
        if polling_interval_seconds is not None:
            instance.polling_interval_seconds = polling_interval_seconds
        if criticality is not None:
            instance.criticality = criticality
        await self._session.commit()
        await self._session.refresh(instance)
        return instance

    async def set_min_lock_time_ms(self, instance_id: UUID, value: int | None) -> Instance | None:
        """Override ad-hoc do "Tempo mínimo para captura de Bloqueios" só para esta instância —
        método dedicado (não mais um parâmetro opcional de `update_settings`) porque `None` AQUI
        tem um significado real e distinto de "não mexer": limpa o override, voltando a usar o
        valor global (`system_settings.blocking_min_duration_ms`). `update_settings` não pode
        expressar isso — lá `None` sempre significa "não veio no payload"."""
        instance = await self.get_by_id(instance_id)
        if instance is None:
            return None
        instance.min_lock_time_ms = value
        await self._session.commit()
        await self._session.refresh(instance)
        return instance

    async def clear_all_min_lock_time_overrides(self) -> int:
        """Reset em massa — usado quando o usuário desmarca "Limites ad hoc" na UI (Configurações
        → Geral): apaga o override de TODAS as instâncias de uma vez (não só a selecionada no
        momento), voltando cada uma a usar o valor global. Devolve quantas linhas foram
        afetadas, só para telemetria/log — a UI não depende do valor exato."""
        result = await self._session.execute(
            update(Instance)
            .where(Instance.min_lock_time_ms.is_not(None))
            .values(min_lock_time_ms=None)
        )
        await self._session.commit()
        return cast(sa.CursorResult, result).rowcount or 0

    async def set_min_sleeping_time_ms(
        self, instance_id: UUID, value: int | None
    ) -> Instance | None:
        """Mesmo padrão de `set_min_lock_time_ms`, para "Transações em Sleeping"."""
        instance = await self.get_by_id(instance_id)
        if instance is None:
            return None
        instance.min_sleeping_time_ms = value
        await self._session.commit()
        await self._session.refresh(instance)
        return instance

    async def clear_all_min_sleeping_time_overrides(self) -> int:
        """Mesmo padrão de `clear_all_min_lock_time_overrides`, para "Transações em Sleeping"."""
        result = await self._session.execute(
            update(Instance)
            .where(Instance.min_sleeping_time_ms.is_not(None))
            .values(min_sleeping_time_ms=None)
        )
        await self._session.commit()
        return cast(sa.CursorResult, result).rowcount or 0

    async def create(self, instance: Instance) -> Instance:
        self._session.add(instance)
        await self._session.commit()
        await self._session.refresh(instance)
        return instance

    async def get_names_by_ids(self, ids: list[UUID]) -> dict[UUID, str]:
        """Devolve {id: name} para os ids pedidos — usado pelos endpoints de transacoes e
        bloqueios para resolver nomes de instancias parceiras (linked server / MSDTC) sem
        precisar de um join completo."""
        if not ids:
            return {}
        result = await self._session.execute(
            select(Instance.id, Instance.name).where(Instance.id.in_(ids))
        )
        return {row.id: row.name for row in result.all()}

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
