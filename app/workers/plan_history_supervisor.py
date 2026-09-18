"""Supervisor de historico de plano de execucao — item priorizado pelo usuario.

Mesmo padrao de baixa frequencia de `MissingIndexSupervisor`/`BackupStatusSupervisor`/
`PlanCompileTimeoutSupervisor`: um unico loop acorda no intervalo configurado
(`plan_history_check_interval_seconds`, default 1 dia), relista instancias ativas e varre o
cache de planos de cada uma (so SQL Server — `collector.scan_plan_history()` ja devolve lista
vazia sem tentar conectar no PostgreSQL, mas este supervisor pula a chamada de proposito pra nao
abrir conexao a toa, mesmo racional do `PlanCompileTimeoutSupervisor`).

Cada snapshot devolvido pelo coletor (um por `sql_hash` distinto com plano em cache) so vira uma
linha nova em `query_plan_snapshots` quando o `plan_hash` mudou desde o ultimo conhecido para o
mesmo par (instance_id, sql_hash) — `QueryPlanSnapshotsRepository.insert_if_changed` faz essa
deduplicacao. Mesma politica de resiliencia dos demais supervisores: erro em uma instancia nunca
derruba outra instancia nem o supervisor inteiro.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.collectors.base import Collector
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.logging import get_logger
from app.repositories.instances_repository import InstancesRepository
from app.repositories.query_plan_snapshots_repository import QueryPlanSnapshotsRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

_CollectorFactory = Callable[..., Collector]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_QueryPlanSnapshotsRepositoryFactory = Callable[[AsyncSession], QueryPlanSnapshotsRepository]


class PlanHistorySupervisor:
    """Roda `collector.scan_plan_history()` para cada instancia SQL Server ativa, no intervalo
    configurado — worker de baixa frequencia, separado do `PollingSupervisor`."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        check_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        query_plan_snapshots_repository_factory: _QueryPlanSnapshotsRepositoryFactory = (
            QueryPlanSnapshotsRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._check_interval_seconds = check_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._query_plan_snapshots_repository_factory = query_plan_snapshots_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "PlanHistorySupervisor iniciado check_interval=%ss", self._check_interval_seconds
        )
        while not self._stop_event.is_set():
            try:
                await self._run_all_instances()
            except Exception:  # noqa: BLE001 - um ciclo nunca derruba o supervisor
                logger.exception("Erro inesperado em PlanHistorySupervisor._run_all_instances")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("PlanHistorySupervisor finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _run_all_instances(self) -> None:
        async with self._session_factory() as session:
            instances = await self._instances_repository_factory(session).list_active()

        for instance in instances:
            await self._run_one_instance(instance.id)

    async def _run_one_instance(self, instance_id: UUID) -> None:
        try:
            async with self._session_factory() as session:
                instances_repository = self._instances_repository_factory(session)
                instance = await instances_repository.get_by_id(instance_id)

                if instance is None or not instance.active:
                    logger.info(
                        "Instancia instance_id=%s inativa/removida - varredura de historico de "
                        "plano pulada",
                        instance_id,
                    )
                    return

                if instance.db_type != "sqlserver":
                    # Sem tentar conectar - PostgreSQL nao tem cache de plano exposto do mesmo
                    # jeito (mesmo racional de `Collector.scan_plan_history`).
                    return

                username: str | None = None
                secret_value: str | None = None
                if instance.auth_method == "encrypted_fallback":
                    if instance.encrypted_credential_ref is None:
                        logger.error(
                            "encrypted_credential_ref ausente para instance_id=%s "
                            "(auth_method=encrypted_fallback) - varredura pulada",
                            instance_id,
                        )
                        return
                    try:
                        resolved = await self._credential_resolver.resolve(
                            CredentialReference(instance.encrypted_credential_ref)
                        )
                    except CredentialResolutionError:
                        logger.exception(
                            "Falha ao resolver credencial de instance_id=%s - varredura pulada",
                            instance_id,
                        )
                        return
                    username, secret_value = resolved.username, resolved.secret_value

                connection = ConnectionParams(
                    host=instance.host,
                    port=instance.port,
                    auth_method=instance.auth_method,
                    username=username,
                    secret_value=secret_value,
                )
                collector = self._create_collector(
                    instance.db_type, connection=connection, instance_id=instance.id
                )
                try:
                    snapshots = await collector.scan_plan_history()
                    new_rows = []
                    if snapshots:
                        repository = self._query_plan_snapshots_repository_factory(session)
                        new_rows = await repository.insert_if_changed(snapshots)
                    logger.info(
                        "Varredura de historico de plano concluida instance_id=%s scanned=%s "
                        "changed=%s",
                        instance_id,
                        len(snapshots),
                        len(new_rows),
                    )
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001 - varredura de 1 instancia nunca derruba o supervisor
            logger.exception(
                "Erro inesperado na varredura de historico de plano de instance_id=%s",
                instance_id,
            )
