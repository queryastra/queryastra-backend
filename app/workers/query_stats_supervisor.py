"""Supervisor de snapshots de pg_stat_statements — Fase 6, T60.

Mesmo padrao de `ExtendedHealthMetricsSupervisor`: intervalo unico, itera instancias ativas
em sequencia, resiliencia por instancia (erro em uma nunca derruba outra).

Coleta apenas PostgreSQL — instancias SQL Server sao puladas (collect_query_stats_snapshot
retorna lista vazia via no-op do mixin, mas evitamos a conexao desnecessaria checando db_type).

Intervalo padrao: 3600s (1 hora). Retencao de dados: 30 dias (politica TimescaleDB na migration).
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.collectors.base import Collector
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.logging import get_logger
from app.repositories.instances_repository import InstancesRepository
from app.repositories.query_stats_trend_repository import QueryStatsTrendRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

_CollectorFactory = Callable[..., Collector]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_QueryStatsTrendRepositoryFactory = Callable[[AsyncSession], QueryStatsTrendRepository]


class QueryStatsSupervisor:
    """Captura periodica de pg_stat_statements para trending de performance de queries."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        check_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        query_stats_trend_repository_factory: (
            _QueryStatsTrendRepositoryFactory
        ) = QueryStatsTrendRepository,
    ) -> None:
        self._session_factory = session_factory
        self._check_interval_seconds = check_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._query_stats_trend_repository_factory = query_stats_trend_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "QueryStatsSupervisor iniciado check_interval=%ss",
            self._check_interval_seconds,
        )
        while not self._stop_event.is_set():
            try:
                await self._run_all_instances()
            except Exception:  # noqa: BLE001
                logger.exception("Erro inesperado em QueryStatsSupervisor._run_all_instances")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("QueryStatsSupervisor finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _run_all_instances(self) -> None:
        async with self._session_factory() as session:
            instances = await self._instances_repository_factory(session).list_active()

        for instance in instances:
            if instance.db_type != "postgresql":
                continue
            await self._run_one_instance(instance.id)

    async def _run_one_instance(self, instance_id: UUID) -> None:
        try:
            async with self._session_factory() as session:
                instances_repository = self._instances_repository_factory(session)
                instance = await instances_repository.get_by_id(instance_id)

                if instance is None or not instance.active:
                    logger.info(
                        "Instancia instance_id=%s inativa/removida - snapshot pulado",
                        instance_id,
                    )
                    return

                username: str | None = None
                secret_value: str | None = None
                if instance.auth_method == "encrypted_fallback":
                    if instance.encrypted_credential_ref is None:
                        logger.error(
                            "encrypted_credential_ref ausente para instance_id=%s - snapshot pulado",
                            instance_id,
                        )
                        return
                    try:
                        resolved = await self._credential_resolver.resolve(
                            CredentialReference(instance.encrypted_credential_ref)
                        )
                    except CredentialResolutionError:
                        logger.exception(
                            "Falha ao resolver credencial de instance_id=%s - snapshot pulado",
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
                    snapshots = await collector.collect_query_stats_snapshot()
                    if snapshots:
                        now = datetime.now(UTC)
                        repository = self._query_stats_trend_repository_factory(session)
                        await repository.insert_snapshot(
                            instance_id=instance.id,
                            time=now,
                            snapshots=[
                                {
                                    "queryid": s.queryid,
                                    "query_text": s.query_text,
                                    "calls": s.calls,
                                    "total_exec_time": s.total_exec_time,
                                    "total_rows": s.total_rows,
                                }
                                for s in snapshots
                            ],
                        )
                    logger.info(
                        "Snapshot pg_stat_statements concluido instance_id=%s rows=%s",
                        instance_id,
                        len(snapshots),
                    )
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001
            logger.exception(
                "Erro inesperado no snapshot pg_stat_statements de instance_id=%s", instance_id
            )
