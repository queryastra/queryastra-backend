"""Supervisor de metricas estendidas (freespace + replicacao) — Fase 2, item 2.

Mesmo padrao de `MissingIndexSupervisor`/`BackupStatusSupervisor`: so ha UM intervalo
compartilhado (`extended_health_metrics_check_interval_seconds`, default 1 hora — mais frequente
que backup/missing-index, ja que freespace/replicacao mudam mais rapido que isso, mas ainda bem
mais espacado que o ciclo de poll de 1-10s). Um unico loop acorda no intervalo configurado,
relista instancias ativas e roda uma coleta por instancia, em sequencia.

`collect_freespace_metrics()`/`collect_replication_lag_metrics()` (ver `app.collectors.base.
Collector`) ja degradam graciosamente por conta propria (lista vazia sem replica/AG configurado,
lista vazia sempre no PostgreSQL para freespace) — este supervisor so persiste o que veio,
seguindo o mesmo `MetricSamplesRepository.insert_batch` usado pelo `PollingWorker`.

Mesma politica de resiliencia dos demais supervisores: erro em uma instancia nunca derruba outra
instancia nem o supervisor inteiro.
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
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

_CollectorFactory = Callable[..., Collector]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_MetricSamplesRepositoryFactory = Callable[[AsyncSession], MetricSamplesRepository]


class ExtendedHealthMetricsSupervisor:
    """Roda `collector.collect_freespace_metrics()` e `collect_replication_lag_metrics()` para
    cada instancia ativa, no intervalo configurado — worker de baixa frequencia, separado do
    `PollingSupervisor`."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        check_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        metric_samples_repository_factory: (
            _MetricSamplesRepositoryFactory
        ) = MetricSamplesRepository,
    ) -> None:
        self._session_factory = session_factory
        self._check_interval_seconds = check_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._metric_samples_repository_factory = metric_samples_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "ExtendedHealthMetricsSupervisor iniciado check_interval=%ss",
            self._check_interval_seconds,
        )
        while not self._stop_event.is_set():
            try:
                await self._run_all_instances()
            except Exception:  # noqa: BLE001 - um ciclo nunca derruba o supervisor
                logger.exception(
                    "Erro inesperado em ExtendedHealthMetricsSupervisor._run_all_instances"
                )

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("ExtendedHealthMetricsSupervisor finalizado")

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
                        "Instancia instance_id=%s inativa/removida - coleta de metricas "
                        "estendidas pulada",
                        instance_id,
                    )
                    return

                username: str | None = None
                secret_value: str | None = None
                if instance.auth_method == "encrypted_fallback":
                    if instance.encrypted_credential_ref is None:
                        logger.error(
                            "encrypted_credential_ref ausente para instance_id=%s "
                            "(auth_method=encrypted_fallback) - coleta pulada",
                            instance_id,
                        )
                        return
                    try:
                        resolved = await self._credential_resolver.resolve(
                            CredentialReference(instance.encrypted_credential_ref)
                        )
                    except CredentialResolutionError:
                        logger.exception(
                            "Falha ao resolver credencial de instance_id=%s - coleta pulada",
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
                    freespace_samples = await collector.collect_freespace_metrics()
                    replication_samples = await collector.collect_replication_lag_metrics()
                    samples = freespace_samples + replication_samples
                    if samples:
                        repository = self._metric_samples_repository_factory(session)
                        await repository.insert_batch(samples)
                    logger.info(
                        "Coleta de metricas estendidas concluida instance_id=%s sample_count=%s",
                        instance_id,
                        len(samples),
                    )
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001 - coleta de 1 instancia nunca derruba o supervisor
            logger.exception(
                "Erro inesperado na coleta de metricas estendidas de instance_id=%s", instance_id
            )
