"""Supervisor de coleta continua — reconciliacao dinamica de PollingWorker por instancia ativa.

Nao altera PollingWorker (construtor/`_run_cycle`/`run`/`stop` sao contrato testado e congelado,
ver app/workers/polling_worker.py). Este supervisor constroi um PollingWorker "descartavel" a
cada ciclo, com sessao/repositorios/collector novos, e chama `await worker._run_cycle()`
diretamente (reuso interno deliberado, mesmo pacote — nao e violacao de encapsulamento de API
publica).

Uma sessao NUNCA e reaproveitada entre ciclos (app/db/session.py: sessao de vida curta por
design). Erro em uma instancia (falha de conexao, credencial invalida, bug) nunca derruba outra
instancia nem o supervisor inteiro — sempre `try/except Exception` + `logger.exception` por
nivel (reconciliacao, ciclo de 1 instancia).
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import Callable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.collectors.base import Collector
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.logging import get_logger
from app.models.instance import Instance
from app.repositories.blocking_events_repository import BlockingEventsRepository
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.repositories.query_samples_repository import QuerySamplesRepository
from app.repositories.transaction_events_repository import TransactionEventsRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)
from app.workers.polling_worker import PollingWorker

logger = get_logger(__name__)

_CollectorFactory = Callable[..., Collector]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_MetricSamplesRepositoryFactory = Callable[[AsyncSession], MetricSamplesRepository]
_QuerySamplesRepositoryFactory = Callable[[AsyncSession], QuerySamplesRepository]
_BlockingEventsRepositoryFactory = Callable[[AsyncSession], BlockingEventsRepository]
_TransactionEventsRepositoryFactory = Callable[[AsyncSession], TransactionEventsRepository]
_BlockingIncidentsRepositoryFactory = Callable[[AsyncSession], BlockingIncidentsRepository]


@dataclass(slots=True)
class _InstanceTaskHandle:
    """Estado de uma task de polling de uma unica instancia, dono: PollingSupervisor."""

    task: asyncio.Task[None]
    stop_event: asyncio.Event
    interval_seconds: int


class PollingSupervisor:
    """Reconcilia `instances.active` com tasks `asyncio` de polling, uma por instancia ativa."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        discovery_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        metric_samples_repository_factory: _MetricSamplesRepositoryFactory = (
            MetricSamplesRepository
        ),
        query_samples_repository_factory: _QuerySamplesRepositoryFactory = (QuerySamplesRepository),
        blocking_events_repository_factory: _BlockingEventsRepositoryFactory = (
            BlockingEventsRepository
        ),
        transaction_events_repository_factory: _TransactionEventsRepositoryFactory = (
            TransactionEventsRepository
        ),
        blocking_incidents_repository_factory: _BlockingIncidentsRepositoryFactory = (
            BlockingIncidentsRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._discovery_interval_seconds = discovery_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._metric_samples_repository_factory = metric_samples_repository_factory
        self._query_samples_repository_factory = query_samples_repository_factory
        self._blocking_events_repository_factory = blocking_events_repository_factory
        self._transaction_events_repository_factory = transaction_events_repository_factory
        self._blocking_incidents_repository_factory = blocking_incidents_repository_factory
        self._stop_event = asyncio.Event()
        self._instance_tasks: dict[UUID, _InstanceTaskHandle] = {}

    async def run(self) -> None:
        logger.info(
            "PollingSupervisor iniciado discovery_interval=%ss", self._discovery_interval_seconds
        )
        while not self._stop_event.is_set():
            try:
                await self._reconcile()
            except Exception:  # noqa: BLE001 - reconciliacao nunca derruba o supervisor
                logger.exception("Erro inesperado em PollingSupervisor._reconcile")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._discovery_interval_seconds
                )
            except TimeoutError:
                pass

        await self._stop_all_instance_tasks()
        logger.info("PollingSupervisor finalizado")

    def stop(self) -> None:
        """Sinaliza o loop de reconciliacao para parar (mesma convencao de PollingWorker.stop)."""
        self._stop_event.set()

    async def _reconcile(self) -> None:
        async with self._session_factory() as session:
            instances = await self._instances_repository_factory(session).list_active()

        active_by_id: dict[UUID, Instance] = {instance.id: instance for instance in instances}

        for instance_id in list(self._instance_tasks):
            if instance_id not in active_by_id:
                logger.info(
                    "Instancia instance_id=%s nao esta mais ativa - parando task", instance_id
                )
                await self._stop_instance_task(instance_id)

        for instance_id, instance in active_by_id.items():
            handle = self._instance_tasks.get(instance_id)

            if handle is None:
                self._start_instance_task(instance)
                continue

            if handle.task.done():
                logger.warning(
                    "Task de instance_id=%s ja havia terminado inesperadamente - reiniciando",
                    instance_id,
                )
                self._instance_tasks.pop(instance_id, None)
                self._start_instance_task(instance)
                continue

            if handle.interval_seconds != instance.polling_interval_seconds:
                logger.info(
                    "polling_interval_seconds mudou para instance_id=%s (%ss -> %ss) - "
                    "reiniciando task",
                    instance_id,
                    handle.interval_seconds,
                    instance.polling_interval_seconds,
                )
                await self._stop_instance_task(instance_id)
                self._start_instance_task(instance)

    def _start_instance_task(self, instance: Instance) -> None:
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            self._instance_loop(instance.id, instance.polling_interval_seconds, stop_event)
        )
        self._instance_tasks[instance.id] = _InstanceTaskHandle(
            task=task, stop_event=stop_event, interval_seconds=instance.polling_interval_seconds
        )
        logger.info(
            "Task de polling iniciada instance_id=%s interval=%ss",
            instance.id,
            instance.polling_interval_seconds,
        )

    async def _stop_instance_task(self, instance_id: UUID) -> None:
        handle = self._instance_tasks.pop(instance_id, None)
        if handle is None:
            return
        handle.stop_event.set()
        try:
            await asyncio.wait_for(handle.task, timeout=self._discovery_interval_seconds)
        except TimeoutError:
            logger.warning(
                "Timeout esperando task de instance_id=%s parar - cancelando", instance_id
            )
            handle.task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await handle.task
        logger.info("Task de polling parada instance_id=%s", instance_id)

    async def _stop_all_instance_tasks(self) -> None:
        for instance_id in list(self._instance_tasks):
            await self._stop_instance_task(instance_id)

    async def _instance_loop(
        self, instance_id: UUID, interval_seconds: int, stop_event: asyncio.Event
    ) -> None:
        while not stop_event.is_set():
            await self._run_one_cycle(instance_id)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=interval_seconds)
            except TimeoutError:
                pass

    async def _run_one_cycle(self, instance_id: UUID) -> None:
        try:
            async with self._session_factory() as session:
                instances_repository = self._instances_repository_factory(session)
                instance = await instances_repository.get_by_id(instance_id)

                if instance is None or not instance.active:
                    logger.info(
                        "Instancia instance_id=%s inativa/removida - ciclo pulado", instance_id
                    )
                    return

                username: str | None = None
                secret_value: str | None = None
                if instance.auth_method == "encrypted_fallback":
                    if instance.encrypted_credential_ref is None:
                        logger.error(
                            "encrypted_credential_ref ausente para instance_id=%s "
                            "(auth_method=encrypted_fallback) - ciclo pulado",
                            instance_id,
                        )
                        return
                    try:
                        resolved = await self._credential_resolver.resolve(
                            CredentialReference(instance.encrypted_credential_ref)
                        )
                    except CredentialResolutionError:
                        logger.exception(
                            "Falha ao resolver credencial de instance_id=%s - ciclo pulado",
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
                    worker = PollingWorker(
                        instance_id,
                        collector,
                        instance.polling_interval_seconds,
                        self._metric_samples_repository_factory(session),
                        self._query_samples_repository_factory(session),
                        self._blocking_events_repository_factory(session),
                        instances_repository,
                        self._transaction_events_repository_factory(session),
                        self._blocking_incidents_repository_factory(session),
                    )
                    await worker._run_cycle()  # noqa: SLF001 - reuso interno deliberado
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001 - ciclo de 1 instancia nunca derruba o supervisor
            logger.exception(
                "Erro inesperado no ciclo de supervisor de instance_id=%s", instance_id
            )
