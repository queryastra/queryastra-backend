"""Supervisor de extracao de deadlocks — Etapa 10, item 7.

Mesmo padrao de `BackupStatusSupervisor`/`MissingIndexSupervisor`: um unico intervalo
compartilhado (`deadlock_check_interval_seconds`, default 5 minutos — o ring buffer
`system_health` ja retem historico recente sozinho, nao precisa ser raspado a cada poucos
segundos), um loop acorda no intervalo configurado, relista instancias ativas e roda uma
checagem por instancia, em sequencia. So SQL Server tem deadlock detalhado
(`collect_deadlock_events()` do PostgreSQL sempre devolve lista vazia, ver
`app.collectors.postgresql.adapter`) — nenhuma logica especial de engine necessaria aqui, o
proprio coletor ja decide.

Apos persistir deadlocks NOVOS (`DeadlockEventsRepository.insert_new`, que dedupe contra o que ja
existe), correlaciona com `transaction_events`: qualquer transacao AINDA ABERTA cujo `session_id`
bata com a vitima/sobrevivente ganha a tag `"deadlock"` em `flag_reason` (melhor esforco — ver
`TransactionEventsRepository.tag_open_transactions_by_session`).
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
from app.repositories.deadlock_events_repository import DeadlockEventsRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.transaction_events_repository import TransactionEventsRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

_CollectorFactory = Callable[..., Collector]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_DeadlockEventsRepositoryFactory = Callable[[AsyncSession], DeadlockEventsRepository]
_TransactionEventsRepositoryFactory = Callable[[AsyncSession], TransactionEventsRepository]


class DeadlockSupervisor:
    """Roda `collector.collect_deadlock_events()` para cada instancia ativa, no intervalo
    configurado — worker de baixa frequencia, separado do `PollingSupervisor`."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        check_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        deadlock_events_repository_factory: _DeadlockEventsRepositoryFactory = (
            DeadlockEventsRepository
        ),
        transaction_events_repository_factory: _TransactionEventsRepositoryFactory = (
            TransactionEventsRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._check_interval_seconds = check_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._deadlock_events_repository_factory = deadlock_events_repository_factory
        self._transaction_events_repository_factory = transaction_events_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("DeadlockSupervisor iniciado check_interval=%ss", self._check_interval_seconds)
        while not self._stop_event.is_set():
            try:
                await self._run_all_instances()
            except Exception:  # noqa: BLE001 - um ciclo nunca derruba o supervisor
                logger.exception("Erro inesperado em DeadlockSupervisor._run_all_instances")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("DeadlockSupervisor finalizado")

    def stop(self) -> None:
        """Sinaliza o loop para parar (mesma convencao dos demais supervisores)."""
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
                        "Instancia instance_id=%s inativa/removida - checagem de deadlock pulada",
                        instance_id,
                    )
                    return

                username: str | None = None
                secret_value: str | None = None
                if instance.auth_method == "encrypted_fallback":
                    if instance.encrypted_credential_ref is None:
                        logger.error(
                            "encrypted_credential_ref ausente para instance_id=%s "
                            "(auth_method=encrypted_fallback) - checagem pulada",
                            instance_id,
                        )
                        return
                    try:
                        resolved = await self._credential_resolver.resolve(
                            CredentialReference(instance.encrypted_credential_ref)
                        )
                    except CredentialResolutionError:
                        logger.exception(
                            "Falha ao resolver credencial de instance_id=%s - checagem pulada",
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
                    events = await collector.collect_deadlock_events()
                    new_rows = []
                    if events:
                        repository = self._deadlock_events_repository_factory(session)
                        new_rows = await repository.insert_new(events)
                    logger.info(
                        "Checagem de deadlock concluida instance_id=%s found=%s new=%s",
                        instance_id,
                        len(events),
                        len(new_rows),
                    )
                    if new_rows:
                        await self._tag_correlated_transactions(session, instance_id, new_rows)
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001 - checagem de 1 instancia nunca derruba o supervisor
            logger.exception(
                "Erro inesperado na checagem de deadlock de instance_id=%s", instance_id
            )

    async def _tag_correlated_transactions(
        self, session: AsyncSession, instance_id: UUID, new_rows: list
    ) -> None:
        session_ids = {
            sid
            for row in new_rows
            for sid in (row.victim_session_id, row.survivor_session_id)
            if sid is not None
        }
        if not session_ids:
            return
        transaction_events_repository = self._transaction_events_repository_factory(session)
        await transaction_events_repository.tag_open_transactions_by_session(
            instance_id, list(session_ids), "deadlock"
        )
