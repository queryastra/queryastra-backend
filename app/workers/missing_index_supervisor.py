"""Supervisor de checagem de indice ausente — ARCHITECTURE_SPEC.md Q18/Q21.

Mais simples que `PollingSupervisor` (app/workers/polling_supervisor.py): so ha UM intervalo
compartilhado (`missing_index_check_interval_seconds`, default 1 dia — mesma cadencia baixa do
Monitoramento Autbank real, Docs/monitoramento-autbank-analysis.md §1), nao um intervalo por
instancia como o `polling_interval_seconds` de alta frequencia. Um unico loop acorda no
intervalo configurado, relista instancias ativas e roda uma checagem por instancia, em
sequencia (nao ha requisito de paralelismo numa tarefa que ja e de baixissima frequencia).

Mesma politica de resiliencia do `PollingSupervisor`: erro em uma instancia (falha de conexao,
credencial invalida, bug) nunca derruba outra instancia nem o supervisor inteiro.
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
from app.repositories.missing_index_suggestions_repository import (
    MissingIndexSuggestionsRepository,
)
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

_CollectorFactory = Callable[..., Collector]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_MissingIndexSuggestionsRepositoryFactory = Callable[
    [AsyncSession], MissingIndexSuggestionsRepository
]


class MissingIndexSupervisor:
    """Roda `collector.collect_missing_index_suggestions()` para cada instancia ativa, no
    intervalo configurado — worker de baixa frequencia, separado do `PollingSupervisor`."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        check_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        missing_index_suggestions_repository_factory: (
            _MissingIndexSuggestionsRepositoryFactory
        ) = MissingIndexSuggestionsRepository,
    ) -> None:
        self._session_factory = session_factory
        self._check_interval_seconds = check_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._missing_index_suggestions_repository_factory = (
            missing_index_suggestions_repository_factory
        )
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "MissingIndexSupervisor iniciado check_interval=%ss", self._check_interval_seconds
        )
        while not self._stop_event.is_set():
            try:
                await self._run_all_instances()
            except Exception:  # noqa: BLE001 - um ciclo nunca derruba o supervisor
                logger.exception("Erro inesperado em MissingIndexSupervisor._run_all_instances")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("MissingIndexSupervisor finalizado")

    def stop(self) -> None:
        """Sinaliza o loop para parar (mesma convencao de PollingSupervisor/PollingWorker)."""
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
                        "Instancia instance_id=%s inativa/removida - checagem de indice "
                        "ausente pulada",
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
                    suggestions = await collector.collect_missing_index_suggestions()
                    if suggestions:
                        repository = self._missing_index_suggestions_repository_factory(session)
                        await repository.upsert_batch(suggestions, datetime.now(UTC))
                    logger.info(
                        "Checagem de indice ausente concluida instance_id=%s suggestion_count=%s",
                        instance_id,
                        len(suggestions),
                    )
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001 - checagem de 1 instancia nunca derruba o supervisor
            logger.exception(
                "Erro inesperado na checagem de indice ausente de instance_id=%s", instance_id
            )
