"""Supervisor de captura confiavel de erro por transacao — Etapa 10, item 3.

Mesmo padrao de `DeadlockSupervisor`: um unico intervalo compartilhado
(`transaction_error_check_interval_seconds`, default 2 minutos), um loop acorda no intervalo
configurado, relista instancias ativas e roda uma checagem por instancia, em sequencia. So
SQL Server (`collect_transaction_error_events()` do PostgreSQL sempre devolve lista vazia, ver
`app.collectors.postgresql.adapter`) — nenhuma logica especial de engine necessaria aqui, o
proprio coletor ja decide.

Diferente de `DeadlockSupervisor` (que reaproveita a sessao `system_health`, sempre ativa por
padrao), aqui a sessao Extended Events e' DEDICADA e precisa ser criada/iniciada por este
supervisor (`collector.ensure_transaction_error_capture()`, idempotente) — se isso falhar
(tipicamente por falta da permissao de servidor `ALTER ANY EVENT SESSION`), a checagem desta
instancia e' pulada neste ciclo (log de aviso, nunca propaga excecao), tentando de novo no
proximo ciclo.

Cada erro capturado (`session_id`, `error_number`, `message_masked`) so enriquece uma linha JA
EXISTENTE de `transaction_events` (`TransactionEventsRepository.tag_confirmed_errors_by_session`)
— o evento Extended Events so tem `session_id`, nao `transaction_identifier`, entao nao ha como
construir uma linha nova a partir dele sozinho (mesmo racional de `DeadlockSupervisor` com
`tag_open_transactions_by_session`). Quando mais de um erro aparece para a mesma sessao no mesmo
ciclo, mantem so o mais severo (e, em empate, o mais recente).
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
from app.normalizers.models import NormalizedConfirmedTransactionError
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
_TransactionEventsRepositoryFactory = Callable[[AsyncSession], TransactionEventsRepository]


def _pick_most_severe_error_per_session(
    errors: list[NormalizedConfirmedTransactionError],
) -> dict[str, tuple[int, str]]:
    """Quando mais de um erro aparece para a mesma sessao no mesmo ciclo, mantem so o mais
    severo (empate: o mais recente) — `transaction_events` guarda 1 erro confirmado por
    transacao, nao um historico."""
    best: dict[str, NormalizedConfirmedTransactionError] = {}
    for error in errors:
        current = best.get(error.session_id)
        if current is None or (error.severity, error.time) > (current.severity, current.time):
            best[error.session_id] = error
    return {
        session_id: (error.error_number, error.message_masked) for session_id, error in best.items()
    }


class TransactionErrorSupervisor:
    """Roda `collector.collect_transaction_error_events()` para cada instancia ativa, no
    intervalo configurado — worker de baixa frequencia, separado do `PollingSupervisor`."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        check_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        transaction_events_repository_factory: _TransactionEventsRepositoryFactory = (
            TransactionEventsRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._check_interval_seconds = check_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._transaction_events_repository_factory = transaction_events_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "TransactionErrorSupervisor iniciado check_interval=%ss", self._check_interval_seconds
        )
        while not self._stop_event.is_set():
            try:
                await self._run_all_instances()
            except Exception:  # noqa: BLE001 - um ciclo nunca derruba o supervisor
                logger.exception("Erro inesperado em TransactionErrorSupervisor._run_all_instances")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("TransactionErrorSupervisor finalizado")

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
                        "Instancia instance_id=%s inativa/removida - checagem de erro de "
                        "transacao pulada",
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
                    ready = await collector.ensure_transaction_error_capture()
                    if not ready:
                        logger.warning(
                            "Sessao Extended Events indisponivel para instance_id=%s - "
                            "checagem pulada neste ciclo",
                            instance_id,
                        )
                        return
                    errors = await collector.collect_transaction_error_events()
                    tagged = 0
                    if errors:
                        errors_by_session = _pick_most_severe_error_per_session(errors)
                        repository = self._transaction_events_repository_factory(session)
                        tagged = await repository.tag_confirmed_errors_by_session(
                            instance_id, errors_by_session
                        )
                    logger.info(
                        "Checagem de erro de transacao concluida instance_id=%s found=%s tagged=%s",
                        instance_id,
                        len(errors),
                        tagged,
                    )
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001 - checagem de 1 instancia nunca derruba o supervisor
            logger.exception(
                "Erro inesperado na checagem de erro de transacao de instance_id=%s", instance_id
            )
