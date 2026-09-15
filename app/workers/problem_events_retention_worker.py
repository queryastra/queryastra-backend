"""Worker de expurgo por idade — Etapa 10, redesenho.

`transaction_events`/`blocking_incidents`/`deadlock_events` sao um LOG DE INCIDENTE retido para
analise posterior — nunca deletados so porque a condicao parou de ocorrer agora (fechamento vira
`closed_at`/`ended_at`, nao um DELETE). Este worker e o UNICO lugar que efetivamente apaga essas
linhas, e so as que ja estao fechadas/encerradas ha mais tempo que
`settings.problem_events_retention_days` — uma transacao/incidente ainda ABERTO nunca e removido,
nao importa ha quanto tempo.

Mesmo padrao de baixa frequencia dos demais workers (`BackupStatusSupervisor`/
`MissingIndexSupervisor`) — um unico loop compartilhado (nao por instancia, o expurgo e global),
desligado por padrao.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository
from app.repositories.deadlock_events_repository import DeadlockEventsRepository
from app.repositories.transaction_events_repository import TransactionEventsRepository

logger = get_logger(__name__)

_TransactionEventsRepositoryFactory = Callable[[AsyncSession], TransactionEventsRepository]
_BlockingIncidentsRepositoryFactory = Callable[[AsyncSession], BlockingIncidentsRepository]
_DeadlockEventsRepositoryFactory = Callable[[AsyncSession], DeadlockEventsRepository]


class ProblemEventsRetentionWorker:
    """Expurga, a cada `check_interval_seconds`, linhas fechadas/encerradas ha mais tempo que
    `retention_days` nas 3 tabelas de log de incidente."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        retention_days: int,
        check_interval_seconds: int,
        *,
        transaction_events_repository_factory: _TransactionEventsRepositoryFactory = (
            TransactionEventsRepository
        ),
        blocking_incidents_repository_factory: _BlockingIncidentsRepositoryFactory = (
            BlockingIncidentsRepository
        ),
        deadlock_events_repository_factory: _DeadlockEventsRepositoryFactory = (
            DeadlockEventsRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._retention_days = retention_days
        self._check_interval_seconds = check_interval_seconds
        self._transaction_events_repository_factory = transaction_events_repository_factory
        self._blocking_incidents_repository_factory = blocking_incidents_repository_factory
        self._deadlock_events_repository_factory = deadlock_events_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "ProblemEventsRetentionWorker iniciado retention_days=%s check_interval=%ss",
            self._retention_days,
            self._check_interval_seconds,
        )
        while not self._stop_event.is_set():
            try:
                await self._run_once()
            except Exception:  # noqa: BLE001 - uma rodada de expurgo nunca derruba o worker
                logger.exception("Erro inesperado em ProblemEventsRetentionWorker._run_once")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("ProblemEventsRetentionWorker finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _run_once(self) -> None:
        cutoff = datetime.now(UTC) - timedelta(days=self._retention_days)
        async with self._session_factory() as session:
            transaction_count = await self._transaction_events_repository_factory(
                session
            ).purge_closed_before(cutoff)
            blocking_count = await self._blocking_incidents_repository_factory(
                session
            ).purge_ended_before(cutoff)
            deadlock_count = await self._deadlock_events_repository_factory(
                session
            ).purge_older_than(cutoff)
        logger.info(
            "Expurgo concluido cutoff=%s transaction_events=%s blocking_incidents=%s "
            "deadlock_events=%s",
            cutoff.isoformat(),
            transaction_count,
            blocking_count,
            deadlock_count,
        )
