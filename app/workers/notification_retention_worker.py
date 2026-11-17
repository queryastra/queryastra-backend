"""Expurgo de notificacoes in-app antigas (`notifications.created_at` > `retention_days`).

Worker leve, sempre ligado (a tabela cresce a cada alerta x usuarios com acesso): uma rodada logo
ao iniciar e depois a cada `check_interval_seconds`.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.logging import get_logger
from app.repositories.notifications_repository import NotificationsRepository

logger = get_logger(__name__)

NOTIFICATION_RETENTION_DAYS = 30
NOTIFICATION_RETENTION_CHECK_INTERVAL_SECONDS = 6 * 3600


class NotificationRetentionWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        *,
        retention_days: int = NOTIFICATION_RETENTION_DAYS,
        check_interval_seconds: int = NOTIFICATION_RETENTION_CHECK_INTERVAL_SECONDS,
        repository_factory: Callable[[AsyncSession], NotificationsRepository] = (
            NotificationsRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._retention_days = retention_days
        self._check_interval_seconds = check_interval_seconds
        self._repository_factory = repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "NotificationRetentionWorker iniciado retention_days=%s check_interval=%ss",
            self._retention_days,
            self._check_interval_seconds,
        )
        while not self._stop_event.is_set():
            try:
                await self.run_once()
            except Exception:  # noqa: BLE001 - uma rodada nunca derruba o worker
                logger.exception("Erro inesperado em NotificationRetentionWorker.run_once")
            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("NotificationRetentionWorker finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def run_once(self) -> int:
        cutoff = datetime.now(UTC) - timedelta(days=self._retention_days)
        async with self._session_factory() as session:
            deleted = await self._repository_factory(session).delete_older_than(cutoff)
        if deleted:
            logger.info("NotificationRetentionWorker: %s notificacao(oes) expurgada(s)", deleted)
        return deleted
