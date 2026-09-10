"""Worker de agregacao horaria/diaria — stub.

Na Fase 1, agregacao roda via TimescaleDB Continuous Aggregates
(`metric_samples_hourly`/`metric_samples_daily`, `query_stats_hourly` — `Docs/database-schema.md`
§2/§3), nao por job Python. Este worker fica reservado para agregacoes que nao cabem em SQL puro
(ex.: recalculo de `wait_bucket` resumido, `Docs/wait-taxonomy.md` "Acao") ou para acionar refresh
manual de continuous aggregate quando necessario.
"""

from __future__ import annotations

import asyncio

from app.core.logging import get_logger

logger = get_logger(__name__)


class AggregationWorker:
    """Loop de agregacao periodica (stub — sem logica real ainda)."""

    def __init__(self, interval_seconds: int = 3600) -> None:
        self._interval_seconds = interval_seconds
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("AggregationWorker iniciado (stub, sem agregacao real)")
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop_event.set()
