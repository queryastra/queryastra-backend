"""Worker de avaliacao de alertas — stub.

Deveria, por ciclo: ler `alert_rules` ativas (`app.repositories.alerts_repository`), avaliar
contra a amostra mais recente (threshold `[Min, Max)` semiaberto — ARCHITECTURE_SPEC.md §7a),
aplicar supressao por estado (nao reabrir um `Alert` ja `open` para a mesma condicao) e
`alert_silences` (silenciamento proativo, independente do estado do alerta), e disparar
`AlertNotification` via canal (email/webhook) respeitando o lembrete por tempo em alerta
critico nao reconhecido.
"""

from __future__ import annotations

import asyncio

from app.core.logging import get_logger

logger = get_logger(__name__)


class AlertingWorker:
    """Loop de avaliacao/disparo de alertas (stub — sem logica real ainda)."""

    def __init__(self, interval_seconds: int = 60) -> None:
        self._interval_seconds = interval_seconds
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("AlertingWorker iniciado (stub, sem avaliacao real)")
        while not self._stop_event.is_set():
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass

    def stop(self) -> None:
        self._stop_event.set()
