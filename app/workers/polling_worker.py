"""Worker de polling por instancia — stub.

Contrato (Docs/specs/01-collection-contract.md, Docs/rules/02-safe-collection.md):
    - cada instancia e polada no seu proprio `polling_interval_seconds`
      (`app/models/instance.py`, default 10s — ARCHITECTURE_SPEC.md Q3: 5s de default de
      produto recomendado, configuravel ate 1s em instancia critica; o default de schema fica
      conservador ate o throughput do pipeline ser validado);
    - o loop deve checar `instance_maintenance_windows` antes de cada ciclo — se
      `suppress_polling=true` e o horario atual cai dentro da janela, o ciclo e pulado (nao so
      os alertas, a coleta inteira);
    - erro de capacidade (permissao/versao) degrada so a capacidade afetada
      (`app.services.health_check_service`), nunca derruba o loop da instancia inteira;
    - o loop nunca deve rodar mais rapido que `polling_interval_seconds` nem acumular ciclos
      atrasados (skip, nao enfileira).

Este stub so define o shape do loop assincrono (sleep + chamada de coleta) — nenhuma logica real
de agendamento, backoff ou paralelismo entre instancias esta implementada ainda.
"""

from __future__ import annotations

import asyncio
from uuid import UUID

from app.collectors.base import Collector
from app.core.logging import get_logger

logger = get_logger(__name__)


class PollingWorker:
    """Loop de polling de uma unica instancia.

    Uso pretendido: uma instancia deste worker por `Instance` ativa, orquestrado por um
    supervisor (ainda nao implementado) que le `instances.active` e
    `instances.polling_interval_seconds` para criar/parar workers dinamicamente.
    """

    def __init__(self, instance_id: UUID, collector: Collector, interval_seconds: int) -> None:
        self._instance_id = instance_id
        self._collector = collector
        self._interval_seconds = interval_seconds
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        """Loop principal: coleta, dorme `interval_seconds`, repete ate `stop()`.

        TODO (nao implementado neste scaffold):
            - checar `instance_maintenance_windows` (pular ciclo quando `suppress_polling=true`
              e o horario cair na janela);
            - persistir amostras via `app.repositories.metric_samples_repository` /
              `query_samples_repository`;
            - registrar duracao/erro/cobertura por ciclo (Docs/specs/01-collection-contract.md).
        """
        logger.info(
            "PollingWorker iniciado (stub, sem coleta real) instance_id=%s interval=%ss",
            self._instance_id,
            self._interval_seconds,
        )
        while not self._stop_event.is_set():
            try:
                await self._collector.collect()
            except NotImplementedError:
                logger.info(
                    "Coleta ainda nao implementada para instance_id=%s — aguardando proximo ciclo",
                    self._instance_id,
                )
            except Exception:  # noqa: BLE001 — worker nunca deve morrer por erro de 1 ciclo
                logger.exception(
                    "Erro inesperado no ciclo de polling de instance_id=%s", self._instance_id
                )

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass  # intervalo normal entre ciclos

    def stop(self) -> None:
        """Sinaliza o loop para parar no proximo ponto de checagem."""
        self._stop_event.set()
