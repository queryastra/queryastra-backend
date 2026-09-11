"""Worker de polling por instancia.

Contrato (Docs/specs/01-collection-contract.md, Docs/rules/02-safe-collection.md):
    - cada instancia e polada no seu proprio `polling_interval_seconds`
      (`app/models/instance.py`, default 10s — ARCHITECTURE_SPEC.md Q3: 5s de default de
      produto recomendado, configuravel ate 1s em instancia critica; o default de schema fica
      conservador ate o throughput do pipeline ser validado);
    - o loop checa `instance_maintenance_windows` antes de cada ciclo — se `suppress_polling`
      e o horario atual (UTC) cai dentro da janela, o ciclo inteiro e pulado (nenhuma amostra
      coletada nem persistida, nenhuma telemetria de ciclo gravada);
    - erro de coleta (rede, timeout, qualquer `Exception`) e capturado por ciclo — nunca
      derruba o loop da instancia inteira; ainda assim grava telemetria de duracao/erro;
    - o loop nunca roda mais rapido que `polling_interval_seconds` nem acumula ciclos
      atrasados (skip, nao enfileira).

Convencao de `day_of_week` (`InstanceMaintenanceWindow`, `app/models/instance.py`): 0 = domingo,
conforme o comentario explicito no modelo (`# 0 = domingo`) e a CHECK `BETWEEN 0 AND 6` — NAO e
a convencao de `datetime.weekday()` do Python (Monday=0..Sunday=6). A conversao usada aqui e
`(weekday() + 1) % 7`. A janela e comparada em UTC (`datetime.now(UTC)`) e assume que
`start_time <= end_time` no mesmo dia — janelas que cruzam meia-noite (`end_time < start_time`)
NAO sao tratadas (corte de escopo desta feature, documentado em `_is_within_maintenance_window`).

TODO (fora de escopo desta feature, ver plano): nenhum supervisor auto-inicia `PollingWorker`
por instancia ativa em `app/main.py` — o worker so precisa ser instanciavel/testavel
isoladamente; orquestracao (ler `instances.active`, criar/parar workers dinamicamente) fica
para uma feature futura dedicada.
"""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime
from uuid import UUID

from app.collectors.base import Collector
from app.core.events import event_bus
from app.core.logging import get_logger
from app.models.instance import Instance
from app.normalizers.metric_names import COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT
from app.normalizers.models import NormalizedMetricSample
from app.repositories.blocking_events_repository import BlockingEventsRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.repositories.query_samples_repository import QuerySamplesRepository

logger = get_logger(__name__)


def _is_within_maintenance_window(instance: Instance, now: datetime) -> bool:
    """True se `now` (UTC) cai dentro de uma janela de manutencao com `suppress_polling=True`.

    Assume UTC e `start_time <= end_time` no mesmo dia — NAO trata cruzamento de meia-noite
    (`end_time < start_time`), corte de escopo desta feature.
    """
    schema_day_of_week = (now.weekday() + 1) % 7  # Python Monday=0 -> schema domingo=0
    current_time = now.time()  # datetime.time() ja descarta tzinfo, comparavel a Time (naive)
    for window in instance.maintenance_windows:
        if not window.suppress_polling:
            continue
        if window.day_of_week != schema_day_of_week:
            continue
        if window.start_time <= current_time <= window.end_time:
            return True
    return False


class PollingWorker:
    """Loop de polling de uma unica instancia.

    Uso pretendido: uma instancia deste worker por `Instance` ativa, orquestrado por um
    supervisor (ainda nao implementado, fora de escopo) que le `instances.active` e
    `instances.polling_interval_seconds` para criar/parar workers dinamicamente.
    """

    def __init__(
        self,
        instance_id: UUID,
        collector: Collector,
        interval_seconds: int,
        metric_samples_repository: MetricSamplesRepository,
        query_samples_repository: QuerySamplesRepository,
        blocking_events_repository: BlockingEventsRepository,
        instances_repository: InstancesRepository,
    ) -> None:
        self._instance_id = instance_id
        self._collector = collector
        self._interval_seconds = interval_seconds
        self._metric_samples_repository = metric_samples_repository
        self._query_samples_repository = query_samples_repository
        self._blocking_events_repository = blocking_events_repository
        self._instances_repository = instances_repository
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        """Loop principal: coleta, dorme `interval_seconds`, repete ate `stop()`."""
        logger.info(
            "PollingWorker iniciado instance_id=%s interval=%ss",
            self._instance_id,
            self._interval_seconds,
        )
        while not self._stop_event.is_set():
            await self._run_cycle()

            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass  # intervalo normal entre ciclos

    async def _run_cycle(self) -> None:
        """Executa um unico ciclo: checagem de janela de manutencao, coleta e persistencia.

        Publico o suficiente (nome com `_` mas sem underscore duplo) para ser chamado
        diretamente em teste/smoke-test sem esperar `interval_seconds`.
        """
        instance = await self._instances_repository.get_by_id(self._instance_id)
        now = datetime.now(UTC)

        if instance is not None and _is_within_maintenance_window(instance, now):
            logger.info(
                "Ciclo suprimido por janela de manutencao (suppress_polling) instance_id=%s",
                self._instance_id,
            )
            return

        cycle_start = time.perf_counter()
        error_count = 0
        try:
            result = await self._collector.collect()
            await self._metric_samples_repository.insert_batch(result.metric_samples)
            await self._query_samples_repository.insert_batch(result.query_samples)
            await self._blocking_events_repository.insert_batch(result.blocking_events)
            if result.engine_version is not None and result.engine_version_major is not None:
                await self._instances_repository.update_engine_version(
                    self._instance_id, result.engine_version, result.engine_version_major
                )
        except Exception:  # noqa: BLE001 — worker nunca deve morrer por erro de 1 ciclo
            error_count = 1
            logger.exception(
                "Erro inesperado no ciclo de polling de instance_id=%s", self._instance_id
            )

        cycle_duration_ms = (time.perf_counter() - cycle_start) * 1000
        telemetry_time = datetime.now(UTC)
        await self._metric_samples_repository.insert_batch(
            [
                NormalizedMetricSample(
                    time=telemetry_time,
                    target_type="instance",
                    target_id=self._instance_id,
                    metric_name=COLLECTOR_CYCLE_DURATION_MS,
                    value=cycle_duration_ms,
                ),
                NormalizedMetricSample(
                    time=telemetry_time,
                    target_type="instance",
                    target_id=self._instance_id,
                    metric_name=COLLECTOR_CYCLE_ERROR_COUNT,
                    value=float(error_count),
                ),
            ]
        )

        # Push em tempo real (`/ws/live`): notifica que um ciclo de coleta terminou para esta
        # instancia — nao publicado quando o ciclo e suprimido por janela de manutencao (return
        # antecipado acima, antes deste ponto).
        event_bus.publish(
            {
                "type": "instance_collected",
                "instance_id": str(self._instance_id),
                "error_count": error_count,
                "duration_ms": int(cycle_duration_ms),
            }
        )

    def stop(self) -> None:
        """Sinaliza o loop para parar no proximo ponto de checagem."""
        self._stop_event.set()
