"""Servico de status derivado de instancia — telemetria de ciclo do collector.

Deriva `health_status`/`collection_coverage`/`last_collected_at` a partir da telemetria de ciclo
que `PollingWorker._run_cycle` sempre grava em `metric_samples`
(`COLLECTOR_CYCLE_DURATION_MS`/`COLLECTOR_CYCLE_ERROR_COUNT`, `app/workers/polling_worker.py`),
lendo a hypertable bruta diretamente (nao os continuous aggregates, vazios e nunca atualizados).

Cortes de escopo deste MVP, documentados explicitamente para nao serem confundidos com bug:
    - `health_status` so cobre "healthy"/"unavailable"/"unknown" — "warning"/"critical" dependem
      de alertas (`GET /api/v1/alerts`, ainda 501), fora de escopo aqui;
    - `collection_coverage` so cobre "full"/"unavailable" — "partial" exigiria um catalogo de
      metricas esperadas por engine que ainda nao existe;
    - `capability_errors` e sempre `[]` — nao ha segunda fonte de dado para isso ainda.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID

from app.normalizers.metric_names import COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT
from app.repositories.metric_samples_repository import MetricSamplesRepository

# Janela generosa o suficiente para tolerar a diferenca de clock entre os dois `insert_batch`
# de telemetria do mesmo ciclo de `PollingWorker._run_cycle` (duration/error_count sao inseridos
# juntos, mas a contagem de metricas "reais" do ciclo pode ter timestamp levemente anterior).
_COVERAGE_WINDOW_SECONDS = 30


@dataclass(frozen=True, slots=True)
class InstanceDerivedStatus:
    """Status derivado de uma instancia, calculado a partir da telemetria de ciclo mais recente."""

    health_status: str  # "healthy" | "unavailable" | "unknown"
    collection_coverage: str  # "full" | "unavailable"
    capability_errors: list = field(default_factory=list)
    last_collected_at: datetime | None = None


UNKNOWN_STATUS = InstanceDerivedStatus(
    health_status="unknown",
    collection_coverage="unavailable",
    capability_errors=[],
    last_collected_at=None,
)


class InstanceDerivedStatusService:
    """Calcula `InstanceDerivedStatus` de uma instancia a partir de `metric_samples`."""

    def __init__(self, metric_samples_repository: MetricSamplesRepository) -> None:
        self._repo = metric_samples_repository

    async def compute(self, instance_id: UUID) -> InstanceDerivedStatus:
        duration_row = await self._repo.get_latest_value(
            "instance", instance_id, COLLECTOR_CYCLE_DURATION_MS
        )
        if duration_row is None:
            return UNKNOWN_STATUS

        cycle_time = duration_row.time
        error_row = await self._repo.get_latest_value(
            "instance", instance_id, COLLECTOR_CYCLE_ERROR_COUNT
        )
        error_count = error_row.value if error_row is not None else 0.0
        health_status = "unavailable" if error_count > 0 else "healthy"

        distinct_count = await self._repo.count_distinct_metrics_since(
            "instance",
            instance_id,
            cycle_time - timedelta(seconds=_COVERAGE_WINDOW_SECONDS),
            exclude_metric_names=[COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT],
        )
        collection_coverage = "full" if distinct_count > 0 else "unavailable"

        return InstanceDerivedStatus(
            health_status=health_status,
            collection_coverage=collection_coverage,
            capability_errors=[],
            last_collected_at=cycle_time if cycle_time.tzinfo else cycle_time.replace(tzinfo=UTC),
        )
