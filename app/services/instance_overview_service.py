"""Monta os cards do dashboard de visao geral (um por instancia acessivel).

Toda a leitura e feita em lote por `InstanceOverviewRepository` (uma query por tipo de dado,
nunca por instancia). A unica excecao e o fallback de status derivado: instancias SEM telemetria
de ciclo na janela recente caem no `InstanceDerivedStatusService` (2 queries cada) — sao
justamente as instancias paradas/novas, poucas na pratica, e o fallback preserva exatamente o
mesmo `last_collected_at` mostrado no resto da UI.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from uuid import UUID

from app.models.instance import Instance
from app.normalizers.metric_names import (
    COLLECTOR_CYCLE_DURATION_MS,
    COLLECTOR_CYCLE_ERROR_COUNT,
    DB2_ACTIVE_SESSION_COUNT,
    DB2_DEADLOCKS_TOTAL,
    ORACLE_ACTIVE_SESSION_COUNT,
    ORACLE_DEADLOCKS_TOTAL,
    ORACLE_SESSIONS_TOTAL,
    PG_ACTIVE_SESSION_COUNT,
    PG_DEADLOCKS,
    PG_NUMBACKENDS,
    REDSHIFT_ACTIVE_SESSION_COUNT,
    SQLSERVER_ACTIVE_SESSION_COUNT,
    SQLSERVER_INSTANCE_CPU_PERCENT,
    SQLSERVER_USER_CONNECTIONS,
)
from app.repositories.instance_overview_repository import InstanceOverviewRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.services.instance_derived_status_service import InstanceDerivedStatusService

MYSQL_THREADS_CONNECTED = "mysql_threads_connected"
MYSQL_THREADS_RUNNING = "mysql_threads_running"

# Candidatas por campo, em ordem de preferencia — cada instancia so tem as metricas do proprio
# engine, entao "a primeira presente" resolve o engine sem precisar de mapa por db_type.
CONNECTION_METRICS: tuple[str, ...] = (
    PG_NUMBACKENDS,
    SQLSERVER_USER_CONNECTIONS,
    MYSQL_THREADS_CONNECTED,
    ORACLE_SESSIONS_TOTAL,
)
ACTIVE_SESSION_METRICS: tuple[str, ...] = (
    PG_ACTIVE_SESSION_COUNT,
    SQLSERVER_ACTIVE_SESSION_COUNT,
    MYSQL_THREADS_RUNNING,
    ORACLE_ACTIVE_SESSION_COUNT,
    DB2_ACTIVE_SESSION_COUNT,
    REDSHIFT_ACTIVE_SESSION_COUNT,
)
CPU_METRICS: tuple[str, ...] = (SQLSERVER_INSTANCE_CPU_PERCENT,)
DEADLOCK_COUNTER_METRICS: tuple[str, ...] = (
    PG_DEADLOCKS,
    ORACLE_DEADLOCKS_TOTAL,
    DB2_DEADLOCKS_TOTAL,
)
CYCLE_METRICS: tuple[str, ...] = (COLLECTOR_CYCLE_DURATION_MS, COLLECTOR_CYCLE_ERROR_COUNT)

ALL_LATEST_METRICS: tuple[str, ...] = (
    CYCLE_METRICS + CONNECTION_METRICS + ACTIVE_SESSION_METRICS + CPU_METRICS
)

# Janela do DISTINCT ON de "ultimo valor": curta para manter a query barata; valores mais
# antigos que isso nao sao "atuais" num dashboard ao vivo de qualquer forma.
LATEST_WINDOW = timedelta(minutes=30)
DEADLOCK_WINDOW = timedelta(hours=1)

_STATUS_PRIORITY: dict[str, int] = {"down": 0, "critical": 1, "warning": 2, "unknown": 3, "up": 4}


def aggregate_status(health_status: str, open_alerts: dict[str, int], active: bool) -> str:
    """Mesmos 5 estados de `GET /status/overview`, mais "paused" para monitoramento desligado."""
    if not active:
        return "paused"
    if health_status == "unavailable":
        return "down"
    if open_alerts.get("critical", 0) > 0:
        return "critical"
    if open_alerts.get("warning", 0) > 0:
        return "warning"
    if health_status == "healthy":
        return "up"
    return "unknown"


def first_present(
    values: dict[str, tuple[float, datetime]], names: tuple[str, ...]
) -> float | None:
    for name in names:
        if name in values:
            return values[name][0]
    return None


@dataclass(slots=True)
class InstanceOverviewCard:
    id: UUID
    name: str
    db_type: str
    host: str
    port: int
    criticality: str
    active: bool
    health_status: str
    status: str
    connections: float | None
    active_sessions: float | None
    cpu_percent: float | None
    deadlocks_last_hour: int | None
    open_alerts: dict[str, int] = field(default_factory=dict)
    open_alerts_total: int = 0
    last_collected_at: datetime | None = None


def build_card(
    instance: Instance,
    *,
    latest: dict[str, tuple[float, datetime]],
    open_alerts: dict[str, int],
    deadlocks_last_hour: int | None,
    fallback_health: tuple[str, datetime | None] | None = None,
) -> InstanceOverviewCard:
    """Funcao pura: dados ja lidos em lote -> card. `fallback_health` substitui a derivacao a
    partir de `latest` quando nao ha telemetria de ciclo na janela recente."""
    if COLLECTOR_CYCLE_DURATION_MS in latest:
        cycle_time = latest[COLLECTOR_CYCLE_DURATION_MS][1]
        if cycle_time.tzinfo is None:
            cycle_time = cycle_time.replace(tzinfo=UTC)
        errors = latest.get(COLLECTOR_CYCLE_ERROR_COUNT, (0.0, cycle_time))[0]
        health = "unavailable" if errors > 0 else "healthy"
        last_collected: datetime | None = cycle_time
    elif fallback_health is not None:
        health, last_collected = fallback_health
    else:
        health, last_collected = "unknown", None
    return InstanceOverviewCard(
        id=instance.id,
        name=instance.name,
        db_type=instance.db_type,
        host=instance.host,
        port=instance.port,
        criticality=instance.criticality,
        active=instance.active,
        health_status=health,
        status=aggregate_status(health, open_alerts, instance.active),
        connections=first_present(latest, CONNECTION_METRICS),
        active_sessions=first_present(latest, ACTIVE_SESSION_METRICS),
        cpu_percent=first_present(latest, CPU_METRICS),
        deadlocks_last_hour=deadlocks_last_hour,
        open_alerts=dict(open_alerts),
        open_alerts_total=sum(open_alerts.values()),
        last_collected_at=last_collected,
    )


def sort_cards(cards: list[InstanceOverviewCard]) -> list[InstanceOverviewCard]:
    """Pior status primeiro, depois nome."""
    return sorted(cards, key=lambda c: (_STATUS_PRIORITY.get(c.status, 5), c.name.lower()))


class InstanceOverviewService:
    def __init__(
        self,
        overview_repository: InstanceOverviewRepository,
        metric_samples_repository: MetricSamplesRepository,
    ) -> None:
        self._repo = overview_repository
        self._status_svc = InstanceDerivedStatusService(metric_samples_repository)

    async def build(self, instances: list[Instance]) -> list[InstanceOverviewCard]:
        if not instances:
            return []
        now = datetime.now(UTC)
        ids = [i.id for i in instances]
        latest = await self._repo.latest_metric_values(ids, ALL_LATEST_METRICS, now - LATEST_WINDOW)
        alerts = await self._repo.open_alerts_by_severity(ids)
        sqlserver_ids = [i.id for i in instances if i.db_type == "sqlserver"]
        counter_ids = [i.id for i in instances if i.db_type != "sqlserver"]
        event_counts = await self._repo.deadlock_event_counts(sqlserver_ids, now - DEADLOCK_WINDOW)
        counter_counts = await self._repo.counter_deltas(
            counter_ids, DEADLOCK_COUNTER_METRICS, now - DEADLOCK_WINDOW
        )

        cards: list[InstanceOverviewCard] = []
        for inst in instances:
            values = latest.get(inst.id, {})
            fallback: tuple[str, datetime | None] | None = None
            if COLLECTOR_CYCLE_DURATION_MS not in values:
                derived = await self._status_svc.compute(inst.id)
                fallback = (derived.health_status, derived.last_collected_at)
            if inst.db_type == "sqlserver":
                deadlocks: int | None = event_counts.get(inst.id, 0)
            elif inst.db_type in ("postgresql", "redshift", "oracle", "db2"):
                deadlocks = counter_counts.get(inst.id, 0)
            else:
                deadlocks = None  # engine sem fonte de deadlock
            cards.append(
                build_card(
                    inst,
                    latest=values,
                    open_alerts=alerts.get(inst.id, {}),
                    deadlocks_last_hour=deadlocks,
                    fallback_health=fallback,
                )
            )
        return sort_cards(cards)
