"""Deteccao automatica de anomalias sazonais por instancia.

A cada ciclo, para cada instancia ativa:
1. **Rollup horario**: materializa em `anomaly_metric_hourly` as horas completas ainda nao
   consolidadas (backfill de ate `anomaly_baseline_weeks` semanas na primeira vez, em blocos de
   no maximo `_BACKFILL_DAYS_PER_CYCLE` dias por ciclo para nao pesar no banco).
2. **Valor atual**: mesma derivacao, janela dos ultimos 15 minutos.
3. **Baseline sazonal**: valores da MESMA hora no MESMO dia da semana das ultimas N semanas
   (>= 3 pontos) — "normal de segunda as 10h". Sem historico suficiente, cai para a mesma hora
   dos ultimos 14 dias (>= 7 pontos). Sem isso: `insufficient_data` (nunca inventa alerta).
4. **Desvio robusto**: centro = mediana, espalhamento = 1.4826 * MAD, com piso (10% da mediana
   ou um minimo absoluto por metrica) para nao alertar em serie quase constante.
   z = (atual - mediana) / espalhamento; warning >= `anomaly_warning_z`, critical >=
   `anomaly_critical_z`. A maioria das metricas so alerta para CIMA; throughput alerta nos dois
   sentidos (queda brusca de transacoes tambem e relevante).
5. **Persistencia**: so abre evento apos 2 avaliacoes anomalas seguidas e so fecha apos 2
   normais seguidas (evita pisca-pisca). Cada evento aberto vira um `Alert` da regra de sistema
   `seasonal_anomaly` da instancia (respeitando silenciamento e regra desativada), publicado no
   event_bus e enviado pelos canais de notificacao do `AlertingWorker` quando habilitado.
"""

from __future__ import annotations

import asyncio
import statistics
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.core.events import event_bus
from app.core.logging import get_logger
from app.models.alert import Alert, AlertRule
from app.models.anomaly import AnomalyEvent, AnomalyMetricStatus
from app.models.instance import Instance
from app.repositories.alerts_repository import AlertsRepository
from app.repositories.anomaly_repository import (
    HOUR,
    HOURLY_ORIGIN,
    METRIC_DEFS,
    AnomalyRepository,
    MetricDef,
    floor_hour,
    metrics_for_engine,
)
from app.repositories.instances_repository import InstancesRepository

logger = get_logger(__name__)

CURRENT_WINDOW = timedelta(minutes=15)
OPEN_AFTER_CONSECUTIVE = 2
CLOSE_AFTER_CONSECUTIVE = 2
_MAD_SCALE = 1.4826
_RELATIVE_SPREAD_FLOOR = 0.10
_MIN_WEEKLY_POINTS = 3
_FALLBACK_DAYS = 14
_MIN_DAILY_POINTS = 7
_BACKFILL_DAYS_PER_CYCLE = 7

Status = Literal["normal", "warning", "critical", "insufficient_data"]
BaselineKind = Literal["same_hour_same_weekday", "same_hour_any_day"]

# (alerts_repo, alert, rule, kind, value) — `AlertingWorker.send_notification`.
Notifier = Callable[..., Awaitable[None]]


def rule_name_for_instance(instance_name: str) -> str:
    return f"Anomalias automaticas - {instance_name}"


# ── Logica pura (testada em tests/unit/workers/test_anomaly_detection.py) ────────────────


@dataclass(frozen=True, slots=True)
class Baseline:
    kind: BaselineKind
    values: list[float]


@dataclass(frozen=True, slots=True)
class Evaluation:
    status: Status
    current: float | None
    expected: float | None
    low: float | None
    high: float | None
    z: float | None
    direction: Literal["high", "low"] | None
    baseline_kind: BaselineKind | None
    baseline_samples: int


def robust_center_spread(values: Sequence[float], abs_floor: float) -> tuple[float, float]:
    """Mediana e espalhamento robusto (1.4826*MAD ~ desvio padrao sob normalidade), com piso
    de max(10% da mediana, `abs_floor`) — uma serie constante teria MAD 0 e qualquer variacao
    viraria um z infinito."""
    median = statistics.median(values)
    mad = statistics.median(abs(v - median) for v in values)
    spread = max(_MAD_SCALE * mad, _RELATIVE_SPREAD_FLOOR * abs(median), abs_floor)
    return median, spread


def select_baseline(
    weekly: Sequence[float], daily: Sequence[float]
) -> Baseline | None:
    if len(weekly) >= _MIN_WEEKLY_POINTS:
        return Baseline("same_hour_same_weekday", list(weekly))
    if len(daily) >= _MIN_DAILY_POINTS:
        return Baseline("same_hour_any_day", list(daily))
    return None


def evaluate(
    current: float | None,
    baseline: Baseline | None,
    metric: MetricDef,
    warning_z: float,
    critical_z: float,
) -> Evaluation:
    if baseline is None:
        return Evaluation("insufficient_data", current, None, None, None, None, None, None, 0)
    median, spread = robust_center_spread(baseline.values, metric.abs_floor)
    low = max(0.0, median - warning_z * spread)
    high = median + warning_z * spread
    if current is None:
        return Evaluation(
            "insufficient_data",
            None,
            median,
            low,
            high,
            None,
            None,
            baseline.kind,
            len(baseline.values),
        )
    z = (current - median) / spread
    direction: Literal["high", "low"] | None = None
    magnitude = 0.0
    if z > 0 and "high" in metric.directions:
        direction, magnitude = "high", z
    elif z < 0 and "low" in metric.directions:
        direction, magnitude = "low", -z
    status: Status = "normal"
    if direction is not None and magnitude >= critical_z:
        status = "critical"
    elif direction is not None and magnitude >= warning_z:
        status = "warning"
    return Evaluation(
        status,
        current,
        median,
        low,
        high,
        round(z, 3),
        direction if status in ("warning", "critical") else None,
        baseline.kind,
        len(baseline.values),
    )


def advance_streaks(anomalous: int, normal: int, is_anomalous: bool) -> tuple[int, int]:
    """Contadores de persistencia: so um dos dois cresce, o outro zera."""
    return (anomalous + 1, 0) if is_anomalous else (0, normal + 1)


TransitionAction = Literal["open", "update", "close", "none"]


def decide_transition(
    has_open_event: bool, consecutive_anomalous: int, consecutive_normal: int, is_anomalous: bool
) -> TransitionAction:
    if has_open_event:
        if is_anomalous:
            return "update"
        if consecutive_normal >= CLOSE_AFTER_CONSECUTIVE:
            return "close"
        return "none"
    if is_anomalous and consecutive_anomalous >= OPEN_AFTER_CONSECUTIVE:
        return "open"
    return "none"


def weekly_buckets(hour: datetime, weeks: int) -> list[datetime]:
    return [hour - timedelta(weeks=k) for k in range(1, weeks + 1)]


def daily_buckets(hour: datetime, days: int = _FALLBACK_DAYS) -> list[datetime]:
    return [hour - timedelta(days=k) for k in range(1, days + 1)]


def _fmt(value: float) -> str:
    return f"{value:.2f}".rstrip("0").rstrip(".")


# ── Worker ───────────────────────────────────────────────────────────────────────────────


class AnomalyDetectionWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        interval_seconds: int,
        *,
        baseline_weeks: int = 4,
        warning_z: float = 3.0,
        critical_z: float = 4.0,
        notifier: Notifier | None = None,
        now_fn: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._session_factory = session_factory
        self._interval_seconds = interval_seconds
        self._baseline_weeks = max(1, baseline_weeks)
        self._warning_z = warning_z
        self._critical_z = max(critical_z, warning_z)
        self._notifier = notifier
        self._now = now_fn
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info("AnomalyDetectionWorker iniciado interval=%ss", self._interval_seconds)
        while not self._stop_event.is_set():
            try:
                await self._run_cycle()
            except Exception:  # noqa: BLE001 - ciclo nunca derruba o worker
                logger.exception("Erro inesperado em AnomalyDetectionWorker._run_cycle")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=self._interval_seconds)
            except TimeoutError:
                pass
        logger.info("AnomalyDetectionWorker finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _run_cycle(self) -> None:
        async with self._session_factory() as session:
            instances = await InstancesRepository(session).list_active()
        for instance in instances:
            if self._stop_event.is_set():
                return
            try:
                async with self._session_factory() as session:
                    await self.process_instance(session, instance)
            except Exception:  # noqa: BLE001 - 1 instancia nunca derruba o ciclo
                logger.exception("Erro na deteccao de anomalias instance_id=%s", instance.id)
        try:
            async with self._session_factory() as session:
                repo = AnomalyRepository(session)
                keep_weeks = max(self._baseline_weeks, 2) + 1
                await repo.prune_hourly(self._now() - timedelta(weeks=keep_weeks))
                await repo.commit()
        except Exception:  # noqa: BLE001
            logger.exception("Erro ao expurgar anomaly_metric_hourly")

    async def process_instance(self, session: AsyncSession, instance: Instance) -> None:
        repo = AnomalyRepository(session)
        now = self._now()
        await self._rollup(repo, instance, now)

        window_start = now - CURRENT_WINDOW
        current = await repo.compute_metric_buckets(
            instance.id, instance.db_type, window_start, now, CURRENT_WINDOW, window_start
        )
        statuses = await repo.get_statuses(instance.id)
        open_events = await repo.get_open_events(instance.id)
        hour = floor_hour(now)
        alerts_repo = AlertsRepository(session)
        rule: AlertRule | None = None

        for metric in metrics_for_engine(instance.db_type):
            values = current.get(metric.key) or {}
            current_value = next(iter(values.values()))[0] if values else None
            weekly = await repo.get_hourly_values(
                instance.id, metric.key, weekly_buckets(hour, self._baseline_weeks)
            )
            daily = await repo.get_hourly_values(instance.id, metric.key, daily_buckets(hour))
            ev = evaluate(
                current_value,
                select_baseline(list(weekly.values()), list(daily.values())),
                metric,
                self._warning_z,
                self._critical_z,
            )

            status = statuses.get(metric.key)
            if status is None:
                status = AnomalyMetricStatus(
                    instance_id=instance.id,
                    metric_key=metric.key,
                    unit=metric.unit,
                    status="insufficient_data",
                    consecutive_anomalous=0,
                    consecutive_normal=0,
                    baseline_samples=0,
                    evaluated_at=now,
                )
                repo.add(status)
            is_anomalous = ev.status in ("warning", "critical")
            status.consecutive_anomalous, status.consecutive_normal = advance_streaks(
                status.consecutive_anomalous or 0, status.consecutive_normal or 0, is_anomalous
            )
            status.unit = metric.unit
            status.evaluated_at = now
            status.current_value = ev.current
            status.expected_value = ev.expected
            status.expected_low = ev.low
            status.expected_high = ev.high
            status.z_score = ev.z
            status.status = ev.status
            status.baseline_kind = ev.baseline_kind
            status.baseline_samples = ev.baseline_samples

            event = open_events.get(metric.key)
            action = decide_transition(
                event is not None,
                status.consecutive_anomalous,
                status.consecutive_normal,
                is_anomalous,
            )
            if action == "none":
                continue
            if rule is None:
                rule = await self._get_or_create_rule(alerts_repo, instance)
            if action == "open":
                await self._open_event(repo, alerts_repo, rule, instance, metric, ev, now)
            elif action == "update" and event is not None:
                await self._update_event(alerts_repo, rule, instance, metric, event, ev, now)
            elif action == "close" and event is not None:
                await self._close_event(alerts_repo, rule, event, ev, now)

        await repo.commit()

    async def _rollup(self, repo: AnomalyRepository, instance: Instance, now: datetime) -> None:
        end = floor_hour(now)
        last = await repo.last_rolled_bucket(instance.id)
        earliest = end - timedelta(weeks=self._baseline_weeks)
        start = earliest if last is None else max(last + HOUR, earliest)
        if start >= end:
            return
        end = min(end, start + timedelta(days=_BACKFILL_DAYS_PER_CYCLE))
        chunk_start = start
        while chunk_start < end:
            chunk_end = min(chunk_start + timedelta(days=1), end)
            buckets = await repo.compute_metric_buckets(
                instance.id, instance.db_type, chunk_start, chunk_end, HOUR, HOURLY_ORIGIN
            )
            rows = [
                (key, bucket, value, count)
                for key, per_bucket in buckets.items()
                for bucket, (value, count) in per_bucket.items()
            ]
            await repo.upsert_hourly(instance.id, rows)
            chunk_start = chunk_end
        await repo.commit()

    async def _get_or_create_rule(
        self, alerts_repo: AlertsRepository, instance: Instance
    ) -> AlertRule:
        name = rule_name_for_instance(instance.name)
        rule = await alerts_repo.get_rule_by_name(name)
        if rule is None:
            rule = await alerts_repo.create_alert_rule(
                AlertRule(
                    name=name,
                    target_type="instance",
                    rule_type="seasonal_anomaly",
                    active=True,
                )
            )
        return rule

    def _describe(self, metric: MetricDef, ev: Evaluation) -> str:
        assert ev.current is not None and ev.expected is not None
        arrow = "acima" if ev.direction == "high" else "abaixo"
        return (
            f"[anomaly:{metric.key}] {metric.key} {arrow} do normal para este dia/hora: "
            f"atual {_fmt(ev.current)} {metric.unit}, esperado {_fmt(ev.expected)} "
            f"(faixa {_fmt(ev.low or 0)}-{_fmt(ev.high or 0)}), z={ev.z}"
        )

    async def _open_event(
        self,
        repo: AnomalyRepository,
        alerts_repo: AlertsRepository,
        rule: AlertRule,
        instance: Instance,
        metric: MetricDef,
        ev: Evaluation,
        now: datetime,
    ) -> None:
        assert ev.direction is not None and ev.z is not None and ev.current is not None
        event = AnomalyEvent(
            instance_id=instance.id,
            metric_key=metric.key,
            unit=metric.unit,
            direction=ev.direction,
            severity=ev.status,
            state="open",
            first_detected_at=now,
            last_detected_at=now,
            observed_value=ev.current,
            expected_value=ev.expected,
            expected_low=ev.low,
            expected_high=ev.high,
            peak_z_score=ev.z,
            baseline_kind=ev.baseline_kind,
        )
        repo.add(event)
        if not rule.active:
            return
        if await alerts_repo.get_active_silence("instance", instance.id, rule.id) is not None:
            return
        alert = await alerts_repo.create_alert(
            Alert(
                alert_rule_id=rule.id,
                target_type="instance",
                target_id=instance.id,
                severity=ev.status,
                description=self._describe(metric, ev),
            )
        )
        event.alert_id = alert.id
        event_bus.publish({"type": "alert_created", "alert_id": str(alert.id), "kind": "triggered"})
        await self._notify(alerts_repo, alert, rule, "triggered", ev.current)

    async def _update_event(
        self,
        alerts_repo: AlertsRepository,
        rule: AlertRule,
        instance: Instance,
        metric: MetricDef,
        event: AnomalyEvent,
        ev: Evaluation,
        now: datetime,
    ) -> None:
        assert ev.z is not None and ev.current is not None
        event.last_detected_at = now
        if abs(ev.z) > abs(event.peak_z_score):
            event.peak_z_score = ev.z
            event.observed_value = ev.current
            event.expected_value = ev.expected
            event.expected_low = ev.low
            event.expected_high = ev.high
        if ev.status == event.severity:
            return
        event.severity = ev.status
        if event.alert_id is None:
            return
        alert = await alerts_repo.get_alert_by_id(event.alert_id)
        if alert is None or alert.state == "closed":
            return
        alert.severity = ev.status
        alert.description = self._describe(metric, ev)
        if alert.state == "acknowledged":
            alert.state = "open"
            alert.acknowledged_at = None
        await alerts_repo.update_alert(alert)
        event_bus.publish(
            {"type": "alert_updated", "alert_id": str(alert.id), "kind": "level_changed"}
        )
        await self._notify(alerts_repo, alert, rule, "level_changed", ev.current)

    async def _close_event(
        self,
        alerts_repo: AlertsRepository,
        rule: AlertRule,
        event: AnomalyEvent,
        ev: Evaluation,
        now: datetime,
    ) -> None:
        event.state = "closed"
        event.closed_at = now
        if event.alert_id is None:
            return
        alert = await alerts_repo.get_alert_by_id(event.alert_id)
        if alert is None or alert.state == "closed":
            return
        alert.state = "closed"
        alert.resolved_at = now
        await alerts_repo.update_alert(alert)
        event_bus.publish({"type": "alert_updated", "alert_id": str(alert.id), "kind": "recovered"})
        await self._notify(
            alerts_repo, alert, rule, "recovered", ev.current if ev.current is not None else 0.0
        )

    async def _notify(
        self, alerts_repo: AlertsRepository, alert: Alert, rule: AlertRule, kind: str, value: float
    ) -> None:
        if self._notifier is None:
            return
        try:
            await self._notifier(alerts_repo, alert, rule, kind=kind, value=value)
        except Exception:  # noqa: BLE001 - notificacao nunca derruba a avaliacao
            logger.exception("Falha ao notificar anomalia alert_id=%s", alert.id)


def metric_unit(metric_key: str) -> str:
    metric = METRIC_DEFS.get(metric_key)
    return metric.unit if metric is not None else "count"
