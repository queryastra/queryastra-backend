"""Repositorio da deteccao de anomalias sazonais.

Duas responsabilidades:
1. Derivar as metricas engine-agnosticas (CPU, sessoes, duracao de consulta, waits, throughput)
   a partir das hypertables brutas (`metric_samples`, `query_samples`, `blocking_events`) em
   buckets de largura arbitraria — a MESMA derivacao serve o rollup horario e a janela "atual" de
   15 minutos, para que valor atual e baseline sejam comparaveis.
2. Persistir rollup horario, status por metrica e eventos de anomalia.
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.anomaly import AnomalyEvent, AnomalyMetricHourly, AnomalyMetricStatus
from app.normalizers.metric_names import (
    COLLECTOR_CYCLE_DURATION_MS,
    DB2_ACTIVE_SESSION_COUNT,
    ORACLE_ACTIVE_SESSION_COUNT,
    PG_ACTIVE_SESSION_COUNT,
    PG_XACT_COMMIT,
    PG_XACT_ROLLBACK,
    REDSHIFT_ACTIVE_SESSION_COUNT,
    SQLSERVER_ACTIVE_SESSION_COUNT,
    SQLSERVER_INSTANCE_CPU_PERCENT,
    SQLSERVER_PROCESSES_BLOCKED,
)
from app.repositories.base import BaseRepository
from app.repositories.query_samples_repository import QuerySamplesRepository

# Origem fixa alinhada a hora cheia (segunda-feira 00:00 UTC, mesmo default do TimescaleDB).
HOURLY_ORIGIN = datetime(2000, 1, 3, tzinfo=UTC)
HOUR = timedelta(hours=1)

_MYSQL_THREADS_RUNNING = "mysql_threads_running"
_MYSQL_QUESTIONS_TOTAL = "mysql_questions_total"

_ACTIVE_SESSION_GAUGES: tuple[str, ...] = (
    PG_ACTIVE_SESSION_COUNT,
    SQLSERVER_ACTIVE_SESSION_COUNT,
    ORACLE_ACTIVE_SESSION_COUNT,
    DB2_ACTIVE_SESSION_COUNT,
    REDSHIFT_ACTIVE_SESSION_COUNT,
    _MYSQL_THREADS_RUNNING,
)

_PG_FAMILY = ("postgresql", "aurora_postgresql", "cockroachdb", "redshift")
_MYSQL_FAMILY = ("mysql", "aurora_mysql")


@dataclass(frozen=True, slots=True)
class MetricDef:
    key: str
    unit: str  # "percent" | "count" | "ms" | "per_sec"
    directions: tuple[str, ...]  # ("high",) ou ("high", "low")
    # Desvio minimo absoluto: evita alertar em serie quase constante (MAD ~ 0).
    abs_floor: float


METRIC_DEFS: dict[str, MetricDef] = {
    d.key: d
    for d in (
        MetricDef("cpu_percent", "percent", ("high",), 5.0),
        MetricDef("active_sessions", "count", ("high",), 2.0),
        MetricDef("blocked_sessions", "count", ("high",), 1.0),
        MetricDef("query_duration_avg_ms", "ms", ("high",), 50.0),
        MetricDef("query_duration_p95_ms", "ms", ("high",), 100.0),
        MetricDef("wait_total_ms", "ms", ("high",), 100.0),
        MetricDef("wait_lock_ms", "ms", ("high",), 100.0),
        MetricDef("wait_io_ms", "ms", ("high",), 100.0),
        MetricDef("transactions_per_sec", "per_sec", ("high", "low"), 1.0),
    )
}


def metrics_for_engine(db_type: str) -> list[MetricDef]:
    """Metricas avaliadas para o motor — so as que existe fonte de dado para derivar."""
    keys = [
        "active_sessions",
        "blocked_sessions",
        "query_duration_avg_ms",
        "query_duration_p95_ms",
        "wait_total_ms",
        "wait_lock_ms",
        "wait_io_ms",
    ]
    if db_type == "sqlserver":
        keys.insert(0, "cpu_percent")
    if db_type in _PG_FAMILY or db_type in _MYSQL_FAMILY:
        keys.append("transactions_per_sec")
    return [METRIC_DEFS[k] for k in keys]


def _transaction_counters(db_type: str) -> tuple[str, ...]:
    if db_type in _PG_FAMILY:
        return (PG_XACT_COMMIT, PG_XACT_ROLLBACK)
    if db_type in _MYSQL_FAMILY:
        # MySQL nao expoe commits por segundo de forma barata — "Questions" (statements do
        # cliente) e o proxy de throughput.
        return (_MYSQL_QUESTIONS_TOTAL,)
    return ()


def bucket_start(ts: datetime, origin: datetime, width: timedelta) -> datetime:
    """Mesmo calculo do `time_bucket(width, ts, origin)` do TimescaleDB."""
    steps = (ts - origin) // width
    return origin + steps * width


def floor_hour(ts: datetime) -> datetime:
    return bucket_start(ts, HOURLY_ORIGIN, HOUR)


def compute_counter_rates(
    points: Iterable[tuple[str, datetime, float]],
    *,
    start: datetime,
    end: datetime,
    origin: datetime,
    width: timedelta,
) -> dict[datetime, float]:
    """Taxa por segundo de contadores cumulativos (`*_total`), por bucket.

    `points` deve vir ordenado por (serie, tempo). Cada delta entre 2 pontos consecutivos da
    mesma serie e atribuido ao bucket do ponto posterior (que precisa estar em [start, end)).
    Delta negativo = reset do contador (restart do motor / stats reset): o intervalo e
    descartado, nunca vira taxa negativa nem um pico falso. Taxa do bucket = soma, entre as
    series, de (incremento / segundos cobertos) — series sao p.ex. commit + rollback."""
    acc: dict[datetime, dict[str, list[float]]] = defaultdict(dict)
    prev_series: str | None = None
    prev_time: datetime | None = None
    prev_value: float | None = None
    for series, ts, value in points:
        if series != prev_series:
            prev_series, prev_time, prev_value = series, ts, value
            continue
        assert prev_time is not None and prev_value is not None
        dt = (ts - prev_time).total_seconds()
        delta = value - prev_value
        prev_time, prev_value = ts, value
        if dt <= 0 or delta < 0 or not (start <= ts < end):
            continue
        b = bucket_start(ts, origin, width)
        slot = acc[b].setdefault(series, [0.0, 0.0])
        slot[0] += delta
        slot[1] += dt
    rates: dict[datetime, float] = {}
    for b, per_series in acc.items():
        rates[b] = sum(inc / secs for inc, secs in per_series.values() if secs > 0)
    return rates


# value, sample_count por bucket
BucketValues = dict[datetime, tuple[float, int]]

# Requests ativas ha mais de 1h (jobs de agente, CDC, replicacao, sessoes de manutencao) sao
# amostradas a cada ciclo com duracao crescente e dominariam media/p95 — ficam fora das metricas
# de duracao, que medem a carga de consultas do usuario.
_MAX_QUERY_DURATION_MS = 3_600_000

_BUCKET_EXPR = "time_bucket(CAST(:width AS interval), time, CAST(:origin AS timestamptz))"


class AnomalyRepository(BaseRepository):
    # ── Derivacao de metricas ────────────────────────────────────────────────────────────

    async def compute_metric_buckets(
        self,
        instance_id: UUID,
        db_type: str,
        start: datetime,
        end: datetime,
        width: timedelta,
        origin: datetime,
    ) -> dict[str, BucketValues]:
        """Valor de cada metrica de `metrics_for_engine(db_type)` por bucket em [start, end).

        Metricas de evento (sessoes bloqueadas, waits) sao normalizadas por ciclo de coleta
        (`collector_cycle_duration_ms` = 1 amostra por ciclo): "media por ciclo", insensivel ao
        intervalo de polling e com zero explicito nos buckets em que houve coleta sem evento."""
        params = {
            "iid": instance_id,
            "start": start,
            "end": end,
            "width": width,
            "origin": origin,
        }
        result: dict[str, BucketValues] = {}
        keys = {m.key for m in metrics_for_engine(db_type)}

        cycles = await self._gauge(params, (COLLECTOR_CYCLE_DURATION_MS,), count_only=True)

        if "cpu_percent" in keys:
            result["cpu_percent"] = await self._gauge(params, (SQLSERVER_INSTANCE_CPU_PERCENT,))
        result["active_sessions"] = await self._gauge(params, _ACTIVE_SESSION_GAUGES)

        if db_type == "sqlserver":
            result["blocked_sessions"] = await self._gauge(params, (SQLSERVER_PROCESSES_BLOCKED,))
        else:
            result["blocked_sessions"] = await self._blocked_from_events(params, cycles)

        avg_ms, p95_ms = await self._query_durations(params)
        result["query_duration_avg_ms"] = avg_ms
        result["query_duration_p95_ms"] = p95_ms

        total, lock, io = await self._waits(params, cycles)
        result["wait_total_ms"] = total
        result["wait_lock_ms"] = lock
        result["wait_io_ms"] = io

        counters = _transaction_counters(db_type)
        if counters and "transactions_per_sec" in keys:
            result["transactions_per_sec"] = await self._counter_rates(params, counters)

        return {k: v for k, v in result.items() if k in keys}

    async def _gauge(
        self, params: dict, names: Sequence[str], *, count_only: bool = False
    ) -> BucketValues:
        stmt = sa.text(f"""
            SELECT {_BUCKET_EXPR} AS b, AVG(value) AS v, COUNT(*) AS n
            FROM metric_samples
            WHERE target_type = 'instance' AND target_id = :iid
              AND metric_name = ANY(:names)
              AND time >= :start AND time < :end
            GROUP BY b
        """)
        rows = (await self._session.execute(stmt, {**params, "names": list(names)})).all()
        if count_only:
            return {r.b: (float(r.n), int(r.n)) for r in rows}
        return {r.b: (float(r.v), int(r.n)) for r in rows if r.v is not None}

    async def _blocked_from_events(self, params: dict, cycles: BucketValues) -> BucketValues:
        stmt = sa.text(f"""
            SELECT {_BUCKET_EXPR} AS b, COUNT(DISTINCT (time, blocked_session_id)) AS n
            FROM blocking_events
            WHERE instance_id = :iid AND time >= :start AND time < :end
            GROUP BY b
        """)
        rows = {r.b: int(r.n) for r in (await self._session.execute(stmt, params)).all()}
        return {
            b: (rows.get(b, 0) / n_cycles, int(n_cycles))
            for b, (n_cycles, _) in cycles.items()
            if n_cycles > 0
        }

    async def _query_durations(self, params: dict) -> tuple[BucketValues, BucketValues]:
        stmt = sa.text(f"""
            SELECT b, AVG(duration_ms) AS avg_ms,
                   percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms,
                   COUNT(*) AS n
            FROM (
                SELECT {_BUCKET_EXPR} AS b, duration_ms
                FROM query_samples
                WHERE instance_id = :iid AND time >= :start AND time < :end
                  AND duration_ms IS NOT NULL
                  AND duration_ms < :max_duration_ms
                  AND (wait_type IS NULL OR wait_type <> 'IDLE')
                  AND (wait_event_native IS NULL OR wait_event_native <> ALL(:benign))
            ) s
            GROUP BY b
        """)
        rows = (
            await self._session.execute(
                stmt,
                {
                    **params,
                    "benign": list(QuerySamplesRepository._BENIGN_WAIT_EVENT_NATIVE),
                    "max_duration_ms": _MAX_QUERY_DURATION_MS,
                },
            )
        ).all()
        avg_ms ={r.b: (float(r.avg_ms), int(r.n)) for r in rows if r.avg_ms is not None}
        p95_ms = {r.b: (float(r.p95_ms), int(r.n)) for r in rows if r.p95_ms is not None}
        return avg_ms, p95_ms

    async def _waits(
        self, params: dict, cycles: BucketValues
    ) -> tuple[BucketValues, BucketValues, BucketValues]:
        stmt = sa.text(f"""
            SELECT {_BUCKET_EXPR} AS b,
                   COALESCE(SUM(wait_time_ms) FILTER (
                       WHERE wait_type IS NOT NULL AND wait_type NOT IN ('CPU', 'IDLE')
                   ), 0) AS total,
                   COALESCE(SUM(wait_time_ms) FILTER (WHERE wait_type = 'LOCK'), 0) AS lock_ms,
                   COALESCE(SUM(wait_time_ms) FILTER (WHERE wait_type = 'IO'), 0) AS io_ms
            FROM query_samples
            WHERE instance_id = :iid AND time >= :start AND time < :end
              AND wait_time_ms IS NOT NULL
              AND (wait_event_native IS NULL OR wait_event_native <> ALL(:benign))
            GROUP BY b
        """)
        rows = {
            r.b: (float(r.total), float(r.lock_ms), float(r.io_ms))
            for r in (
                await self._session.execute(
                    stmt,
                    {**params, "benign": list(QuerySamplesRepository._BENIGN_WAIT_EVENT_NATIVE)},
                )
            ).all()
        }
        total: BucketValues = {}
        lock: BucketValues = {}
        io: BucketValues = {}
        for b, (n_cycles, _) in cycles.items():
            if n_cycles <= 0:
                continue
            t, lk, i = rows.get(b, (0.0, 0.0, 0.0))
            total[b] = (t / n_cycles, int(n_cycles))
            lock[b] = (lk / n_cycles, int(n_cycles))
            io[b] = (i / n_cycles, int(n_cycles))
        return total, lock, io

    async def _counter_rates(self, params: dict, names: Sequence[str]) -> BucketValues:
        # Olha 15min antes do inicio para ter o ponto anterior ao primeiro delta da janela.
        stmt = sa.text("""
            SELECT metric_name || ':' || labels::text AS series, time, value
            FROM metric_samples
            WHERE target_type = 'instance' AND target_id = :iid
              AND metric_name = ANY(:names)
              AND time >= :fetch_start AND time < :end
            ORDER BY series, time
        """)
        rows = (
            await self._session.execute(
                stmt,
                {
                    "iid": params["iid"],
                    "names": list(names),
                    "fetch_start": params["start"] - timedelta(minutes=15),
                    "end": params["end"],
                },
            )
        ).all()
        rates = compute_counter_rates(
            ((r.series, r.time, float(r.value)) for r in rows),
            start=params["start"],
            end=params["end"],
            origin=params["origin"],
            width=params["width"],
        )
        return {b: (v, 1) for b, v in rates.items()}

    # ── Rollup horario ───────────────────────────────────────────────────────────────────

    async def last_rolled_bucket(self, instance_id: UUID) -> datetime | None:
        stmt = sa.select(sa.func.max(AnomalyMetricHourly.bucket)).where(
            AnomalyMetricHourly.instance_id == instance_id
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def upsert_hourly(
        self, instance_id: UUID, rows: list[tuple[str, datetime, float, int]]
    ) -> None:
        if not rows:
            return
        stmt = pg_insert(AnomalyMetricHourly).values(
            [
                {
                    "instance_id": instance_id,
                    "metric_key": key,
                    "bucket": bucket,
                    "value": value,
                    "sample_count": count,
                }
                for key, bucket, value, count in rows
            ]
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=["instance_id", "metric_key", "bucket"],
            set_={"value": stmt.excluded.value, "sample_count": stmt.excluded.sample_count},
        )
        await self._session.execute(stmt)

    async def get_hourly_values(
        self, instance_id: UUID, metric_key: str, buckets: Sequence[datetime]
    ) -> dict[datetime, float]:
        if not buckets:
            return {}
        stmt = sa.select(AnomalyMetricHourly.bucket, AnomalyMetricHourly.value).where(
            AnomalyMetricHourly.instance_id == instance_id,
            AnomalyMetricHourly.metric_key == metric_key,
            AnomalyMetricHourly.bucket.in_(list(buckets)),
        )
        return {r.bucket: float(r.value) for r in (await self._session.execute(stmt)).all()}

    async def get_hourly_range(
        self, instance_id: UUID, metric_key: str, since: datetime, until: datetime
    ) -> dict[datetime, float]:
        stmt = sa.select(AnomalyMetricHourly.bucket, AnomalyMetricHourly.value).where(
            AnomalyMetricHourly.instance_id == instance_id,
            AnomalyMetricHourly.metric_key == metric_key,
            AnomalyMetricHourly.bucket >= since,
            AnomalyMetricHourly.bucket < until,
        )
        return {r.bucket: float(r.value) for r in (await self._session.execute(stmt)).all()}

    async def prune_hourly(self, before: datetime) -> None:
        await self._session.execute(
            sa.delete(AnomalyMetricHourly).where(AnomalyMetricHourly.bucket < before)
        )

    # ── Status por metrica ───────────────────────────────────────────────────────────────

    async def get_statuses(self, instance_id: UUID) -> dict[str, AnomalyMetricStatus]:
        stmt = sa.select(AnomalyMetricStatus).where(AnomalyMetricStatus.instance_id == instance_id)
        return {s.metric_key: s for s in (await self._session.execute(stmt)).scalars().all()}

    def add(self, obj: AnomalyMetricStatus | AnomalyEvent) -> None:
        self._session.add(obj)

    # ── Eventos ──────────────────────────────────────────────────────────────────────────

    async def get_open_events(self, instance_id: UUID) -> dict[str, AnomalyEvent]:
        stmt = sa.select(AnomalyEvent).where(
            AnomalyEvent.instance_id == instance_id, AnomalyEvent.state == "open"
        )
        return {e.metric_key: e for e in (await self._session.execute(stmt)).scalars().all()}

    async def list_events(
        self,
        *,
        instance_ids: Sequence[UUID] | None,
        since: datetime,
        open_only: bool,
    ) -> list[AnomalyEvent]:
        """Abertos sempre + (se `open_only=False`) os detectados desde `since`; mais novos
        primeiro. `instance_ids=None` = todas as instancias."""
        stmt = sa.select(AnomalyEvent)
        if instance_ids is not None:
            stmt = stmt.where(AnomalyEvent.instance_id.in_(list(instance_ids)))
        if open_only:
            stmt = stmt.where(AnomalyEvent.state == "open")
        else:
            stmt = stmt.where(
                sa.or_(AnomalyEvent.state == "open", AnomalyEvent.last_detected_at >= since)
            )
        stmt = stmt.order_by(AnomalyEvent.first_detected_at.desc()).limit(500)
        return list((await self._session.execute(stmt)).scalars().all())

    async def commit(self) -> None:
        await self._session.commit()
