"""Historico por instancia: queries lentas, regressoes de desempenho e heatmap de deadlocks.

Montado sob `/instances` (mesmo padrao de `health_stats`/`db_maintenance`). Todo `from` passa
por `PlanService.clamp_from()` — o historico visivel respeita o plano contratado.

- `GET /instances/{id}/slow-queries` — top N por duracao media/max/p95/total nos ultimos D dias,
  com serie diaria (sparkline). Ver `app.repositories.slow_queries_repository` para como
  execucoes sao reconstruidas a partir das amostras.
- `GET /instances/{id}/query-regressions` — media por execucao numa janela "antes" vs "depois"
  de um ponto de corte (padrao: agora - 24h, janelas de 24h cada).
- `GET /instances/{id}/deadlock-history` — heatmap dia-da-semana x hora + serie diaria.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from typing import Annotated
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import PlanService, check_instance_access, get_db_session, get_plan_service
from app.repositories.deadlock_history_repository import (
    DeadlockHistoryRepository,
    resolve_source,
)
from app.repositories.instances_repository import InstancesRepository
from app.repositories.slow_queries_repository import SlowQueriesRepository, SlowQueryOrder

router = APIRouter()


# ── Slow queries ──────────────────────────────────────────────────────────────


class DailyAvgPointResponse(BaseModel):
    day: date
    avg_duration_ms: float
    executions: int


class SlowQueryResponse(BaseModel):
    sql_hash: str
    sql_name: str | None
    sql_text_masked: str | None
    database_name: str | None
    executions: int
    avg_duration_ms: float
    max_duration_ms: float
    p95_duration_ms: float
    total_duration_ms: float
    last_seen: datetime | None
    daily: list[DailyAvgPointResponse]


class SlowQueriesResponse(BaseModel):
    since: datetime
    until: datetime
    days: int
    order_by: SlowQueryOrder
    items: list[SlowQueryResponse]


@router.get("/{instance_id}/slow-queries", response_model=SlowQueriesResponse)
async def get_slow_queries(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    plan: Annotated[PlanService, Depends(get_plan_service)],
    limit: Annotated[int, Query(ge=1, le=100)] = 10,
    days: Annotated[int, Query(ge=1, le=365)] = 7,
    min_executions: Annotated[int, Query(ge=1, le=100000)] = 1,
    order_by: Annotated[SlowQueryOrder, Query()] = "avg",
    database_name: Annotated[str | None, Query()] = None,
) -> SlowQueriesResponse:
    """Top N queries mais lentas (duracao por execucao) nos ultimos `days` dias."""
    until = datetime.now(UTC)
    since = plan.clamp_from(until - timedelta(days=days))
    repo = SlowQueriesRepository(session)
    rows = await repo.top_slow(
        instance_id,
        since,
        until,
        limit=limit,
        min_executions=min_executions,
        database_name=database_name,
        order_by=order_by,
    )
    series = await repo.daily_series(
        instance_id, since, until, [r.sql_hash for r in rows], database_name=database_name
    )
    items = [
        SlowQueryResponse(
            sql_hash=r.sql_hash,
            sql_name=r.sql_name,
            sql_text_masked=r.sql_text_masked,
            database_name=r.database_name,
            executions=int(r.executions),
            avg_duration_ms=float(r.avg_duration_ms or 0),
            max_duration_ms=float(r.max_duration_ms or 0),
            p95_duration_ms=float(r.p95_duration_ms or 0),
            total_duration_ms=float(r.total_duration_ms or 0),
            last_seen=r.last_seen,
            daily=[
                DailyAvgPointResponse(
                    day=p.day, avg_duration_ms=p.avg_duration_ms, executions=p.executions
                )
                for p in series.get(r.sql_hash, [])
            ],
        )
        for r in rows
    ]
    return SlowQueriesResponse(since=since, until=until, days=days, order_by=order_by, items=items)


# ── Regressions ───────────────────────────────────────────────────────────────


class QueryRegressionResponse(BaseModel):
    sql_hash: str
    sql_name: str | None
    sql_text_masked: str | None
    database_name: str | None
    before_executions: int
    before_avg_ms: float
    after_executions: int
    after_avg_ms: float
    after_max_ms: float
    change_pct: float


class QueryRegressionsResponse(BaseModel):
    split: datetime
    before_start: datetime
    after_end: datetime
    min_executions: int
    items: list[QueryRegressionResponse]


@router.get("/{instance_id}/query-regressions", response_model=QueryRegressionsResponse)
async def get_query_regressions(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    plan: Annotated[PlanService, Depends(get_plan_service)],
    split: Annotated[datetime | None, Query()] = None,
    before_hours: Annotated[int, Query(ge=1, le=24 * 90)] = 24,
    after_hours: Annotated[int, Query(ge=1, le=24 * 90)] = 24,
    min_executions: Annotated[int, Query(ge=1, le=100000)] = 3,
    min_change_pct: Annotated[float, Query(ge=-100, le=100000)] = 0.0,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
    database_name: Annotated[str | None, Query()] = None,
) -> QueryRegressionsResponse:
    """Queries cuja duracao media por execucao piorou depois de `split` — ordenadas pela maior
    piora percentual. `min_change_pct` negativo inclui tambem melhorias."""
    now = datetime.now(UTC)
    split_at = split or (now - timedelta(hours=24))
    if split_at.tzinfo is None:
        split_at = split_at.replace(tzinfo=UTC)
    if split_at > now:
        raise HTTPException(status_code=422, detail="split nao pode estar no futuro.")
    before_start = plan.clamp_from(split_at - timedelta(hours=before_hours))
    if before_start >= split_at:
        raise HTTPException(
            status_code=422, detail="split esta fora do historico permitido pelo plano."
        )
    after_end = min(split_at + timedelta(hours=after_hours), now)
    rows = await SlowQueriesRepository(session).regressions(
        instance_id,
        before_start=before_start,
        split=split_at,
        after_end=after_end,
        limit=limit,
        min_executions=min_executions,
        min_change_pct=min_change_pct,
        database_name=database_name,
    )
    items = [
        QueryRegressionResponse(
            sql_hash=r.sql_hash,
            sql_name=r.sql_name,
            sql_text_masked=r.sql_text_masked,
            database_name=r.database_name,
            before_executions=int(r.before_executions),
            before_avg_ms=float(r.before_avg_ms or 0),
            after_executions=int(r.after_executions),
            after_avg_ms=float(r.after_avg_ms or 0),
            after_max_ms=float(r.after_max_ms or 0),
            change_pct=float(r.change_pct),
        )
        for r in rows
    ]
    return QueryRegressionsResponse(
        split=split_at,
        before_start=before_start,
        after_end=after_end,
        min_executions=min_executions,
        items=items,
    )


# ── Deadlock history ──────────────────────────────────────────────────────────


class DeadlockHeatmapCellResponse(BaseModel):
    weekday: int  # ISO: 1 = segunda ... 7 = domingo
    hour: int
    count: int


class DeadlockDailyPointResponse(BaseModel):
    day: date
    count: int


class DeadlockHistoryResponse(BaseModel):
    source: str  # "events" | "counter" | "none"
    since: datetime
    until: datetime
    tz: str
    total: int
    heatmap: list[DeadlockHeatmapCellResponse]
    daily: list[DeadlockDailyPointResponse]


def _validate_tz(tz: str) -> str:
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError) as exc:
        raise HTTPException(status_code=422, detail="Fuso horario invalido.") from exc
    return tz


@router.get("/{instance_id}/deadlock-history", response_model=DeadlockHistoryResponse)
async def get_deadlock_history(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    plan: Annotated[PlanService, Depends(get_plan_service)],
    days: Annotated[int, Query(ge=1, le=365)] = 30,
    tz: Annotated[str, Query(max_length=64)] = "UTC",
) -> DeadlockHistoryResponse:
    """Deadlocks por (dia-da-semana, hora) e por dia no fuso `tz`. SQL Server: `deadlock_events`;
    PostgreSQL/Oracle/DB2: deltas do contador cumulativo (resets ignorados)."""
    tz = _validate_tz(tz)
    instance = await InstancesRepository(session).get_by_id(instance_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Instancia nao encontrada.")
    until = datetime.now(UTC)
    since = plan.clamp_from(until - timedelta(days=days))
    repo = DeadlockHistoryRepository(session)
    heatmap = await repo.heatmap(
        instance_id=instance_id, db_type=instance.db_type, since=since, until=until, tz=tz
    )
    daily = await repo.daily(
        instance_id=instance_id, db_type=instance.db_type, since=since, until=until, tz=tz
    )
    return DeadlockHistoryResponse(
        source=resolve_source(instance.db_type),
        since=since,
        until=until,
        tz=tz,
        total=sum(c for _, c in daily),
        heatmap=[DeadlockHeatmapCellResponse(weekday=w, hour=h, count=c) for w, h, c in heatmap],
        daily=[DeadlockDailyPointResponse(day=d, count=c) for d, c in daily],
    )
