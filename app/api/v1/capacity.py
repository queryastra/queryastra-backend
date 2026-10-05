"""Previsão de capacidade por instância monitorada.

Calcula regressão linear simples sobre as médias diárias dos últimos 90 dias
de cada métrica de recurso relevante (CPU, memória, disco, sessões, PLE) e
projeta os próximos 7/30/90 dias — sem dependência de numpy/scipy.

Degrade graciosamente: menos de `_MIN_DATA_POINTS` dias com dado → retorna
`available: false` para aquela métrica, sem inventar uma tendência.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import check_instance_access, get_current_user, get_db_session
from app.normalizers.metric_names import (
    PG_ACTIVE_SESSION_COUNT,
    SQLSERVER_ACTIVE_SESSION_COUNT,
    SQLSERVER_INSTANCE_CPU_PERCENT,
    SQLSERVER_OS_CPU_PERCENT,
    SQLSERVER_PAGE_LIFE_EXPECTANCY,
    SQLSERVER_TOTAL_SERVER_MEMORY_KB,
    SQLSERVER_VOLUME_FREE_PERCENT,
)
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository

router = APIRouter()

_MIN_DATA_POINTS = 5
_FORECAST_WINDOW_DAYS = 90


class _MetricSpec:
    """Metadados estáticos de uma métrica para previsão."""

    def __init__(
        self,
        metric_name: str,
        display_name: str,
        unit: str,
        # Quando não None: valor que representa "capacidade esgotada".
        capacity_threshold: float | None,
        # True = métrica sobe até capacity_threshold (CPU); False = desce até ela (disco livre).
        threshold_is_upper: bool = True,
        aggregate_fn: str = "AVG",
    ) -> None:
        self.metric_name = metric_name
        self.display_name = display_name
        self.unit = unit
        self.capacity_threshold = capacity_threshold
        self.threshold_is_upper = threshold_is_upper
        self.aggregate_fn = aggregate_fn


_SQLSERVER_SPECS: list[_MetricSpec] = [
    _MetricSpec(SQLSERVER_INSTANCE_CPU_PERCENT, "CPU da Instância", "%", 100.0, True),
    _MetricSpec(SQLSERVER_OS_CPU_PERCENT, "CPU do SO", "%", 100.0, True),
    _MetricSpec(SQLSERVER_TOTAL_SERVER_MEMORY_KB, "Memória Usada pelo SQL Server", "KB", None),
    _MetricSpec(
        SQLSERVER_VOLUME_FREE_PERCENT,
        "Disco Livre (mínimo)",
        "%",
        0.0,
        False,
        "MIN",
    ),
    _MetricSpec(SQLSERVER_PAGE_LIFE_EXPECTANCY, "Page Life Expectancy", "s", 300.0, False),
    _MetricSpec(SQLSERVER_ACTIVE_SESSION_COUNT, "Sessões Ativas", "", None),
]

_PG_SPECS: list[_MetricSpec] = [
    _MetricSpec(PG_ACTIVE_SESSION_COUNT, "Sessões Ativas", "", None),
]


class DailyPoint(BaseModel):
    date: str
    value: float


class MetricForecast(BaseModel):
    metric_name: str
    display_name: str
    unit: str
    available: bool
    current_value: float | None = None
    slope_per_day: float | None = None
    trend: str | None = None
    capacity_threshold: float | None = None
    days_to_capacity: int | None = None
    projected_at_7d: float | None = None
    projected_at_30d: float | None = None
    projected_at_90d: float | None = None
    data_points: int = 0
    history: list[DailyPoint] = []


class CapacityForecastResponse(BaseModel):
    instance_id: str
    db_type: str
    as_of: str
    forecasts: list[MetricForecast]


def _linear_regression(xs: list[float], ys: list[float]) -> tuple[float, float]:
    """Regressão linear pelos mínimos quadrados. Retorna (slope, intercept)."""
    n = len(xs)
    if n < 2:
        return 0.0, ys[0] if ys else 0.0
    sx = sum(xs)
    sy = sum(ys)
    sxy = sum(x * y for x, y in zip(xs, ys, strict=False))
    sx2 = sum(x * x for x in xs)
    denom = n * sx2 - sx * sx
    if abs(denom) < 1e-10:
        return 0.0, sy / n
    slope = (n * sxy - sx * sy) / denom
    intercept = (sy - slope * sx) / n
    return slope, intercept


def _days_to_threshold(
    current: float,
    slope: float,
    threshold: float,
    is_upper: bool,
) -> int | None:
    """Dias até o valor atingir o threshold via regressão linear."""
    if is_upper:
        if slope <= 0 or current >= threshold:
            return None
        days = (threshold - current) / slope
    else:
        if slope >= 0 or current <= threshold:
            return None
        days = (current - threshold) / (-slope)
    if days < 0 or not math.isfinite(days):
        return None
    return max(1, round(days))


def _build_forecast(
    spec: _MetricSpec,
    history: list[tuple[str, float]],
    now: datetime,
) -> MetricForecast:
    from datetime import date as date_type  # noqa: PLC0415

    base = MetricForecast(
        metric_name=spec.metric_name,
        display_name=spec.display_name,
        unit=spec.unit,
        available=False,
        capacity_threshold=spec.capacity_threshold,
        data_points=len(history),
        history=[DailyPoint(date=d, value=v) for d, v in history],
    )
    if len(history) < _MIN_DATA_POINTS:
        return base

    dates = [date_type.fromisoformat(d) for d, _ in history]
    values = [v for _, v in history]
    origin = dates[0]
    xs = [float((d - origin).days) for d in dates]
    slope, intercept = _linear_regression(xs, values)

    today_x = float((now.date() - origin).days)
    current = slope * today_x + intercept

    if abs(slope) < 0.01:
        trend = "stable"
    elif slope > 0:
        trend = "up"
    else:
        trend = "down"

    def project(days_ahead: float) -> float:
        return slope * (today_x + days_ahead) + intercept

    days_to_cap: int | None = None
    if spec.capacity_threshold is not None:
        days_to_cap = _days_to_threshold(
            current, slope, spec.capacity_threshold, spec.threshold_is_upper
        )

    base.available = True
    base.current_value = round(current, 2)
    base.slope_per_day = round(slope, 4)
    base.trend = trend
    base.days_to_capacity = days_to_cap
    base.projected_at_7d = round(project(7), 2)
    base.projected_at_30d = round(project(30), 2)
    base.projected_at_90d = round(project(90), 2)
    return base


def get_metric_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MetricSamplesRepository:
    return MetricSamplesRepository(session)


def get_instances_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstancesRepository:
    return InstancesRepository(session)


@router.get("/forecast/{instance_id}", response_model=CapacityForecastResponse)
async def get_capacity_forecast(
    instance_id: UUID,
    _: Annotated[object, Depends(get_current_user)],
    _access: Annotated[None, Depends(check_instance_access)],
    inst_repo: Annotated[InstancesRepository, Depends(get_instances_repository)],
    metric_repo: Annotated[MetricSamplesRepository, Depends(get_metric_samples_repository)],
) -> CapacityForecastResponse:
    """Previsão de capacidade: regressão linear das médias diárias dos últimos 90 dias."""
    instance = await inst_repo.get_by_id(instance_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Instância não encontrada")

    db_type: str = instance.db_type
    specs = _SQLSERVER_SPECS if db_type == "sqlserver" else _PG_SPECS

    since = datetime.now(tz=UTC) - timedelta(days=_FORECAST_WINDOW_DAYS)
    now = datetime.now(tz=UTC)

    forecasts: list[MetricForecast] = []
    for spec in specs:
        history = await metric_repo.get_daily_averages_for_forecast(
            target_type="instance",
            target_id=instance_id,
            metric_name=spec.metric_name,
            since=since,
            aggregate_fn=spec.aggregate_fn,
        )
        forecasts.append(_build_forecast(spec, history, now))

    # Omite métricas com zero dados (não coletadas para esta instância/engine).
    forecasts = [f for f in forecasts if f.data_points > 0]

    return CapacityForecastResponse(
        instance_id=str(instance_id),
        db_type=db_type,
        as_of=now.isoformat(),
        forecasts=forecasts,
    )
