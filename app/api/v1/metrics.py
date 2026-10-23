"""Endpoints de leitura de metricas de recurso.

Inclui:
* `GET /{target_type}/{target_id}` — serie bruta (hypertable ou aggregate conforme janela).
* `GET /trends/{instance_id}` — tendencia horaria/diaria (Item 1 long-term trends) a partir
  de `metric_samples_hourly` / `metric_samples_daily`.
* `GET /available-metrics/{instance_id}` — nomes de metricas distintos nos ultimos 7 dias,
  para popular o seletor de metrica no frontend.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (
    PlanService,
    check_instance_access,
    get_current_user,
    get_db_session,
    get_plan_service,
    resolve_time_window,
)
from app.core.config import get_settings
from app.repositories.metric_samples_repository import MetricSamplesRepository

router = APIRouter()


class MetricPointResponse(BaseModel):
    """Um ponto de serie temporal de `metric_samples`."""

    time: datetime
    value: float
    labels: dict


class MetricSeriesResponse(BaseModel):
    """Serie temporal de uma metrica para um alvo (instancia ou servidor)."""

    target_type: str
    target_id: UUID
    metric_name: str
    points: list[MetricPointResponse]


class TrendPointResponse(BaseModel):
    """Um bucket de tendencia horaria ou diaria (continuous aggregate)."""

    bucket: datetime
    avg_value: float
    min_value: float
    max_value: float


class MetricTrendResponse(BaseModel):
    """Resposta de `/trends/{instance_id}` — serie de buckets agregados."""

    instance_id: UUID
    metric_name: str
    granularity: str
    points: list[TrendPointResponse]


class AvailableMetricsResponse(BaseModel):
    """Nomes de metricas disponiveis para uma instancia nos ultimos 7 dias."""

    instance_id: UUID
    metric_names: list[str]


# --- Dependency providers -------------------------------------------------------------------


def get_metric_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MetricSamplesRepository:
    return MetricSamplesRepository(session)


# --- Endpoints -------------------------------------------------------------------------------

_MAX_TREND_DAYS = 365


@router.get("/trends/{instance_id}", response_model=MetricTrendResponse)
async def get_metric_trend(
    instance_id: UUID,
    _: Annotated[object, Depends(get_current_user)],
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[MetricSamplesRepository, Depends(get_metric_samples_repository)],
    metric_name: Annotated[str, Query()],
    granularity: Annotated[Literal["hourly", "daily"], Query()] = "daily",
    days: Annotated[int, Query(ge=1, le=_MAX_TREND_DAYS)] = 30,
) -> MetricTrendResponse:
    """Tendencia de uma metrica ao longo do tempo usando continuous aggregates.

    `granularity=hourly` le `metric_samples_hourly` (retencao 90 dias, recomendado para janelas
    ate 45 dias). `granularity=daily` le `metric_samples_daily` (retencao 5 anos, ate 365 dias).
    `days` define a janela retroativa a partir de agora.
    """
    now = datetime.now(tz=UTC)
    since = now - timedelta(days=days)

    if granularity == "hourly":
        raw_points = await repository.get_metric_trend_hourly(
            instance_id=instance_id,
            metric_name=metric_name,
            since=since,
            until=now,
        )
    else:
        raw_points = await repository.get_metric_trend_daily(
            instance_id=instance_id,
            metric_name=metric_name,
            since=since,
            until=now,
        )

    points = [
        TrendPointResponse(
            bucket=p["bucket"],
            avg_value=p["avg_value"],
            min_value=p["min_value"],
            max_value=p["max_value"],
        )
        for p in raw_points
    ]
    return MetricTrendResponse(
        instance_id=instance_id,
        metric_name=metric_name,
        granularity=granularity,
        points=points,
    )


@router.get("/available-metrics/{instance_id}", response_model=AvailableMetricsResponse)
async def get_available_metrics(
    instance_id: UUID,
    _: Annotated[object, Depends(get_current_user)],
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[MetricSamplesRepository, Depends(get_metric_samples_repository)],
) -> AvailableMetricsResponse:
    """Nomes de metricas distintos coletados para a instancia nos ultimos 7 dias.

    Lido da hypertable bruta para garantir visibilidade de metricas recentes. Usado pelo
    frontend para popular o seletor de metrica no painel de tendencias.
    """
    since = datetime.now(tz=UTC) - timedelta(days=7)
    metric_names = await repository.get_distinct_metric_names(
        target_type="instance",
        target_id=instance_id,
        since=since,
    )
    return AvailableMetricsResponse(instance_id=instance_id, metric_names=metric_names)


@router.get("/{target_type}/{target_id}", response_model=list[MetricSeriesResponse])
async def get_metric_series(
    target_type: Literal["server", "instance"],
    target_id: UUID,
    repository: Annotated[MetricSamplesRepository, Depends(get_metric_samples_repository)],
    plan: Annotated[PlanService, Depends(get_plan_service)],
    metric_name: Annotated[list[str] | None, Query()] = None,
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[MetricSeriesResponse]:
    """Le as series temporais de metricas de um alvo no periodo.

    `metric_name` e repetivel (`?metric_name=a&metric_name=b`); omitido devolve todas as
    metricas do alvo no periodo. Sem `from`/`to`, olha `API_READ_DEFAULT_LOOKBACK_SECONDS` para
    tras a partir de agora. O historico e limitado ao plano efetivo do sistema.
    """
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    resolved_since = plan.clamp_from(resolved_since)
    rows = await repository.list_series(
        target_type,
        target_id,
        metric_name,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
    )
    grouped: dict[str, list[MetricPointResponse]] = defaultdict(list)
    for row in rows:
        grouped[row.metric_name].append(
            MetricPointResponse(time=row.time, value=row.value, labels=row.labels)
        )
    return [
        MetricSeriesResponse(
            target_type=target_type, target_id=target_id, metric_name=name, points=points
        )
        for name, points in grouped.items()
    ]
