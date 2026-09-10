"""Endpoints de leitura de metricas de recurso.

Destino de dados: hypertable bruta `metric_samples` (`Docs/database-schema.md` §2), consultada
diretamente — os continuous aggregates (`metric_samples_hourly`/`daily`) foram criados
`WITH NO DATA` e sem policy de refresh, estao vazios e nunca atualizados, portanto nunca sao
usados aqui. Cobre metricas de SO (node_exporter/windows_exporter) e metricas numericas simples
de banco.

Quebra de contrato intencional: o endpoint `GET /{target_type}/{target_id}` deixa de devolver
uma unica serie (`metric_name` obrigatorio, um objeto de resposta) e passa a devolver
`list[MetricSeriesResponse]` — uma serie por `metric_name` presente no periodo, com o parametro
de query `metric_name` agora repetivel e opcional (omitido = todas as metricas do alvo no
periodo).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session, resolve_time_window
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


# --- Dependency providers -------------------------------------------------------------------


def get_metric_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MetricSamplesRepository:
    return MetricSamplesRepository(session)


# --- Endpoints -------------------------------------------------------------------------------


@router.get("/{target_type}/{target_id}", response_model=list[MetricSeriesResponse])
async def get_metric_series(
    target_type: Literal["server", "instance"],
    target_id: UUID,
    repository: Annotated[MetricSamplesRepository, Depends(get_metric_samples_repository)],
    metric_name: Annotated[list[str] | None, Query()] = None,
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[MetricSeriesResponse]:
    """Le as series temporais de metricas de um alvo no periodo.

    `metric_name` e repetivel (`?metric_name=a&metric_name=b`); omitido devolve todas as
    metricas do alvo no periodo. Sem `from`/`to`, olha `API_READ_DEFAULT_LOOKBACK_SECONDS` para
    tras a partir de agora.
    """
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
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
