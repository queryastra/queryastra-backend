"""Endpoints de leitura de metricas de recurso — stub.

Destino de dados: hypertable `metric_samples` (`Docs/database-schema.md` §2). Cobre metricas de
SO (node_exporter/windows_exporter) e metricas numericas simples de banco.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

router = APIRouter()


class MetricSamplePoint(BaseModel):
    """Um ponto de serie temporal de `metric_samples`."""

    time: datetime
    metric_name: str
    value: float
    labels: dict


class MetricSeriesResponse(BaseModel):
    """Serie temporal de uma metrica para um alvo (instancia ou servidor)."""

    target_type: str
    target_id: UUID
    metric_name: str
    points: list[MetricSamplePoint]


@router.get("/{target_type}/{target_id}", response_model=MetricSeriesResponse)
async def get_metric_series(
    target_type: str, target_id: UUID, metric_name: str
) -> MetricSeriesResponse:
    """Le a serie temporal de uma metrica para um alvo. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Leitura de serie de metricas ainda nao implementada.",
    )
