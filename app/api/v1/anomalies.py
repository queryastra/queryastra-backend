"""Deteccao automatica de anomalias sazonais — leitura do que o `AnomalyDetectionWorker` gravou.

- `instance_router` (montado em `/instances`): status por metrica, eventos e serie
  observado-vs-esperado de uma instancia.
- `router` (montado em `/anomalies`): eventos de todas as instancias acessiveis ao usuario.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, check_instance_access, get_current_user, get_db_session
from app.core.config import get_settings
from app.models.anomaly import AnomalyEvent
from app.repositories.anomaly_repository import (
    HOUR,
    METRIC_DEFS,
    AnomalyRepository,
    floor_hour,
)
from app.repositories.instances_repository import InstancesRepository
from app.repositories.user_instance_access_repository import UserInstanceAccessRepository
from app.workers.anomaly_detection_worker import (
    daily_buckets,
    evaluate,
    select_baseline,
    weekly_buckets,
)

instance_router = APIRouter()
router = APIRouter()

Unit = Literal["percent", "count", "ms", "per_sec"]
BaselineKindField = Literal["same_hour_same_weekday", "same_hour_any_day"]


class AnomalyMetricResponse(BaseModel):
    metric_key: str
    unit: Unit
    current_value: float | None
    expected_value: float | None
    expected_low: float | None
    expected_high: float | None
    z_score: float | None
    status: Literal["normal", "warning", "critical", "insufficient_data"]
    baseline_kind: BaselineKindField | None
    baseline_samples: int


class AnomalyEventResponse(BaseModel):
    id: UUID
    instance_id: UUID
    metric_key: str
    unit: Unit
    direction: Literal["high", "low"]
    severity: Literal["warning", "critical"]
    state: Literal["open", "closed"]
    first_detected_at: datetime
    last_detected_at: datetime
    closed_at: datetime | None
    observed_value: float
    expected_value: float
    expected_low: float
    expected_high: float
    peak_z_score: float
    baseline_kind: BaselineKindField
    alert_id: UUID | None


class AnomalyEventWithInstanceResponse(AnomalyEventResponse):
    instance_name: str


class InstanceAnomaliesResponse(BaseModel):
    enabled: bool
    evaluated_at: datetime | None
    metrics: list[AnomalyMetricResponse]
    events: list[AnomalyEventResponse]


class AnomalySeriesPointResponse(BaseModel):
    bucket: datetime
    observed: float | None
    expected: float | None
    lower: float | None
    upper: float | None
    is_anomaly: bool


def _event_response(event: AnomalyEvent) -> AnomalyEventResponse:
    return AnomalyEventResponse.model_validate(event, from_attributes=True)


@instance_router.get("/{instance_id}/anomalies", response_model=InstanceAnomaliesResponse)
async def get_instance_anomalies(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    hours: int = Query(default=24, ge=1, le=720),
) -> InstanceAnomaliesResponse:
    """Ultima avaliacao de cada metrica + eventos (todos os abertos e os da janela)."""
    repo = AnomalyRepository(session)
    statuses = await repo.get_statuses(instance_id)
    order = {key: i for i, key in enumerate(METRIC_DEFS)}
    metrics = [
        AnomalyMetricResponse.model_validate(s, from_attributes=True)
        for s in sorted(statuses.values(), key=lambda s: order.get(s.metric_key, 999))
    ]
    evaluated_at = max((s.evaluated_at for s in statuses.values()), default=None)
    events = await repo.list_events(
        instance_ids=[instance_id],
        since=datetime.now(UTC) - timedelta(hours=hours),
        open_only=False,
    )
    return InstanceAnomaliesResponse(
        enabled=get_settings().anomaly_detection_enabled,
        evaluated_at=evaluated_at,
        metrics=metrics,
        events=[_event_response(e) for e in events],
    )


@instance_router.get(
    "/{instance_id}/anomalies/series", response_model=list[AnomalySeriesPointResponse]
)
async def get_instance_anomaly_series(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    metric_key: str = Query(..., max_length=64),
    hours: int = Query(default=48, ge=1, le=336),
) -> list[AnomalySeriesPointResponse]:
    """Serie horaria observado vs faixa esperada (mesmo baseline sazonal do worker)."""
    metric = METRIC_DEFS.get(metric_key)
    if metric is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Metrica desconhecida.")
    settings = get_settings()
    weeks = max(1, settings.anomaly_baseline_weeks)
    end = floor_hour(datetime.now(UTC))
    start = end - timedelta(hours=hours)
    history_start = min(start - timedelta(weeks=weeks), start - timedelta(days=14))
    values = await AnomalyRepository(session).get_hourly_range(
        instance_id, metric_key, history_start, end
    )

    points: list[AnomalySeriesPointResponse] = []
    bucket = start
    while bucket < end:
        weekly = [values[b] for b in weekly_buckets(bucket, weeks) if b in values]
        daily = [values[b] for b in daily_buckets(bucket) if b in values]
        observed = values.get(bucket)
        ev = evaluate(
            observed,
            select_baseline(weekly, daily),
            metric,
            settings.anomaly_warning_z,
            settings.anomaly_critical_z,
        )
        points.append(
            AnomalySeriesPointResponse(
                bucket=bucket,
                observed=observed,
                expected=ev.expected,
                lower=ev.low,
                upper=ev.high,
                is_anomaly=ev.status in ("warning", "critical"),
            )
        )
        bucket += HOUR
    return points


@router.get("", response_model=list[AnomalyEventWithInstanceResponse])
async def list_anomalies(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    state: Literal["open", "all"] = Query(default="open"),
    hours: int = Query(default=24, ge=1, le=720),
) -> list[AnomalyEventWithInstanceResponse]:
    """Eventos de anomalia de todas as instancias acessiveis ao usuario."""
    instances = await InstancesRepository(session).list_all()
    if not current_user.is_admin:
        accessible = await UserInstanceAccessRepository(session).get_instance_ids_for_user(
            current_user.user_id
        )
        instances = [i for i in instances if i.id in accessible]
    names = {i.id: i.name for i in instances}
    if not names:
        return []
    events = await AnomalyRepository(session).list_events(
        instance_ids=list(names),
        since=datetime.now(UTC) - timedelta(hours=hours),
        open_only=state == "open",
    )
    return [
        AnomalyEventWithInstanceResponse(
            **_event_response(e).model_dump(), instance_name=names.get(e.instance_id, "")
        )
        for e in events
    ]
