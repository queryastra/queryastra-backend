"""Endpoints de configuracao e gestao de alertas.

`AlertRule`/`Alert`/`AlertSilence` conforme `app/models/alert.py`, `Docs/database-schema.md` §4.
CORTE DE ESCOPO: so threshold simples (1 metrica + comparacao + threshold_warning/critical) —
sem anomaly detection, sem os "5 metodos de agregacao"; a avaliacao continua em si roda em
`app.workers.alerting_worker`, este modulo so expoe CRUD/consulta.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.models.alert import AlertRule, AlertSilence
from app.repositories.alerts_repository import AlertsRepository, AlertWithNames

router = APIRouter()


class AlertRuleCreateRequest(BaseModel):
    """Corpo de criacao de regra de alerta (threshold simples)."""

    name: str
    target_type: str = Field(pattern="^(server|instance)$")
    metric_name: str | None = None
    comparison: str | None = Field(default=None, pattern="^(gt|lt|gte|lte)$")
    threshold_warning: float | None = None
    threshold_critical: float | None = None

    @model_validator(mode="after")
    def _validate_metric_rule_is_coherent(self) -> AlertRuleCreateRequest:
        if self.metric_name is not None:
            if self.comparison is None:
                raise ValueError("comparison e obrigatorio quando metric_name e informado")
            if self.threshold_warning is None and self.threshold_critical is None:
                raise ValueError("informe threshold_warning e/ou threshold_critical")
        return self


class AlertRuleResponse(BaseModel):
    """Resposta de leitura de regra de alerta."""

    model_config = {"from_attributes": True}

    id: UUID
    name: str
    target_type: str
    metric_name: str | None
    comparison: str | None
    threshold_warning: float | None
    threshold_critical: float | None
    active: bool


class AlertResponse(BaseModel):
    """Resposta de leitura de um alerta disparado — inclui nome da regra e do alvo resolvidos."""

    id: UUID
    alert_rule_id: UUID
    alert_name: str
    target_type: str
    target_id: UUID
    target_name: str
    severity: str
    state: str
    triggered_at: datetime
    acknowledged_at: datetime | None
    resolved_at: datetime | None
    last_reminder_at: datetime | None
    description: str | None


class AlertSilenceCreateRequest(BaseModel):
    """Corpo de criacao de silenciamento pontual (independente do estado do alerta)."""

    target_type: str = Field(pattern="^(server|instance)$")
    target_id: UUID
    alert_rule_id: UUID | None = None
    silenced_until: datetime | None = None
    reason: str | None = None
    created_by: str | None = None


class AlertSilenceResponse(BaseModel):
    """Resposta de leitura de um silenciamento."""

    model_config = {"from_attributes": True}

    id: UUID
    target_type: str
    target_id: UUID
    alert_rule_id: UUID | None
    silenced_at: datetime
    silenced_until: datetime | None
    reason: str | None
    created_by: str | None


def get_alerts_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AlertsRepository:
    return AlertsRepository(session)


def _to_alert_response(item: AlertWithNames) -> AlertResponse:
    a = item.alert
    return AlertResponse(
        id=a.id,
        alert_rule_id=a.alert_rule_id,
        alert_name=item.alert_name,
        target_type=a.target_type,
        target_id=a.target_id,
        target_name=item.target_name,
        severity=a.severity,
        state=a.state,
        triggered_at=a.triggered_at,
        acknowledged_at=a.acknowledged_at,
        resolved_at=a.resolved_at,
        last_reminder_at=a.last_reminder_at,
        description=a.description,
    )


@router.post("/rules", response_model=AlertRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_alert_rule(
    payload: AlertRuleCreateRequest,
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> AlertRuleResponse:
    """Cria uma regra de alerta."""
    rule = await repository.create_alert_rule(AlertRule(**payload.model_dump()))
    return AlertRuleResponse.model_validate(rule)


@router.get("/rules", response_model=list[AlertRuleResponse])
async def list_alert_rules(
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> list[AlertRuleResponse]:
    """Lista todas as regras de alerta, ativas ou nao."""
    rules = await repository.list_rules()
    return [AlertRuleResponse.model_validate(r) for r in rules]


@router.get("", response_model=list[AlertResponse])
async def list_alerts(
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
    state: str | None = None,
    limit: Annotated[int, Query(ge=1, le=500)] = 200,
) -> list[AlertResponse]:
    """Lista alertas disparados, mais recentes primeiro, opcionalmente filtrados por estado."""
    items = await repository.list_alerts(state, limit)
    return [_to_alert_response(i) for i in items]


@router.post("/{alert_id}/acknowledge", response_model=AlertResponse)
async def acknowledge_alert(
    alert_id: UUID,
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> AlertResponse:
    """Reconhece um alerta aberto.

    Nota: reconhecimento nao e permanente — fica atrelado ao episodio; se a severidade mudar
    depois, o alerta reabre visibilidade (`state` volta a `open`) mesmo com reconhecimento
    anterior (ARCHITECTURE_SPEC.md §7a).
    """
    alert = await repository.get_alert_by_id(alert_id)
    if alert is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alerta nao encontrado.")
    if alert.state != "open":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Alerta nao pode ser reconhecido no estado atual ({alert.state}).",
        )
    alert.state = "acknowledged"
    alert.acknowledged_at = datetime.now(UTC)
    await repository.update_alert(alert)
    item = await repository.get_alert_with_names(alert_id)
    assert item is not None
    return _to_alert_response(item)


@router.post("/silences", response_model=AlertSilenceResponse, status_code=status.HTTP_201_CREATED)
async def create_alert_silence(
    payload: AlertSilenceCreateRequest,
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> AlertSilenceResponse:
    """Cria um silenciamento pontual — `alert_rule_id=None` silencia todas as regras do alvo."""
    silence = await repository.create_alert_silence(AlertSilence(**payload.model_dump()))
    return AlertSilenceResponse.model_validate(silence)


@router.get("/silences", response_model=list[AlertSilenceResponse])
async def list_alert_silences(
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
    target_id: UUID | None = None,
) -> list[AlertSilenceResponse]:
    """Lista silenciamentos, opcionalmente filtrados por alvo."""
    silences = await repository.list_alert_silences(target_id)
    return [AlertSilenceResponse.model_validate(s) for s in silences]


@router.delete("/silences/{silence_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_alert_silence(
    silence_id: UUID,
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> None:
    """Remove um silenciamento."""
    deleted = await repository.delete_alert_silence(silence_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Silenciamento nao encontrado."
        )
