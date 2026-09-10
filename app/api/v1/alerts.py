"""Endpoints de configuracao de alertas — stub.

`AlertRule`/`Alert`/`AlertSilence` conforme `app/models/alert.py`, `Docs/database-schema.md` §4.
Threshold usa intervalo semiaberto `[Min, Max)` (Max vazio no nivel mais alto = sem teto) —
ARCHITECTURE_SPEC.md §7a.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

router = APIRouter()


class AlertRuleCreateRequest(BaseModel):
    """Corpo de criacao de regra de alerta."""

    name: str
    target_type: str = Field(pattern="^(server|instance)$")
    metric_name: str | None = None
    comparison: str | None = Field(default=None, pattern="^(gt|lt|gte|lte)$")
    threshold_warning: float | None = None
    threshold_critical: float | None = None


class AlertRuleResponse(BaseModel):
    """Resposta de leitura de regra de alerta."""

    id: UUID
    name: str
    target_type: str
    metric_name: str | None
    comparison: str | None
    threshold_warning: float | None
    threshold_critical: float | None
    active: bool


class AlertResponse(BaseModel):
    """Resposta de leitura de um alerta disparado."""

    id: UUID
    alert_rule_id: UUID
    target_type: str
    target_id: UUID
    severity: str
    state: str
    triggered_at: datetime
    acknowledged_at: datetime | None
    resolved_at: datetime | None
    description: str | None


@router.post("/rules", response_model=AlertRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_alert_rule(payload: AlertRuleCreateRequest) -> AlertRuleResponse:
    """Cria uma regra de alerta. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Criacao de regra de alerta ainda nao implementada.",
    )


@router.get("/rules", response_model=list[AlertRuleResponse])
async def list_alert_rules() -> list[AlertRuleResponse]:
    """Lista regras de alerta. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Listagem de regras de alerta ainda nao implementada.",
    )


@router.get("", response_model=list[AlertResponse])
async def list_alerts(state: str | None = None) -> list[AlertResponse]:
    """Lista alertas disparados, opcionalmente filtrados por estado. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Listagem de alertas ainda nao implementada.",
    )


@router.post("/{alert_id}/acknowledge", response_model=AlertResponse)
async def acknowledge_alert(alert_id: UUID) -> AlertResponse:
    """Reconhece um alerta aberto. Ainda nao implementado.

    Nota: reconhecimento nao e permanente — fica atrelado ao episodio; se o alerta normalizar e
    disparar de novo, notifica de novo mesmo com reconhecimento anterior (ARCHITECTURE_SPEC.md §7a).
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Reconhecimento de alerta ainda nao implementado.",
    )
