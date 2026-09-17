"""Endpoints de configuracao e gestao de alertas.

`AlertRule`/`Alert`/`AlertSilence`/`AlertEscalationLevel` conforme `app/models/alert.py`,
`Docs/database-schema.md` §4. Fase 2 (`Docs/product-vision-and-roadmap.md`) expandiu o catalogo
alem do threshold simples original — ver `app.workers.alerting_worker` para a logica de
avaliacao de cada `rule_type`; este modulo so expoe CRUD/consulta e a validacao de cadastro
(`custom_sql` passa por `app.services.custom_sql_guard` antes de ser persistido).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.models.alert import AlertEscalationLevel, AlertRule, AlertSilence
from app.repositories.alerts_repository import AlertsRepository, AlertWithNames
from app.services.custom_sql_guard import CustomSqlRejectedError, validate_custom_sql

router = APIRouter()

_RULE_TYPES = ("threshold", "anomaly", "custom_sql", "blocking_recurrence", "plan_compile_timeout")
_RESET_MODES = (
    "auto_when_false",
    "auto_after_duration",
    "no_reset_every_poll",
    "no_reset_manual",
)


class AlertEscalationLevelRequest(BaseModel):
    """Corpo de um nivel de escalonamento (Fase 2, item 3) — nivel 1 e implicito (disparo
    normal), so niveis 2+ sao cadastrados aqui."""

    level_number: int = Field(ge=2)
    wait_minutes: int = Field(ge=1)
    email_recipients: list[str] | None = None


class AlertEscalationLevelResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: UUID
    level_number: int
    wait_minutes: int
    email_recipients: list[str] | None


class AlertRuleCreateRequest(BaseModel):
    """Corpo de criacao de regra de alerta — `rule_type` decide quais campos sao relevantes
    (ver docstring de `app.workers.alerting_worker` para o desenho de cada tipo)."""

    name: str
    target_type: str = Field(pattern="^(server|instance)$")
    metric_name: str | None = None
    comparison: str | None = Field(default=None, pattern="^(gt|lt|gte|lte)$")
    threshold_warning: float | None = None
    threshold_critical: float | None = None
    rule_type: str = Field(default="threshold", pattern="^(" + "|".join(_RULE_TYPES) + ")$")
    baseline_window_days: int | None = None
    custom_sql: str | None = None
    reset_mode: str = Field(default="auto_when_false", pattern="^(" + "|".join(_RESET_MODES) + ")$")
    reset_after_seconds: int | None = None
    recurrence_threshold_count: int | None = None
    recurrence_window_days: int | None = None
    escalation_levels: list[AlertEscalationLevelRequest] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_rule_is_coherent(self) -> AlertRuleCreateRequest:
        if self.rule_type == "threshold" and self.metric_name is not None:
            if self.comparison is None:
                raise ValueError("comparison e obrigatorio quando metric_name e informado")
            if self.threshold_warning is None and self.threshold_critical is None:
                raise ValueError("informe threshold_warning e/ou threshold_critical")
        if self.rule_type == "anomaly" and not self.metric_name:
            raise ValueError("metric_name e obrigatorio para rule_type=anomaly")
        if self.rule_type == "custom_sql":
            if not self.custom_sql:
                raise ValueError("custom_sql e obrigatorio para rule_type=custom_sql")
            if self.comparison is None:
                raise ValueError("comparison e obrigatorio para rule_type=custom_sql")
            try:
                validate_custom_sql(self.custom_sql)
            except CustomSqlRejectedError as exc:
                raise ValueError(str(exc)) from exc
        if self.rule_type == "blocking_recurrence":
            if self.comparison is None:
                raise ValueError("comparison e obrigatorio para rule_type=blocking_recurrence")
            if self.recurrence_window_days is None:
                raise ValueError(
                    "recurrence_window_days e obrigatorio para rule_type=blocking_recurrence"
                )
        if self.reset_mode == "auto_after_duration" and self.reset_after_seconds is None:
            raise ValueError(
                "reset_after_seconds e obrigatorio quando reset_mode=auto_after_duration"
            )
        level_numbers = [lv.level_number for lv in self.escalation_levels]
        if len(level_numbers) != len(set(level_numbers)):
            raise ValueError("escalation_levels nao pode repetir o mesmo level_number")
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
    rule_type: str
    baseline_window_days: int | None
    custom_sql: str | None
    reset_mode: str
    reset_after_seconds: int | None
    recurrence_threshold_count: int | None
    recurrence_window_days: int | None
    escalation_levels: list[AlertEscalationLevelResponse] = Field(default_factory=list)


class AlertResponse(BaseModel):
    """Resposta de leitura de um alerta disparado — inclui nome da regra e do alvo resolvidos."""

    id: UUID
    alert_rule_id: UUID
    alert_name: str
    rule_type: str
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
        rule_type=item.rule_type,
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


def _to_alert_rule_response(
    rule: AlertRule, levels: list[AlertEscalationLevel]
) -> AlertRuleResponse:
    return AlertRuleResponse(
        id=rule.id,
        name=rule.name,
        target_type=rule.target_type,
        metric_name=rule.metric_name,
        comparison=rule.comparison,
        threshold_warning=rule.threshold_warning,
        threshold_critical=rule.threshold_critical,
        active=rule.active,
        rule_type=rule.rule_type,
        baseline_window_days=rule.baseline_window_days,
        custom_sql=rule.custom_sql,
        reset_mode=rule.reset_mode,
        reset_after_seconds=rule.reset_after_seconds,
        recurrence_threshold_count=rule.recurrence_threshold_count,
        recurrence_window_days=rule.recurrence_window_days,
        escalation_levels=[AlertEscalationLevelResponse.model_validate(lv) for lv in levels],
    )


@router.post("/rules", response_model=AlertRuleResponse, status_code=status.HTTP_201_CREATED)
async def create_alert_rule(
    payload: AlertRuleCreateRequest,
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> AlertRuleResponse:
    """Cria uma regra de alerta. `custom_sql` (quando presente) ja foi validado pelo
    bloqueio por palavra-chave no `model_validator` de `AlertRuleCreateRequest` — nunca
    persistido sem passar por essa checagem."""
    fields = payload.model_dump(exclude={"escalation_levels"})
    rule = await repository.create_alert_rule(AlertRule(**fields))
    levels: list[AlertEscalationLevel] = []
    if payload.escalation_levels:
        levels = await repository.create_escalation_levels(
            rule.id,
            [
                AlertEscalationLevel(
                    level_number=lv.level_number,
                    wait_minutes=lv.wait_minutes,
                    email_recipients=lv.email_recipients,
                )
                for lv in payload.escalation_levels
            ],
        )
    return _to_alert_rule_response(rule, levels)


@router.get("/rules", response_model=list[AlertRuleResponse])
async def list_alert_rules(
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> list[AlertRuleResponse]:
    """Lista todas as regras de alerta, ativas ou nao."""
    rules = await repository.list_rules()
    result: list[AlertRuleResponse] = []
    for rule in rules:
        levels = await repository.list_escalation_levels(rule.id)
        result.append(_to_alert_rule_response(rule, levels))
    return result


@router.delete("/rules/{rule_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_alert_rule(
    rule_id: UUID,
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> None:
    """Remove uma regra de alerta e todo o historico de `Alert`/notificacao/silenciamento
    associado (ver docstring de `AlertsRepository.delete_alert_rule`)."""
    deleted = await repository.delete_alert_rule(rule_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Regra de alerta nao encontrada."
        )


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


@router.post("/{alert_id}/close", response_model=AlertResponse)
async def close_alert(
    alert_id: UUID,
    repository: Annotated[AlertsRepository, Depends(get_alerts_repository)],
) -> AlertResponse:
    """Fecha manualmente um alerta aberto (Fase 2, item 3) — usado pelo `reset_mode
    ="no_reset_manual"` (unica forma de fechar esse modo) e disponivel como acao manual geral
    para qualquer outro modo de reset."""
    alert = await repository.get_alert_by_id(alert_id)
    if alert is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Alerta nao encontrado.")
    if alert.state == "closed":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Alerta ja esta fechado.")
    alert.state = "closed"
    alert.resolved_at = datetime.now(UTC)
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
