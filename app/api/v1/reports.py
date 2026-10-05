"""API de relatorios agendados â€" Item 4.

Rotas:
  GET    /reports                       â€" lista todos os relatorios configurados
  POST   /reports                       â€" cria um novo relatorio agendado
  GET    /reports/{id}                  â€" detalhe de um relatorio
  PUT    /reports/{id}                  â€" atualiza titulo/params/agenda/destinatarios
  DELETE /reports/{id}                  â€" remove a configuracao
  PATCH  /reports/{id}/toggle           â€" ativa/desativa
  POST   /reports/{id}/download         â€" gera e baixa o PDF imediatamente (on-demand)
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Any

from croniter import croniter as Croniter
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import Response
from pydantic import BaseModel, Field, field_validator

from app.api.deps import get_db_session, require_admin
from app.models.scheduled_report import ScheduledReport
from app.repositories.instances_repository import InstancesRepository
from app.repositories.scheduled_reports_repository import ScheduledReportsRepository
from app.services.report_service import generate_report
from app.workers.report_worker import _compute_next_run

router = APIRouter()

_VALID_TYPES = {"top_sql", "average_wait", "typical_day"}


# ── Schemas ──────────────────────────────────────────────────────────────────


class ReportParamsInput(BaseModel):
    top_n: int = Field(default=10, ge=1, le=50)
    lookback_days: int = Field(default=7, ge=1, le=90)


class ScheduledReportUpdateRequest(BaseModel):
    title: str | None = Field(default=None, min_length=1, max_length=200)
    params: ReportParamsInput | None = None
    schedule_cron: str | None = Field(default=None, min_length=1, max_length=100)
    email_recipients: list[str] | None = None
    is_recurring: bool | None = None

    @field_validator("schedule_cron")
    @classmethod
    def validate_cron(cls, v: str | None) -> str | None:
        if v is not None and not Croniter.is_valid(v):
            raise ValueError(f"Expressao cron invalida: {v!r}")
        return v


class ScheduledReportCreateRequest(BaseModel):
    instance_id: uuid.UUID
    report_type: str
    title: str = Field(min_length=1, max_length=200)
    params: ReportParamsInput = Field(default_factory=ReportParamsInput)
    schedule_cron: str = Field(min_length=1, max_length=100)
    email_recipients: list[str] = Field(default_factory=list)
    is_recurring: bool = True

    @field_validator("report_type")
    @classmethod
    def validate_type(cls, v: str) -> str:
        if v not in _VALID_TYPES:
            raise ValueError(f"report_type deve ser um de: {sorted(_VALID_TYPES)}")
        return v

    @field_validator("schedule_cron")
    @classmethod
    def validate_cron(cls, v: str) -> str:
        if not Croniter.is_valid(v):
            raise ValueError(f"Expressao cron invalida: {v!r}")
        return v


class ScheduledReportResponse(BaseModel):
    model_config = {"from_attributes": True}

    id: uuid.UUID
    instance_id: uuid.UUID
    report_type: str
    title: str
    params: dict[str, Any]
    schedule_cron: str
    email_recipients: list[str]
    is_active: bool
    is_recurring: bool
    next_run_at: datetime | None
    last_run_at: datetime | None
    last_run_status: str | None
    last_run_error: str | None
    created_at: datetime

    @classmethod
    def from_orm(cls, r: ScheduledReport) -> ScheduledReportResponse:
        return cls(
            id=r.id,
            instance_id=r.instance_id,
            report_type=r.report_type,
            title=r.title,
            params=r.params,
            schedule_cron=r.schedule_cron,
            email_recipients=r.email_recipients or [],
            is_active=r.is_active,
            is_recurring=r.is_recurring,
            next_run_at=r.next_run_at,
            last_run_at=r.last_run_at,
            last_run_status=r.last_run_status,
            last_run_error=r.last_run_error,
            created_at=r.created_at,
        )


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.get("", response_model=list[ScheduledReportResponse])
async def list_reports(
    _admin: Annotated[None, Depends(require_admin)],
    session: Annotated[Any, Depends(get_db_session)],
) -> list[ScheduledReportResponse]:
    repo = ScheduledReportsRepository(session)
    reports = await repo.list_all()
    return [ScheduledReportResponse.from_orm(r) for r in reports]


@router.post("", response_model=ScheduledReportResponse, status_code=201)
async def create_report(
    body: ScheduledReportCreateRequest,
    _admin: Annotated[None, Depends(require_admin)],
    session: Annotated[Any, Depends(get_db_session)],
) -> ScheduledReportResponse:
    instances_repo = InstancesRepository(session)
    instance = await instances_repo.get_by_id(body.instance_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Instancia nao encontrada.")

    next_run = _compute_next_run(body.schedule_cron, datetime.now(UTC))
    repo = ScheduledReportsRepository(session)
    report = await repo.create(
        instance_id=body.instance_id,
        report_type=body.report_type,
        title=body.title,
        params=body.params.model_dump(),
        schedule_cron=body.schedule_cron,
        email_recipients=body.email_recipients,
        next_run_at=next_run,
        is_recurring=body.is_recurring,
    )
    await session.commit()
    await session.refresh(report)
    return ScheduledReportResponse.from_orm(report)


@router.get("/{report_id}", response_model=ScheduledReportResponse)
async def get_report(
    report_id: uuid.UUID,
    _admin: Annotated[None, Depends(require_admin)],
    session: Annotated[Any, Depends(get_db_session)],
) -> ScheduledReportResponse:
    repo = ScheduledReportsRepository(session)
    report = await repo.get_by_id(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Relatorio nao encontrado.")
    return ScheduledReportResponse.from_orm(report)


@router.put("/{report_id}", response_model=ScheduledReportResponse)
async def update_report(
    report_id: uuid.UUID,
    body: ScheduledReportUpdateRequest,
    _admin: Annotated[None, Depends(require_admin)],
    session: Annotated[Any, Depends(get_db_session)],
) -> ScheduledReportResponse:
    """Atualiza titulo, params, agenda e/ou destinatarios. Tipo e instancia sao imutaveis."""
    repo = ScheduledReportsRepository(session)
    report = await repo.get_by_id(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Relatorio nao encontrado.")
    if body.title is not None:
        report.title = body.title
    if body.params is not None:
        report.params = {"top_n": body.params.top_n, "lookback_days": body.params.lookback_days}
    if body.schedule_cron is not None:
        report.schedule_cron = body.schedule_cron
        report.next_run_at = _compute_next_run(body.schedule_cron, datetime.now(UTC))
    if body.email_recipients is not None:
        report.email_recipients = body.email_recipients
    if body.is_recurring is not None:
        report.is_recurring = body.is_recurring
    session.add(report)
    await session.commit()
    await session.refresh(report)
    return ScheduledReportResponse.from_orm(report)


@router.delete("/{report_id}", status_code=204)
async def delete_report(
    report_id: uuid.UUID,
    _admin: Annotated[None, Depends(require_admin)],
    session: Annotated[Any, Depends(get_db_session)],
) -> None:
    repo = ScheduledReportsRepository(session)
    report = await repo.get_by_id(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Relatorio nao encontrado.")
    await repo.delete(report)
    await session.commit()


@router.patch("/{report_id}/toggle", response_model=ScheduledReportResponse)
async def toggle_report(
    report_id: uuid.UUID,
    _admin: Annotated[None, Depends(require_admin)],
    session: Annotated[Any, Depends(get_db_session)],
) -> ScheduledReportResponse:
    repo = ScheduledReportsRepository(session)
    report = await repo.get_by_id(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Relatorio nao encontrado.")
    await repo.set_active(report, not report.is_active)
    await session.commit()
    await session.refresh(report)
    return ScheduledReportResponse.from_orm(report)


@router.post("/{report_id}/download")
async def download_report(
    report_id: uuid.UUID,
    _admin: Annotated[None, Depends(require_admin)],
    session: Annotated[Any, Depends(get_db_session)],
) -> Response:
    """Gera o PDF sob demanda e o retorna como download direto (application/pdf)."""
    repo = ScheduledReportsRepository(session)
    report = await repo.get_by_id(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Relatorio nao encontrado.")

    instances_repo = InstancesRepository(session)
    instance = await instances_repo.get_by_id(report.instance_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Instancia nao encontrada.")

    try:
        pdf_bytes = await generate_report(
            session=session,
            report_type=report.report_type,
            instance=instance,
            title=report.title,
            params=report.params,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erro ao gerar relatorio: {exc}") from exc

    filename = f"relatorio-{report.report_type}-{datetime.now(UTC).strftime('%Y%m%d-%H%M')}.pdf"
    return Response(
        content=pdf_bytes,
        media_type="application/pdf",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
