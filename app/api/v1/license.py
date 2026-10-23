"""Endpoints de licença — consulta de plano efetivo e ativação de chave de licença.

GET  /license/         — retorna o status atual do plano (plan_id, dias restantes, expiração).
POST /license/activate — valida uma chave de licença contra a tabela `licenses` do Supabase e
                         atualiza o `system_license` local.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta
from typing import Annotated

import httpx
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_db_session, get_plan_service, require_admin
from app.core.config import get_settings
from app.repositories.system_license_repository import SystemLicenseRepository
from app.services.plan_service import PLANS, PlanService

router = APIRouter()

_KEY_RE = re.compile(r"^QA[PT]-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}$")
_PREFIX_TO_PLAN = {"QAP": "pro", "QAT": "team"}


class LicenseStatusResponse(BaseModel):
    plan_id: str
    effective_plan_id: str
    label: str
    trial_days_remaining: int | None
    subscription_expires_at: datetime | None
    license_key_masked: str | None
    max_instances: int | None
    history_days: int


def _to_response(
    row_plan_id: str,
    row_trial_started_at: datetime,
    row_subscription_expires_at: datetime | None,
    row_license_key: str | None,
    plan_svc: PlanService,
) -> LicenseStatusResponse:
    effective = plan_svc.effective_plan()
    masked = None
    if row_license_key:
        masked = row_license_key[:4] + "-****-****-****-" + row_license_key[-4:]
    return LicenseStatusResponse(
        plan_id=row_plan_id,
        effective_plan_id=effective.plan_id,
        label=effective.label,
        trial_days_remaining=plan_svc.trial_days_remaining(),
        subscription_expires_at=row_subscription_expires_at,
        license_key_masked=masked,
        max_instances=effective.max_instances,
        history_days=effective.history_days,
    )


@router.get("/", response_model=LicenseStatusResponse)
async def get_license_status(
    session: Annotated[AsyncSession, Depends(get_db_session)],
    plan_svc: Annotated[PlanService, Depends(get_plan_service)],
    _: Annotated[CurrentUser, Depends(get_current_user)],
) -> LicenseStatusResponse:
    repo = SystemLicenseRepository(session)
    row = await repo.get_or_create()
    await session.commit()
    return _to_response(
        row.plan_id,
        row.trial_started_at,
        row.subscription_expires_at,
        row.license_key,
        plan_svc,
    )


class ActivateLicenseRequest(BaseModel):
    key: str


@router.post("/activate", response_model=LicenseStatusResponse)
async def activate_license(
    payload: ActivateLicenseRequest,
    session: Annotated[AsyncSession, Depends(get_db_session)],
    admin: Annotated[CurrentUser, Depends(require_admin)],
) -> LicenseStatusResponse:
    key = payload.key.strip().upper()
    if not _KEY_RE.match(key):
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Formato de chave inválido. Use QAP-XXXX-XXXX-XXXX-XXXX (Pro) ou QAT-XXXX-XXXX-XXXX-XXXX (Team).",
        )

    prefix = key[:3]
    plan_id = _PREFIX_TO_PLAN.get(prefix)
    if plan_id is None:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="Prefixo de chave desconhecido.",
        )

    cfg = get_settings()
    if cfg.supabase_url and cfg.supabase_service_key:
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                resp = await client.get(
                    f"{cfg.supabase_url}/rest/v1/licenses",
                    params={"key": f"eq.{key}", "select": "key,plan_id,active,expires_at"},
                    headers={
                        "apikey": cfg.supabase_service_key,
                        "Authorization": f"Bearer {cfg.supabase_service_key}",
                    },
                )
        except httpx.RequestError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail=f"Não foi possível verificar a chave com o servidor de licenças: {exc}",
            ) from exc

        if resp.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_502_BAD_GATEWAY,
                detail="Servidor de licenças retornou erro inesperado.",
            )

        rows = resp.json()
        if not rows:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Chave de licença não encontrada.",
            )

        lic = rows[0]
        if not lic.get("active", False):
            raise HTTPException(
                status_code=status.HTTP_402_PAYMENT_REQUIRED,
                detail="Esta chave de licença está inativa ou foi revogada.",
            )

        # Respeita o plan_id gravado no Supabase (autoridade) se presente e reconhecido
        remote_plan = lic.get("plan_id")
        if remote_plan in PLANS:
            plan_id = remote_plan

        # Expiration from Supabase row (ISO datetime string or None)
        expires_at: datetime | None = None
        raw_expires = lic.get("expires_at")
        if raw_expires:
            try:
                expires_at = datetime.fromisoformat(raw_expires.replace("Z", "+00:00"))
            except ValueError:
                pass
        if expires_at is None:
            # Default: 1 year subscription from today
            expires_at = datetime.now(UTC) + timedelta(days=365)
    else:
        # Offline mode — no Supabase configured; trust the key format only (dev/testing)
        expires_at = datetime.now(UTC) + timedelta(days=365)

    repo = SystemLicenseRepository(session)
    row = await repo.update_plan(plan_id, expires_at, license_key=key)
    await session.commit()

    new_plan_svc = PlanService(
        trial_started_at=row.trial_started_at,
        plan_id=row.plan_id,
        subscription_expires_at=row.subscription_expires_at,
    )
    return _to_response(
        row.plan_id,
        row.trial_started_at,
        row.subscription_expires_at,
        row.license_key,
        new_plan_svc,
    )
