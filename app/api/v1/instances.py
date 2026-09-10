"""Endpoints de cadastro, teste de conexao e health-check de instancia.

Contrato: `Docs/specs/02-backend-contract.md`, campos conforme `Docs/database-schema.md` §1.
Toda escrita real ainda esta pendente de implementacao (`app.services.instance_service`) —
os endpoints abaixo tem o shape correto (Pydantic request/response) e respondem `501 Not
Implemented` de forma explicita, exceto onde marcado.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field

router = APIRouter()


class InstanceCreateRequest(BaseModel):
    """Corpo do cadastro de instancia — campos conforme `Docs/database-schema.md` §1.

    Nunca inclui senha: `auth_method='encrypted_fallback'` exige `encrypted_credential_ref` (uma
    referencia ja resolvida pelo fluxo de cofre/criptografia), nunca uma senha em texto puro
    (Docs/rules/01-credential-security.md).
    """

    name: str
    db_type: str = Field(pattern="^(postgresql|sqlserver)$")
    host: str
    port: int
    cloud_provider: str | None = Field(default=None, pattern="^(aws_rds|azure|gcp)$")
    criticality: str = Field(default="standard", pattern="^(critical|standard)$")
    polling_interval_seconds: int = Field(default=10, ge=1)
    auth_method: str = Field(pattern="^(integrated|encrypted_fallback)$")
    encrypted_credential_ref: str | None = None


class InstanceResponse(BaseModel):
    """Resposta de leitura de instancia — inclui campos detectados no health-check."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    db_type: str
    host: str
    port: int
    cloud_provider: str | None
    engine_version: str | None
    engine_version_major: int | None
    engine_support_tier: str | None
    criticality: str
    polling_interval_seconds: int
    auth_method: str
    active: bool
    created_at: datetime
    updated_at: datetime


class InstanceHealthCheckResponse(BaseModel):
    """Resposta do health-check de capacidade — `Docs/rules/04-engine-capabilities.md`."""

    instance_id: UUID
    overall_status: str  # CapabilityStatus: full | degraded | unavailable
    capabilities: list[dict]


class ConnectionTestResponse(BaseModel):
    """Resposta do teste de conexao pontual, sem persistir a instancia."""

    success: bool
    detail: str | None = None


@router.post("", response_model=InstanceResponse, status_code=status.HTTP_201_CREATED)
async def create_instance(payload: InstanceCreateRequest) -> InstanceResponse:
    """Cadastra uma nova instancia monitorada. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Cadastro de instancia ainda nao implementado.",
    )


@router.get("", response_model=list[InstanceResponse])
async def list_instances() -> list[InstanceResponse]:
    """Lista instancias cadastradas. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Listagem de instancias ainda nao implementada.",
    )


@router.get("/{instance_id}", response_model=InstanceResponse)
async def get_instance(instance_id: UUID) -> InstanceResponse:
    """Detalhe de uma instancia. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Leitura de instancia ainda nao implementada.",
    )


@router.post("/test-connection", response_model=ConnectionTestResponse)
async def test_connection(payload: InstanceCreateRequest) -> ConnectionTestResponse:
    """Testa a conexao com uma instancia antes de cadastrar. Ainda nao implementado."""
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Teste de conexao ainda nao implementado.",
    )


@router.get("/{instance_id}/health-check", response_model=InstanceHealthCheckResponse)
async def health_check_instance(instance_id: UUID) -> InstanceHealthCheckResponse:
    """Health-check de capacidade de uma instancia ja cadastrada. Ainda nao implementado.

    Nota: este e o health-check *de instancia monitorada* (capacidade completa/limitada/
    indisponivel). O smoke test do backend em si e `GET /api/v1/health`
    (`app/api/v1/router.py`), que responde 200 OK de verdade.
    """
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail="Health-check de instancia ainda nao implementado.",
    )
