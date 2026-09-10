"""Endpoints de cadastro, teste de conexao e health-check de instancia.

Contrato: `Docs/specs/02-backend-contract.md`, campos conforme `Docs/database-schema.md` §1.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.collectors.base import CapabilityResult
from app.collectors.capabilities import has_missing_mandatory_capability
from app.repositories.instances_repository import InstancesRepository
from app.security.credential_reference import CredentialResolver
from app.services.exceptions import InstanceNotFoundError
from app.services.health_check_service import HealthCheckService
from app.services.instance_service import (
    ConnectionTestInput,
    InstanceRegistrationRejected,
    InstanceService,
    RegisterInstanceInput,
)

router = APIRouter()


class InstanceCreateRequest(BaseModel):
    """Corpo do cadastro de instancia — campos conforme `Docs/database-schema.md` §1.

    Nunca aceita `encrypted_credential_ref` como input: a referencia cifrada e derivada
    internamente (`app.security.credential_encryption.encrypt_credential`) a partir de
    `username`/`password`, nunca informada pelo cliente. `password` so existe em memoria pelo
    tempo do teste de conexao/cifragem (Docs/rules/01-credential-security.md).
    """

    name: str
    db_type: str = Field(pattern="^(postgresql|sqlserver)$")
    host: str
    port: int
    cloud_provider: str | None = Field(default=None, pattern="^(aws_rds|azure|gcp)$")
    criticality: str = Field(default="standard", pattern="^(critical|standard)$")
    polling_interval_seconds: int = Field(default=10, ge=1)
    auth_method: str = Field(pattern="^(integrated|encrypted_fallback)$")
    username: str | None = None
    password: SecretStr | None = None

    @model_validator(mode="after")
    def _validate_credential_matches_auth_method(self) -> InstanceCreateRequest:
        if self.auth_method == "encrypted_fallback" and self.password is None:
            raise ValueError("password e obrigatorio quando auth_method='encrypted_fallback'")
        if self.auth_method == "integrated" and self.password is not None:
            raise ValueError("password nao deve ser informado quando auth_method='integrated'")
        return self


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
    capabilities: list[dict] = Field(default_factory=list)


def _capabilities_to_dicts(capabilities: list[CapabilityResult]) -> list[dict]:
    return [
        {"capability": c.capability, "status": c.status.value, "reason": c.reason}
        for c in capabilities
    ]


# --- Dependency providers -----------------------------------------------------------------
# Especificas de instancias (nao entram em `app/api/deps.py`, que fica generico a
# sessao/paginacao entre todos os routers).


def get_instances_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstancesRepository:
    return InstancesRepository(session)


def get_credential_resolver() -> CredentialResolver:
    return CredentialResolver()


def get_instance_service(
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
) -> InstanceService:
    return InstanceService(repository)


def get_health_check_service(
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    credential_resolver: Annotated[CredentialResolver, Depends(get_credential_resolver)],
) -> HealthCheckService:
    return HealthCheckService(repository, credential_resolver)


# --- Endpoints -----------------------------------------------------------------------------


@router.post("", response_model=InstanceResponse, status_code=status.HTTP_201_CREATED)
async def create_instance(
    payload: InstanceCreateRequest,
    service: Annotated[InstanceService, Depends(get_instance_service)],
) -> InstanceResponse:
    """Testa a conexao e, se as capacidades mandatorias estiverem OK, cadastra a instancia."""
    password = payload.password.get_secret_value() if payload.password is not None else None
    register_input = RegisterInstanceInput(
        name=payload.name,
        db_type=payload.db_type,
        host=payload.host,
        port=payload.port,
        auth_method=payload.auth_method,
        cloud_provider=payload.cloud_provider,
        criticality=payload.criticality,
        polling_interval_seconds=payload.polling_interval_seconds,
        username=payload.username,
        password=password,
    )
    try:
        instance = await service.register_instance(register_input)
    except InstanceRegistrationRejected as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "Cadastro reprovado: capacidade mandatoria indisponivel.",
                "capabilities": _capabilities_to_dicts(exc.capabilities),
            },
        ) from exc
    return InstanceResponse.model_validate(instance)


@router.get("", response_model=list[InstanceResponse])
async def list_instances(
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
) -> list[InstanceResponse]:
    """Lista instancias ativas cadastradas."""
    instances = await repository.list_active()
    return [InstanceResponse.model_validate(instance) for instance in instances]


@router.get("/{instance_id}", response_model=InstanceResponse)
async def get_instance(
    instance_id: UUID,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
) -> InstanceResponse:
    """Detalhe de uma instancia cadastrada."""
    instance = await repository.get_by_id(instance_id)
    if instance is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )
    return InstanceResponse.model_validate(instance)


@router.post("/test-connection", response_model=ConnectionTestResponse)
async def test_connection(
    payload: InstanceCreateRequest,
    service: Annotated[InstanceService, Depends(get_instance_service)],
) -> ConnectionTestResponse:
    """Testa a conexao com uma instancia antes de cadastrar — nao persiste nada."""
    password = payload.password.get_secret_value() if payload.password is not None else None
    capabilities = await service.test_connection(
        ConnectionTestInput(
            db_type=payload.db_type,
            host=payload.host,
            port=payload.port,
            auth_method=payload.auth_method,
            username=payload.username,
            password=password,
        )
    )
    success = not has_missing_mandatory_capability(payload.db_type, capabilities)
    return ConnectionTestResponse(
        success=success, detail=None, capabilities=_capabilities_to_dicts(capabilities)
    )


@router.get("/{instance_id}/health-check", response_model=InstanceHealthCheckResponse)
async def health_check_instance(
    instance_id: UUID,
    health_check_service: Annotated[HealthCheckService, Depends(get_health_check_service)],
) -> InstanceHealthCheckResponse:
    """Health-check de capacidade de uma instancia ja cadastrada.

    Nota: este e o health-check *de instancia monitorada* (capacidade completa/limitada/
    indisponivel). O smoke test do backend em si e `GET /api/v1/health`
    (`app/api/v1/router.py`), que responde 200 OK de verdade.
    """
    try:
        capabilities = await health_check_service.check_instance(instance_id)
    except InstanceNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    overall = HealthCheckService.overall_status(capabilities)
    return InstanceHealthCheckResponse(
        instance_id=instance_id,
        overall_status=overall.value,
        capabilities=_capabilities_to_dicts(capabilities),
    )
