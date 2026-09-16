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
from app.repositories.alerts_repository import AlertsRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.security.credential_reference import CredentialResolver
from app.services.exceptions import InstanceNotFoundError
from app.services.health_check_service import HealthCheckService
from app.services.instance_derived_status_service import (
    UNKNOWN_STATUS,
    InstanceDerivedStatusService,
)
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


class CapabilityErrorResponse(BaseModel):
    """Erro de capacidade ocorrido durante a coleta — sempre `[]` neste MVP (TODO: sem segunda
    fonte de dado ainda, ver `app.services.instance_derived_status_service`)."""

    capability: str
    message: str
    occurred_at: datetime


class InstanceDetailResponse(InstanceResponse):
    """Detalhe de instancia com status derivado da telemetria de ciclo do collector."""

    health_status: str
    collection_coverage: str
    capability_errors: list[CapabilityErrorResponse] = Field(default_factory=list)
    last_collected_at: datetime | None


class InstanceSummaryResponse(BaseModel):
    """Linha resumida de instancia para listagem — status derivado, sem os campos completos de
    `InstanceResponse`."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    db_type: str
    host: str
    port: int
    engine_version: str | None  # detectado no ciclo de coleta mais recente (PollingWorker)
    # Aditivo — permite o frontend traduzir pra ano de marketing (ex.: SQL Server 2019) na
    # listagem, sem precisar de round-trip pro detalhe da instancia.
    engine_version_major: int | None
    criticality: str
    # Aditivo — tela de Configurações (`/settings`) lista todas as instâncias com o intervalo
    # atual, sem round-trip por instância pro detalhe completo.
    polling_interval_seconds: int
    health_status: str
    collection_coverage: str
    active: bool
    open_alert_count: (
        int  # alertas com state != 'closed' (AlertsRepository.count_open_alerts_by_target)
    )
    last_collected_at: datetime | None


class InstanceActiveUpdateRequest(BaseModel):
    """Corpo de `PATCH /instances/{id}/active` — liga/desliga monitoramento sem apagar cadastro."""

    active: bool


class InstanceSettingsUpdateRequest(BaseModel):
    """Corpo de `PATCH /instances/{id}/settings` — tela de Configurações (ainda sem restrição de
    acesso; será limitada por RBAC quando essa feature existir, Fase 2). Todo campo é opcional —
    o cliente manda só o que quer mudar (sub-tela "Intervalo de Coleta" manda só
    `polling_interval_seconds`; sub-tela "Geral" manda só `criticality`); pelo menos um precisa
    vir preenchido."""

    polling_interval_seconds: int | None = Field(default=None, ge=1)
    criticality: str | None = Field(default=None, pattern="^(critical|standard)$")

    @model_validator(mode="after")
    def _require_at_least_one_field(self) -> InstanceSettingsUpdateRequest:
        if self.polling_interval_seconds is None and self.criticality is None:
            raise ValueError("informe ao menos um campo para atualizar")
        return self


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


def get_alerts_repository_for_summary(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AlertsRepository:
    return AlertsRepository(session)


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


def get_metric_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MetricSamplesRepository:
    return MetricSamplesRepository(session)


def get_instance_derived_status_service(
    repository: Annotated[MetricSamplesRepository, Depends(get_metric_samples_repository)],
) -> InstanceDerivedStatusService:
    return InstanceDerivedStatusService(repository)


# --- Endpoints -----------------------------------------------------------------------------


@router.post("", response_model=InstanceDetailResponse, status_code=status.HTTP_201_CREATED)
async def create_instance(
    payload: InstanceCreateRequest,
    service: Annotated[InstanceService, Depends(get_instance_service)],
) -> InstanceDetailResponse:
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
    # Instancia recem-criada nunca tem telemetria ainda — usa `UNKNOWN_STATUS` direto, sem
    # `InstanceDerivedStatusService`/sem Depends novo neste endpoint especifico. Isso evita
    # exigir `MetricSamplesRepository` no fluxo de criacao, o que quebraria
    # `test_instances_registration.py` (usa repositorio fake via `dependency_overrides`, so para
    # `InstancesRepository`).
    return InstanceDetailResponse(
        **InstanceResponse.model_validate(instance).model_dump(),
        health_status=UNKNOWN_STATUS.health_status,
        collection_coverage=UNKNOWN_STATUS.collection_coverage,
        capability_errors=[],
        last_collected_at=None,
    )


@router.get("", response_model=list[InstanceSummaryResponse])
async def list_instances(
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    alerts_repository: Annotated[AlertsRepository, Depends(get_alerts_repository_for_summary)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
) -> list[InstanceSummaryResponse]:
    """Lista todas as instancias cadastradas (ativas e pausadas), com status derivado da
    telemetria de ciclo.

    Usa `list_all()`, nao `list_active()` — uma instancia pausada (`active=False`) precisa
    continuar aparecendo aqui pra poder ser reativada pela UI; so os workers/supervisors de
    coleta e que devem ignorar instancias pausadas (via `list_active()`, sem mudanca).

    N+1 aceitavel no MVP (poucas instancias esperadas) — uma consulta de status derivado por
    instancia da lista, em vez de uma unica query agregada. `open_alert_count` ja e uma unica
    query agregada pra todas as instancias (`count_open_alerts_by_target`), sem N+1.
    """
    instances = await repository.list_all()
    open_alert_counts = await alerts_repository.count_open_alerts_by_target(
        [instance.id for instance in instances]
    )
    summaries = []
    for instance in instances:
        derived = await status_service.compute(instance.id)
        summaries.append(
            InstanceSummaryResponse(
                id=instance.id,
                name=instance.name,
                db_type=instance.db_type,
                host=instance.host,
                port=instance.port,
                engine_version=instance.engine_version,
                engine_version_major=instance.engine_version_major,
                criticality=instance.criticality,
                polling_interval_seconds=instance.polling_interval_seconds,
                health_status=derived.health_status,
                collection_coverage=derived.collection_coverage,
                active=instance.active,
                open_alert_count=open_alert_counts.get(instance.id, 0),
                last_collected_at=derived.last_collected_at,
            )
        )
    return summaries


@router.delete("/{instance_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_instance(
    instance_id: UUID,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
) -> None:
    """Exclui o cadastro da instância e seus dados dependentes."""
    deleted = await repository.delete(instance_id)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )


@router.get("/{instance_id}", response_model=InstanceDetailResponse)
async def get_instance(
    instance_id: UUID,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
) -> InstanceDetailResponse:
    """Detalhe de uma instancia cadastrada, com status derivado da telemetria de ciclo."""
    instance = await repository.get_by_id(instance_id)
    if instance is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )
    derived = await status_service.compute(instance_id)
    return InstanceDetailResponse(
        **InstanceResponse.model_validate(instance).model_dump(),
        health_status=derived.health_status,
        collection_coverage=derived.collection_coverage,
        capability_errors=[],
        last_collected_at=derived.last_collected_at,
    )


@router.patch("/{instance_id}/active", response_model=InstanceDetailResponse)
async def set_instance_active(
    instance_id: UUID,
    payload: InstanceActiveUpdateRequest,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
) -> InstanceDetailResponse:
    """Liga/desliga o monitoramento de uma instancia (equivalente ao botao ON/OFF do SolarWinds)
    sem apagar o cadastro. Efeito real: `PollingSupervisor`/`MissingIndexSupervisor`/
    `BackupStatusSupervisor`/`AlertingWorker` re-leem `instances.active` a cada reconciliacao
    (ver `Docs/collector-queries.md`/supervisors) — a instancia pausada para de ser coletada e
    avaliada na proxima janela de reconciliacao, sem exigir restart do backend."""
    instance = await repository.set_active(instance_id, payload.active)
    if instance is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )
    derived = await status_service.compute(instance_id)
    return InstanceDetailResponse(
        **InstanceResponse.model_validate(instance).model_dump(),
        health_status=derived.health_status,
        collection_coverage=derived.collection_coverage,
        capability_errors=[],
        last_collected_at=derived.last_collected_at,
    )


@router.patch("/{instance_id}/settings", response_model=InstanceDetailResponse)
async def update_instance_settings(
    instance_id: UUID,
    payload: InstanceSettingsUpdateRequest,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
) -> InstanceDetailResponse:
    """Tela de Configurações — atualiza qualquer combinação dos campos informados, sem
    restrição de acesso hoje (será limitada por RBAC quando essa feature existir, Fase 2).
    `PollingSupervisor` reconcilia instâncias a cada `supervisor_discovery_interval_seconds` — um
    novo intervalo passa a valer sozinho na próxima reconciliação, sem restart do backend."""
    instance = await repository.update_settings(
        instance_id,
        polling_interval_seconds=payload.polling_interval_seconds,
        criticality=payload.criticality,
    )
    if instance is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )
    derived = await status_service.compute(instance_id)
    return InstanceDetailResponse(
        **InstanceResponse.model_validate(instance).model_dump(),
        health_status=derived.health_status,
        collection_coverage=derived.collection_coverage,
        capability_errors=[],
        last_collected_at=derived.last_collected_at,
    )


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
