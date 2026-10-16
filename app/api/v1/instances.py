"""Endpoints de cadastro, teste de conexao e health-check de instancia.

Contrato: `Docs/specs/02-backend-contract.md`, campos conforme `Docs/database-schema.md` §1.
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, ConfigDict, Field, SecretStr, model_validator
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import (
    CurrentUser,
    check_instance_access,
    get_current_user,
    get_db_session,
    require_admin,
)
from app.collectors.base import CapabilityResult
from app.collectors.capabilities import has_missing_mandatory_capability
from app.models.instance_database_lock_override import InstanceDatabaseLockOverride
from app.models.instance_database_sleeping_override import InstanceDatabaseSleepingOverride
from app.repositories.alerts_repository import AlertsRepository
from app.repositories.instance_database_lock_overrides_repository import (
    InstanceDatabaseLockOverridesRepository,
)
from app.repositories.instance_database_sleeping_overrides_repository import (
    InstanceDatabaseSleepingOverridesRepository,
)
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.repositories.user_instance_access_repository import UserInstanceAccessRepository
from app.security.credential_reference import CredentialResolver
from app.services.audit_service import record_audit
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
    db_type: str = Field(pattern="^(postgresql|sqlserver|mysql|cockroachdb|aurora_postgresql|aurora_mysql|oracle|db2|redshift)$")
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
    min_lock_time_ms: int | None
    min_sleeping_time_ms: int | None
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
    # Aditivo — sub-tela "Tempo mínimo de bloqueio" (override ad-hoc por instância). `None` =
    # sem override, usa o valor global (`system_settings.blocking_min_duration_ms`).
    min_lock_time_ms: int | None
    # Mesmo padrão, para "Transações em Sleeping" — `None` usa
    # `system_settings.sleeping_min_duration_ms`.
    min_sleeping_time_ms: int | None
    health_status: str
    collection_coverage: str
    active: bool
    open_alert_count: (
        int  # alertas com state != 'closed' (AlertsRepository.count_open_alerts_by_target)
    )
    worst_alert_severity: str | None  # 'critical' | 'warning' | None (sem alerta aberto)
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

    name: str | None = Field(default=None, min_length=1)
    polling_interval_seconds: int | None = Field(default=None, ge=1)
    criticality: str | None = Field(default=None, pattern="^(critical|standard)$")

    @model_validator(mode="after")
    def _require_at_least_one_field(self) -> InstanceSettingsUpdateRequest:
        if self.name is None and self.polling_interval_seconds is None and self.criticality is None:
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


def get_instance_database_lock_overrides_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstanceDatabaseLockOverridesRepository:
    return InstanceDatabaseLockOverridesRepository(session)


def get_instance_database_sleeping_overrides_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstanceDatabaseSleepingOverridesRepository:
    return InstanceDatabaseSleepingOverridesRepository(session)


# --- Endpoints -----------------------------------------------------------------------------


@router.post("", response_model=InstanceDetailResponse, status_code=status.HTTP_201_CREATED)
async def create_instance(
    request: Request,
    payload: InstanceCreateRequest,
    service: Annotated[InstanceService, Depends(get_instance_service)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
) -> InstanceDetailResponse:
    """Testa a conexao e, se as capacidades mandatorias estiverem OK, cadastra a instancia."""
    ip = request.client.host if request.client else None
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
        await record_audit(
            session,
            actor=current_admin.username,
            action="instance.create_failed",
            ip_address=ip,
            target_type="instance",
            target_name=payload.name,
            detail={"db_type": payload.db_type, "host": payload.host, "port": payload.port},
        )
        await session.commit()
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail={
                "message": "Cadastro reprovado: capacidade mandatoria indisponivel.",
                "capabilities": _capabilities_to_dicts(exc.capabilities),
            },
        ) from exc
    await record_audit(
        session,
        actor=current_admin.username,
        action="instance.create",
        ip_address=ip,
        target_type="instance",
        target_id=str(instance.id),
        target_name=instance.name,
        detail={"db_type": instance.db_type, "host": instance.host, "port": instance.port},
    )
    await session.commit()
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
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> list[InstanceSummaryResponse]:
    """Lista todas as instancias cadastradas (ativas e pausadas), com status derivado da
    telemetria de ciclo.

    Usa `list_all()`, nao `list_active()` — uma instancia pausada (`active=False`) precisa
    continuar aparecendo aqui pra poder ser reativada pela UI; so os workers/supervisors de
    coleta e que devem ignorar instancias pausadas (via `list_active()`, sem mudanca).

    N+1 aceitavel no MVP (poucas instancias esperadas) — uma consulta de status derivado por
    instancia da lista, em vez de uma unica query agregada. `open_alert_count` ja e uma unica
    query agregada pra todas as instancias (`count_open_alerts_by_target`), sem N+1.

    Filtro RBAC por recurso: admins enxergam todas as instancias; operator/viewer so enxergam
    as instancias explicitamente concedidas via `user_instance_access`.
    """
    instances = await repository.list_all()

    if not current_user.is_admin:
        access_repo = UserInstanceAccessRepository(session)
        accessible_ids = await access_repo.get_instance_ids_for_user(current_user.user_id)
        instances = [i for i in instances if i.id in accessible_ids]

    instance_ids = [instance.id for instance in instances]
    open_alert_counts = await alerts_repository.count_open_alerts_by_target(instance_ids)
    worst_severities = await alerts_repository.get_worst_severity_by_instance_ids(instance_ids)
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
                min_lock_time_ms=instance.min_lock_time_ms,
                min_sleeping_time_ms=instance.min_sleeping_time_ms,
                health_status=derived.health_status,
                collection_coverage=derived.collection_coverage,
                active=instance.active,
                open_alert_count=open_alert_counts.get(instance.id, 0),
                worst_alert_severity=worst_severities.get(instance.id),
                last_collected_at=derived.last_collected_at,
            )
        )
    return summaries


@router.delete("/{instance_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_instance(
    request: Request,
    instance_id: UUID,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
) -> None:
    """Exclui o cadastro da instância e seus dados dependentes."""
    instance = await repository.get_by_id(instance_id)
    if instance is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )
    instance_name = instance.name
    await repository.delete(instance_id)
    await record_audit(
        session,
        actor=current_admin.username,
        action="instance.delete",
        ip_address=request.client.host if request.client else None,
        target_type="instance",
        target_id=str(instance_id),
        target_name=instance_name,
    )
    await session.commit()


@router.get("/{instance_id}", response_model=InstanceDetailResponse)
async def get_instance(
    instance_id: UUID,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
    _: Annotated[CurrentUser, Depends(get_current_user)],
    _access: Annotated[None, Depends(check_instance_access)],
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
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstanceDetailResponse:
    """Liga/desliga o monitoramento de uma instancia (equivalente ao botao ON/OFF de monitoramento)
    sem apagar o cadastro. Efeito real: `PollingSupervisor`/`MissingIndexSupervisor`/
    `BackupStatusSupervisor`/`AlertingWorker` re-leem `instances.active` a cada reconciliacao
    (ver `Docs/collector-queries.md`/supervisors) — a instancia pausada para de ser coletada e
    avaliada na proxima janela de reconciliacao, sem exigir restart do backend."""
    instance = await repository.set_active(instance_id, payload.active)
    if instance is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )
    await record_audit(
        session,
        actor=current_admin.username,
        action="instance.monitoring_on" if payload.active else "instance.monitoring_off",
        target_type="instance",
        target_id=str(instance_id),
        target_name=instance.name,
    )
    await session.commit()
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
    request: Request,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
) -> InstanceDetailResponse:
    """Tela de Configurações — atualiza qualquer combinação dos campos informados, sem
    restrição de acesso hoje (será limitada por RBAC quando essa feature existir, Fase 2).
    `PollingSupervisor` reconcilia instâncias a cada `supervisor_discovery_interval_seconds` — um
    novo intervalo passa a valer sozinho na próxima reconciliação, sem restart do backend."""
    instance = await repository.update_settings(
        instance_id,
        name=payload.name,
        polling_interval_seconds=payload.polling_interval_seconds,
        criticality=payload.criticality,
    )
    if instance is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )
    changed: dict[str, object] = {}
    if payload.name is not None:
        changed["name"] = payload.name
    if payload.polling_interval_seconds is not None:
        changed["polling_interval_seconds"] = payload.polling_interval_seconds
    if payload.criticality is not None:
        changed["criticality"] = payload.criticality
    await record_audit(
        session,
        actor=current_admin.username,
        action="instance.settings_changed",
        target_type="instance",
        target_id=str(instance_id),
        target_name=instance.name,
        ip_address=request.client.host if request.client else None,
        detail={"changed": changed},
    )
    await session.commit()
    derived = await status_service.compute(instance_id)
    return InstanceDetailResponse(
        **InstanceResponse.model_validate(instance).model_dump(),
        health_status=derived.health_status,
        collection_coverage=derived.collection_coverage,
        capability_errors=[],
        last_collected_at=derived.last_collected_at,
    )


class InstanceMinLockTimeUpdateRequest(BaseModel):
    """Corpo de `PATCH /instances/{id}/min-lock-time` — override AD-HOC do "Tempo mínimo para
    captura de Bloqueios" só para esta instância (a pedido explícito do usuário: investigar um
    caso específico com um cliente sem afetar as demais instâncias monitoradas).

    Endpoint dedicado (não mais um parâmetro de `InstanceSettingsUpdateRequest`) porque aqui
    `min_lock_time_ms=null` tem significado real e distinto de "campo não enviado": limpa o
    override, voltando a usar o valor global. O campo é OBRIGATÓRIO (sem default) justamente
    para forçar o cliente a mandar `null` explicitamente quando quiser limpar, em vez de
    simplesmente omitir a chave (que teria o mesmo efeito ambíguo de "não toque neste campo" do
    outro endpoint)."""

    min_lock_time_ms: int | None = Field(ge=0)


@router.patch("/{instance_id}/min-lock-time", response_model=InstanceDetailResponse)
async def update_instance_min_lock_time(
    instance_id: UUID,
    payload: InstanceMinLockTimeUpdateRequest,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> InstanceDetailResponse:
    """`min_lock_time_ms=null` limpa o override (volta a usar o valor global de
    `system_settings.blocking_min_duration_ms`); um número define o override ad-hoc. Lido a cada
    ciclo por `PollingWorker` (não cacheado) — vale a partir do próximo ciclo de poll, sem
    restart do backend."""
    instance = await repository.set_min_lock_time_ms(instance_id, payload.min_lock_time_ms)
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


class InstanceDatabaseLockOverrideResponse(BaseModel):
    """Um override ad-hoc de "Tempo mínimo para captura de Bloqueios" por BANCO DE DADOS dentro
    de uma instância — mais granular que `min_lock_time_ms` (que é por instância inteira)."""

    model_config = ConfigDict(from_attributes=True)

    database_name: str
    min_lock_time_ms: int


class InstanceDatabaseLockOverrideUpsertRequest(BaseModel):
    """Corpo de `PUT /instances/{id}/database-lock-time-overrides` — cria ou atualiza o override
    de um banco específico. Diferente do override por instância, aqui não existe "limpar
    mandando null": limpar é `DELETE /instances/{id}/database-lock-time-overrides/{database_name}`
    (a linha simplesmente não existe mais, em vez de existir com um valor nulo)."""

    database_name: str = Field(min_length=1)
    min_lock_time_ms: int = Field(ge=0)


class LockTimeOverridesResetResponse(BaseModel):
    """Resposta do reset em massa (`DELETE /instances/lock-time-overrides/reset`) — só pra
    telemetria/confirmação visual na UI, nenhum consumidor depende do valor exato."""

    instances_cleared: int
    database_overrides_cleared: int


@router.get(
    "/{instance_id}/database-lock-time-overrides",
    response_model=list[InstanceDatabaseLockOverrideResponse],
)
async def list_instance_database_lock_overrides(
    instance_id: UUID,
    repository: Annotated[
        InstanceDatabaseLockOverridesRepository,
        Depends(get_instance_database_lock_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(get_current_user)],
) -> list[InstanceDatabaseLockOverrideResponse]:
    overrides = await repository.list_for_instance(instance_id)
    return [InstanceDatabaseLockOverrideResponse.model_validate(o) for o in overrides]


@router.put(
    "/{instance_id}/database-lock-time-overrides",
    response_model=InstanceDatabaseLockOverrideResponse,
)
async def set_instance_database_lock_override(
    instance_id: UUID,
    payload: InstanceDatabaseLockOverrideUpsertRequest,
    repository: Annotated[
        InstanceDatabaseLockOverridesRepository,
        Depends(get_instance_database_lock_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> InstanceDatabaseLockOverrideResponse:
    """Cria ou atualiza o override ad-hoc de um banco específico — lido a cada ciclo por
    `PollingWorker` (não cacheado), vale a partir do próximo ciclo de poll dessa instância, sem
    restart do backend. Não valida que `instance_id` existe/que `database_name` já apareceu em
    algum dado coletado — a FK cuida da integridade referencial (404 implícito via violação de
    FK se a instância não existir; a UI só oferece bancos já vistos, então isso não deveria
    acontecer no fluxo normal)."""
    override: InstanceDatabaseLockOverride = await repository.set(
        instance_id, payload.database_name, payload.min_lock_time_ms
    )
    return InstanceDatabaseLockOverrideResponse.model_validate(override)


@router.delete(
    "/{instance_id}/database-lock-time-overrides/{database_name}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def clear_instance_database_lock_override(
    instance_id: UUID,
    database_name: str,
    repository: Annotated[
        InstanceDatabaseLockOverridesRepository,
        Depends(get_instance_database_lock_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> None:
    cleared = await repository.clear(instance_id, database_name)
    if not cleared:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Nenhum override encontrado para esse banco nessa instância.",
        )


@router.delete("/lock-time-overrides/reset", response_model=LockTimeOverridesResetResponse)
async def reset_all_lock_time_overrides(
    instances_repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    database_overrides_repository: Annotated[
        InstanceDatabaseLockOverridesRepository,
        Depends(get_instance_database_lock_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> LockTimeOverridesResetResponse:
    """Reset em massa — usado quando o usuário desmarca "Limites ad hoc" na UI (Configurações →
    Geral): apaga TODO override ad-hoc (por instância E por banco de dados) de TODAS as
    instâncias, não só a que estava selecionada no momento. A partir daí, toda instância volta a
    usar o valor global (`system_settings.blocking_min_duration_ms`)."""
    instances_cleared = await instances_repository.clear_all_min_lock_time_overrides()
    database_overrides_cleared = await database_overrides_repository.clear_all()
    return LockTimeOverridesResetResponse(
        instances_cleared=instances_cleared,
        database_overrides_cleared=database_overrides_cleared,
    )


class InstanceMinSleepingTimeUpdateRequest(BaseModel):
    """Mesmo padrão de `InstanceMinLockTimeUpdateRequest`, para "Transações em Sleeping"."""

    min_sleeping_time_ms: int | None = Field(ge=0)


@router.patch("/{instance_id}/min-sleeping-time", response_model=InstanceDetailResponse)
async def update_instance_min_sleeping_time(
    instance_id: UUID,
    payload: InstanceMinSleepingTimeUpdateRequest,
    repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> InstanceDetailResponse:
    instance = await repository.set_min_sleeping_time_ms(instance_id, payload.min_sleeping_time_ms)
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


class InstanceDatabaseSleepingOverrideResponse(BaseModel):
    """Mesmo padrão de `InstanceDatabaseLockOverrideResponse`, para "Transações em Sleeping"."""

    model_config = ConfigDict(from_attributes=True)

    database_name: str
    min_sleeping_time_ms: int


class InstanceDatabaseSleepingOverrideUpsertRequest(BaseModel):
    database_name: str = Field(min_length=1)
    min_sleeping_time_ms: int = Field(ge=0)


class SleepingTimeOverridesResetResponse(BaseModel):
    instances_cleared: int
    database_overrides_cleared: int


@router.get(
    "/{instance_id}/database-sleeping-time-overrides",
    response_model=list[InstanceDatabaseSleepingOverrideResponse],
)
async def list_instance_database_sleeping_overrides(
    instance_id: UUID,
    repository: Annotated[
        InstanceDatabaseSleepingOverridesRepository,
        Depends(get_instance_database_sleeping_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(get_current_user)],
) -> list[InstanceDatabaseSleepingOverrideResponse]:
    overrides = await repository.list_for_instance(instance_id)
    return [InstanceDatabaseSleepingOverrideResponse.model_validate(o) for o in overrides]


@router.put(
    "/{instance_id}/database-sleeping-time-overrides",
    response_model=InstanceDatabaseSleepingOverrideResponse,
)
async def set_instance_database_sleeping_override(
    instance_id: UUID,
    payload: InstanceDatabaseSleepingOverrideUpsertRequest,
    repository: Annotated[
        InstanceDatabaseSleepingOverridesRepository,
        Depends(get_instance_database_sleeping_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> InstanceDatabaseSleepingOverrideResponse:
    override: InstanceDatabaseSleepingOverride = await repository.set(
        instance_id, payload.database_name, payload.min_sleeping_time_ms
    )
    return InstanceDatabaseSleepingOverrideResponse.model_validate(override)


@router.delete(
    "/{instance_id}/database-sleeping-time-overrides/{database_name}",
    status_code=status.HTTP_204_NO_CONTENT,
)
async def clear_instance_database_sleeping_override(
    instance_id: UUID,
    database_name: str,
    repository: Annotated[
        InstanceDatabaseSleepingOverridesRepository,
        Depends(get_instance_database_sleeping_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> None:
    cleared = await repository.clear(instance_id, database_name)
    if not cleared:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Nenhum override encontrado para esse banco nessa instância.",
        )


@router.delete("/sleeping-time-overrides/reset", response_model=SleepingTimeOverridesResetResponse)
async def reset_all_sleeping_time_overrides(
    instances_repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    database_overrides_repository: Annotated[
        InstanceDatabaseSleepingOverridesRepository,
        Depends(get_instance_database_sleeping_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(require_admin)],
) -> SleepingTimeOverridesResetResponse:
    instances_cleared = await instances_repository.clear_all_min_sleeping_time_overrides()
    database_overrides_cleared = await database_overrides_repository.clear_all()
    return SleepingTimeOverridesResetResponse(
        instances_cleared=instances_cleared,
        database_overrides_cleared=database_overrides_cleared,
    )


class AdHocOverrideInstanceEntry(BaseModel):
    """Uma instância com override ATIVO de um parâmetro ad-hoc (bloqueio ou sleeping)."""

    instance_id: UUID
    instance_name: str
    min_duration_ms: int


class AdHocOverrideDatabaseEntry(BaseModel):
    """Um banco específico com override ATIVO de um parâmetro ad-hoc."""

    instance_id: UUID
    instance_name: str
    database_name: str
    min_duration_ms: int


class AdHocOverrideGroup(BaseModel):
    instance_overrides: list[AdHocOverrideInstanceEntry]
    database_overrides: list[AdHocOverrideDatabaseEntry]


class AdHocOverridesSummaryResponse(BaseModel):
    """Resumo de TODOS os overrides ad-hoc ativos (bloqueio e sleeping, todas as instâncias) —
    pedido explícito do usuário: "não tem como descobrir quais servidores/BDs já tinham tempos
    ad hoc configurados". Usado pela UI (Configurações → Geral) tanto para exibir a lista quanto
    para inicializar corretamente o checkbox "Limites ad hoc" (que antes sempre resetava pra
    desmarcado ao remontar, mesmo com overrides ativos no backend)."""

    blocking: AdHocOverrideGroup
    sleeping: AdHocOverrideGroup


@router.get("/ad-hoc-overrides/summary", response_model=AdHocOverridesSummaryResponse)
async def get_ad_hoc_overrides_summary(
    instances_repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    lock_overrides_repository: Annotated[
        InstanceDatabaseLockOverridesRepository,
        Depends(get_instance_database_lock_overrides_repository),
    ],
    sleeping_overrides_repository: Annotated[
        InstanceDatabaseSleepingOverridesRepository,
        Depends(get_instance_database_sleeping_overrides_repository),
    ],
    _: Annotated[CurrentUser, Depends(get_current_user)],
) -> AdHocOverridesSummaryResponse:
    instances = await instances_repository.list_all()
    instance_name_by_id = {instance.id: instance.name for instance in instances}

    blocking_instance_overrides = [
        AdHocOverrideInstanceEntry(
            instance_id=instance.id,
            instance_name=instance.name,
            min_duration_ms=instance.min_lock_time_ms,
        )
        for instance in instances
        if instance.min_lock_time_ms is not None
    ]
    sleeping_instance_overrides = [
        AdHocOverrideInstanceEntry(
            instance_id=instance.id,
            instance_name=instance.name,
            min_duration_ms=instance.min_sleeping_time_ms,
        )
        for instance in instances
        if instance.min_sleeping_time_ms is not None
    ]
    lock_database_overrides = [
        AdHocOverrideDatabaseEntry(
            instance_id=override.instance_id,
            instance_name=instance_name_by_id.get(override.instance_id, "desconhecida"),
            database_name=override.database_name,
            min_duration_ms=override.min_lock_time_ms,
        )
        for override in await lock_overrides_repository.list_all()
    ]
    sleeping_database_overrides = [
        AdHocOverrideDatabaseEntry(
            instance_id=override.instance_id,
            instance_name=instance_name_by_id.get(override.instance_id, "desconhecida"),
            database_name=override.database_name,
            min_duration_ms=override.min_sleeping_time_ms,
        )
        for override in await sleeping_overrides_repository.list_all()
    ]
    return AdHocOverridesSummaryResponse(
        blocking=AdHocOverrideGroup(
            instance_overrides=blocking_instance_overrides,
            database_overrides=lock_database_overrides,
        ),
        sleeping=AdHocOverrideGroup(
            instance_overrides=sleeping_instance_overrides,
            database_overrides=sleeping_database_overrides,
        ),
    )


@router.post("/test-connection", response_model=ConnectionTestResponse)
async def test_connection(
    payload: InstanceCreateRequest,
    service: Annotated[InstanceService, Depends(get_instance_service)],
    _: Annotated[CurrentUser, Depends(get_current_user)],
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
    _: Annotated[CurrentUser, Depends(get_current_user)],
    _access: Annotated[None, Depends(check_instance_access)],
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
