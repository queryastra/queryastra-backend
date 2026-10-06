"""Endpoint de leitura de sugestoes de indice ausente — ARCHITECTURE_SPEC.md Q18/Q21.

Le direto da tabela relacional `missing_index_suggestions` (estado mais recente por
instancia/banco/tabela/sugestao, atualizado pelo `MissingIndexSupervisor` de baixa frequencia —
`app/workers/missing_index_supervisor.py`), sem janela de tempo (nao e serie temporal bruta como
`query_samples`/`metric_samples`)."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import check_instance_access, get_db_session, require_operator
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.config import get_settings
from app.core.logging import get_logger
from app.repositories.instances_repository import InstancesRepository
from app.repositories.missing_index_collection_runs_repository import (
    MissingIndexCollectionRunsRepository,
)
from app.repositories.missing_index_suggestions_repository import (
    MissingIndexSuggestionsRepository,
)
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

router = APIRouter()


class MissingIndexCollectionInfoResponse(BaseModel):
    """Item 4 (pedido explicito do usuario): quando foi a ultima coleta e desde quando os dados
    de uso acumulados sao validos — ver docstring de `MissingIndexCollectionRun`. `None`/`None`
    quando a instancia ainda nunca foi checada (nao confundir com "checou e nao achou nada", que
    tem `last_collected_at` preenchido e a lista de sugestoes vazia).

    `first_collected_at`: quando o monitoramento desta instancia comecou (independente de ter
    encontrado sugestoes). Cobre o caso em que a instancia nunca teve sugestoes — nesse caso
    `min(first_seen_at)` das sugestoes seria NULL, mas `first_collected_at` fica definido."""

    last_collected_at: datetime | None
    stats_since: datetime | None
    first_collected_at: datetime | None


class MissingIndexSuggestionResponse(BaseModel):
    """Sugestao de indice ausente — `impact_score` NAO e comparavel entre engines (ver docstring
    de `NormalizedMissingIndexSuggestion`)."""

    id: UUID
    instance_id: UUID
    database_name: str | None
    table_name: str
    suggestion: str
    impact_score: float | None
    detail: dict | None
    first_seen_at: datetime
    last_seen_at: datetime

    model_config = {"from_attributes": True}


def get_missing_index_suggestions_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MissingIndexSuggestionsRepository:
    return MissingIndexSuggestionsRepository(session)


def get_missing_index_collection_runs_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MissingIndexCollectionRunsRepository:
    return MissingIndexCollectionRunsRepository(session)


@router.get("/{instance_id}/collection-info", response_model=MissingIndexCollectionInfoResponse)
async def get_missing_index_collection_info(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[
        MissingIndexCollectionRunsRepository,
        Depends(get_missing_index_collection_runs_repository),
    ],
) -> MissingIndexCollectionInfoResponse:
    """Endpoint separado do de listagem (mesmo padrao de outras informacoes auxiliares deste
    projeto) - a tela consulta os dois, sem acoplar um ao outro."""
    row = await repository.get_for_instance(instance_id)
    if row is None:
        return MissingIndexCollectionInfoResponse(
            last_collected_at=None, stats_since=None, first_collected_at=None
        )
    return MissingIndexCollectionInfoResponse(
        last_collected_at=row.collected_at,
        stats_since=row.stats_since,
        first_collected_at=row.first_collected_at,
    )


@router.post(
    "/{instance_id}/collect",
    response_model=MissingIndexCollectionInfoResponse,
    status_code=status.HTTP_200_OK,
)
async def trigger_missing_index_collection(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    _op: Annotated[None, Depends(require_operator)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> MissingIndexCollectionInfoResponse:
    """Dispara uma coleta sob demanda para a instancia e aguarda o resultado.

    Requer papel operator ou admin. Bloqueia ate a coleta terminar (pode levar alguns segundos).
    Retorna o estado atualizado de `collection-info` apos a coleta."""
    instance = await InstancesRepository(session).get_by_id(instance_id)
    if instance is None or not instance.active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instancia nao encontrada ou inativa.",
        )

    username: str | None = None
    secret_value: str | None = None
    if instance.auth_method == "encrypted_fallback":
        if instance.encrypted_credential_ref is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Referencia de credencial ausente na instancia.",
            )
        try:
            resolved = await credential_resolver.resolve(
                CredentialReference(instance.encrypted_credential_ref)
            )
            username, secret_value = resolved.username, resolved.secret_value
        except CredentialResolutionError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Falha ao resolver credencial da instancia.",
            ) from exc

    connection = ConnectionParams(
        host=instance.host,
        port=instance.port,
        auth_method=instance.auth_method,
        username=username,
        secret_value=secret_value,
    )
    collector = create_collector(instance.db_type, connection=connection, instance_id=instance.id)
    try:
        suggestions = await collector.collect_missing_index_suggestions()
        now = datetime.now(UTC)
        if suggestions:
            await MissingIndexSuggestionsRepository(session).upsert_batch(suggestions, now)

        try:
            stats_since = await collector.get_engine_start_time()
        except Exception:  # noqa: BLE001
            logger.warning(
                "Falha ao obter engine_start_time de instance_id=%s na coleta sob demanda",
                instance_id,
            )
            stats_since = None

        runs_repo = MissingIndexCollectionRunsRepository(session)
        await runs_repo.upsert(instance_id, now, stats_since)
        row = await runs_repo.get_for_instance(instance_id)
    except HTTPException:
        raise
    except Exception as exc:
        logger.exception(
            "Falha na coleta sob demanda de missing indexes para instance_id=%s", instance_id
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Falha na coleta: {exc}",
        ) from exc
    finally:
        await collector.close()

    return MissingIndexCollectionInfoResponse(
        last_collected_at=row.collected_at if row else now,
        stats_since=row.stats_since if row else stats_since,
        first_collected_at=row.first_collected_at if row else now,
    )


@router.get("/{instance_id}", response_model=list[MissingIndexSuggestionResponse])
async def list_missing_index_suggestions(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[
        MissingIndexSuggestionsRepository, Depends(get_missing_index_suggestions_repository)
    ],
) -> list[MissingIndexSuggestionResponse]:
    """Sugestoes de indice ausente de uma instancia, ordenadas por `impact_score` desc — estado
    mais recente por (banco, tabela, sugestao), nao historico bruto."""
    settings = get_settings()
    rows = await repository.list_for_instance(instance_id, settings.api_read_max_rows)
    return [MissingIndexSuggestionResponse.model_validate(row) for row in rows]
