"""Endpoint de leitura de sugestoes de indice ausente — ARCHITECTURE_SPEC.md Q18/Q21.

Le direto da tabela relacional `missing_index_suggestions` (estado mais recente por
instancia/banco/tabela/sugestao, atualizado pelo `MissingIndexSupervisor` de baixa frequencia —
`app/workers/missing_index_supervisor.py`), sem janela de tempo (nao e serie temporal bruta como
`query_samples`/`metric_samples`)."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import check_instance_access, get_db_session
from app.core.config import get_settings
from app.repositories.missing_index_collection_runs_repository import (
    MissingIndexCollectionRunsRepository,
)
from app.repositories.missing_index_suggestions_repository import (
    MissingIndexSuggestionsRepository,
)

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
