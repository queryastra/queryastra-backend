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

from app.api.deps import get_db_session
from app.core.config import get_settings
from app.repositories.missing_index_suggestions_repository import (
    MissingIndexSuggestionsRepository,
)

router = APIRouter()


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


@router.get("/{instance_id}", response_model=list[MissingIndexSuggestionResponse])
async def list_missing_index_suggestions(
    instance_id: UUID,
    repository: Annotated[
        MissingIndexSuggestionsRepository, Depends(get_missing_index_suggestions_repository)
    ],
) -> list[MissingIndexSuggestionResponse]:
    """Sugestoes de indice ausente de uma instancia, ordenadas por `impact_score` desc — estado
    mais recente por (banco, tabela, sugestao), nao historico bruto."""
    settings = get_settings()
    rows = await repository.list_for_instance(instance_id, settings.api_read_max_rows)
    return [MissingIndexSuggestionResponse.model_validate(row) for row in rows]
