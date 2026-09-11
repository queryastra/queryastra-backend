"""Teste de integracao de `GET /api/v1/missing-index-suggestions/{instance_id}` contra o banco
real `ab_xql_pm_dev` — ARCHITECTURE_SPEC.md Q18/Q21."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedMissingIndexSuggestion
from app.repositories.missing_index_suggestions_repository import (
    MissingIndexSuggestionsRepository,
)


async def test_list_missing_index_suggestions_returns_empty_list_when_none(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/missing-index-suggestions/{test_instance.id}")

    assert response.status_code == 200
    assert response.json() == []


async def test_list_missing_index_suggestions_returns_seeded_rows_ordered_by_impact(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MissingIndexSuggestionsRepository(db_session)
    observed_at = datetime.now(UTC)
    await repo.upsert_batch(
        [
            NormalizedMissingIndexSuggestion(
                instance_id=test_instance.id,
                database_name="db_a",
                table_name="public.baixo_impacto",
                suggestion="candidato baixo",
                impact_score=1.0,
                detail={"seq_scan": 10},
            ),
            NormalizedMissingIndexSuggestion(
                instance_id=test_instance.id,
                database_name="db_a",
                table_name="public.alto_impacto",
                suggestion="candidato alto",
                impact_score=500.0,
                detail={"seq_scan": 900},
            ),
        ],
        observed_at,
    )

    response = await client.get(f"/api/v1/missing-index-suggestions/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert [row["table_name"] for row in body] == [
        "public.alto_impacto",
        "public.baixo_impacto",
    ]
    assert body[0]["impact_score"] == 500.0
    assert body[0]["detail"] == {"seq_scan": 900}
    assert body[0]["instance_id"] == str(test_instance.id)


async def test_list_missing_index_suggestions_for_unknown_instance_returns_empty_list(
    client: AsyncClient,
) -> None:
    response = await client.get(f"/api/v1/missing-index-suggestions/{uuid4()}")

    assert response.status_code == 200
    assert response.json() == []
