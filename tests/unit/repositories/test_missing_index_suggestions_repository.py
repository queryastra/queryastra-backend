"""Testes de `MissingIndexSuggestionsRepository` contra o banco real `ab_xql_pm_dev`."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from sqlalchemy import delete
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.models.missing_index_suggestion import MissingIndexSuggestion
from app.normalizers.models import NormalizedMissingIndexSuggestion
from app.repositories.missing_index_suggestions_repository import (
    MissingIndexSuggestionsRepository,
)


async def test_upsert_batch_inserts_new_suggestion(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MissingIndexSuggestionsRepository(db_session)
    observed_at = datetime.now(UTC)
    suggestion = NormalizedMissingIndexSuggestion(
        instance_id=test_instance.id,
        database_name="db_a",
        table_name="public.pedidos",
        suggestion="Igualdade: cliente_id; Desigualdade: -; Incluidas: -",
        impact_score=42.5,
        detail={"user_seeks": 10, "user_scans": 2},
    )

    await repo.upsert_batch([suggestion], observed_at)

    rows = await repo.list_for_instance(test_instance.id, limit=10)
    assert len(rows) == 1
    row = rows[0]
    assert row.table_name == "public.pedidos"
    assert row.impact_score == 42.5
    assert row.detail == {"user_seeks": 10, "user_scans": 2}
    assert row.first_seen_at == observed_at
    assert row.last_seen_at == observed_at

    await db_session.execute(
        delete(MissingIndexSuggestion).where(MissingIndexSuggestion.instance_id == test_instance.id)
    )
    await db_session.commit()


async def test_upsert_batch_preserves_first_seen_at_and_updates_rest(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """Segunda chamada com a MESMA identidade (instance_id/database_name/table_name/suggestion)
    deve preservar `first_seen_at` da primeira vez, atualizando so `last_seen_at`/`impact_score`/
    `detail` — o "estado mais recente", nao um historico bruto."""
    repo = MissingIndexSuggestionsRepository(db_session)
    first_seen_at = datetime.now(UTC) - timedelta(days=1)
    last_seen_at = datetime.now(UTC)

    base_suggestion = NormalizedMissingIndexSuggestion(
        instance_id=test_instance.id,
        database_name="db_a",
        table_name="public.pedidos",
        suggestion="Igualdade: cliente_id; Desigualdade: -; Incluidas: -",
        impact_score=10.0,
        detail={"user_seeks": 1, "user_scans": 1},
    )
    await repo.upsert_batch([base_suggestion], first_seen_at)

    updated_suggestion = NormalizedMissingIndexSuggestion(
        instance_id=test_instance.id,
        database_name="db_a",
        table_name="public.pedidos",
        suggestion="Igualdade: cliente_id; Desigualdade: -; Incluidas: -",
        impact_score=99.0,
        detail={"user_seeks": 100, "user_scans": 5},
    )
    await repo.upsert_batch([updated_suggestion], last_seen_at)

    rows = await repo.list_for_instance(test_instance.id, limit=10)
    assert len(rows) == 1
    row = rows[0]
    assert row.first_seen_at == first_seen_at
    assert row.last_seen_at == last_seen_at
    assert row.impact_score == 99.0
    assert row.detail == {"user_seeks": 100, "user_scans": 5}

    await db_session.execute(
        delete(MissingIndexSuggestion).where(MissingIndexSuggestion.instance_id == test_instance.id)
    )
    await db_session.commit()


async def test_upsert_batch_noop_for_empty_list(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MissingIndexSuggestionsRepository(db_session)
    await repo.upsert_batch([], datetime.now(UTC))
    rows = await repo.list_for_instance(test_instance.id, limit=10)
    assert rows == []


async def test_list_for_instance_orders_by_impact_score_desc(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = MissingIndexSuggestionsRepository(db_session)
    observed_at = datetime.now(UTC)
    await repo.upsert_batch(
        [
            NormalizedMissingIndexSuggestion(
                instance_id=test_instance.id,
                database_name="db_a",
                table_name="public.low_impact",
                suggestion="sugestao baixa",
                impact_score=1.0,
            ),
            NormalizedMissingIndexSuggestion(
                instance_id=test_instance.id,
                database_name="db_a",
                table_name="public.high_impact",
                suggestion="sugestao alta",
                impact_score=500.0,
            ),
        ],
        observed_at,
    )

    rows = await repo.list_for_instance(test_instance.id, limit=10)
    assert [row.table_name for row in rows] == ["public.high_impact", "public.low_impact"]

    await db_session.execute(
        delete(MissingIndexSuggestion).where(MissingIndexSuggestion.instance_id == test_instance.id)
    )
    await db_session.commit()
