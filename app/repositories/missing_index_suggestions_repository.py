"""Repositorio de `missing_index_suggestions` — ARCHITECTURE_SPEC.md Q18/Q21.

Tabela relacional comum (nao hypertable): cada linha e o estado mais recente de UMA sugestao,
identificada por (`instance_id`, `database_name`, `table_name`, `suggestion`) — atualizada via
UPSERT (`first_seen_at` preservado, `last_seen_at`/`impact_score`/`detail` atualizados) a cada
ciclo do `MissingIndexSupervisor`, nunca inserida como serie temporal bruta.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.missing_index_suggestion import MissingIndexSuggestion
from app.normalizers.models import NormalizedMissingIndexSuggestion
from app.repositories.base import BaseRepository


class MissingIndexSuggestionsRepository(BaseRepository):
    async def upsert_batch(
        self,
        suggestions: list[NormalizedMissingIndexSuggestion],
        observed_at: datetime,
    ) -> None:
        """Insere ou atualiza cada sugestao — `first_seen_at` so e definido na primeira vez (via
        `INSERT ... ON CONFLICT DO UPDATE`, preservando o valor existente), `last_seen_at` sempre
        avanca para `observed_at`. `database_name` entra na chave de conflito mesmo sendo
        nullable — Postgres trata `NULL` como distinto em `UNIQUE`/`ON CONFLICT` por padrao, mas
        hoje ambos os engines sempre preenchem `database_name` (ver docstring do modelo), entao
        isso nao e um problema pratico."""
        if not suggestions:
            return

        rows = [
            {
                "instance_id": s.instance_id,
                "database_name": s.database_name,
                "table_name": s.table_name,
                "suggestion": s.suggestion,
                "impact_score": s.impact_score,
                "detail": s.detail,
                "first_seen_at": observed_at,
                "last_seen_at": observed_at,
            }
            for s in suggestions
        ]

        stmt = pg_insert(MissingIndexSuggestion).values(rows)
        stmt = stmt.on_conflict_do_update(
            constraint="uq_missing_index_suggestions_identity",
            set_={
                "impact_score": stmt.excluded.impact_score,
                "detail": stmt.excluded.detail,
                "last_seen_at": stmt.excluded.last_seen_at,
            },
        )
        await self._session.execute(stmt)
        await self._session.commit()

    async def list_for_instance(
        self, instance_id: UUID, limit: int
    ) -> list[MissingIndexSuggestion]:
        """Sugestoes mais recentes de uma instancia, ordenadas por `impact_score` desc (mais
        relevante primeiro) — `impact_score` NULL (nunca deveria ocorrer na pratica, mas os dois
        engines o declaram nullable) vai por ultimo via `NULLS LAST`."""
        stmt = (
            sa.select(MissingIndexSuggestion)
            .where(MissingIndexSuggestion.instance_id == instance_id)
            .order_by(sa.desc(MissingIndexSuggestion.impact_score).nulls_last())
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())
