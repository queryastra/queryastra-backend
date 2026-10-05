"""Repositorio de `MissingIndexCollectionRun` — ver docstring do modelo."""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.missing_index_collection_run import MissingIndexCollectionRun
from app.repositories.base import BaseRepository


class MissingIndexCollectionRunsRepository(BaseRepository):
    async def upsert(
        self, instance_id: UUID, collected_at: datetime, stats_since: datetime | None
    ) -> MissingIndexCollectionRun:
        insert_stmt = pg_insert(MissingIndexCollectionRun).values(
            instance_id=instance_id,
            collected_at=collected_at,
            stats_since=stats_since,
            first_collected_at=collected_at,
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            constraint="pk_missing_index_collection_runs",
            set_={
                "collected_at": insert_stmt.excluded.collected_at,
                "stats_since": insert_stmt.excluded.stats_since,
                # Preserva o valor original — COALESCE garante que o campo e definido na
                # primeira coleta e NUNCA sobrescrito em coletas subsequentes.
                "first_collected_at": sa.func.coalesce(
                    MissingIndexCollectionRun.__table__.c.first_collected_at,
                    insert_stmt.excluded.first_collected_at,
                ),
            },
        ).returning(MissingIndexCollectionRun)
        row = (await self._session.execute(upsert_stmt)).scalar_one()
        await self._session.commit()
        return row

    async def get_for_instance(self, instance_id: UUID) -> MissingIndexCollectionRun | None:
        return await self._session.get(MissingIndexCollectionRun, instance_id)
