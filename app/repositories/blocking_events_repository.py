"""Repositorio da hypertable `blocking_events`.

`blocking_events` (`Docs/database-schema.md` §3) e hypertable, criada por
`migrations/versions/66ae4664300c_hypertables.py`. Nivel direto apenas (sem arvore recursiva
completa de bloqueio — corte de escopo, ver `app.normalizers.models.NormalizedBlockingEvent`).
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa

from app.normalizers.models import NormalizedBlockingEvent
from app.repositories._tables import blocking_events_table
from app.repositories.base import BaseRepository


class BlockingEventsRepository(BaseRepository):
    """Persistencia de `NormalizedBlockingEvent` na hypertable `blocking_events`."""

    async def insert_batch(self, events: list[NormalizedBlockingEvent]) -> None:
        if not events:
            return
        rows = [
            {
                "time": e.time,
                "instance_id": e.instance_id,
                "blocker_session_id": e.blocker_session_id,
                "blocked_session_id": e.blocked_session_id,
                "blocker_sql_hash": e.blocker_sql_hash,
                "blocked_sql_hash": e.blocked_sql_hash,
                "wait_time_ms": e.wait_time_ms,
            }
            for e in events
        ]
        await self._session.execute(sa.insert(blocking_events_table), rows)
        await self._session.commit()

    async def list_latest_snapshot(
        self, instance_id: UUID, since: datetime, limit: int
    ) -> list[sa.Row]:
        """So o ultimo ciclo que registrou bloqueio — snapshot no timestamp mais recente dentro
        da janela, NAO uma janela deslizante inteira (misturaria bloqueios ja resolvidos com os
        atuais)."""
        latest_time_stmt = sa.select(sa.func.max(blocking_events_table.c.time)).where(
            blocking_events_table.c.instance_id == instance_id,
            blocking_events_table.c.time >= since,
        )
        latest_time = (await self._session.execute(latest_time_stmt)).scalar_one_or_none()
        if latest_time is None:
            return []
        stmt = (
            sa.select(blocking_events_table)
            .where(
                blocking_events_table.c.instance_id == instance_id,
                blocking_events_table.c.time == latest_time,
            )
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).fetchall())
