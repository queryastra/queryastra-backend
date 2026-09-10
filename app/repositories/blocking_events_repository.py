"""Repositorio da hypertable `blocking_events`.

`blocking_events` (`Docs/database-schema.md` §3) e hypertable, criada por
`migrations/versions/66ae4664300c_hypertables.py`. Nivel direto apenas (sem arvore recursiva
completa de bloqueio — corte de escopo, ver `app.normalizers.models.NormalizedBlockingEvent`).
"""

from __future__ import annotations

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
