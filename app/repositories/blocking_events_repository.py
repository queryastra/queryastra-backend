"""Repositorio da hypertable `blocking_events`.

`blocking_events` (`Docs/database-schema.md` §3) e hypertable, criada por
`migrations/versions/66ae4664300c_hypertables.py`, colunas de enriquecimento adicionadas por
`migrations/versions/0607d7592ff6_add_blocking_event_enrichment_columns.py`. Uma linha por aresta
DIRETA (bloqueador imediato -> bloqueado) — a arvore completa de bloqueio e reconstruida no app
(`app.api.v1.waits_blocking._build_blocking_tree`) encadeando as arestas de um mesmo snapshot, ver
`app.normalizers.models.NormalizedBlockingEvent`.
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
                "blocker_db_user": e.blocker_db_user,
                "blocker_program": e.blocker_program,
                "blocker_machine": e.blocker_machine,
                "blocker_wait_event_native": e.blocker_wait_event_native,
                "blocked_db_user": e.blocked_db_user,
                "blocked_program": e.blocked_program,
                "blocked_machine": e.blocked_machine,
                "blocked_wait_event_native": e.blocked_wait_event_native,
                "blocked_wait_resource": e.blocked_wait_resource,
                "blocked_object": e.blocked_object,
                "blocker_transaction_id": e.blocker_transaction_id,
                "blocked_transaction_id": e.blocked_transaction_id,
                "blocker_database_name": e.blocker_database_name,
                "blocked_database_name": e.blocked_database_name,
            }
            for e in events
        ]
        await self._session.execute(sa.insert(blocking_events_table), rows)
        await self._session.commit()

    async def list_latest_snapshot(
        self, instance_id: UUID, since: datetime, limit: int, until: datetime | None = None
    ) -> list[sa.Row]:
        """So o ultimo ciclo que registrou bloqueio — snapshot no timestamp mais recente dentro
        da janela, NAO uma janela deslizante inteira (misturaria bloqueios ja resolvidos com os
        atuais). `until` (opcional, aditivo) delimita um dia calendario especifico no passado
        (`DayFilter` do frontend) — `None` mantem o comportamento historico de "desde `since`, o
        mais recente", sem limite superior."""
        latest_time_stmt = sa.select(sa.func.max(blocking_events_table.c.time)).where(
            blocking_events_table.c.instance_id == instance_id,
            blocking_events_table.c.time >= since,
        )
        if until is not None:
            latest_time_stmt = latest_time_stmt.where(blocking_events_table.c.time <= until)
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
