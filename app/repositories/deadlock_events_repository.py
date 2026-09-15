"""Repositorio de `deadlock_events` — Etapa 10, item 7.

Tabela relacional INSERT-ONLY (um deadlock e um evento instantaneo — sem "abrir/fechar"). O
mesmo deadlock pode aparecer em chamadas consecutivas de `collect_deadlock_events()` enquanto
ainda estiver no ring buffer `system_health` — `insert_new` deduplica por
`(instance_id, time, victim_session_id, survivor_session_id)` (melhor esforco: o ring buffer nao
tem um ID estavel de deadlock, mas o timestamp do evento + os 2 SPIDs envolvidos e uma chave
pratica o suficiente para este volume baixo).
"""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID

import sqlalchemy as sa

from app.models.deadlock_event import DeadlockEvent
from app.normalizers.models import NormalizedDeadlockEvent
from app.repositories.base import BaseRepository


class DeadlockEventsRepository(BaseRepository):
    async def insert_new(self, events: list[NormalizedDeadlockEvent]) -> list[DeadlockEvent]:
        """Insere so os eventos ainda nao vistos (mesmo `instance_id`/`time`/`victim_session_id`/
        `survivor_session_id`) — devolve as linhas REALMENTE novas (usado pela correlacao com
        `transaction_events`, que so precisa agir sobre deadlocks novos)."""
        if not events:
            return []

        keys = [(e.instance_id, e.time, e.victim_session_id, e.survivor_session_id) for e in events]
        instance_ids = {k[0] for k in keys}
        existing_rows = (
            await self._session.execute(
                sa.select(
                    DeadlockEvent.instance_id,
                    DeadlockEvent.time,
                    DeadlockEvent.victim_session_id,
                    DeadlockEvent.survivor_session_id,
                ).where(DeadlockEvent.instance_id.in_(instance_ids))
            )
        ).all()
        existing_keys = {
            (row.instance_id, row.time, row.victim_session_id, row.survivor_session_id)
            for row in existing_rows
        }

        new_events = [
            e
            for e in events
            if (e.instance_id, e.time, e.victim_session_id, e.survivor_session_id)
            not in existing_keys
        ]
        if not new_events:
            return []

        new_rows = [
            DeadlockEvent(
                instance_id=e.instance_id,
                time=e.time,
                database_name=e.database_name,
                victim_session_id=e.victim_session_id,
                victim_sql_text_masked=e.victim_sql_text_masked,
                survivor_session_id=e.survivor_session_id,
                survivor_sql_text_masked=e.survivor_sql_text_masked,
                resource_description=e.resource_description,
                raw_xml_masked=e.raw_xml_masked,
                detail=e.detail or None,
            )
            for e in new_events
        ]
        self._session.add_all(new_rows)
        await self._session.commit()
        for row in new_rows:
            await self._session.refresh(row)
        return new_rows

    async def list_for_instance(
        self,
        instance_id: UUID,
        limit: int,
        *,
        since: datetime | None = None,
        until: datetime | None = None,
        database_name: str | None = None,
    ) -> list[DeadlockEvent]:
        """Mais recentes primeiro. `since`/`until` (opcionais, aditivos) filtram por `time` —
        deixa de exigir o historico inteiro quando o usuario quer so um periodo especifico."""
        conditions = [DeadlockEvent.instance_id == instance_id]
        if since is not None:
            conditions.append(DeadlockEvent.time >= since)
        if until is not None:
            conditions.append(DeadlockEvent.time <= until)
        if database_name is not None:
            conditions.append(DeadlockEvent.database_name == database_name)
        stmt = (
            sa.select(DeadlockEvent)
            .where(*conditions)
            .order_by(sa.desc(DeadlockEvent.time))
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def get_by_id(self, instance_id: UUID, deadlock_id: UUID) -> DeadlockEvent | None:
        stmt = sa.select(DeadlockEvent).where(
            DeadlockEvent.instance_id == instance_id, DeadlockEvent.id == deadlock_id
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def purge_older_than(self, cutoff: datetime) -> int:
        """Expurgo por idade — deadlock e sempre um fato ja concluido, entao (ao contrario de
        transacao/bloqueio) nao ha distincao aberto/encerrado: `time < cutoff` basta."""
        result = await self._session.execute(
            sa.delete(DeadlockEvent).where(DeadlockEvent.time < cutoff)
        )
        await self._session.commit()
        return cast(sa.CursorResult, result).rowcount or 0
