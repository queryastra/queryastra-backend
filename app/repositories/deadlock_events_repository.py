"""Repositorio de `deadlock_events` — Etapa 10, item 7.

Tabela relacional INSERT-ONLY (um deadlock e um evento instantaneo — sem "abrir/fechar"). O
mesmo deadlock pode aparecer em chamadas consecutivas de `collect_deadlock_events()` enquanto
ainda estiver no `system_health` — `insert_new` deduplica por
`(instance_id, time, victim_session_id, survivor_session_id)` (melhor esforco: o `system_health`
nao tem um ID estavel de deadlock, mas o timestamp do evento + os 2 SPIDs envolvidos e uma chave
pratica o suficiente para este volume baixo).

Deteccao instantanea (bug real relatado ao vivo pelo usuario: o `system_health` demorou 25-35min
pra publicar o relatorio completo, mesmo a instancia sendo polada a cada 1s) — `insert_pending`
cria um registro so com `instance_id`/`time` (`pending=True`) assim que o `PollingWorker` percebe
o contador `sqlserver_deadlocks_total` incrementar, MUITO antes do relatorio completo existir.
`insert_new` reconcilia esse registro pendente (substitui no lugar, nunca duplica) assim que o
relatorio completo chega, usando `_PENDING_RECONCILIATION_WINDOW` para casar por proximidade de
tempo (o timestamp exato do XML pode diferir por alguns segundos do momento em que o contador foi
percebido incrementado).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import cast
from uuid import UUID

import sqlalchemy as sa

from app.models.deadlock_event import DeadlockEvent
from app.normalizers.models import NormalizedDeadlockEvent
from app.repositories.base import BaseRepository

# Generosa de proposito: o "instante" da deteccao instantanea (quando o PollingWorker percebeu o
# contador incrementar) e o timestamp do XML completo (quando o motor efetivamente registrou o
# deadlock) sao a mesma coisa na pratica, quase sempre a poucos segundos um do outro — nunca
# visto divergir minutos. 10min da folga de sobra sem risco pratico de casar com o pendente
# ERRADO (2 deadlocks diferentes tao proximos um do outro na mesma instancia seria raridade).
_PENDING_RECONCILIATION_WINDOW = timedelta(minutes=10)


class DeadlockEventsRepository(BaseRepository):
    async def insert_pending(self, instance_id: UUID, time: datetime) -> DeadlockEvent:
        """Deteccao instantanea (`PollingWorker`, contador `sqlserver_deadlocks_total`) — cria um
        registro so com `instance_id`/`time` (todo o resto None, `pending=True`), sinalizando que
        UM deadlock foi detectado mas os detalhes completos ainda nao chegaram do
        `system_health`. Ver `insert_new` para a reconciliacao."""
        row = DeadlockEvent(instance_id=instance_id, time=time, pending=True)
        self._session.add(row)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def insert_new(self, events: list[NormalizedDeadlockEvent]) -> list[DeadlockEvent]:
        """Insere so os eventos ainda nao vistos (mesmo `instance_id`/`time`/`victim_session_id`/
        `survivor_session_id`) — devolve as linhas REALMENTE novas OU recem-reconciliadas (usado
        pela correlacao com `transaction_events`, que so precisa agir sobre deadlocks com detalhe
        completo pela primeira vez).

        Antes de inserir, tenta RECONCILIAR com um registro pendente (`insert_pending`) da mesma
        instancia dentro de `_PENDING_RECONCILIATION_WINDOW` — atualiza no lugar (preenche todos
        os campos, `pending=False`) em vez de criar uma segunda linha pro mesmo evento."""
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
                ).where(
                    DeadlockEvent.instance_id.in_(instance_ids), DeadlockEvent.pending.is_(False)
                )
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

        pending_candidates = list(
            (
                await self._session.execute(
                    sa.select(DeadlockEvent).where(
                        DeadlockEvent.instance_id.in_({e.instance_id for e in new_events}),
                        DeadlockEvent.pending.is_(True),
                    )
                )
            )
            .scalars()
            .all()
        )

        reconciled_rows: list[DeadlockEvent] = []
        unmatched_events: list[NormalizedDeadlockEvent] = []
        for event in new_events:
            candidates = [
                pending
                for pending in pending_candidates
                if pending.instance_id == event.instance_id
                and abs((pending.time - event.time).total_seconds())
                <= _PENDING_RECONCILIATION_WINDOW.total_seconds()
            ]
            if not candidates:
                unmatched_events.append(event)
                continue
            match = min(candidates, key=lambda p: abs((p.time - event.time).total_seconds()))
            pending_candidates.remove(match)
            match.time = event.time
            match.database_name = event.database_name
            match.victim_session_id = event.victim_session_id
            match.victim_sql_text_masked = event.victim_sql_text_masked
            match.survivor_session_id = event.survivor_session_id
            match.survivor_sql_text_masked = event.survivor_sql_text_masked
            match.resource_description = event.resource_description
            match.raw_xml_masked = event.raw_xml_masked
            match.detail = event.detail or None
            match.pending = False
            reconciled_rows.append(match)

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
            for e in unmatched_events
        ]
        self._session.add_all(new_rows)
        await self._session.commit()
        all_rows = new_rows + reconciled_rows
        for row in all_rows:
            await self._session.refresh(row)
        return all_rows

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
