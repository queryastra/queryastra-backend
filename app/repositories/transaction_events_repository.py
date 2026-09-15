"""Repositorio de `transaction_events` — Etapa 10 (itens 2/3), rastreamento de transacoes.

Tabela relacional comum (nao hypertable), mas com uma semantica diferente de
`missing_index_suggestions`/`backup_status`: cada linha representa uma transacao ATUALMENTE
aberta. `upsert_batch` reconcilia o conjunto inteiro por instancia a cada ciclo — alem de
upsert nos eventos vistos, DELETA qualquer linha da mesma instancia que nao apareceu neste ciclo
(a transacao fechou desde o ciclo anterior). Sem isso, uma transacao ja encerrada ficaria
"presa" pra sempre na tabela como se ainda estivesse aberta.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.transaction_event import TransactionEvent
from app.normalizers.models import NormalizedTransactionEvent
from app.repositories.base import BaseRepository


class TransactionEventsRepository(BaseRepository):
    async def upsert_batch(
        self,
        instance_id: UUID,
        events: list[NormalizedTransactionEvent],
        observed_at: datetime,
    ) -> None:
        """Reconcilia as transacoes abertas de `instance_id` neste ciclo: upsert em cada uma
        (`first_seen_at` preservado, resto atualizado) e remove qualquer linha da mesma
        instancia ausente de `events` (transacao ja fechou). Lista vazia remove tudo daquela
        instancia — nenhuma transacao aberta neste ciclo."""
        seen_identifiers = [e.transaction_identifier for e in events]

        if events:
            rows = [
                {
                    "instance_id": instance_id,
                    "transaction_identifier": e.transaction_identifier,
                    "session_id": e.session_id,
                    "database_name": e.database_name,
                    "transaction_type": e.transaction_type,
                    "transaction_uow": e.transaction_uow,
                    "begin_time": e.begin_time,
                    "state": e.state,
                    "last_error_code": e.last_error_code,
                    "first_seen_at": observed_at,
                    "last_seen_at": observed_at,
                }
                for e in events
            ]
            stmt = pg_insert(TransactionEvent).values(rows)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_transaction_events_identity",
                set_={
                    "session_id": stmt.excluded.session_id,
                    "database_name": stmt.excluded.database_name,
                    "transaction_type": stmt.excluded.transaction_type,
                    "transaction_uow": stmt.excluded.transaction_uow,
                    "begin_time": stmt.excluded.begin_time,
                    "state": stmt.excluded.state,
                    "last_error_code": stmt.excluded.last_error_code,
                    "last_seen_at": stmt.excluded.last_seen_at,
                },
            )
            await self._session.execute(stmt)

        stale_condition = (
            TransactionEvent.transaction_identifier.notin_(seen_identifiers)
            if seen_identifiers
            else sa.true()
        )
        await self._session.execute(
            sa.delete(TransactionEvent).where(
                TransactionEvent.instance_id == instance_id, stale_condition
            )
        )
        await self._session.commit()

    async def list_for_instance(self, instance_id: UUID, limit: int) -> list[TransactionEvent]:
        """Transacoes abertas de uma instancia, mais antigas primeiro (a mais provavel de ser
        uma orfa/esquecida aparece no topo)."""
        stmt = (
            sa.select(TransactionEvent)
            .where(TransactionEvent.instance_id == instance_id)
            .order_by(sa.asc(TransactionEvent.begin_time).nulls_last())
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def get_by_identifier(
        self, instance_id: UUID, transaction_identifier: str
    ) -> TransactionEvent | None:
        stmt = sa.select(TransactionEvent).where(
            TransactionEvent.instance_id == instance_id,
            TransactionEvent.transaction_identifier == transaction_identifier,
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()
