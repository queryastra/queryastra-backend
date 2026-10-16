"""Repositorio de `query_plan_snapshots` — historico de plano de execucao.

Tabela relacional INSERT-ONLY (mesmo padrao de `DeadlockEventsRepository`). `insert_if_changed`
faz a deduplicacao: um snapshot só é persistido quando `plan_hash` difere do último conhecido
para o mesmo par (`instance_id`, `sql_hash`) — evita gravar a mesma linha a cada varredura
periódica enquanto o plano não mudar de fato.
"""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID

import sqlalchemy as sa

from app.models.query_plan_snapshot import QueryPlanSnapshot
from app.normalizers.models import NormalizedPlanSnapshot
from app.repositories.base import BaseRepository


class QueryPlanSnapshotsRepository(BaseRepository):
    async def insert_if_changed(
        self, snapshots: list[NormalizedPlanSnapshot]
    ) -> list[QueryPlanSnapshot]:
        """Para cada `(instance_id, sql_hash)` em `snapshots`, busca o `plan_hash` mais recente
        já conhecido e só insere quando ausente ou diferente — devolve as linhas REALMENTE
        novas."""
        if not snapshots:
            return []

        instance_ids = {s.instance_id for s in snapshots}
        latest_by_key: dict[tuple[UUID, str], str] = {}
        for instance_id in instance_ids:
            keys_for_instance = {s.sql_hash for s in snapshots if s.instance_id == instance_id}
            rows = (
                await self._session.execute(
                    sa.select(
                        QueryPlanSnapshot.sql_hash,
                        QueryPlanSnapshot.plan_hash,
                        QueryPlanSnapshot.captured_at,
                    )
                    .where(
                        QueryPlanSnapshot.instance_id == instance_id,
                        QueryPlanSnapshot.sql_hash.in_(keys_for_instance),
                    )
                    .order_by(QueryPlanSnapshot.captured_at.desc())
                )
            ).all()
            for row in rows:
                key = (instance_id, row.sql_hash)
                if key not in latest_by_key:
                    latest_by_key[key] = row.plan_hash

        new_rows = [
            QueryPlanSnapshot(
                instance_id=s.instance_id,
                sql_hash=s.sql_hash,
                plan_hash=s.plan_hash,
                plan_xml_masked=s.plan_xml_masked,
                plan_format=s.plan_format,
                last_execution_time=s.last_execution_time,
            )
            for s in snapshots
            if latest_by_key.get((s.instance_id, s.sql_hash)) != s.plan_hash
        ]
        if not new_rows:
            return []

        self._session.add_all(new_rows)
        await self._session.commit()
        for new_row in new_rows:
            await self._session.refresh(new_row)
        return new_rows

    async def list_for_query(
        self, instance_id: UUID, sql_hash: str, limit: int
    ) -> list[QueryPlanSnapshot]:
        """Histórico de planos distintos desta query — mais recente primeiro."""
        stmt = (
            sa.select(QueryPlanSnapshot)
            .where(
                QueryPlanSnapshot.instance_id == instance_id,
                QueryPlanSnapshot.sql_hash == sql_hash,
            )
            .order_by(sa.desc(QueryPlanSnapshot.captured_at))
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def purge_older_than(self, cutoff: datetime) -> int:
        """Expurgo por idade — um snapshot de plano é sempre um fato já concluído (não tem
        aberto/encerrado), `captured_at < cutoff` basta."""
        result = await self._session.execute(
            sa.delete(QueryPlanSnapshot).where(QueryPlanSnapshot.captured_at < cutoff)
        )
        await self._session.commit()
        return cast(sa.CursorResult, result).rowcount or 0
