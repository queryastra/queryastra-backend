"""Repositorio de `blocking_incidents` — Etapa 10, redesenho (log de incidente de bloqueio).

Tabela relacional comum (nao hypertable — ver `app/models/blocking_incident.py` para por que
`blocking_events`, hypertable, nao serve pra isso). `upsert_batch` grava/atualiza cada par
(bloqueador, bloqueado) OBSERVADO neste ciclo (`started_at` preservado do primeiro registro,
`last_seen_at`/enriquecimento atualizados) e FECHA (`ended_at`, nunca deleta) qualquer par ja
rastreado (`ended_at IS NULL`) que nao aparece mais no batch — mesmo padrao de
`TransactionEventsRepository.upsert_batch`. Todo par observado neste ciclo e, por definicao,
"digno de nota" (ao contrario de transacao, nao ha filtro adicional aqui — um bloqueio direto ja
e o proprio criterio).
"""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.blocking_incident import BlockingIncident
from app.normalizers.models import NormalizedBlockingEvent
from app.repositories.base import BaseRepository


class BlockingIncidentsRepository(BaseRepository):
    async def upsert_batch(
        self,
        instance_id: UUID,
        events: list[NormalizedBlockingEvent],
        observed_at: datetime,
    ) -> None:
        """Reconcilia os incidentes de bloqueio de `instance_id` neste ciclo: upsert em cada
        aresta observada (`started_at` so no INSERT, nunca sobrescrito no UPDATE) e fecha
        (`ended_at`, nunca deleta) qualquer par ja rastreado ausente de `events`."""
        open_edge_keys = {(e.blocker_session_id, e.blocked_session_id) for e in events}

        if events:
            rows = [
                {
                    "instance_id": instance_id,
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
                    "blocker_transaction_begin_time": e.blocker_transaction_begin_time,
                    "blocked_transaction_begin_time": e.blocked_transaction_begin_time,
                    "started_at": observed_at,
                    "last_seen_at": observed_at,
                    "ended_at": None,
                }
                for e in events
            ]
            stmt = pg_insert(BlockingIncident).values(rows)
            stmt = stmt.on_conflict_do_update(
                constraint="uq_blocking_incidents_identity",
                set_={
                    "blocker_sql_hash": stmt.excluded.blocker_sql_hash,
                    "blocked_sql_hash": stmt.excluded.blocked_sql_hash,
                    "wait_time_ms": stmt.excluded.wait_time_ms,
                    "blocker_db_user": stmt.excluded.blocker_db_user,
                    "blocker_program": stmt.excluded.blocker_program,
                    "blocker_machine": stmt.excluded.blocker_machine,
                    "blocker_wait_event_native": stmt.excluded.blocker_wait_event_native,
                    "blocked_db_user": stmt.excluded.blocked_db_user,
                    "blocked_program": stmt.excluded.blocked_program,
                    "blocked_machine": stmt.excluded.blocked_machine,
                    "blocked_wait_event_native": stmt.excluded.blocked_wait_event_native,
                    "blocked_wait_resource": stmt.excluded.blocked_wait_resource,
                    "blocked_object": stmt.excluded.blocked_object,
                    "blocker_transaction_id": stmt.excluded.blocker_transaction_id,
                    "blocked_transaction_id": stmt.excluded.blocked_transaction_id,
                    "blocker_database_name": stmt.excluded.blocker_database_name,
                    "blocked_database_name": stmt.excluded.blocked_database_name,
                    "blocker_transaction_begin_time": stmt.excluded.blocker_transaction_begin_time,
                    "blocked_transaction_begin_time": stmt.excluded.blocked_transaction_begin_time,
                    "last_seen_at": stmt.excluded.last_seen_at,
                    # Reabre defensivamente (mesmo raciocinio de TransactionEventsRepository):
                    # se o par (session_id reciclado) reaparece depois de ja ter sido fechado.
                    "ended_at": None,
                },
            )
            await self._session.execute(stmt)

        # Fecha qualquer incidente ja aberto (desta instancia) cujo par nao esta neste batch —
        # SQLAlchemy nao tem um "NOT IN lista de tuplas" direto e portavel; como o volume de
        # incidentes simultaneos e tipicamente pequeno, comparamos em Python por linha.
        open_rows = (
            (
                await self._session.execute(
                    sa.select(BlockingIncident).where(
                        BlockingIncident.instance_id == instance_id,
                        BlockingIncident.ended_at.is_(None),
                    )
                )
            )
            .scalars()
            .all()
        )
        for row in open_rows:
            if (row.blocker_session_id, row.blocked_session_id) not in open_edge_keys:
                row.ended_at = observed_at
        await self._session.commit()

    async def list_open(self, instance_id: UUID, limit: int) -> list[BlockingIncident]:
        """Incidentes AINDA EM ANDAMENTO — alimenta a arvore ao vivo (`GET /blocking/{id}`)."""
        stmt = (
            sa.select(BlockingIncident)
            .where(BlockingIncident.instance_id == instance_id, BlockingIncident.ended_at.is_(None))
            .order_by(sa.desc(BlockingIncident.started_at))
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def list_incidents(
        self,
        instance_id: UUID,
        limit: int,
        *,
        open_only: bool = False,
        database_name: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[BlockingIncident]:
        """Lista plana (nao arvore) para investigacao/historico — mais recentes primeiro.
        `database_name` casa contra QUALQUER lado (bloqueador ou bloqueado). `since`/`until`
        (opcionais) filtram por SOBREPOSICAO com a janela — um incidente que comecou antes de
        `since` mas so terminou depois (ou continua em andamento) ainda conta como ativo
        durante o periodo pedido."""
        conditions = [BlockingIncident.instance_id == instance_id]
        if open_only:
            conditions.append(BlockingIncident.ended_at.is_(None))
        if database_name is not None:
            conditions.append(
                sa.or_(
                    BlockingIncident.blocker_database_name == database_name,
                    BlockingIncident.blocked_database_name == database_name,
                )
            )
        if since is not None:
            conditions.append(
                sa.or_(BlockingIncident.ended_at.is_(None), BlockingIncident.ended_at >= since)
            )
        if until is not None:
            conditions.append(BlockingIncident.started_at <= until)
        stmt = (
            sa.select(BlockingIncident)
            .where(*conditions)
            .order_by(sa.desc(BlockingIncident.last_seen_at))
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def find_involving_sessions(
        self, instance_id: UUID, session_ids: list[str], limit: int
    ) -> list[BlockingIncident]:
        """Versao em lote de `find_involving_session` — incidentes (em andamento ou ja
        encerrados) que envolvem QUALQUER sessao de `session_ids` de qualquer lado. Usado pela
        tela de Transacoes para correlacionar cada transacao com um eventual bloqueio relacionado
        numa unica consulta (evita N+1 - uma por linha)."""
        if not session_ids:
            return []
        stmt = (
            sa.select(BlockingIncident)
            .where(
                BlockingIncident.instance_id == instance_id,
                sa.or_(
                    BlockingIncident.blocker_session_id.in_(session_ids),
                    BlockingIncident.blocked_session_id.in_(session_ids),
                ),
            )
            .order_by(sa.desc(BlockingIncident.last_seen_at))
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def find_involving_session(
        self, instance_id: UUID, session_id: str, limit: int
    ) -> list[BlockingIncident]:
        """Incidentes (em andamento OU ja encerrados, dentro da janela de retencao) que
        envolvem `session_id` de qualquer lado — folha do drill-down (detalhe de sessao)."""
        stmt = (
            sa.select(BlockingIncident)
            .where(
                BlockingIncident.instance_id == instance_id,
                sa.or_(
                    BlockingIncident.blocker_session_id == session_id,
                    BlockingIncident.blocked_session_id == session_id,
                ),
            )
            .order_by(sa.desc(BlockingIncident.last_seen_at))
            .limit(limit)
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def purge_ended_before(self, cutoff: datetime) -> int:
        """Expurgo por idade — so incidentes JA ENCERRADOS ha mais tempo que `cutoff`; um
        incidente ainda em andamento nunca e removido."""
        result = await self._session.execute(
            sa.delete(BlockingIncident).where(
                BlockingIncident.ended_at.is_not(None), BlockingIncident.ended_at < cutoff
            )
        )
        await self._session.commit()
        return cast(sa.CursorResult, result).rowcount or 0
