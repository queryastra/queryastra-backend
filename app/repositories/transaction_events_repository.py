"""Repositorio de `transaction_events` — Etapa 10 (itens 2/3), log de incidente de transacao.

Redesenho pos-uso real (ver plano "Etapa 10, redesenho"): cada linha e uma transacao "digna de
nota" (bloqueio, erro, orfa ou deadlock — ver `app.collectors.transaction_flagging`), retida para
analise posterior ate `settings.problem_events_retention_days`
(`app/workers/problem_events_retention_worker.py`) — NUNCA deletada so porque a transacao fechou.
`upsert_batch` grava/atualiza as transacoes flagged deste ciclo (`flag_reason` uniao de todos os
motivos ja vistos, nunca sobrescrita) e FECHA (`closed_at`) qualquer linha ja rastreada cuja
transacao nao esta mais entre as abertas deste ciclo — nunca deleta.
"""

from __future__ import annotations

from datetime import datetime
from typing import cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.transaction_event import TransactionEvent
from app.normalizers.models import NormalizedTransactionEvent
from app.repositories.base import BaseRepository


def _merge_tags(old: str | None, new: str | None) -> str | None:
    """Uniao de tags separadas por virgula, preservando ordem de primeira aparicao — sem
    duplicatas. `None` quando nenhum dos dois lados tem tag nenhuma."""
    tags: list[str] = []
    for part in (old or "").split(",") + (new or "").split(","):
        if part and part not in tags:
            tags.append(part)
    return ",".join(tags) if tags else None


class TransactionEventsRepository(BaseRepository):
    async def upsert_batch(
        self,
        instance_id: UUID,
        events: list[NormalizedTransactionEvent],
        open_transaction_identifiers: set[str],
        observed_at: datetime,
    ) -> None:
        """Grava/atualiza as transacoes FLAGGED (`events`, ja filtradas pelo coletor — so quem
        tem `flag_reason`) e fecha (`closed_at`, nunca deleta) qualquer transacao ja rastreada
        (`closed_at IS NULL`) que nao esta mais em `open_transaction_identifiers` (TODAS as
        transacoes abertas neste ciclo, flagged ou nao — necessario para nao fechar por engano
        uma transacao que so nao foi re-flagged neste ciclo mas continua aberta)."""
        if events:
            identifiers = [e.transaction_identifier for e in events]
            existing_rows = (
                await self._session.execute(
                    sa.select(
                        TransactionEvent.transaction_identifier, TransactionEvent.flag_reason
                    ).where(
                        TransactionEvent.instance_id == instance_id,
                        TransactionEvent.transaction_identifier.in_(identifiers),
                    )
                )
            ).all()
            existing_flag_reason = {
                row.transaction_identifier: row.flag_reason for row in existing_rows
            }

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
                    "flag_reason": _merge_tags(
                        existing_flag_reason.get(e.transaction_identifier), e.flag_reason
                    ),
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
                    "flag_reason": stmt.excluded.flag_reason,
                    "last_seen_at": stmt.excluded.last_seen_at,
                    # Reabre defensivamente: se o identificador foi reciclado pelo motor depois
                    # de ja ter sido fechado (raro, mas possivel numa janela de retencao longa),
                    # trata como uma nova ocorrencia da mesma transacao, nao um erro.
                    "closed_at": None,
                },
            )
            await self._session.execute(stmt)

        stale_condition = (
            TransactionEvent.transaction_identifier.notin_(open_transaction_identifiers)
            if open_transaction_identifiers
            else sa.true()
        )
        await self._session.execute(
            sa.update(TransactionEvent)
            .where(
                TransactionEvent.instance_id == instance_id,
                TransactionEvent.closed_at.is_(None),
                stale_condition,
            )
            .values(closed_at=observed_at, state="encerrada")
        )
        await self._session.commit()

    async def list_for_instance(
        self,
        instance_id: UUID,
        limit: int,
        *,
        open_only: bool = True,
        database_name: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
    ) -> list[TransactionEvent]:
        """`open_only=True` (padrao): so transacoes ainda abertas, mais antigas primeiro (mais
        provavel de ser uma orfa/esquecida no topo). `open_only=False`: inclui encerradas
        tambem (retidas ate a janela de retencao), mais recentes primeiro (feed de historico,
        nao teria sentido enterrar atividade recente sob transacoes antigas ja fechadas).

        `since`/`until` (opcionais, so fazem sentido com `open_only=False` — uma transacao ainda
        aberta esta sempre "no periodo atual" por definicao): filtra por SOBREPOSICAO com a
        janela, nao so pelo inicio — uma transacao que comecou antes de `since` mas so fechou
        depois (ou continua aberta) ainda contava como "ativa" durante a janela pedida."""
        conditions = [TransactionEvent.instance_id == instance_id]
        if open_only:
            conditions.append(TransactionEvent.closed_at.is_(None))
        if database_name is not None:
            conditions.append(TransactionEvent.database_name == database_name)
        if since is not None:
            conditions.append(
                sa.or_(TransactionEvent.closed_at.is_(None), TransactionEvent.closed_at >= since)
            )
        if until is not None:
            conditions.append(TransactionEvent.begin_time <= until)
        stmt = sa.select(TransactionEvent).where(*conditions)
        stmt = (
            stmt.order_by(sa.asc(TransactionEvent.begin_time).nulls_last())
            if open_only
            else stmt.order_by(sa.desc(TransactionEvent.last_seen_at))
        )
        stmt = stmt.limit(limit)
        return list((await self._session.execute(stmt)).scalars().all())

    async def get_by_identifier(
        self, instance_id: UUID, transaction_identifier: str
    ) -> TransactionEvent | None:
        """Resolve tanto aberta quanto ja encerrada (retida) — o detalhe de uma transacao
        continua acessivel apos ela fechar, ate a janela de retencao expirar."""
        stmt = sa.select(TransactionEvent).where(
            TransactionEvent.instance_id == instance_id,
            TransactionEvent.transaction_identifier == transaction_identifier,
        )
        return (await self._session.execute(stmt)).scalar_one_or_none()

    async def tag_open_transactions_by_session(
        self, instance_id: UUID, session_ids: list[str], tag: str
    ) -> None:
        """Correlacao assincrona com deadlock (`app/workers/deadlock_supervisor.py`): quando um
        novo deadlock e persistido, adiciona `tag` (ex. `"deadlock"`) a `flag_reason` de qualquer
        transacao AINDA ABERTA cujo `session_id` bata com a vitima/sobrevivente do deadlock —
        melhor esforco, sem problema se a transacao ja tiver fechado (o `deadlock_events` em si e
        o registro durável, nao depende desta correlacao para existir)."""
        if not session_ids:
            return
        rows = (
            (
                await self._session.execute(
                    sa.select(TransactionEvent).where(
                        TransactionEvent.instance_id == instance_id,
                        TransactionEvent.closed_at.is_(None),
                        TransactionEvent.session_id.in_(session_ids),
                    )
                )
            )
            .scalars()
            .all()
        )
        for row in rows:
            merged = _merge_tags(row.flag_reason, tag)
            if merged != row.flag_reason:
                row.flag_reason = merged
        await self._session.commit()

    async def purge_closed_before(self, cutoff: datetime) -> int:
        """Expurgo por idade (`app/workers/problem_events_retention_worker.py`) — so linhas JA
        ENCERRADAS ha mais tempo que `cutoff`; uma transacao ainda aberta nunca e removida,
        nao importa ha quanto tempo esta aberta (a orfa mais antiga e exatamente o caso mais
        importante de manter visivel)."""
        result = await self._session.execute(
            sa.delete(TransactionEvent).where(
                TransactionEvent.closed_at.is_not(None), TransactionEvent.closed_at < cutoff
            )
        )
        await self._session.commit()
        return cast(sa.CursorResult, result).rowcount or 0
