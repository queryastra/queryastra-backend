"""Modelo `TransactionEvent` — Etapa 10 (itens 2/3), rastreamento de transacoes abertas.

Ao contrario das hypertables de coleta continua, esta e uma tabela relacional comum, atualizada
por UPSERT — cada linha e o estado mais recente de UMA transacao aberta (`instance_id`,
`transaction_identifier`). Diferente de `MissingIndexSuggestion`/`BackupStatus` (worker de baixa
frequencia, 1x/dia), esta e atualizada a cada ciclo do `PollingWorker` de alta frequencia — uma
transacao orfa/esquecida precisa ser vista com a mesma cadencia da coleta normal, nao 1x/dia.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class TransactionEvent(Base):
    """Log de incidente de uma transacao "digna de nota" (bloqueio, erro, aberta por tempo
    suspeito ou envolvida em deadlock — ver `app.collectors.transaction_flagging`), retido para
    analise posterior ate `settings.problem_events_retention_days`
    (`app/workers/problem_events_retention_worker.py`) — NAO um espelho de toda transacao aberta
    (redesenho pos-uso real; modelo anterior reconciliava/deletava a cada ciclo, o que gerava
    escrita desnecessaria e fazia a transacao sumir da tela antes de dar tempo de investigar).

    `transaction_identifier`: SQL Server usa `sys.dm_tran_session_transactions.transaction_id`
    (texto); PostgreSQL nao tem o conceito de transacao nomeada separada da sessao — usa o
    proprio `pid` do backend. `transaction_type`/`transaction_uow` so fazem sentido para SQL
    Server (`transaction_uow` so preenchido quando a transacao e uma distribuida promovida pelo
    MSDTC — ver `Docs/collector-queries.md`). `last_error_code` e melhor esforco
    (`sys.dm_exec_requests.prev_error`), so existe enquanto a sessao ainda tem uma request ativa
    — objetivamente ausente exatamente no caso "a transacao ja ficou idle depois do erro".

    `closed_at`: `NULL` enquanto a transacao ainda esta aberta; preenchido (e nunca mais limpo)
    quando ela some do conjunto de transacoes abertas de um ciclo. `flag_reason`: tags
    combinadas separadas por virgula (`"bloqueio"`, `"erro"`, `"orfa"`, `"deadlock"`) explicando
    por que esta linha existe — uniao de todos os motivos ja vistos, nunca sobrescrita.
    """

    __tablename__ = "transaction_events"
    __table_args__ = (
        UniqueConstraint(
            "instance_id",
            "transaction_identifier",
            name="uq_transaction_events_identity",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    transaction_identifier: Mapped[str] = mapped_column(Text, nullable=False)
    session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    database_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    transaction_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    transaction_uow: Mapped[str | None] = mapped_column(Text, nullable=True)
    begin_time: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    state: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_error_code: Mapped[int | None] = mapped_column(Integer, nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    flag_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
