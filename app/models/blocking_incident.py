"""Modelo `BlockingIncident` — Etapa 10, redesenho (log de incidente de bloqueio retido).

`blocking_events` (hypertable TimescaleDB, particionada por `time` — `app/repositories/
_tables.py`) continua existindo INALTERADA como log bruto insert-only por ciclo — util pra quem
quiser consultar o historico bruto por SQL direto, mas nao suporta upsert-in-place por chave de
negocio sem incluir `time` numa constraint unica (o que impediria rastrear um `started_at` de
verdade). Esta tabela e a versao "resumida por incidente" dela: uma linha por par (`instance_id`,
`blocker_session_id`, `blocked_session_id`), upsertada a cada ciclo (mesmo padrao de
`TransactionEvent`) — `started_at` (primeira vez observado), `last_seen_at` (ultima vez), e
`ended_at` (preenchido quando o par some do conjunto de bloqueios abertos deste ciclo, NUNCA
deletado — so expurgado por idade via `app/workers/problem_events_retention_worker.py`).

Identidade por `(instance_id, blocker_session_id, blocked_session_id)` e melhor esforco:
`session_id` pode ser reciclado pelo motor ao longo de uma janela de retencao de dias — mesma
ressalva ja documentada para `session_id` como identidade nao-permanente na tela de detalhe de
sessao.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import BigInteger, DateTime, ForeignKey, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class BlockingIncident(Base):
    """Ciclo de vida de um par (bloqueador, bloqueado) observado — ver docstring do modulo."""

    __tablename__ = "blocking_incidents"
    __table_args__ = (
        UniqueConstraint(
            "instance_id",
            "blocker_session_id",
            "blocked_session_id",
            name="uq_blocking_incidents_identity",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    blocker_session_id: Mapped[str] = mapped_column(Text, nullable=False)
    blocked_session_id: Mapped[str] = mapped_column(Text, nullable=False)
    blocker_sql_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_sql_hash: Mapped[str | None] = mapped_column(Text, nullable=True)
    wait_time_ms: Mapped[int] = mapped_column(BigInteger, nullable=False)
    blocker_db_user: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocker_program: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocker_machine: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocker_wait_event_native: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_db_user: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_program: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_machine: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_wait_event_native: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_wait_resource: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_object: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocker_transaction_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_transaction_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocker_database_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocked_database_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    blocker_transaction_begin_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    blocked_transaction_begin_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    blocker_linked_instance_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="SET NULL"), nullable=True
    )
    blocked_linked_instance_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="SET NULL"), nullable=True
    )
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    ended_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
