"""Modelo `DeadlockEvent` — Etapa 10, item 7.

Tabela relacional INSERT-ONLY (um deadlock é um evento instantâneo — não tem "abrir/fechar" como
transação/bloqueio) — persistida por um worker de baixa frequência
(`app/workers/deadlock_supervisor.py`, mesmo padrão de `BackupStatusSupervisor`/
`MissingIndexSupervisor`), retida até `settings.problem_events_retention_days` via
`app/workers/problem_events_retention_worker.py`.

Só SQL Server (`sys.dm_xe_session_targets`/ring buffer `system_health`, evento
`xml_deadlock_report`) — PostgreSQL não expõe detalhe de deadlock via SQL, só o contador agregado
`pg_stat_database.deadlocks` (já coletado como métrica). `raw_xml_masked` guarda o XML do
deadlock graph com qualquer texto de query embutido mascarado (`mask_sql_literals`) antes de
persistir — regra de privacidade do projeto.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class DeadlockEvent(Base):
    """Um deadlock reportado pelo motor — vítima escolhida pra rollback e sobrevivente."""

    __tablename__ = "deadlock_events"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    time: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    victim_session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    victim_sql_text_masked: Mapped[str | None] = mapped_column(Text, nullable=True)
    survivor_session_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    survivor_sql_text_masked: Mapped[str | None] = mapped_column(Text, nullable=True)
    resource_description: Mapped[str | None] = mapped_column(Text, nullable=True)
    raw_xml_masked: Mapped[str | None] = mapped_column(Text, nullable=True)
