"""Modelo `QueryPlanSnapshot` — historico de plano de execucao (item priorizado pelo usuario).

Tabela relacional INSERT-ONLY (mesmo padrao de `DeadlockEvent`): cada linha e um plano DISTINTO
visto para um (`instance_id`, `sql_hash`) durante a varredura periodica do cache
(`app/workers/plan_history_supervisor.py`, so SQL Server). So grava quando `plan_hash` muda em
relacao ao ultimo snapshot conhecido para o mesmo par — `QueryPlanSnapshotsRepository.
insert_if_changed` faz essa deduplicacao. Retida ate `settings.problem_events_retention_days`
via `app/workers/problem_events_retention_worker.py`.

Sempre plano CACHEADO/estimado (nunca "actual" — ver docstring de
`app.normalizers.models.NormalizedPlanSnapshot`). `plan_xml_masked` ja vem mascarado
(`mask_showplan_xml_literals`) antes de persistir — regra de privacidade do projeto.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, String, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class QueryPlanSnapshot(Base):
    """Um plano distinto visto no cache para uma query, num instante do tempo."""

    __tablename__ = "query_plan_snapshots"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    sql_hash: Mapped[str] = mapped_column(Text, nullable=False)
    plan_hash: Mapped[str] = mapped_column(Text, nullable=False)
    plan_xml_masked: Mapped[str] = mapped_column(Text, nullable=False)
    plan_format: Mapped[str] = mapped_column(
        String(30), nullable=False, server_default="sqlserver_xml"
    )
    last_execution_time: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    captured_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
