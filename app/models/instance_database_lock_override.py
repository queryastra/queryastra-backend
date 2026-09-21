"""Modelo `InstanceDatabaseLockOverride` — override ad-hoc do "Tempo mínimo para captura de
Bloqueios (Locks)" por BANCO DE DADOS dentro de uma instância — pedido explícito do usuário,
redesenho da tela "Configurações → Geral" (item A): o override por instância inteira
(`instances.min_lock_time_ms`, opção "Todos" na UI) já existia; este é um nível mais granular,
opcional, só para um banco específico dentro da instância selecionada.

Uma linha só existe quando há um override ATIVO pra aquele (instância, banco) — sem linha =
usa o override da instância (`instances.min_lock_time_ms`) se houver, senão o valor global
(`system_settings.blocking_min_duration_ms`). Ver precedência completa em
`app/workers/polling_worker.py`.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, Text, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class InstanceDatabaseLockOverride(Base):
    """Override ad-hoc do tempo mínimo de bloqueio para um banco específico de uma instância."""

    __tablename__ = "instance_database_lock_overrides"
    __table_args__ = (
        UniqueConstraint(
            "instance_id",
            "database_name",
            name="uq_instance_database_lock_overrides_identity",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    database_name: Mapped[str] = mapped_column(Text, nullable=False)
    min_lock_time_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    # `func.now()` no INSERT e a cada UPDATE — sem coluna `created_at` separada, isto e um valor
    # ad-hoc de investigacao (quando foi definido/alterado por ultimo), nao um log de historico.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
