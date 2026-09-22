"""Modelo `InstanceDatabaseSleepingOverride` — override ad-hoc do "Tempo mínimo para Transações em
Sleeping" por BANCO DE DADOS dentro de uma instância — mesmo padrão de
`InstanceDatabaseLockOverride` (ver docstring desse modelo para o contrato de precedência
completo), aplicado a um segundo parâmetro independente.

Uma linha só existe quando há um override ATIVO pra aquele (instância, banco) — sem linha = usa o
override da instância (`instances.min_sleeping_time_ms`) se houver, senão o valor global
(`system_settings.sleeping_min_duration_ms`). Ver precedência completa em
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


class InstanceDatabaseSleepingOverride(Base):
    """Override ad-hoc do tempo mínimo de transação em sleeping para um banco específico."""

    __tablename__ = "instance_database_sleeping_overrides"
    __table_args__ = (
        UniqueConstraint(
            "instance_id",
            "database_name",
            name="uq_instance_database_sleeping_overrides_identity",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    database_name: Mapped[str] = mapped_column(Text, nullable=False)
    min_sleeping_time_ms: Mapped[int] = mapped_column(Integer, nullable=False)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )
