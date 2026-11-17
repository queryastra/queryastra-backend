"""Modelo `Annotation` — anotacoes de eventos (deploy, mudanca de config, incidente...).

Marcos de tempo exibidos como linhas verticais sobre os graficos de metricas/queries, para
correlacionar uma mudanca de comportamento com o evento que a causou ("desde o deploy v2.3").
`instance_id` nulo = anotacao GLOBAL (vale para todas as instancias, ex.: janela de manutencao
da infraestrutura inteira).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base

ANNOTATION_CATEGORIES: tuple[str, ...] = ("deploy", "config", "incident", "maintenance", "other")


class Annotation(Base):
    """Um evento anotado manualmente por um operador/admin."""

    __tablename__ = "annotations"

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=True
    )
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    title: Mapped[str] = mapped_column(Text, nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(Text, nullable=False, server_default="other")
    created_by_user_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True
    )
    created_by_username: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
