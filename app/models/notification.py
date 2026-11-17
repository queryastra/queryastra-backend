"""Notificacoes in-app por usuario (sino do TopNav).

Criadas por `app.services.notification_service.NotificationService` (assinante do `event_bus`)
quando um alerta e criado ou escala de severidade — uma linha por usuario com acesso a instancia
do alerta. Podadas apos 30 dias pelo `NotificationRetentionWorker`.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    String,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Notification(Base):
    __tablename__ = "notifications"
    __table_args__ = (
        CheckConstraint(
            "severity IN ('info', 'warning', 'critical')", name="ck_notifications_severity"
        ),
        Index("ix_notifications_user_id_created_at", "user_id", "created_at"),
        Index(
            "ix_notifications_user_id_unread",
            "user_id",
            postgresql_where=text("read_at IS NULL"),
        ),
        Index("ix_notifications_created_at", "created_at"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"), nullable=False
    )
    # ex.: "alert_created", "alert_escalated"
    kind: Mapped[str] = mapped_column(String(64), nullable=False)
    severity: Mapped[str] = mapped_column(String(16), nullable=False, default="info")
    title: Mapped[str] = mapped_column(Text, nullable=False)
    body: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Caminho do frontend (ex.: "/instances/<id>/alerts") — nunca URL absoluta.
    link: Mapped[str | None] = mapped_column(Text, nullable=True)
    instance_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=True
    )
    alert_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("alerts.id", ondelete="SET NULL"), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    read_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
