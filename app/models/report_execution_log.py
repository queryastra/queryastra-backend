"""Modelo `ReportExecutionLog` — histórico de execuções de relatórios agendados."""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import DateTime, ForeignKey, Integer, Text
from sqlalchemy.dialects.postgresql import ARRAY, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class ReportExecutionLog(Base):
    """Registro imutável de cada execução de um relatório agendado.

    `report_id` é SET NULL ao deletar o relatório — o histórico é preservado.
    `report_title`, `report_type` e `instance_name` são desnormalizados para que o log
    seja legível mesmo após deletar o relatório ou a instância.
    `status` pode ser "ok" ou "error".
    """

    __tablename__ = "report_execution_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    report_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True),
        ForeignKey("scheduled_reports.id", ondelete="SET NULL"),
        nullable=True,
    )
    report_title: Mapped[str] = mapped_column(Text, nullable=False)
    report_type: Mapped[str] = mapped_column(Text, nullable=False)
    instance_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    instance_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    executed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    status: Mapped[str] = mapped_column(Text, nullable=False)
    error: Mapped[str | None] = mapped_column(Text, nullable=True)
    email_recipients: Mapped[list[str]] = mapped_column(
        ARRAY(Text), nullable=False, server_default="{}"
    )
