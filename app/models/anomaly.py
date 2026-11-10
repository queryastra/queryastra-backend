"""Deteccao automatica de anomalias sazonais por instancia.

- `AnomalyMetricHourly`: rollup horario compacto de cada metrica derivada (CPU, sessoes, duracao
  de consulta, waits, throughput). Base do baseline "normal deste dia da semana e hora" — barato de
  consultar e sobrevive a retencao das hypertables brutas.
- `AnomalyMetricStatus`: ultima avaliacao de cada metrica (1 linha por instancia/metrica) + os
  contadores de persistencia (quantas avaliacoes seguidas anomalas/normais) que decidem abrir e
  fechar um evento — persistidos para nao perder o estado num restart do backend.
- `AnomalyEvent`: episodio de anomalia (aberto quando persiste, fechado quando volta ao normal).
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, Integer, String
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class AnomalyMetricHourly(Base):
    __tablename__ = "anomaly_metric_hourly"

    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), primary_key=True
    )
    metric_key: Mapped[str] = mapped_column(String, primary_key=True)
    bucket: Mapped[datetime] = mapped_column(DateTime(timezone=True), primary_key=True)
    value: Mapped[float] = mapped_column(nullable=False)
    sample_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")


class AnomalyMetricStatus(Base):
    __tablename__ = "anomaly_metric_status"
    __table_args__ = (
        CheckConstraint(
            "status IN ('normal', 'warning', 'critical', 'insufficient_data')",
            name="ck_anomaly_metric_status_status",
        ),
    )

    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), primary_key=True
    )
    metric_key: Mapped[str] = mapped_column(String, primary_key=True)
    unit: Mapped[str] = mapped_column(String, nullable=False)
    evaluated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    current_value: Mapped[float | None] = mapped_column(nullable=True)
    expected_value: Mapped[float | None] = mapped_column(nullable=True)
    expected_low: Mapped[float | None] = mapped_column(nullable=True)
    expected_high: Mapped[float | None] = mapped_column(nullable=True)
    z_score: Mapped[float | None] = mapped_column(nullable=True)
    status: Mapped[str] = mapped_column(String, nullable=False)
    baseline_kind: Mapped[str | None] = mapped_column(String, nullable=True)
    baseline_samples: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    consecutive_anomalous: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    consecutive_normal: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")


class AnomalyEvent(Base):
    __tablename__ = "anomaly_events"
    __table_args__ = (
        CheckConstraint("direction IN ('high', 'low')", name="ck_anomaly_events_direction"),
        CheckConstraint("severity IN ('warning', 'critical')", name="ck_anomaly_events_severity"),
        CheckConstraint("state IN ('open', 'closed')", name="ck_anomaly_events_state"),
        Index("ix_anomaly_events_instance_state", "instance_id", "state"),
        Index("ix_anomaly_events_last_detected_at", "last_detected_at"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    instance_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("instances.id", ondelete="CASCADE"), nullable=False
    )
    metric_key: Mapped[str] = mapped_column(String, nullable=False)
    unit: Mapped[str] = mapped_column(String, nullable=False)
    direction: Mapped[str] = mapped_column(String, nullable=False)
    severity: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False, server_default="open")
    first_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    last_detected_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    closed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Valores no instante de maior desvio (pico) do episodio.
    observed_value: Mapped[float] = mapped_column(nullable=False)
    expected_value: Mapped[float] = mapped_column(nullable=False)
    expected_low: Mapped[float] = mapped_column(nullable=False)
    expected_high: Mapped[float] = mapped_column(nullable=False)
    peak_z_score: Mapped[float] = mapped_column(nullable=False)
    baseline_kind: Mapped[str] = mapped_column(String, nullable=False)
    alert_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("alerts.id", ondelete="SET NULL"), nullable=True
    )
