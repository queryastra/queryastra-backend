"""Modelos de alertas: `AlertRule`, `Alert`, `AlertNotification`, `AlertSilence`.

Conforme `Docs/database-schema.md` §4 — implementa a supressao por estado e o lembrete por
tempo (ARCHITECTURE_SPEC.md Q8) e o silenciamento proativo independente de estado
(`alert_silences`, inspirado em `CON_PROBLEM_SILENCE` do Ignite).

Nota de correcao: todas as colunas `datetime` aqui usam `DateTime(timezone=True)` explicito -
`migrations/versions/9cec68412ba8_baseline_schema.py` ja criou as 4 tabelas com
`TIMESTAMP WITH TIME ZONE` (mesmo padrao de `Instance.created_at`/`Server.created_at`), e o
resto do codigo (`app/workers`, `app/api/deps.py`) usa `datetime.now(UTC)` (timezone-aware)
universalmente. Sem o `timezone=True` aqui, o asyncpg rejeita esses valores ao gravar
(`DataError: can't subtract offset-naive and offset-aware datetimes`) — confirmado rodando os
testes de `AlertsRepository` contra o banco real.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import CheckConstraint, DateTime, ForeignKey, Index, String, Text
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.sql import func

from app.db.base import Base


class AlertRule(Base):
    """Regra de alerta: threshold Min/Max (semiaberto) por metrica ou wait time anomaly."""

    __tablename__ = "alert_rules"
    __table_args__ = (
        CheckConstraint("target_type IN ('server', 'instance')", name="ck_alert_rules_target_type"),
        CheckConstraint(
            "comparison IN ('gt', 'lt', 'gte', 'lte') OR comparison IS NULL",
            name="ck_alert_rules_comparison",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    name: Mapped[str] = mapped_column(String, nullable=False)
    target_type: Mapped[str] = mapped_column(String, nullable=False)
    # NULL para regras que nao sao de metrica simples (ex.: wait time anomaly, Fase 2).
    metric_name: Mapped[str | None] = mapped_column(String, nullable=True)
    comparison: Mapped[str | None] = mapped_column(String, nullable=True)
    threshold_warning: Mapped[float | None] = mapped_column(nullable=True)
    threshold_critical: Mapped[float | None] = mapped_column(nullable=True)
    active: Mapped[bool] = mapped_column(nullable=False, server_default="true")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class Alert(Base):
    """Ocorrencia de alerta disparada por uma `AlertRule` para um alvo (instancia/servidor)."""

    __tablename__ = "alerts"
    __table_args__ = (
        CheckConstraint("severity IN ('warning', 'critical')", name="ck_alerts_severity"),
        CheckConstraint("state IN ('open', 'acknowledged', 'closed')", name="ck_alerts_state"),
        # `CREATE INDEX ON alerts (target_id, state);` — Docs/database-schema.md §4.
        Index("ix_alerts_target_id_state", "target_id", "state"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    alert_rule_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("alert_rules.id"), nullable=False
    )
    target_type: Mapped[str] = mapped_column(String, nullable=False)
    target_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    severity: Mapped[str] = mapped_column(String, nullable=False)
    state: Mapped[str] = mapped_column(String, nullable=False, server_default="open")
    triggered_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    acknowledged_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    resolved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Lembrete por tempo para alertas criticos nao reconhecidos (ARCHITECTURE_SPEC.md Q8).
    last_reminder_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    description: Mapped[str | None] = mapped_column(Text, nullable=True)


class AlertNotification(Base):
    """Envio de notificacao de um alerta por um canal (email/webhook)."""

    __tablename__ = "alert_notifications"
    __table_args__ = (
        CheckConstraint("channel IN ('email', 'webhook')", name="ck_alert_notifications_channel"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    alert_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("alerts.id"), nullable=False
    )
    channel: Mapped[str] = mapped_column(String, nullable=False)
    sent_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    # Deve incluir a lista de destinatarios resolvida no momento do envio (nao so o canal), para
    # o historico continuar correto mesmo se a configuracao de notificacao mudar depois — padrao
    # observado em CON_ALERT_HISTORY.CONTACTEES do Ignite.
    payload: Mapped[dict | None] = mapped_column(JSONB, nullable=True)


class AlertSilence(Base):
    """Silenciamento proativo por tempo, independente do estado de qualquer alerta disparado."""

    __tablename__ = "alert_silences"
    __table_args__ = (
        CheckConstraint(
            "target_type IN ('server', 'instance')", name="ck_alert_silences_target_type"
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    target_type: Mapped[str] = mapped_column(String, nullable=False)
    target_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    # NULL = silencia todas as regras desse alvo.
    alert_rule_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("alert_rules.id"), nullable=True
    )
    silenced_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    # NULL = indefinido, ate remocao manual.
    silenced_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_by: Mapped[str | None] = mapped_column(String, nullable=True)
