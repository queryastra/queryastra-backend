"""Modelos de alertas: `AlertRule`, `Alert`, `AlertNotification`, `AlertSilence`.

Conforme `Docs/database-schema.md` §4 — implementa a supressao por estado e o lembrete por
tempo (ARCHITECTURE_SPEC.md Q8) e o silenciamento proativo independente de estado
(`alert_silences`, silenciamento proativo por periodo).

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
    """Regra de alerta: threshold Min/Max (semiaberto) por metrica, ou um dos `rule_type`
    especiais da Fase 2 (anomaly detection, recorrencia de bloqueio, SQL customizado, plano com
    timeout de compilacao) — ver `app.workers.alerting_worker` para a logica de avaliacao de
    cada tipo e `Docs/product-vision-and-roadmap.md` para o desenho completo.
    """

    __tablename__ = "alert_rules"
    __table_args__ = (
        CheckConstraint("target_type IN ('server', 'instance')", name="ck_alert_rules_target_type"),
        CheckConstraint(
            "comparison IN ('gt', 'lt', 'gte', 'lte') OR comparison IS NULL",
            name="ck_alert_rules_comparison",
        ),
        CheckConstraint(
            "rule_type IN ('threshold', 'anomaly', 'custom_sql', 'blocking_recurrence', "
            "'plan_compile_timeout', 'seasonal_anomaly')",
            name="ck_alert_rules_rule_type",
        ),
        CheckConstraint(
            "reset_mode IN ('auto_when_false', 'auto_after_duration', 'no_reset_every_poll', "
            "'no_reset_manual')",
            name="ck_alert_rules_reset_mode",
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

    # --- Fase 2, itens 1-5 (Docs/product-vision-and-roadmap.md) ---------------------------------
    # Discrimina qual metodo de avaliacao o worker usa. `'threshold'` (default) preserva o
    # comportamento de todas as regras ja existentes sem migracao de dado. `freespace`/
    # `replication_lag` (item 2) NAO tem rule_type proprio - sao so um `metric_name` novo,
    # avaliados como `threshold` comum.
    rule_type: Mapped[str] = mapped_column(String, nullable=False, server_default="threshold")
    # Item 1 (anomaly): janela de baseline em dias (media/desvio padrao dos ultimos N dias).
    baseline_window_days: Mapped[int | None] = mapped_column(nullable=True)
    # Item 2 (custom_sql): query validada por `app.services.custom_sql_guard` antes de rodar -
    # nunca persistida sem passar por essa checagem (ver `create_alert_rule` na API).
    custom_sql: Mapped[str | None] = mapped_column(Text, nullable=True)
    # Item 3: modo de reset (4 dos 5 modos suportados - "condicao customizada" fora de escopo).
    reset_mode: Mapped[str] = mapped_column(
        String, nullable=False, server_default="auto_when_false"
    )
    reset_after_seconds: Mapped[int | None] = mapped_column(nullable=True)
    # Item 4 (recorrencia): NULL/NULL = recorrencia desligada para esta regra (default).
    recurrence_threshold_count: Mapped[int | None] = mapped_column(nullable=True)
    recurrence_window_days: Mapped[int | None] = mapped_column(nullable=True)


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
    # Item 3 (escalonamento): ultimo nivel de `AlertEscalationLevel.level_number` ja notificado
    # para este episodio - 0 = nenhum nivel de escalonamento disparado ainda (so o nivel 1
    # implicito, a notificacao normal de `triggered`).
    last_escalation_level: Mapped[int] = mapped_column(nullable=False, server_default="0")


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
    # o historico continuar correto mesmo se a configuracao de notificacao mudar depois.
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


class AlertEscalationLevel(Base):
    """Nivel de escalonamento de uma `AlertRule` (Fase 2, item 3) — nivel 1 e implicito (a
    notificacao normal de `triggered`); cada linha aqui e um nivel 2+ que so dispara se o
    `Alert` continuar `open` (nao reconhecido) por `wait_minutes` desde `triggered_at`.
    `email_recipients=NULL` usa os destinatarios globais do canal e-mail (mesmo remetente/SMTP
    configurado em `Settings`, so uma lista de destinatarios diferente por nivel)."""

    __tablename__ = "alert_escalation_levels"
    __table_args__ = (
        CheckConstraint("level_number >= 2", name="ck_alert_escalation_levels_level_number"),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), primary_key=True, server_default=func.gen_random_uuid()
    )
    alert_rule_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("alert_rules.id", ondelete="CASCADE"), nullable=False
    )
    level_number: Mapped[int] = mapped_column(nullable=False)
    wait_minutes: Mapped[int] = mapped_column(nullable=False)
    email_recipients: Mapped[list[str] | None] = mapped_column(JSONB, nullable=True)
