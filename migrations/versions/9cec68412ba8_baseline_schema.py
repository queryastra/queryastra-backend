"""baseline schema

Revision ID: 9cec68412ba8
Revises:
Create Date: 2026-09-10 00:00:00.000000

NOTA IMPORTANTE (desvio documentado): esta migration foi escrita a mao, espelhando
exatamente `app/models/*.py`, em vez de gerada por `alembic revision --autogenerate`. O
ambiente de desenvolvimento nao tinha um TimescaleDB/PostgreSQL acessivel com credencial
conhecida para este projeto (ha um servico `postgresql-x64-18` local, mas sem credencial
fornecida para esta tarefa — nunca adivinhar/forcar senha de um servico, ver
Docs/rules/01-credential-security.md). Quando um Postgres de desenvolvimento com
`DATABASE_URL` valido estiver disponivel, esta migration pode ser substituida por uma gerada
via autogenerate para confirmar que bate 1:1 com os modelos.

Cobre apenas as tabelas relacionais simples do MVP (Docs/database-schema.md §1 e §4):
Instance, InstanceMaintenanceWindow, Server, InstanceServer, AlertRule, Alert,
AlertNotification, AlertSilence. NAO cria hypertables/continuous aggregates
(metric_samples, query_samples, blocking_events, deadlock_events, sql_exclusions,
*_hourly, *_daily) — essas exigem a extensao TimescaleDB de verdade e ficam para uma
migration dedicada futura (ver app/repositories/metric_samples_repository.py).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "9cec68412ba8"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "instances",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("db_type", sa.String(), nullable=False),
        sa.Column("host", sa.String(), nullable=False),
        sa.Column("port", sa.Integer(), nullable=False),
        sa.Column("cloud_provider", sa.String(), nullable=True),
        sa.Column("engine_version", sa.String(), nullable=True),
        sa.Column("engine_version_major", sa.Integer(), nullable=True),
        sa.Column("engine_support_tier", sa.String(), nullable=True),
        sa.Column("criticality", sa.String(), server_default="standard", nullable=False),
        sa.Column("polling_interval_seconds", sa.Integer(), server_default="10", nullable=False),
        sa.Column("auth_method", sa.String(), nullable=False),
        sa.Column("encrypted_credential_ref", sa.String(), nullable=True),
        sa.Column("active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("db_type IN ('postgresql', 'sqlserver')", name="ck_instances_db_type"),
        sa.CheckConstraint(
            "cloud_provider IN ('aws_rds', 'azure', 'gcp') OR cloud_provider IS NULL",
            name="ck_instances_cloud_provider",
        ),
        sa.CheckConstraint(
            "engine_support_tier IN ('formal', 'degraded', 'unsupported') "
            "OR engine_support_tier IS NULL",
            name="ck_instances_engine_support_tier",
        ),
        sa.CheckConstraint(
            "criticality IN ('critical', 'standard')", name="ck_instances_criticality"
        ),
        sa.CheckConstraint(
            "auth_method IN ('integrated', 'encrypted_fallback')",
            name="ck_instances_auth_method",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_instances"),
    )

    op.create_table(
        "instance_maintenance_windows",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("day_of_week", sa.SmallInteger(), nullable=False),
        sa.Column("start_time", sa.Time(), nullable=False),
        sa.Column("end_time", sa.Time(), nullable=False),
        sa.Column("suppress_polling", sa.Boolean(), server_default="false", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "day_of_week BETWEEN 0 AND 6", name="ck_maintenance_windows_day_of_week"
        ),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_instance_maintenance_windows_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_instance_maintenance_windows"),
    )

    op.create_table(
        "servers",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("hostname", sa.String(), nullable=False),
        sa.Column("os_type", sa.String(), nullable=False),
        sa.Column("exporter_endpoint", sa.String(), nullable=False),
        sa.Column("criticality", sa.String(), server_default="standard", nullable=False),
        sa.Column("active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint("os_type IN ('linux', 'windows')", name="ck_servers_os_type"),
        sa.CheckConstraint(
            "criticality IN ('critical', 'standard')", name="ck_servers_criticality"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_servers"),
    )

    op.create_table(
        "instance_servers",
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("server_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_instance_servers_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["server_id"],
            ["servers.id"],
            name="fk_instance_servers_server_id_servers",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("instance_id", "server_id", name="pk_instance_servers"),
    )

    op.create_table(
        "alert_rules",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("target_type", sa.String(), nullable=False),
        sa.Column("metric_name", sa.String(), nullable=True),
        sa.Column("comparison", sa.String(), nullable=True),
        sa.Column("threshold_warning", sa.Float(), nullable=True),
        sa.Column("threshold_critical", sa.Float(), nullable=True),
        sa.Column("active", sa.Boolean(), server_default="true", nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.CheckConstraint(
            "target_type IN ('server', 'instance')", name="ck_alert_rules_target_type"
        ),
        sa.CheckConstraint(
            "comparison IN ('gt', 'lt', 'gte', 'lte') OR comparison IS NULL",
            name="ck_alert_rules_comparison",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_alert_rules"),
    )

    op.create_table(
        "alerts",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("alert_rule_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("target_type", sa.String(), nullable=False),
        sa.Column("target_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("state", sa.String(), server_default="open", nullable=False),
        sa.Column(
            "triggered_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("resolved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_reminder_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.CheckConstraint("severity IN ('warning', 'critical')", name="ck_alerts_severity"),
        sa.CheckConstraint("state IN ('open', 'acknowledged', 'closed')", name="ck_alerts_state"),
        sa.ForeignKeyConstraint(
            ["alert_rule_id"], ["alert_rules.id"], name="fk_alerts_alert_rule_id_alert_rules"
        ),
        sa.PrimaryKeyConstraint("id", name="pk_alerts"),
    )
    op.create_index("ix_alerts_target_id_state", "alerts", ["target_id", "state"])

    op.create_table(
        "alert_notifications",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("alert_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel", sa.String(), nullable=False),
        sa.Column(
            "sent_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.CheckConstraint(
            "channel IN ('email', 'webhook')", name="ck_alert_notifications_channel"
        ),
        sa.ForeignKeyConstraint(
            ["alert_id"],
            ["alerts.id"],
            name="fk_alert_notifications_alert_id_alerts",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_alert_notifications"),
    )

    op.create_table(
        "alert_silences",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("target_type", sa.String(), nullable=False),
        sa.Column("target_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("alert_rule_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column(
            "silenced_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False
        ),
        sa.Column("silenced_until", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reason", sa.Text(), nullable=True),
        sa.Column("created_by", sa.String(), nullable=True),
        sa.CheckConstraint(
            "target_type IN ('server', 'instance')", name="ck_alert_silences_target_type"
        ),
        sa.ForeignKeyConstraint(
            ["alert_rule_id"],
            ["alert_rules.id"],
            name="fk_alert_silences_alert_rule_id_alert_rules",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_alert_silences"),
    )


def downgrade() -> None:
    op.drop_table("alert_silences")
    op.drop_table("alert_notifications")
    op.drop_index("ix_alerts_target_id_state", table_name="alerts")
    op.drop_table("alerts")
    op.drop_table("alert_rules")
    op.drop_table("instance_servers")
    op.drop_table("servers")
    op.drop_table("instance_maintenance_windows")
    op.drop_table("instances")
