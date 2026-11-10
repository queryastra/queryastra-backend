"""Seasonal anomaly detection: hourly rollup, per-metric status, anomaly events, rule_type

Revision ID: b3c4d5e6f7a8
Revises: a9b0c1d2e3f4
Create Date: 2026-11-10
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "b3c4d5e6f7a8"
down_revision: str | None = "a9b0c1d2e3f4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_RULE_TYPES_OLD = (
    "rule_type IN ('threshold', 'anomaly', 'custom_sql', 'blocking_recurrence', "
    "'plan_compile_timeout')"
)
_RULE_TYPES_NEW = (
    "rule_type IN ('threshold', 'anomaly', 'custom_sql', 'blocking_recurrence', "
    "'plan_compile_timeout', 'seasonal_anomaly')"
)


def upgrade() -> None:
    op.drop_constraint("ck_alert_rules_rule_type", "alert_rules", type_="check")
    op.create_check_constraint("ck_alert_rules_rule_type", "alert_rules", _RULE_TYPES_NEW)

    op.create_table(
        "anomaly_metric_hourly",
        sa.Column(
            "instance_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("instances.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("metric_key", sa.String(), primary_key=True),
        sa.Column("bucket", sa.DateTime(timezone=True), primary_key=True),
        sa.Column("value", sa.Float(), nullable=False),
        sa.Column("sample_count", sa.Integer(), server_default="0", nullable=False),
    )

    op.create_table(
        "anomaly_metric_status",
        sa.Column(
            "instance_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("instances.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column("metric_key", sa.String(), primary_key=True),
        sa.Column("unit", sa.String(), nullable=False),
        sa.Column("evaluated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("current_value", sa.Float(), nullable=True),
        sa.Column("expected_value", sa.Float(), nullable=True),
        sa.Column("expected_low", sa.Float(), nullable=True),
        sa.Column("expected_high", sa.Float(), nullable=True),
        sa.Column("z_score", sa.Float(), nullable=True),
        sa.Column("status", sa.String(), nullable=False),
        sa.Column("baseline_kind", sa.String(), nullable=True),
        sa.Column("baseline_samples", sa.Integer(), server_default="0", nullable=False),
        sa.Column("consecutive_anomalous", sa.Integer(), server_default="0", nullable=False),
        sa.Column("consecutive_normal", sa.Integer(), server_default="0", nullable=False),
        sa.CheckConstraint(
            "status IN ('normal', 'warning', 'critical', 'insufficient_data')",
            name="ck_anomaly_metric_status_status",
        ),
    )

    op.create_table(
        "anomaly_events",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            primary_key=True,
        ),
        sa.Column(
            "instance_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("instances.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("metric_key", sa.String(), nullable=False),
        sa.Column("unit", sa.String(), nullable=False),
        sa.Column("direction", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("state", sa.String(), server_default="open", nullable=False),
        sa.Column("first_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_detected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("closed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("observed_value", sa.Float(), nullable=False),
        sa.Column("expected_value", sa.Float(), nullable=False),
        sa.Column("expected_low", sa.Float(), nullable=False),
        sa.Column("expected_high", sa.Float(), nullable=False),
        sa.Column("peak_z_score", sa.Float(), nullable=False),
        sa.Column("baseline_kind", sa.String(), nullable=False),
        sa.Column(
            "alert_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("alerts.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.CheckConstraint("direction IN ('high', 'low')", name="ck_anomaly_events_direction"),
        sa.CheckConstraint(
            "severity IN ('warning', 'critical')", name="ck_anomaly_events_severity"
        ),
        sa.CheckConstraint("state IN ('open', 'closed')", name="ck_anomaly_events_state"),
    )
    op.create_index(
        "ix_anomaly_events_instance_state", "anomaly_events", ["instance_id", "state"]
    )
    op.create_index(
        "ix_anomaly_events_last_detected_at", "anomaly_events", ["last_detected_at"]
    )


def downgrade() -> None:
    op.drop_index("ix_anomaly_events_last_detected_at", table_name="anomaly_events")
    op.drop_index("ix_anomaly_events_instance_state", table_name="anomaly_events")
    op.drop_table("anomaly_events")
    op.drop_table("anomaly_metric_status")
    op.drop_table("anomaly_metric_hourly")
    op.execute(
        "DELETE FROM alert_notifications WHERE alert_id IN (SELECT a.id FROM alerts a "
        "JOIN alert_rules r ON r.id = a.alert_rule_id WHERE r.rule_type = 'seasonal_anomaly')"
    )
    op.execute(
        "DELETE FROM alerts WHERE alert_rule_id IN "
        "(SELECT id FROM alert_rules WHERE rule_type = 'seasonal_anomaly')"
    )
    op.execute("DELETE FROM alert_rules WHERE rule_type = 'seasonal_anomaly'")
    op.drop_constraint("ck_alert_rules_rule_type", "alert_rules", type_="check")
    op.create_check_constraint("ck_alert_rules_rule_type", "alert_rules", _RULE_TYPES_OLD)
