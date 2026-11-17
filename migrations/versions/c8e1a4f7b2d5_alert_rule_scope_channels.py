"""alert_rules: escopo por instancia, modo de avaliacao (taxa), canais por regra;
system_settings: provedor de e-mail (smtp | brevo)

Revision ID: c8e1a4f7b2d5
Revises: b3c4d5e6f7a8
Create Date: 2026-11-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c8e1a4f7b2d5"
down_revision: str | None = "b3c4d5e6f7a8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "alert_rules",
        sa.Column(
            "instance_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("instances.id", ondelete="CASCADE", name="fk_alert_rules_instance_id"),
            nullable=True,
        ),
    )
    op.create_index("ix_alert_rules_instance_id", "alert_rules", ["instance_id"])
    op.add_column(
        "alert_rules",
        sa.Column("evaluation_mode", sa.String(), nullable=False, server_default="value"),
    )
    op.create_check_constraint(
        "ck_alert_rules_evaluation_mode",
        "alert_rules",
        "evaluation_mode IN ('value', 'rate_per_minute')",
    )
    op.add_column(
        "alert_rules",
        sa.Column("notify_emails", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
    )
    op.add_column("alert_rules", sa.Column("webhook_url", sa.Text(), nullable=True))

    op.add_column(
        "system_settings",
        sa.Column("email_provider", sa.Text(), nullable=False, server_default="smtp"),
    )
    op.create_check_constraint(
        "ck_system_settings_email_provider",
        "system_settings",
        "email_provider IN ('smtp', 'brevo')",
    )
    op.add_column("system_settings", sa.Column("brevo_api_key_encrypted", sa.Text(), nullable=True))
    op.add_column("system_settings", sa.Column("email_from_name", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("system_settings", "email_from_name")
    op.drop_column("system_settings", "brevo_api_key_encrypted")
    op.drop_constraint("ck_system_settings_email_provider", "system_settings", type_="check")
    op.drop_column("system_settings", "email_provider")

    op.drop_column("alert_rules", "webhook_url")
    op.drop_column("alert_rules", "notify_emails")
    op.drop_constraint("ck_alert_rules_evaluation_mode", "alert_rules", type_="check")
    op.drop_column("alert_rules", "evaluation_mode")
    op.drop_index("ix_alert_rules_instance_id", table_name="alert_rules")
    op.drop_column("alert_rules", "instance_id")
