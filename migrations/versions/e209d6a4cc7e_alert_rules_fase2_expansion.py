"""alert rules fase2 expansion

Revision ID: e209d6a4cc7e
Revises: a1b2c3d4e5f6
Create Date: 2026-09-17 12:10:08.073638

Fase 2, itens 1-5 (Docs/product-vision-and-roadmap.md): colunas novas em `alert_rules` (todas
nullable ou com server_default - aditivo, nao quebra nenhuma regra ja existente) para os novos
`rule_type` (anomaly/custom_sql/blocking_recurrence/plan_compile_timeout), reset_mode e
recorrencia; `alerts.last_escalation_level` para o escalonamento em niveis; nova tabela
`alert_escalation_levels`.

Escrita a mao (nao autogerada) - `alembic revision --autogenerate` detectou um monte de ruido
nao relacionado (drop/recreate de metric_samples/query_samples/blocking_events/sql_exclusions,
que sao Core Table em app/repositories/_tables.py, fora do Base.metadata do env.py, e mudancas
de tipo TIMESTAMPTZ->DateTime em varias tabelas) - mesmo descompasso ja conhecido entre
metadata ORM e as hypertables Core deste projeto, sem relacao com esta mudanca.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "e209d6a4cc7e"
down_revision: str | None = "a1b2c3d4e5f6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "alert_rules",
        sa.Column("rule_type", sa.String(), server_default="threshold", nullable=False),
    )
    op.add_column("alert_rules", sa.Column("baseline_window_days", sa.Integer(), nullable=True))
    op.add_column("alert_rules", sa.Column("custom_sql", sa.Text(), nullable=True))
    op.add_column(
        "alert_rules",
        sa.Column("reset_mode", sa.String(), server_default="auto_when_false", nullable=False),
    )
    op.add_column("alert_rules", sa.Column("reset_after_seconds", sa.Integer(), nullable=True))
    op.add_column(
        "alert_rules", sa.Column("recurrence_threshold_count", sa.Integer(), nullable=True)
    )
    op.add_column("alert_rules", sa.Column("recurrence_window_days", sa.Integer(), nullable=True))
    op.create_check_constraint(
        "ck_alert_rules_rule_type",
        "alert_rules",
        "rule_type IN ('threshold', 'anomaly', 'custom_sql', 'blocking_recurrence', "
        "'plan_compile_timeout')",
    )
    op.create_check_constraint(
        "ck_alert_rules_reset_mode",
        "alert_rules",
        "reset_mode IN ('auto_when_false', 'auto_after_duration', 'no_reset_every_poll', "
        "'no_reset_manual')",
    )

    op.add_column(
        "alerts",
        sa.Column("last_escalation_level", sa.Integer(), server_default="0", nullable=False),
    )

    op.create_table(
        "alert_escalation_levels",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("alert_rule_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("level_number", sa.Integer(), nullable=False),
        sa.Column("wait_minutes", sa.Integer(), nullable=False),
        sa.Column("email_recipients", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.CheckConstraint("level_number >= 2", name="ck_alert_escalation_levels_level_number"),
        sa.ForeignKeyConstraint(["alert_rule_id"], ["alert_rules.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )


def downgrade() -> None:
    op.drop_table("alert_escalation_levels")
    op.drop_column("alerts", "last_escalation_level")
    op.drop_constraint("ck_alert_rules_reset_mode", "alert_rules", type_="check")
    op.drop_constraint("ck_alert_rules_rule_type", "alert_rules", type_="check")
    op.drop_column("alert_rules", "recurrence_window_days")
    op.drop_column("alert_rules", "recurrence_threshold_count")
    op.drop_column("alert_rules", "reset_after_seconds")
    op.drop_column("alert_rules", "reset_mode")
    op.drop_column("alert_rules", "custom_sql")
    op.drop_column("alert_rules", "baseline_window_days")
    op.drop_column("alert_rules", "rule_type")
