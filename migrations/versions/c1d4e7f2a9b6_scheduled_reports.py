"""scheduled_reports — configuracao de relatorios PDF agendados (Item 4)

Revision ID: c1d4e7f2a9b6
Revises: b5c8d2e6f1a3
Create Date: 2026-10-02 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

revision: str = "c1d4e7f2a9b6"
down_revision: str | None = "b5c8d2e6f1a3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "scheduled_reports",
        sa.Column(
            "id",
            sa.UUID(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", sa.UUID(), nullable=False),
        # Tipo do relatorio: 'top_sql' | 'average_wait' | 'typical_day'
        sa.Column("report_type", sa.Text(), nullable=False),
        sa.Column("title", sa.Text(), nullable=False),
        # Parametros: top_n (int), lookback_days (int)
        sa.Column("params", JSONB(), server_default=sa.text("'{}'::jsonb"), nullable=False),
        # Expressao cron (ex: "0 8 * * 1" = segunda-feira 8h)
        sa.Column("schedule_cron", sa.Text(), nullable=False),
        # Destinatarios de email; vazio = so download manual
        sa.Column(
            "email_recipients",
            sa.ARRAY(sa.Text()),
            server_default=sa.text("'{}'::text[]"),
            nullable=False,
        ),
        sa.Column("is_active", sa.Boolean(), server_default=sa.text("true"), nullable=False),
        # Proximo horario de execucao (calculado ao criar/atualizar)
        sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
        # 'ok' | 'error' | NULL (nunca executado)
        sa.Column("last_run_status", sa.Text(), nullable=True),
        sa.Column("last_run_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_scheduled_reports_instance",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_scheduled_reports"),
    )
    op.create_index(
        "ix_scheduled_reports_instance_id",
        "scheduled_reports",
        ["instance_id"],
    )
    op.create_index(
        "ix_scheduled_reports_next_run_at",
        "scheduled_reports",
        ["next_run_at"],
        postgresql_where=sa.text("is_active = true AND next_run_at IS NOT NULL"),
    )


def downgrade() -> None:
    op.drop_index("ix_scheduled_reports_next_run_at", table_name="scheduled_reports")
    op.drop_index("ix_scheduled_reports_instance_id", table_name="scheduled_reports")
    op.drop_table("scheduled_reports")
