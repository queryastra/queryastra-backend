"""report_execution_logs

Revision ID: f2a4c6e8b0d1
Revises: c4d6e8f2a0b1, d3e4f5a6b1c2
Create Date: 2026-10-07 12:00:00.000000

Cria a tabela `report_execution_logs` para histórico de execuções de relatórios agendados.
`report_id` usa ON DELETE SET NULL para preservar o histórico ao deletar o relatório.
Merge das duas heads anteriores (c4d6e8f2a0b1, d3e4f5a6b1c2) numa única head.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f2a4c6e8b0d1"
down_revision: str | Sequence[str] | None = ("c4d6e8f2a0b1", "d3e4f5a6b1c2")
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "report_execution_logs",
        sa.Column("id", sa.Integer(), autoincrement=True, nullable=False),
        sa.Column("report_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("report_title", sa.Text(), nullable=False),
        sa.Column("report_type", sa.Text(), nullable=False),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("instance_name", sa.Text(), nullable=True),
        sa.Column(
            "executed_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("status", sa.Text(), nullable=False),
        sa.Column("error", sa.Text(), nullable=True),
        sa.Column(
            "email_recipients",
            postgresql.ARRAY(sa.Text()),
            server_default="{}",
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["report_id"],
            ["scheduled_reports.id"],
            ondelete="SET NULL",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_report_execution_logs_executed_at",
        "report_execution_logs",
        ["executed_at"],
    )
    op.create_index(
        "ix_report_execution_logs_report_id",
        "report_execution_logs",
        ["report_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_report_execution_logs_report_id", table_name="report_execution_logs")
    op.drop_index("ix_report_execution_logs_executed_at", table_name="report_execution_logs")
    op.drop_table("report_execution_logs")
