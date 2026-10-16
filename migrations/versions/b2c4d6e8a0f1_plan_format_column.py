"""plan_format — suporte a planos de múltiplos motores (PostgreSQL JSON, MySQL JSON, Oracle)

Revision ID: b2c4d6e8a0f1
Revises: f8b2e4a1c3d7
Create Date: 2026-10-15 12:00:00.000000

Adiciona coluna `plan_format` em `query_plan_snapshots` para distinguir o formato do plano
armazenado em `plan_xml_masked` (nome mantido por compatibilidade com SQL Server). Valores:
  - 'sqlserver_xml'    — XML ShowPlanXML (SQL Server, default para linhas existentes)
  - 'postgresql_json'  — JSON de EXPLAIN (FORMAT JSON) do PostgreSQL/CockroachDB/Aurora PG/Redshift
  - 'mysql_json'       — JSON de EXPLAIN FORMAT=JSON do MySQL/MariaDB/Aurora MySQL
  - 'oracle_text'      — Tabela texto formatada a partir de V$SQL_PLAN (Oracle)
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b2c4d6e8a0f1"
down_revision: str | None = "e6f7a8b9c0d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "query_plan_snapshots",
        sa.Column(
            "plan_format",
            sa.String(30),
            nullable=False,
            server_default="sqlserver_xml",
        ),
    )


def downgrade() -> None:
    op.drop_column("query_plan_snapshots", "plan_format")
