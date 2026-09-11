"""add sql_text_full_masked to query_samples

Revision ID: ef208ca3271e
Revises: d5452e5a9c0b
Create Date: 2026-09-11 15:31:26.186905

`sql_text_masked` passa a guardar so o STATEMENT em execucao (nao o batch/procedure inteiro que o
SQL Server as vezes devolve via `sys.dm_exec_sql_text` - ver `Docs/collector-queries.md` §1,
nuance de texto). Esta coluna nova guarda o contexto completo mascarado, usado so na tela de
detalhe da query para destacar o statement dentro do texto inteiro (equivalente ao "negrito" do
SolarWinds DPA). Nullable e sem backfill: amostras antigas simplesmente nao tem contexto completo
(a tela de detalhe cai de volta pro `sql_text_masked` sozinho quando `None` - ver frontend).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "ef208ca3271e"
down_revision: str | None = "d5452e5a9c0b"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("query_samples", sa.Column("sql_text_full_masked", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("query_samples", "sql_text_full_masked")
