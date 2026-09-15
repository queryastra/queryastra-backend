"""deadlock_events: database_name + detail (full process/resource graph)

Revision ID: d3a7b9c1e6f4
Revises: c7a1f3d9e5b2
Create Date: 2026-09-15 20:00:00.000000

A pedido explicito do usuario: gravar TODAS as informacoes do deadlock (nao so vitima+1
sobrevivente) - todos os processos envolvidos (sessao, banco, transacao, comando, pilha de
execucao/procedures) e todos os recursos em disputa (tipo, objeto, quem segurava/esperava).
`detail` (JSONB, mesmo padrao ja usado por `missing_index_suggestions`/`backup_status`) guarda
esse grafo completo; `database_name` fica como coluna propria (melhor esforco - banco do
primeiro processo com `currentdbname` disponivel) para permitir filtro/exibicao direta sem
precisar abrir o JSON.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "d3a7b9c1e6f4"
down_revision: str | None = "c7a1f3d9e5b2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("deadlock_events", sa.Column("database_name", sa.Text(), nullable=True))
    op.add_column("deadlock_events", sa.Column("detail", postgresql.JSONB(), nullable=True))


def downgrade() -> None:
    op.drop_column("deadlock_events", "detail")
    op.drop_column("deadlock_events", "database_name")
