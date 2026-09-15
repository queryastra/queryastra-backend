"""add blocker/blocked database_name to blocking_events

Revision ID: b4d8e6f1a2c9
Revises: 7a3f9c1b2e4d
Create Date: 2026-09-15 15:00:00.000000

A pedido explicito do usuario: mostrar o banco de dados na tela de Bloqueios (e no detalhe de
sessao). Aditivas/nullable, mesmo padrao das demais colunas de enriquecimento de `blocking_events`
(Etapa 9/10) - capturadas no MOMENTO da coleta, nao reconstruidas depois via join.

Lado bloqueado sempre vem de `DB_NAME(r.database_id)` (SQL Server, tem request ativa por
definicao) / `datname` (PostgreSQL, disponivel pra qualquer sessao). Lado bloqueador, quando
ocioso no SQL Server (sem request ativa), cai pro banco resolvido via `blocker_transaction_id` ->
`sys.dm_tran_database_transactions` (mesmo lookup ja usado por `transaction_events`) - `None` so
no caso raro de bloqueador ocioso sem transacao rastreavel. Ver
`app/normalizers/models.py::NormalizedBlockingEvent`.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b4d8e6f1a2c9"
down_revision: str | None = "7a3f9c1b2e4d"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("blocking_events", sa.Column("blocker_database_name", sa.Text(), nullable=True))
    op.add_column("blocking_events", sa.Column("blocked_database_name", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("blocking_events", "blocked_database_name")
    op.drop_column("blocking_events", "blocker_database_name")
