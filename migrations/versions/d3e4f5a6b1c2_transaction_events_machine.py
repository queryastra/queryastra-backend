"""add machine to transaction_events

Revision ID: d3e4f5a6b1c2
Revises: c2d3e4f5a6b1
Create Date: 2026-10-06

`machine`: hostname/IP da maquina que iniciou a sessao dona da transacao — SQL Server:
`host_name` de `sys.dm_exec_sessions`; PostgreSQL: `client_addr` de `pg_stat_activity`.
Base para linked server detection na tela de Transacoes.
"""

from alembic import op
import sqlalchemy as sa

revision = "d3e4f5a6b1c2"
down_revision = "c2d3e4f5a6b1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("transaction_events", sa.Column("machine", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("transaction_events", "machine")
