"""confirmed transaction error (Etapa 10, item 3)

Revision ID: c3e5a9f2d8b4
Revises: b7c2f4e9a1d6
Create Date: 2026-09-22 13:20:00.000000

Etapa 10, item 3 — captura confiavel de erro por transacao. `transaction_events.last_error_code`
(existente) continua vindo de `sys.dm_exec_requests.prev_error` (melhor esforco, some assim que a
sessao fica idle apos o erro — ver docstring de `_build_transaction_events` no adaptador SQL
Server). As 2 colunas novas guardam o resultado da sessao Extended Events dedicada
(`sqlserver.error_reported`, severidade >= 16, `TransactionErrorSupervisor`) — capturado no
INSTANTE em que o erro e levantado, retido pela sessao XE independente do estado atual da sessao
(resolve a lacuna documentada de `prev_error` desaparecer quando a sessao fica idle).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3e5a9f2d8b4"
down_revision: str | None = "b7c2f4e9a1d6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "transaction_events", sa.Column("confirmed_error_code", sa.Integer(), nullable=True)
    )
    op.add_column(
        "transaction_events", sa.Column("confirmed_error_message_masked", sa.Text(), nullable=True)
    )


def downgrade() -> None:
    op.drop_column("transaction_events", "confirmed_error_message_masked")
    op.drop_column("transaction_events", "confirmed_error_code")
