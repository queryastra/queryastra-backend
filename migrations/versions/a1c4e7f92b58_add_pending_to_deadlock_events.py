"""add pending to deadlock_events

Revision ID: a1c4e7f92b58
Revises: f3e8a2c6b9d1
Create Date: 2026-09-22 00:00:00.000000

Deteccao instantanea de deadlock (bug real relatado ao vivo pelo usuario: o `system_health` do
SQL Server demorou 25-35min pra publicar o relatorio completo do deadlock, mesmo tendo
`polling_interval_seconds=1` na instancia) — o `PollingWorker` agora compara o contador
cumulativo `sqlserver_deadlocks_total` (`sys.dm_os_performance_counters`) ciclo a ciclo e, ao
detectar um incremento, insere um registro "pendente" (`pending=true`, so `instance_id`/`time`
preenchidos) na hora em que o incremento e' percebido — bem antes do `DeadlockEventsRepository`
conseguir o relatorio completo (vitima/sobrevivente/SQL/recurso) via `system_health`, que
substitui o registro pendente no lugar (nunca duplica) assim que aparece.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1c4e7f92b58"
down_revision: str | None = "f3e8a2c6b9d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "deadlock_events",
        sa.Column("pending", sa.Boolean(), nullable=False, server_default=sa.false()),
    )


def downgrade() -> None:
    op.drop_column("deadlock_events", "pending")
