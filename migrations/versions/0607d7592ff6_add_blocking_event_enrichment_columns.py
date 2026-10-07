"""add blocking event enrichment columns

Revision ID: 0607d7592ff6
Revises: de1abfb93174
Create Date: 2026-09-14 15:43:36.377425

Fecha o gap real da arvore de bloqueio de nivel unico (Etapa 9 - arvore completa de bloqueio):
ate aqui `blocking_events` so guardava sql_hash de cada lado, obrigando
a tela principal a fazer join de leitura contra `query_samples` (que nem sempre tem amostra
fresca pro bloqueador, especialmente um bloqueador ocioso segurando lock sem request ativa).
Todas as colunas sao aditivas/nullable, sem backfill (eventos antigos simplesmente ficam sem
esse detalhe).

`blocked_wait_resource`/`blocked_object` sao melhor esforco: SQL Server via
`sys.dm_exec_requests.wait_resource` + `sys.dm_tran_locks JOIN sys.partitions`; PostgreSQL via
`pg_locks WHERE NOT granted`. Podem ficar NULL quando o recurso nao e resolvivel (ex. lock de
metadado) ou a linha ja foi liberada entre a consulta de bloqueio e a de recurso no mesmo ciclo.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "0607d7592ff6"
down_revision: str | None = "de1abfb93174"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NEW_COLUMNS = [
    "blocker_db_user",
    "blocker_program",
    "blocker_machine",
    "blocker_wait_event_native",
    "blocked_db_user",
    "blocked_program",
    "blocked_machine",
    "blocked_wait_event_native",
    "blocked_wait_resource",
    "blocked_object",
]


def upgrade() -> None:
    for column_name in _NEW_COLUMNS:
        op.add_column("blocking_events", sa.Column(column_name, sa.Text(), nullable=True))


def downgrade() -> None:
    for column_name in _NEW_COLUMNS:
        op.drop_column("blocking_events", column_name)
