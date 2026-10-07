"""add cpu_time_ms to query_samples

Revision ID: de1abfb93174
Revises: 4d920ff54af8
Create Date: 2026-09-14 12:25:10.416188

CPU consumida pela request atual (ms), so SQL Server (`sys.dm_exec_requests.cpu_time`) —
PostgreSQL nao expoe um equivalente direto, fica sempre NULL la. Fecha o gap de CPU nao coberto por esperas nomeadas: sem isso,
tempo gasto rodando na CPU sem nenhuma espera ativa nao contava em lugar nenhum (nem no total da
query, nem na decomposicao por wait). Nullable e sem backfill: amostras antigas simplesmente nao
tem esse dado (`effective_cpu_ms` cai pra 0 quando `cpu_time_ms IS NULL`, ver
`QuerySamplesRepository._EFFECTIVE_WAIT_SELECT`).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "de1abfb93174"
down_revision: str | None = "4d920ff54af8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("query_samples", sa.Column("cpu_time_ms", sa.BigInteger(), nullable=True))


def downgrade() -> None:
    op.drop_column("query_samples", "cpu_time_ms")
