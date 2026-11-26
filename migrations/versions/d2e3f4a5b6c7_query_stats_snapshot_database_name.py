"""add database_name to query_stats_snapshot

O `pg_stat_statements` e do cluster todo: a mesma tela (Saude do BD → Tendencias de queries)
mistura queries de varios bancos. Guarda o banco de cada linha (`pg_database.datname` do
`dbid`) para mostrar na tabela. Nullable: snapshots antigos ficam sem banco.

Revisao: d2e3f4a5b6c7
Anterior: f7a8b9c0d1e2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "d2e3f4a5b6c7"
down_revision: str | None = "f7a8b9c0d1e2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("query_stats_snapshot", sa.Column("database_name", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("query_stats_snapshot", "database_name")
