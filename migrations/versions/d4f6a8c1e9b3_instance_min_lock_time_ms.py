"""instances.min_lock_time_ms — override ad-hoc do tempo minimo de bloqueio por instancia

Revision ID: d4f6a8c1e9b3
Revises: a1c9e7f3b5d2
Create Date: 2026-09-21 11:00:00.000000

A pedido explicito do usuario: alem do parametro GLOBAL ja implementado
(`system_settings.blocking_min_duration_ms`, "Configuracoes -> Geral"), permite sobrescrever o
limite so para UMA instancia especifica (ex.: investigar um caso concreto com um cliente sem
afetar as demais instancias monitoradas). NULL (padrao) = usa o valor global.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "d4f6a8c1e9b3"
down_revision: str | None = "a1c9e7f3b5d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("instances", sa.Column("min_lock_time_ms", sa.Integer(), nullable=True))


def downgrade() -> None:
    op.drop_column("instances", "min_lock_time_ms")
