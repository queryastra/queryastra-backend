"""sleeping transaction min duration — parametro global + override por instancia/banco

Revision ID: e2b7c4a8f6d3
Revises: d8e1f4a7c2b6
Create Date: 2026-09-22 13:02:24.000000

Novas tarefas do usuario (itens 1/2): "mostre tb as transacoes em sleeping e que estao abertas ha
mais de x tempo" + "o x do item 1 e um parametro que deve ser criado na tela Configuracoes - Geral,
no mesmo padrao do Tempo minimo para captura de Bloqueios, mas dessa vez, para Transacoes em
Sleeping" — mesma arquitetura de 3 niveis ja usada por `blocking_min_duration_ms`: global
(`system_settings`), override por instancia (`instances.min_sleeping_time_ms`) e override ad-hoc
por banco (`instance_database_sleeping_overrides`, tabela nova).
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "e2b7c4a8f6d3"
down_revision: str | None = "d8e1f4a7c2b6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "system_settings",
        sa.Column("sleeping_min_duration_ms", sa.Integer(), nullable=False, server_default="0"),
    )
    op.add_column(
        "instances",
        sa.Column("min_sleeping_time_ms", sa.Integer(), nullable=True),
    )
    op.create_table(
        "instance_database_sleeping_overrides",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("database_name", sa.Text(), nullable=False),
        sa.Column("min_sleeping_time_ms", sa.Integer(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_instance_database_sleeping_overrides_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_instance_database_sleeping_overrides"),
        sa.UniqueConstraint(
            "instance_id",
            "database_name",
            name="uq_instance_database_sleeping_overrides_identity",
        ),
    )


def downgrade() -> None:
    op.drop_table("instance_database_sleeping_overrides")
    op.drop_column("instances", "min_sleeping_time_ms")
    op.drop_column("system_settings", "sleeping_min_duration_ms")
