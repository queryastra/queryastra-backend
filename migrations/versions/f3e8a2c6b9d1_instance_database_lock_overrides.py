"""instance_database_lock_overrides — override ad-hoc de tempo minimo de bloqueio por BANCO

Revision ID: f3e8a2c6b9d1
Revises: d4f6a8c1e9b3
Create Date: 2026-09-21 14:00:00.000000

Redesenho da tela "Configuracoes -> Geral" (item A), a pedido explicito do usuario: alem do
override por instancia inteira (`instances.min_lock_time_ms`, ja existente — opcao "Todos" na
UI), agora e possivel sobrescrever o limite pra um BANCO DE DADOS especifico dentro de uma
instancia, sem afetar os demais bancos da mesma instancia. Uma linha so existe quando ha um
override ATIVO — sem linha, cai pro override da instancia (se houver) e depois pro valor global.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "f3e8a2c6b9d1"
down_revision: str | None = "d4f6a8c1e9b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "instance_database_lock_overrides",
        sa.Column(
            "id",
            postgresql.UUID(as_uuid=True),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("instance_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("database_name", sa.Text(), nullable=False),
        sa.Column("min_lock_time_ms", sa.Integer(), nullable=False),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_instance_database_lock_overrides_instance_id_instances",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name="pk_instance_database_lock_overrides"),
        sa.UniqueConstraint(
            "instance_id",
            "database_name",
            name="uq_instance_database_lock_overrides_identity",
        ),
    )


def downgrade() -> None:
    op.drop_table("instance_database_lock_overrides")
