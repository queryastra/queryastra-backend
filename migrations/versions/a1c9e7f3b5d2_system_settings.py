"""system_settings — preferencias globais editaveis (Configuracoes -> Geral)

Revision ID: a1c9e7f3b5d2
Revises: f8b2e4a1c3d7
Create Date: 2026-09-21 09:00:00.000000

Primeira preferencia global editavel pelo usuario e persistida no banco (nao .env): tempo minimo
de espera (ms) para um par bloqueador/bloqueado ser persistido em blocking_events. Tabela
singleton (1 linha, id fixo em 1) - mais simples que o system_settings generico chave/valor
documentado em Docs/database-schema.md Sec 5 (Fase 2, nunca implementado), ja que hoje so existe
esse unico parametro.
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "a1c9e7f3b5d2"
down_revision: str | None = "f8b2e4a1c3d7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "system_settings",
        sa.Column("id", sa.SmallInteger(), nullable=False),
        sa.Column(
            "blocking_min_duration_ms", sa.Integer(), nullable=False, server_default="0"
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
        sa.CheckConstraint("id = 1", name="ck_system_settings_singleton"),
        sa.PrimaryKeyConstraint("id", name="pk_system_settings"),
    )
    op.execute(
        "INSERT INTO system_settings (id, blocking_min_duration_ms) VALUES (1, 0)"
    )


def downgrade() -> None:
    op.drop_table("system_settings")
