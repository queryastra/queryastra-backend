"""user_instance_access — controle de acesso por instância (RBAC por recurso)

Revision ID: b5c8d2e6f1a3
Revises: a4f7b2e9c1d8
Create Date: 2026-10-01 00:00:00.000000
"""

from __future__ import annotations

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b5c8d2e6f1a3"
down_revision: str | None = "a4f7b2e9c1d8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Admins sempre enxergam todas as instâncias (checado em runtime, não aqui).
    # Para operator/viewer: só as instâncias presentes nesta tabela.
    op.create_table(
        "user_instance_access",
        sa.Column("user_id", sa.Integer(), nullable=False),
        sa.Column("instance_id", sa.UUID(), nullable=False),
        sa.Column(
            "granted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name="fk_uia_user",
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["instance_id"],
            ["instances.id"],
            name="fk_uia_instance",
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("user_id", "instance_id", name="pk_user_instance_access"),
    )
    op.create_index(
        "ix_user_instance_access_user_id",
        "user_instance_access",
        ["user_id"],
    )
    op.create_index(
        "ix_user_instance_access_instance_id",
        "user_instance_access",
        ["instance_id"],
    )


def downgrade() -> None:
    op.drop_index("ix_user_instance_access_instance_id", table_name="user_instance_access")
    op.drop_index("ix_user_instance_access_user_id", table_name="user_instance_access")
    op.drop_table("user_instance_access")
