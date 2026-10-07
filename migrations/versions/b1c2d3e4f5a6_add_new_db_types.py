"""add_new_db_types: mysql, cockroachdb, aurora_postgresql, aurora_mysql

Revision ID: b1c2d3e4f5a6
Revises: ef208ca3271e
Create Date: 2026-10-06 00:00:00.000000

Expande o CHECK CONSTRAINT de `db_type` em `instances` para suportar os novos motores:
mysql, cockroachdb, aurora_postgresql, aurora_mysql.

O constraint e dropado e recriado porque a sintaxe de ALTER CONSTRAINT nao e padrao
no PostgreSQL — o drop+add e idiomatico e seguro em migrations do Alembic.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "b1c2d3e4f5a6"
down_revision: str | None = "ef208ca3271e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NEW_TYPES = (
    "postgresql",
    "sqlserver",
    "mysql",
    "cockroachdb",
    "aurora_postgresql",
    "aurora_mysql",
)
_OLD_TYPES = ("postgresql", "sqlserver")

_CONSTRAINT_NAME = "ck_instances_db_type"
_TABLE_NAME = "instances"


def _db_type_check(types: tuple[str, ...]) -> str:
    quoted = ", ".join(f"'{t}'" for t in types)
    return f"db_type IN ({quoted})"


def upgrade() -> None:
    op.drop_constraint(_CONSTRAINT_NAME, _TABLE_NAME, type_="check")
    op.create_check_constraint(
        _CONSTRAINT_NAME,
        _TABLE_NAME,
        _db_type_check(_NEW_TYPES),
    )


def downgrade() -> None:
    op.drop_constraint(_CONSTRAINT_NAME, _TABLE_NAME, type_="check")
    op.create_check_constraint(
        _CONSTRAINT_NAME,
        _TABLE_NAME,
        _db_type_check(_OLD_TYPES),
    )
