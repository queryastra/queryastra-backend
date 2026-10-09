"""add_oracle_db2_redshift: expandir ck_instances_db_type

Revision ID: c3d4e5f6a7b8
Revises: b1c2d3e4f5a6
Create Date: 2026-10-07 00:00:00.000000

Expande o CHECK CONSTRAINT de `db_type` em `instances` para suportar Oracle, IBM DB2
e Amazon Redshift.
"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c3d4e5f6a7b8"
down_revision: str | None = "b1c2d3e4f5a6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_NEW_TYPES = (
    "postgresql",
    "sqlserver",
    "mysql",
    "cockroachdb",
    "aurora_postgresql",
    "aurora_mysql",
    "oracle",
    "db2",
    "redshift",
)
_OLD_TYPES = (
    "postgresql",
    "sqlserver",
    "mysql",
    "cockroachdb",
    "aurora_postgresql",
    "aurora_mysql",
)

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
