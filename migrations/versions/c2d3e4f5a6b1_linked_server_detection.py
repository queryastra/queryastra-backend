"""add linked server detection to blocking_incidents

Revision ID: c2d3e4f5a6b1
Revises: b1c2d3e4f5a6
Create Date: 2026-10-06

`blocker_linked_instance_id` / `blocked_linked_instance_id`: UUID da instancia monitorada que
originou a sessao bloqueadora/bloqueada quando `blocker_machine`/`blocked_machine` bate com o
`host` de outra instancia cadastrada (linked server detection). FK → instances.id, ON DELETE
SET NULL: se a instancia for removida do cadastro, o campo vira NULL automaticamente.
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import UUID

revision = "c2d3e4f5a6b1"
# Merge: depends on both add_new_db_types (b1c2d3) AND the migration that creates
# blocking_incidents (c7a1f3). Without this, Alembic runs c2 before c7 on fresh DBs.
down_revision = ("b1c2d3e4f5a6", "c7a1f3d9e5b2")
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "blocking_incidents",
        sa.Column("blocker_linked_instance_id", UUID(as_uuid=True), nullable=True),
    )
    op.add_column(
        "blocking_incidents",
        sa.Column("blocked_linked_instance_id", UUID(as_uuid=True), nullable=True),
    )
    op.create_foreign_key(
        "fk_blocking_incidents_blocker_linked_instance",
        "blocking_incidents",
        "instances",
        ["blocker_linked_instance_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "fk_blocking_incidents_blocked_linked_instance",
        "blocking_incidents",
        "instances",
        ["blocked_linked_instance_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "fk_blocking_incidents_blocker_linked_instance", "blocking_incidents", type_="foreignkey"
    )
    op.drop_constraint(
        "fk_blocking_incidents_blocked_linked_instance", "blocking_incidents", type_="foreignkey"
    )
    op.drop_column("blocking_incidents", "blocker_linked_instance_id")
    op.drop_column("blocking_incidents", "blocked_linked_instance_id")
