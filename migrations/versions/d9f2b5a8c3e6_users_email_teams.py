"""users.email, times (teams/team_members/team_instance_access), alert_rules.notify_team_id

Revision ID: d9f2b5a8c3e6
Revises: c8e1a4f7b2d5
Create Date: 2026-11-17
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "d9f2b5a8c3e6"
down_revision: str | None = "c8e1a4f7b2d5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("users", sa.Column("email", sa.String(length=320), nullable=True))
    op.create_unique_constraint("uq_users_email", "users", ["email"])

    op.create_table(
        "teams",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("name", sa.String(length=128), nullable=False),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.UniqueConstraint("name", name="uq_teams_name"),
    )
    op.create_table(
        "team_members",
        sa.Column(
            "team_id",
            sa.Integer(),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "user_id",
            sa.Integer(),
            sa.ForeignKey("users.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "added_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("ix_team_members_user_id", "team_members", ["user_id"])
    op.create_table(
        "team_instance_access",
        sa.Column(
            "team_id",
            sa.Integer(),
            sa.ForeignKey("teams.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "instance_id",
            postgresql.UUID(as_uuid=True),
            sa.ForeignKey("instances.id", ondelete="CASCADE"),
            primary_key=True,
        ),
        sa.Column(
            "granted_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_team_instance_access_instance_id", "team_instance_access", ["instance_id"]
    )

    op.add_column(
        "alert_rules",
        sa.Column(
            "notify_team_id",
            sa.Integer(),
            sa.ForeignKey("teams.id", ondelete="SET NULL", name="fk_alert_rules_notify_team_id"),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("alert_rules", "notify_team_id")
    op.drop_index("ix_team_instance_access_instance_id", table_name="team_instance_access")
    op.drop_table("team_instance_access")
    op.drop_index("ix_team_members_user_id", table_name="team_members")
    op.drop_table("team_members")
    op.drop_table("teams")
    op.drop_constraint("uq_users_email", "users", type_="unique")
    op.drop_column("users", "email")
