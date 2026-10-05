"""system_settings_smtp — configuracao SMTP editavel pela UI

Revision ID: c4d6e8f2a0b1
Revises: b3c5d7e9f1a2
Create Date: 2026-10-05 11:00:00.000000

Adiciona campos de configuracao SMTP em `system_settings` (linha singleton) para que o envio
de e-mail de relatorios possa ser configurado pela UI em vez de exigir edicao manual do .env.
A senha SMTP e armazenada cifrada com Fernet (mesma chave de `CREDENTIAL_ENCRYPTION_KEY`).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "c4d6e8f2a0b1"
down_revision = "b3c5d7e9f1a2"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("system_settings", sa.Column("smtp_host", sa.Text(), nullable=True))
    op.add_column(
        "system_settings",
        sa.Column("smtp_port", sa.SmallInteger(), nullable=False, server_default=sa.text("587")),
    )
    op.add_column("system_settings", sa.Column("smtp_username", sa.Text(), nullable=True))
    op.add_column(
        "system_settings",
        sa.Column("smtp_password_encrypted", sa.Text(), nullable=True),
    )
    op.add_column("system_settings", sa.Column("smtp_from_address", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("system_settings", "smtp_from_address")
    op.drop_column("system_settings", "smtp_password_encrypted")
    op.drop_column("system_settings", "smtp_username")
    op.drop_column("system_settings", "smtp_port")
    op.drop_column("system_settings", "smtp_host")
