"""system_settings_ai — configuracao de IA (BYOK) editavel pela UI

Revision ID: d5e6f7a8b9c0
Revises: c4d5e6f7a8b9
Create Date: 2026-10-14 10:00:00.000000

Adiciona campos de configuracao de IA em `system_settings` para que o provider, chave de API
e modelo possam ser configurados pela UI (Configuracoes → IA) em vez de exigir variaveis de
ambiente. A chave de API e armazenada cifrada com Fernet (mesma chave de
`CREDENTIAL_ENCRYPTION_KEY` usada para SMTP). Quando presente, tem precedencia sobre `AI_API_KEY`
do .env.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "d5e6f7a8b9c0"
down_revision = "c4d5e6f7a8b9"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column("system_settings", sa.Column("ai_provider", sa.Text(), nullable=True))
    op.add_column("system_settings", sa.Column("ai_api_key_encrypted", sa.Text(), nullable=True))
    op.add_column("system_settings", sa.Column("ai_base_url", sa.Text(), nullable=True))
    op.add_column("system_settings", sa.Column("ai_model", sa.Text(), nullable=True))


def downgrade() -> None:
    op.drop_column("system_settings", "ai_model")
    op.drop_column("system_settings", "ai_base_url")
    op.drop_column("system_settings", "ai_api_key_encrypted")
    op.drop_column("system_settings", "ai_provider")
