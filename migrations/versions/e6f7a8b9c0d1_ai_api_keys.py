"""ai_api_keys — gerenciamento de multiplas chaves de IA com failover e rastreio de tokens

Revision ID: e6f7a8b9c0d1
Revises: d5e6f7a8b9c0
Create Date: 2026-10-14 11:00:00.000000

Tabela `ai_api_keys`: armazena multiplas chaves de API de IA (BYOK) para failover automatico.
A chave ativa de menor `priority` e usada primeiro; em caso de erro 429/quota, a proxima chave
habilitada e tentada automaticamente. Tokens consumidos sao contados por chamada bem-sucedida.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
import sqlalchemy.dialects.postgresql as pg

revision = "e6f7a8b9c0d1"
down_revision = "d5e6f7a8b9c0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "ai_api_keys",
        sa.Column("id", pg.UUID(as_uuid=True), primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("name", sa.Text(), nullable=False),
        sa.Column("provider", sa.Text(), nullable=False, server_default=sa.text("'openai'")),
        sa.Column("api_key_encrypted", sa.Text(), nullable=False),
        sa.Column("base_url", sa.Text(), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("is_enabled", sa.Boolean(), nullable=False, server_default=sa.text("true")),
        sa.Column("priority", sa.Integer(), nullable=False, server_default=sa.text("0")),
        sa.Column("expiration_date", sa.Date(), nullable=True),
        sa.Column("token_limit", sa.BigInteger(), nullable=True),
        sa.Column("tokens_used", sa.BigInteger(), nullable=False, server_default=sa.text("0")),
        sa.Column("last_used_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("now()")),
    )
    op.create_index("ix_ai_api_keys_priority_enabled", "ai_api_keys", ["priority", "is_enabled"])


def downgrade() -> None:
    op.drop_index("ix_ai_api_keys_priority_enabled", "ai_api_keys")
    op.drop_table("ai_api_keys")
