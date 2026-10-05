"""Repositorio de tokens de redefinicao de senha.

O token bruto nunca passa por aqui — apenas o SHA-256 (`token_hash`).
Ver `app/api/v1/auth.py` para a logica de geracao e uso.
"""

from __future__ import annotations

import uuid
from datetime import datetime

from sqlalchemy import select, text, update

from app.models.password_reset_token import PasswordResetToken
from app.repositories.base import BaseRepository


class PasswordResetTokensRepository(BaseRepository):
    """Repositorio de acesso a `password_reset_tokens`."""

    async def create(
        self,
        user_id: int,
        token_hash: str,
        expires_at: datetime,
    ) -> PasswordResetToken:
        """Persiste um novo token (hash). A sessao deve ser commitada pelo chamador."""
        token = PasswordResetToken(
            user_id=user_id,
            token_hash=token_hash,
            expires_at=expires_at,
        )
        self._session.add(token)
        await self._session.flush()
        await self._session.refresh(token)
        return token

    async def get_valid_by_hash(self, token_hash: str) -> PasswordResetToken | None:
        """Retorna o token se existir, nao estiver usado e nao tiver expirado.

        Retorna `None` em qualquer outro caso.
        """
        result = await self._session.execute(
            select(PasswordResetToken).where(
                PasswordResetToken.token_hash == token_hash,
                PasswordResetToken.used_at.is_(None),
                PasswordResetToken.expires_at > text("NOW()"),
            )
        )
        return result.scalar_one_or_none()

    async def mark_used(self, token_id: uuid.UUID) -> None:
        """Marca o token como utilizado (idempotente). A sessao deve ser commitada pelo chamador."""
        await self._session.execute(
            update(PasswordResetToken)
            .where(PasswordResetToken.id == token_id)
            .values(used_at=text("NOW()"))
        )
        await self._session.flush()
