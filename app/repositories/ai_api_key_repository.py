from __future__ import annotations

import uuid
from datetime import datetime, timezone, timedelta
from typing import Sequence

from cryptography.fernet import Fernet
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.models.ai_api_key import AiApiKey


class AiApiKeyRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    # ── helpers ──────────────────────────────────────────────────────────────

    def _encrypt(self, raw: str) -> str:
        key = get_settings().credential_encryption_key.encode()
        return Fernet(key).encrypt(raw.encode()).decode()

    def _decrypt(self, token: str) -> str:
        key = get_settings().credential_encryption_key.encode()
        return Fernet(key).decrypt(token.encode()).decode()

    def get_plain_api_key(self, row: AiApiKey) -> str:
        return self._decrypt(row.api_key_encrypted)

    # ── reads ─────────────────────────────────────────────────────────────────

    async def list_all(self) -> Sequence[AiApiKey]:
        result = await self._session.execute(
            select(AiApiKey).order_by(AiApiKey.priority, AiApiKey.created_at)
        )
        return result.scalars().all()

    async def get(self, key_id: uuid.UUID) -> AiApiKey | None:
        return await self._session.get(AiApiKey, key_id)

    async def get_enabled_for_failover(self) -> Sequence[AiApiKey]:
        """Retorna chaves habilitadas por prioridade, para uso no failover automático."""
        result = await self._session.execute(
            select(AiApiKey)
            .where(AiApiKey.is_enabled == True)  # noqa: E712
            .order_by(AiApiKey.priority, AiApiKey.created_at)
        )
        return result.scalars().all()

    # ── writes ────────────────────────────────────────────────────────────────

    async def create(
        self,
        *,
        name: str,
        provider: str,
        raw_api_key: str,
        base_url: str | None,
        model: str | None,
        priority: int,
        expiration_date=None,
        token_limit: int | None,
    ) -> AiApiKey:
        row = AiApiKey(
            id=uuid.uuid4(),
            name=name,
            provider=provider,
            api_key_encrypted=self._encrypt(raw_api_key),
            base_url=base_url,
            model=model,
            priority=priority,
            expiration_date=expiration_date,
            token_limit=token_limit,
        )
        self._session.add(row)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def update(
        self,
        row: AiApiKey,
        *,
        name: str | None = None,
        provider: str | None = None,
        raw_api_key: str | None = None,
        base_url: str | None = None,
        model: str | None = None,
        is_enabled: bool | None = None,
        priority: int | None = None,
        expiration_date=None,
        token_limit: int | None = None,
        _clear_expiration: bool = False,
        _clear_token_limit: bool = False,
    ) -> AiApiKey:
        if name is not None:
            row.name = name
        if provider is not None:
            row.provider = provider
        if raw_api_key is not None:
            row.api_key_encrypted = self._encrypt(raw_api_key)
        if base_url is not None:
            row.base_url = base_url
        if model is not None:
            row.model = model
        if is_enabled is not None:
            row.is_enabled = is_enabled
        if priority is not None:
            row.priority = priority
        if _clear_expiration:
            row.expiration_date = None
        elif expiration_date is not None:
            row.expiration_date = expiration_date
        if _clear_token_limit:
            row.token_limit = None
        elif token_limit is not None:
            row.token_limit = token_limit
        row.updated_at = datetime.now(timezone.utc)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def delete(self, row: AiApiKey) -> None:
        await self._session.delete(row)
        await self._session.commit()

    async def reset_tokens(self, row: AiApiKey) -> AiApiKey:
        row.tokens_used = 0
        row.updated_at = datetime.now(timezone.utc)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def reset_error(self, row: AiApiKey) -> AiApiKey:
        row.last_error_at = None
        row.updated_at = datetime.now(timezone.utc)
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def record_success(self, row: AiApiKey, tokens_consumed: int) -> None:
        """Atualiza contadores após chamada bem-sucedida (sem commit extra — chamado no mesmo ciclo)."""
        row.tokens_used = (row.tokens_used or 0) + tokens_consumed
        row.last_used_at = datetime.now(timezone.utc)
        await self._session.commit()

    async def record_error(self, row: AiApiKey) -> None:
        """Marca última falha para o cooldown de 5 minutos no failover."""
        row.last_error_at = datetime.now(timezone.utc)
        await self._session.commit()

    @staticmethod
    def is_in_error_cooldown(row: AiApiKey, cooldown_minutes: int = 5) -> bool:
        if not row.last_error_at:
            return False
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=cooldown_minutes)
        return row.last_error_at > cutoff
