"""Repositorio de `system_settings` — linha singleton de preferencias globais editaveis.

`get()` cria a linha com valores padrao (`blocking_min_duration_ms=0`) se ela ainda nao existir —
defesa simples para um banco onde a migration correu mas, por algum motivo, a linha seed nao
existe (ou um teste que so aplica o schema, sem rodar `INSERT` de dados). Evita exigir um passo
de seed separado do resto do fluxo.
"""

from __future__ import annotations

from cryptography.fernet import Fernet
from sqlalchemy.exc import IntegrityError

from app.core.config import get_settings
from app.models.system_settings import SYSTEM_SETTINGS_SINGLETON_ID, SystemSettings
from app.repositories.base import BaseRepository


class SystemSettingsRepository(BaseRepository):
    async def get(self) -> SystemSettings:
        row = await self._session.get(SystemSettings, SYSTEM_SETTINGS_SINGLETON_ID)
        if row is not None:
            return row

        row = SystemSettings(
            id=SYSTEM_SETTINGS_SINGLETON_ID,
            blocking_min_duration_ms=0,
            sleeping_min_duration_ms=0,
        )
        self._session.add(row)
        try:
            await self._session.commit()
        except IntegrityError:
            # Corrida rara (2 chamadores tentando criar a linha seed ao mesmo tempo) — quem
            # perdeu so relê o que o outro ja criou.
            await self._session.rollback()
            existing = await self._session.get(SystemSettings, SYSTEM_SETTINGS_SINGLETON_ID)
            if existing is not None:
                return existing
            raise
        await self._session.refresh(row)
        return row

    async def update_blocking_min_duration_ms(self, value: int) -> SystemSettings:
        row = await self.get()
        row.blocking_min_duration_ms = value
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def update_sleeping_min_duration_ms(self, value: int) -> SystemSettings:
        row = await self.get()
        row.sleeping_min_duration_ms = value
        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def update_smtp_settings(
        self,
        host: str | None,
        port: int,
        username: str | None,
        raw_password: str | None,
        from_address: str | None,
    ) -> SystemSettings:
        """Atualiza config SMTP. raw_password=None preserva a senha já armazenada."""
        row = await self.get()
        row.smtp_host = host
        row.smtp_port = port
        row.smtp_username = username
        row.smtp_from_address = from_address
        if raw_password is not None:
            key = get_settings().credential_encryption_key.encode()
            row.smtp_password_encrypted = Fernet(key).encrypt(raw_password.encode()).decode()
        await self._session.commit()
        await self._session.refresh(row)
        return row

    def get_smtp_password_plain(self, row: SystemSettings) -> str | None:
        """Decifra a senha SMTP armazenada. Retorna None se não configurada."""
        if not row.smtp_password_encrypted:
            return None
        key = get_settings().credential_encryption_key.encode()
        return Fernet(key).decrypt(row.smtp_password_encrypted.encode()).decode()
