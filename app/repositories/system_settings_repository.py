"""Repositorio de `system_settings` — linha singleton de preferencias globais editaveis.

`get()` cria a linha com valores padrao (`blocking_min_duration_ms=0`) se ela ainda nao existir —
defesa simples para um banco onde a migration correu mas, por algum motivo, a linha seed nao
existe (ou um teste que so aplica o schema, sem rodar `INSERT` de dados). Evita exigir um passo
de seed separado do resto do fluxo.
"""

from __future__ import annotations

from sqlalchemy.exc import IntegrityError

from app.models.system_settings import SYSTEM_SETTINGS_SINGLETON_ID, SystemSettings
from app.repositories.base import BaseRepository


class SystemSettingsRepository(BaseRepository):
    async def get(self) -> SystemSettings:
        row = await self._session.get(SystemSettings, SYSTEM_SETTINGS_SINGLETON_ID)
        if row is not None:
            return row

        row = SystemSettings(id=SYSTEM_SETTINGS_SINGLETON_ID, blocking_min_duration_ms=0)
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
