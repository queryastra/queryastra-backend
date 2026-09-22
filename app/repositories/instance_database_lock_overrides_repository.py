"""Repositorio de `InstanceDatabaseLockOverride` — ver docstring do modelo
(`app/models/instance_database_lock_override.py`) para o contrato de precedencia."""

from __future__ import annotations

from typing import cast
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.instance_database_lock_override import InstanceDatabaseLockOverride
from app.repositories.base import BaseRepository


class InstanceDatabaseLockOverridesRepository(BaseRepository):
    async def list_for_instance(self, instance_id: UUID) -> list[InstanceDatabaseLockOverride]:
        stmt = sa.select(InstanceDatabaseLockOverride).where(
            InstanceDatabaseLockOverride.instance_id == instance_id
        )
        return list((await self._session.execute(stmt)).scalars().all())

    async def list_all(self) -> list[InstanceDatabaseLockOverride]:
        """Todas as instâncias/bancos com override ativo — usado pelo resumo "Limites ad hoc"
        (Configurações → Geral), pra saber o que já está configurado sem precisar navegar
        instância por instância."""
        stmt = sa.select(InstanceDatabaseLockOverride)
        return list((await self._session.execute(stmt)).scalars().all())

    async def set(
        self, instance_id: UUID, database_name: str, min_lock_time_ms: int
    ) -> InstanceDatabaseLockOverride:
        """Cria ou atualiza (upsert) o override para `(instance_id, database_name)`."""
        insert_stmt = pg_insert(InstanceDatabaseLockOverride).values(
            instance_id=instance_id,
            database_name=database_name,
            min_lock_time_ms=min_lock_time_ms,
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            constraint="uq_instance_database_lock_overrides_identity",
            set_={
                "min_lock_time_ms": insert_stmt.excluded.min_lock_time_ms,
                "updated_at": sa.func.now(),
            },
        ).returning(InstanceDatabaseLockOverride)
        row = (await self._session.execute(upsert_stmt)).scalar_one()
        await self._session.commit()
        return row

    async def clear(self, instance_id: UUID, database_name: str) -> bool:
        result = await self._session.execute(
            sa.delete(InstanceDatabaseLockOverride).where(
                InstanceDatabaseLockOverride.instance_id == instance_id,
                InstanceDatabaseLockOverride.database_name == database_name,
            )
        )
        await self._session.commit()
        return (cast(sa.CursorResult, result).rowcount or 0) > 0

    async def clear_all(self) -> int:
        """Reset em massa (todas as instâncias/bancos) — usado quando o usuário desmarca
        "Limites ad hoc" na UI (Configurações → Geral). Devolve a contagem só para
        telemetria/log."""
        result = await self._session.execute(sa.delete(InstanceDatabaseLockOverride))
        await self._session.commit()
        return cast(sa.CursorResult, result).rowcount or 0
