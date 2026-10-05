"""Repositório de controle de acesso por instância (RBAC por recurso).

Admins enxergam todas as instâncias — checado no nível da dependency FastAPI, não aqui.
Para operator/viewer: só as instâncias presentes nesta tabela são acessíveis.
"""

from __future__ import annotations

from uuid import UUID

import sqlalchemy as sa

from app.repositories.base import BaseRepository

_TABLE = sa.table(
    "user_instance_access",
    sa.column("user_id", sa.Integer()),
    sa.column("instance_id", sa.UUID()),
    sa.column("granted_at", sa.DateTime(timezone=True)),
)


class UserInstanceAccessRepository(BaseRepository):
    """Repositório de `user_instance_access`."""

    async def get_instance_ids_for_user(self, user_id: int) -> set[UUID]:
        """Retorna o conjunto de instance_id acessíveis por um usuário.

        Para admins, a chamada nem é feita (checado no dep).
        """
        stmt = sa.select(_TABLE.c.instance_id).where(_TABLE.c.user_id == user_id)
        result = await self._session.execute(stmt)
        return {row[0] for row in result.fetchall()}

    async def has_access(self, user_id: int, instance_id: UUID) -> bool:
        """Retorna True se o usuário tem acesso explícito a esta instância."""
        stmt = sa.select(sa.literal(1)).where(
            _TABLE.c.user_id == user_id,
            _TABLE.c.instance_id == instance_id,
        )
        result = await self._session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def grant(self, user_id: int, instance_id: UUID) -> None:
        """Concede acesso a uma instância. Idempotente (ON CONFLICT DO NOTHING)."""
        stmt = sa.text(
            """
            INSERT INTO user_instance_access (user_id, instance_id)
            VALUES (:user_id, :instance_id)
            ON CONFLICT (user_id, instance_id) DO NOTHING
            """
        )
        await self._session.execute(stmt, {"user_id": user_id, "instance_id": str(instance_id)})

    async def revoke(self, user_id: int, instance_id: UUID) -> bool:
        """Remove acesso a uma instância. Retorna True se existia."""
        stmt = (
            sa.delete(_TABLE)
            .where(_TABLE.c.user_id == user_id, _TABLE.c.instance_id == instance_id)
            .returning(_TABLE.c.instance_id)
        )
        result = await self._session.execute(stmt)
        return result.fetchone() is not None

    async def list_granted_instances(self, user_id: int) -> list[UUID]:
        """Lista os instance_ids concedidos a um usuário, ordenados por granted_at."""
        stmt = (
            sa.select(_TABLE.c.instance_id)
            .where(_TABLE.c.user_id == user_id)
            .order_by(_TABLE.c.granted_at)
        )
        result = await self._session.execute(stmt)
        return [row[0] for row in result.fetchall()]
