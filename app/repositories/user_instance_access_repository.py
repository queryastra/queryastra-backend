"""Repositório de controle de acesso por instância (RBAC por recurso).

Admins enxergam todas as instâncias — checado no nível da dependency FastAPI, não aqui.
Para operator/viewer: acesso efetivo = concessão direta (`user_instance_access`) OU concessão a
qualquer time do qual o usuário é membro (`team_instance_access` via `team_members`).
`get_instance_ids_for_user`/`has_access` aplicam essa união (usados por `check_instance_access`,
`GET /instances`, anomalias, WebSocket e notificações); `list_granted_instances`/`grant`/`revoke`
continuam operando só sobre as concessões DIRETAS (tela de administração de usuários).
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
_TEAM_MEMBERS = sa.table(
    "team_members",
    sa.column("team_id", sa.Integer()),
    sa.column("user_id", sa.Integer()),
)
_TEAM_ACCESS = sa.table(
    "team_instance_access",
    sa.column("team_id", sa.Integer()),
    sa.column("instance_id", sa.UUID()),
)
_USERS = sa.table(
    "users",
    sa.column("id", sa.Integer()),
    sa.column("role", sa.String()),
    sa.column("is_active", sa.Boolean()),
)


def _team_instances_for_user(user_id: int) -> sa.Select:
    return (
        sa.select(_TEAM_ACCESS.c.instance_id)
        .select_from(
            _TEAM_ACCESS.join(_TEAM_MEMBERS, _TEAM_MEMBERS.c.team_id == _TEAM_ACCESS.c.team_id)
        )
        .where(_TEAM_MEMBERS.c.user_id == user_id)
    )


class UserInstanceAccessRepository(BaseRepository):
    """Repositório de `user_instance_access`."""

    async def get_instance_ids_for_user(self, user_id: int) -> set[UUID]:
        """Retorna o conjunto de instance_id acessíveis por um usuário (diretos + times).

        Para admins, a chamada nem é feita (checado no dep).
        """
        stmt = sa.union(
            sa.select(_TABLE.c.instance_id).where(_TABLE.c.user_id == user_id),
            _team_instances_for_user(user_id),
        )
        result = await self._session.execute(stmt)
        return {row[0] for row in result.fetchall()}

    async def has_access(self, user_id: int, instance_id: UUID) -> bool:
        """Retorna True se o usuário tem acesso à instância — direto ou via algum time."""
        direct = sa.select(sa.literal(1)).where(
            _TABLE.c.user_id == user_id,
            _TABLE.c.instance_id == instance_id,
        )
        via_team = _team_instances_for_user(user_id).where(
            _TEAM_ACCESS.c.instance_id == instance_id
        )
        stmt = sa.select(sa.or_(sa.exists(direct), sa.exists(via_team)))
        result = await self._session.execute(stmt)
        return bool(result.scalar_one())

    async def list_user_ids_with_access(self, instance_id: UUID) -> set[int]:
        """Usuários ATIVOS que enxergam a instância: todos os admins + concessões diretas +
        membros de times com concessão. Usado para fan-out de notificações in-app."""
        admins = sa.select(_USERS.c.id).where(
            _USERS.c.role == "admin", _USERS.c.is_active.is_(True)
        )
        direct = sa.select(_TABLE.c.user_id).where(_TABLE.c.instance_id == instance_id)
        via_team = (
            sa.select(_TEAM_MEMBERS.c.user_id)
            .select_from(
                _TEAM_MEMBERS.join(_TEAM_ACCESS, _TEAM_ACCESS.c.team_id == _TEAM_MEMBERS.c.team_id)
            )
            .where(_TEAM_ACCESS.c.instance_id == instance_id)
        )
        granted = sa.union(direct, via_team).subquery()
        active_granted = sa.select(_USERS.c.id).where(
            _USERS.c.is_active.is_(True), _USERS.c.id.in_(sa.select(granted.c.user_id))
        )
        result = await self._session.execute(sa.union(admins, active_granted))
        return {row[0] for row in result.fetchall()}

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
