"""Repositorio de times (`teams`, `team_members`, `team_instance_access`).

Ver `app/models/team.py`. A resolucao de acesso efetivo (direto OU via time) fica em
`app.repositories.user_instance_access_repository`; aqui so o CRUD de times e membros.
"""

from __future__ import annotations

from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.models.team import Team, TeamInstanceAccess, TeamMember
from app.models.user import User
from app.repositories.base import BaseRepository


class TeamsRepository(BaseRepository):
    async def list_all(self) -> list[Team]:
        result = await self._session.execute(select(Team).order_by(Team.name))
        return list(result.scalars().all())

    async def get_by_id(self, team_id: int) -> Team | None:
        return await self._session.get(Team, team_id)

    async def get_by_name(self, name: str) -> Team | None:
        result = await self._session.execute(
            select(Team).where(sa.func.lower(Team.name) == name.strip().lower())
        )
        return result.scalar_one_or_none()

    async def create(self, name: str, description: str | None) -> Team:
        """A sessao deve ser commitada pelo chamador."""
        team = Team(name=name.strip(), description=description)
        self._session.add(team)
        await self._session.flush()
        await self._session.refresh(team)
        return team

    async def update(self, team: Team, *, name: str | None, description: str | None) -> Team:
        if name is not None:
            team.name = name.strip()
        team.description = description
        await self._session.flush()
        await self._session.refresh(team)
        return team

    async def delete(self, team_id: int) -> bool:
        team = await self._session.get(Team, team_id)
        if team is None:
            return False
        await self._session.delete(team)
        await self._session.flush()
        return True

    # --- membros ---------------------------------------------------------------------------------

    async def list_member_ids(self, team_id: int) -> list[int]:
        result = await self._session.execute(
            select(TeamMember.user_id)
            .where(TeamMember.team_id == team_id)
            .order_by(TeamMember.added_at)
        )
        return list(result.scalars().all())

    async def member_ids_by_team(self, team_ids: list[int]) -> dict[int, list[int]]:
        if not team_ids:
            return {}
        result = await self._session.execute(
            select(TeamMember.team_id, TeamMember.user_id).where(TeamMember.team_id.in_(team_ids))
        )
        mapping: dict[int, list[int]] = {team_id: [] for team_id in team_ids}
        for team_id, user_id in result.all():
            mapping[team_id].append(user_id)
        return mapping

    async def add_member(self, team_id: int, user_id: int) -> None:
        """Idempotente."""
        stmt = (
            pg_insert(TeamMember)
            .values(team_id=team_id, user_id=user_id)
            .on_conflict_do_nothing(index_elements=["team_id", "user_id"])
        )
        await self._session.execute(stmt)

    async def remove_member(self, team_id: int, user_id: int) -> bool:
        result = await self._session.execute(
            sa.delete(TeamMember)
            .where(TeamMember.team_id == team_id, TeamMember.user_id == user_id)
            .returning(TeamMember.user_id)
        )
        return result.first() is not None

    async def teams_by_user(self, user_ids: list[int]) -> dict[int, list[Team]]:
        """Times de cada usuario — usado pela tela de usuarios (coluna "Times")."""
        if not user_ids:
            return {}
        result = await self._session.execute(
            select(TeamMember.user_id, Team)
            .join(Team, Team.id == TeamMember.team_id)
            .where(TeamMember.user_id.in_(user_ids))
            .order_by(Team.name)
        )
        mapping: dict[int, list[Team]] = {user_id: [] for user_id in user_ids}
        for user_id, team in result.all():
            mapping[user_id].append(team)
        return mapping

    async def list_member_emails(self, team_id: int) -> list[str]:
        """E-mails dos membros ATIVOS do time com e-mail cadastrado (canal de alerta)."""
        result = await self._session.execute(
            select(User.email)
            .join(TeamMember, TeamMember.user_id == User.id)
            .where(
                TeamMember.team_id == team_id,
                User.is_active.is_(True),
                User.email.is_not(None),
            )
            .order_by(User.id)
        )
        return [email for email in result.scalars().all() if email]

    # --- acesso a instancias ---------------------------------------------------------------------

    async def list_instance_ids(self, team_id: int) -> list[UUID]:
        result = await self._session.execute(
            select(TeamInstanceAccess.instance_id)
            .where(TeamInstanceAccess.team_id == team_id)
            .order_by(TeamInstanceAccess.granted_at)
        )
        return list(result.scalars().all())

    async def instance_ids_by_team(self, team_ids: list[int]) -> dict[int, list[UUID]]:
        if not team_ids:
            return {}
        result = await self._session.execute(
            select(TeamInstanceAccess.team_id, TeamInstanceAccess.instance_id).where(
                TeamInstanceAccess.team_id.in_(team_ids)
            )
        )
        mapping: dict[int, list[UUID]] = {team_id: [] for team_id in team_ids}
        for team_id, instance_id in result.all():
            mapping[team_id].append(instance_id)
        return mapping

    async def grant_instance(self, team_id: int, instance_id: UUID) -> None:
        """Idempotente."""
        stmt = (
            pg_insert(TeamInstanceAccess)
            .values(team_id=team_id, instance_id=instance_id)
            .on_conflict_do_nothing(index_elements=["team_id", "instance_id"])
        )
        await self._session.execute(stmt)

    async def revoke_instance(self, team_id: int, instance_id: UUID) -> bool:
        result = await self._session.execute(
            sa.delete(TeamInstanceAccess)
            .where(
                TeamInstanceAccess.team_id == team_id,
                TeamInstanceAccess.instance_id == instance_id,
            )
            .returning(TeamInstanceAccess.team_id)
        )
        return result.first() is not None
