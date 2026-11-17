"""Gestao de times (admin only) — `/auth/teams`.

- GET    /auth/teams                                  — lista times com membros e instancias
- POST   /auth/teams                                  — cria time
- GET    /auth/teams/{team_id}                        — detalhe
- PATCH  /auth/teams/{team_id}                        — renomeia/descricao
- DELETE /auth/teams/{team_id}                        — remove (membros/concessoes em cascata)
- POST   /auth/teams/{team_id}/members/{user_id}      — adiciona membro (idempotente)
- DELETE /auth/teams/{team_id}/members/{user_id}      — remove membro
- POST   /auth/teams/{team_id}/instances/{instance_id} — concede instancia ao time (idempotente)
- DELETE /auth/teams/{team_id}/instances/{instance_id} — revoga instancia do time

Acesso efetivo de um usuario = concessao direta OU via qualquer time (ver
`app.repositories.user_instance_access_repository`).
"""

from __future__ import annotations

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Response, status
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_db_session, require_admin
from app.models.team import Team
from app.repositories.instances_repository import InstancesRepository
from app.repositories.teams_repository import TeamsRepository
from app.repositories.users_repository import UsersRepository
from app.services.audit_service import record_audit

router = APIRouter()


class TeamCreateRequest(BaseModel):
    name: str = Field(min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=1000)


class TeamPatchRequest(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=128)
    description: str | None = Field(default=None, max_length=1000)


class TeamResponse(BaseModel):
    id: int
    name: str
    description: str | None
    created_at: datetime
    member_ids: list[int]
    instance_ids: list[UUID]


def get_teams_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TeamsRepository:
    return TeamsRepository(session)


async def _to_response(repo: TeamsRepository, team: Team) -> TeamResponse:
    return TeamResponse(
        id=team.id,
        name=team.name,
        description=team.description,
        created_at=team.created_at,
        member_ids=await repo.list_member_ids(team.id),
        instance_ids=await repo.list_instance_ids(team.id),
    )


async def _get_team_or_404(repo: TeamsRepository, team_id: int) -> Team:
    team = await repo.get_by_id(team_id)
    if team is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Time nao encontrado.")
    return team


@router.get("", response_model=list[TeamResponse])
async def list_teams(
    _: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
) -> list[TeamResponse]:
    teams = await repo.list_all()
    ids = [t.id for t in teams]
    members = await repo.member_ids_by_team(ids)
    instances = await repo.instance_ids_by_team(ids)
    return [
        TeamResponse(
            id=t.id,
            name=t.name,
            description=t.description,
            created_at=t.created_at,
            member_ids=members.get(t.id, []),
            instance_ids=instances.get(t.id, []),
        )
        for t in teams
    ]


@router.post("", response_model=TeamResponse, status_code=status.HTTP_201_CREATED)
async def create_team(
    payload: TeamCreateRequest,
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TeamResponse:
    if await repo.get_by_name(payload.name) is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Ja existe um time chamado '{payload.name.strip()}'.",
        )
    team = await repo.create(payload.name, payload.description)
    await record_audit(
        session,
        actor=current_admin.username,
        action="team.create",
        target_type="team",
        target_id=str(team.id),
        target_name=team.name,
    )
    await session.commit()
    return await _to_response(repo, team)


@router.get("/{team_id}", response_model=TeamResponse)
async def get_team(
    team_id: int,
    _: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
) -> TeamResponse:
    return await _to_response(repo, await _get_team_or_404(repo, team_id))


@router.patch("/{team_id}", response_model=TeamResponse)
async def patch_team(
    team_id: int,
    payload: TeamPatchRequest,
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TeamResponse:
    team = await _get_team_or_404(repo, team_id)
    if payload.name is not None:
        existing = await repo.get_by_name(payload.name)
        if existing is not None and existing.id != team_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Ja existe um time chamado '{payload.name.strip()}'.",
            )
    team = await repo.update(team, name=payload.name, description=payload.description)
    await record_audit(
        session,
        actor=current_admin.username,
        action="team.update",
        target_type="team",
        target_id=str(team.id),
        target_name=team.name,
    )
    await session.commit()
    return await _to_response(repo, team)


@router.delete("/{team_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_team(
    team_id: int,
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> Response:
    team = await _get_team_or_404(repo, team_id)
    name = team.name
    await repo.delete(team_id)
    await record_audit(
        session,
        actor=current_admin.username,
        action="team.delete",
        target_type="team",
        target_id=str(team_id),
        target_name=name,
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{team_id}/members/{user_id}", response_model=TeamResponse)
async def add_team_member(
    team_id: int,
    user_id: int,
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TeamResponse:
    team = await _get_team_or_404(repo, team_id)
    user = await UsersRepository(session).get_by_id(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao encontrado.")
    await repo.add_member(team_id, user_id)
    await record_audit(
        session,
        actor=current_admin.username,
        action="team.member_added",
        target_type="team",
        target_id=str(team_id),
        target_name=team.name,
        detail={"user_id": user_id, "username": user.username},
    )
    await session.commit()
    return await _to_response(repo, team)


@router.delete("/{team_id}/members/{user_id}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_team_member(
    team_id: int,
    user_id: int,
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> Response:
    team = await _get_team_or_404(repo, team_id)
    if not await repo.remove_member(team_id, user_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Usuario nao e membro deste time."
        )
    await record_audit(
        session,
        actor=current_admin.username,
        action="team.member_removed",
        target_type="team",
        target_id=str(team_id),
        target_name=team.name,
        detail={"user_id": user_id},
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)


@router.post("/{team_id}/instances/{instance_id}", response_model=TeamResponse)
async def grant_team_instance(
    team_id: int,
    instance_id: UUID,
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> TeamResponse:
    team = await _get_team_or_404(repo, team_id)
    if await InstancesRepository(session).get_by_id(instance_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail="Instancia nao encontrada."
        )
    await repo.grant_instance(team_id, instance_id)
    await record_audit(
        session,
        actor=current_admin.username,
        action="team.instance_granted",
        target_type="team",
        target_id=str(team_id),
        target_name=team.name,
        detail={"instance_id": str(instance_id)},
    )
    await session.commit()
    return await _to_response(repo, team)


@router.delete("/{team_id}/instances/{instance_id}", status_code=status.HTTP_204_NO_CONTENT)
async def revoke_team_instance(
    team_id: int,
    instance_id: UUID,
    current_admin: Annotated[CurrentUser, Depends(require_admin)],
    repo: Annotated[TeamsRepository, Depends(get_teams_repository)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> Response:
    team = await _get_team_or_404(repo, team_id)
    if not await repo.revoke_instance(team_id, instance_id):
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Acesso nao encontrado para este time/instancia.",
        )
    await record_audit(
        session,
        actor=current_admin.username,
        action="team.instance_revoked",
        target_type="team",
        target_id=str(team_id),
        target_name=team.name,
        detail={"instance_id": str(instance_id)},
    )
    await session.commit()
    return Response(status_code=status.HTTP_204_NO_CONTENT)
