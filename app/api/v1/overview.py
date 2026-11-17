"""Dashboard de visao geral — um card de saude por instancia acessivel ao usuario.

`GET /overview/instances`: status derivado + alertas, conexoes, sessoes ativas, CPU (quando o
engine expoe), deadlocks na ultima hora e ultima coleta. Leitura em lote, ver
`app.services.instance_overview_service`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import CurrentUser, get_current_user, get_db_session
from app.repositories.instance_overview_repository import InstanceOverviewRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.repositories.user_instance_access_repository import UserInstanceAccessRepository
from app.services.instance_overview_service import InstanceOverviewService

router = APIRouter()


class InstanceOverviewCardResponse(BaseModel):
    id: UUID
    name: str
    db_type: str
    host: str
    port: int
    criticality: str
    active: bool
    health_status: str
    status: str  # up | warning | critical | down | unknown | paused
    connections: float | None
    active_sessions: float | None
    cpu_percent: float | None
    deadlocks_last_hour: int | None
    open_alerts: dict[str, int]
    open_alerts_total: int
    last_collected_at: datetime | None

    model_config = {"from_attributes": True}


class InstancesOverviewResponse(BaseModel):
    generated_at: datetime
    instances: list[InstanceOverviewCardResponse]


@router.get("/instances", response_model=InstancesOverviewResponse)
async def get_instances_overview(
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstancesOverviewResponse:
    instances = await InstancesRepository(session).list_all()
    if not current_user.is_admin:
        accessible = await UserInstanceAccessRepository(session).get_instance_ids_for_user(
            current_user.user_id
        )
        instances = [i for i in instances if i.id in accessible]
    service = InstanceOverviewService(
        InstanceOverviewRepository(session), MetricSamplesRepository(session)
    )
    cards = await service.build(instances)
    return InstancesOverviewResponse(
        generated_at=datetime.now(UTC),
        instances=[InstanceOverviewCardResponse.model_validate(c) for c in cards],
    )
