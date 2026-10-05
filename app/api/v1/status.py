"""Visao agregada de status: servidores -> instancias, rollup pai-filho (Item 16).

8 estados mapeados de SAM/Orion: down, critical, warning, unknown, up (os mais relevantes aqui).
Rollup: status do servidor = pior status dentre suas instancias filhas.
"""

from __future__ import annotations

from typing import Annotated, Any
from uuid import UUID

import sqlalchemy as sa
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user, get_db_session
from app.models.instance import Instance
from app.models.server import InstanceServer, Server
from app.repositories.alerts_repository import AlertsRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.services.instance_derived_status_service import InstanceDerivedStatusService

router = APIRouter()

# Ordem de severidade para rollup — menor numero = pior
_STATUS_PRIORITY: dict[str, int] = {
    "down": 0,
    "critical": 1,
    "warning": 2,
    "unknown": 3,
    "up": 4,
}


def _aggregate_status(health_status: str, worst_alert_severity: str | None) -> str:
    """Converte os campos derivados de coleta + alertas nos 5 estados do painel de status."""
    if health_status == "unavailable":
        return "down"
    if worst_alert_severity == "critical":
        return "critical"
    if worst_alert_severity == "warning":
        return "warning"
    if health_status == "healthy":
        return "up"
    return "unknown"


def _worst_status(statuses: list[str]) -> str:
    if not statuses:
        return "unknown"
    return min(statuses, key=lambda s: _STATUS_PRIORITY.get(s, 3))


class InstanceStatusItem(BaseModel):
    id: UUID
    name: str
    db_type: str
    host: str
    port: int
    active: bool
    health_status: str
    worst_alert_severity: str | None
    aggregate_status: str
    open_alert_count: int
    last_collected_at: str | None


class ServerStatusItem(BaseModel):
    id: UUID
    hostname: str
    os_type: str
    criticality: str
    aggregate_status: str
    instances: list[InstanceStatusItem]


class StatusOverviewResponse(BaseModel):
    servers: list[ServerStatusItem]
    unassigned: list[InstanceStatusItem]


@router.get("/overview", response_model=StatusOverviewResponse)
async def get_status_overview(
    _: Annotated[Any, Depends(get_current_user)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> StatusOverviewResponse:
    """Retorna todas as instancias agrupadas por servidor, com rollup de status.

    - Servidores sao ordenados pelo pior status (down primeiro, up por ultimo).
    - Instancias dentro de cada servidor idem.
    - Instancias sem vinculo a nenhum servidor aparecem em `unassigned`.
    """
    instances_repo = InstancesRepository(session)
    alerts_repo = AlertsRepository(session)
    metric_repo = MetricSamplesRepository(session)
    status_svc = InstanceDerivedStatusService(metric_repo)

    all_instances = await instances_repo.list_all()
    if not all_instances:
        return StatusOverviewResponse(servers=[], unassigned=[])

    instance_ids = [i.id for i in all_instances]

    open_alert_counts = await alerts_repo.count_open_alerts_by_target(instance_ids)
    worst_severities = await alerts_repo.get_worst_severity_by_instance_ids(instance_ids)

    # Derived status por instancia (N queries — mesmo padrao aceito no MVP em list_instances)
    derived_map: dict[UUID, Any] = {}
    for inst in all_instances:
        derived_map[inst.id] = await status_svc.compute(inst.id)

    def _make_instance_item(inst: Instance) -> InstanceStatusItem:
        derived = derived_map[inst.id]
        ws = worst_severities.get(inst.id)
        agg = _aggregate_status(derived.health_status, ws)
        lc = derived.last_collected_at
        return InstanceStatusItem(
            id=inst.id,
            name=inst.name,
            db_type=inst.db_type,
            host=inst.host,
            port=inst.port,
            active=inst.active,
            health_status=derived.health_status,
            worst_alert_severity=ws,
            aggregate_status=agg,
            open_alert_count=open_alert_counts.get(inst.id, 0),
            last_collected_at=lc.isoformat() if lc else None,
        )

    # Busca vinculos instance_servers
    stmt = sa.select(InstanceServer.instance_id, InstanceServer.server_id).where(
        InstanceServer.instance_id.in_(instance_ids)
    )
    rows = (await session.execute(stmt)).all()

    # instance_id -> list of server_ids  (e  server_id -> list of instance_ids)
    server_to_instances: dict[UUID, list[UUID]] = {}
    assigned_instance_ids: set[UUID] = set()
    for row in rows:
        server_to_instances.setdefault(row.server_id, []).append(row.instance_id)
        assigned_instance_ids.add(row.instance_id)

    instance_by_id = {i.id: i for i in all_instances}

    # Busca servidores referenciados
    server_ids = list(server_to_instances.keys())
    servers: list[ServerStatusItem] = []
    if server_ids:
        srv_stmt = sa.select(Server).where(Server.id.in_(server_ids))
        server_rows = (await session.execute(srv_stmt)).scalars().all()
        for server in server_rows:
            child_ids = server_to_instances.get(server.id, [])
            child_items = [
                _make_instance_item(instance_by_id[cid])
                for cid in child_ids
                if cid in instance_by_id
            ]
            child_items.sort(key=lambda it: _STATUS_PRIORITY.get(it.aggregate_status, 3))
            agg = _worst_status([it.aggregate_status for it in child_items])
            servers.append(
                ServerStatusItem(
                    id=server.id,
                    hostname=server.hostname,
                    os_type=server.os_type,
                    criticality=server.criticality,
                    aggregate_status=agg,
                    instances=child_items,
                )
            )
        servers.sort(key=lambda s: _STATUS_PRIORITY.get(s.aggregate_status, 3))

    unassigned = [
        _make_instance_item(instance_by_id[iid])
        for iid in instance_ids
        if iid not in assigned_instance_ids
    ]
    unassigned.sort(key=lambda it: _STATUS_PRIORITY.get(it.aggregate_status, 3))

    return StatusOverviewResponse(servers=servers, unassigned=unassigned)
