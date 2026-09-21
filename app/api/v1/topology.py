"""Endpoint do "Mapa de Dependências" — ver docstring de `app.services.topology_service` para o
escopo/limitações (correlação agentless via `query_samples.machine`, sem sonda de rede)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated

from fastapi import APIRouter, Depends, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.repositories.instances_repository import InstancesRepository
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.repositories.query_samples_repository import QuerySamplesRepository
from app.repositories.servers_repository import ServersRepository
from app.services.instance_derived_status_service import InstanceDerivedStatusService
from app.services.topology_service import TopologyService

router = APIRouter()

# Janela padrão generosa (bem maior que o lookback de leitura comum de ~1h) — uma dependência
# real (ex.: um job noturno via linked server) pode só se manifestar poucas vezes por dia.
_DEFAULT_LOOKBACK = timedelta(hours=24)


class TopologyNodeResponse(BaseModel):
    id: str
    kind: str
    label: str
    db_type: str | None = None
    health_status: str | None = None


class TopologyEdgeResponse(BaseModel):
    source: str
    target: str
    sample_count: int


class TopologyGraphResponse(BaseModel):
    nodes: list[TopologyNodeResponse]
    edges: list[TopologyEdgeResponse]


def get_query_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> QuerySamplesRepository:
    return QuerySamplesRepository(session)


def get_instances_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstancesRepository:
    return InstancesRepository(session)


def get_servers_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> ServersRepository:
    return ServersRepository(session)


def get_metric_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MetricSamplesRepository:
    return MetricSamplesRepository(session)


def get_instance_derived_status_service(
    repository: Annotated[MetricSamplesRepository, Depends(get_metric_samples_repository)],
) -> InstanceDerivedStatusService:
    return InstanceDerivedStatusService(repository)


def get_topology_service(
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
) -> TopologyService:
    return TopologyService(repository)


@router.get("", response_model=TopologyGraphResponse)
async def get_topology(
    instances_repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    servers_repository: Annotated[ServersRepository, Depends(get_servers_repository)],
    status_service: Annotated[
        InstanceDerivedStatusService, Depends(get_instance_derived_status_service)
    ],
    topology_service: Annotated[TopologyService, Depends(get_topology_service)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
) -> TopologyGraphResponse:
    """Grafo de dependências detectadas entre instâncias (e servidores monitorados) —
    correlação sobre `query_samples.machine` já coletado, ver `TopologyService`."""
    resolved_since = since or (datetime.now(UTC) - _DEFAULT_LOOKBACK)
    instances = await instances_repository.list_active()
    servers = await servers_repository.list_active()

    health_status_by_instance_id = {
        str(instance.id): (await status_service.compute(instance.id)).health_status
        for instance in instances
    }

    graph = await topology_service.build_dependency_graph(
        resolved_since, instances, servers, health_status_by_instance_id
    )
    return TopologyGraphResponse(
        nodes=[
            TopologyNodeResponse(
                id=node.id,
                kind=node.kind,
                label=node.label,
                db_type=node.db_type,
                health_status=node.health_status,
            )
            for node in graph.nodes
        ],
        edges=[
            TopologyEdgeResponse(
                source=edge.source, target=edge.target, sample_count=edge.sample_count
            )
            for edge in graph.edges
        ],
    )
