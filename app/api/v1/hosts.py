"""Endpoints de ingestao/leitura de metricas de host (servidor).

Decisao de escopo (ARCHITECTURE_SPEC.md Q2): o protocolo Prometheus `remote_write` real
(protobuf + snappy) foi decidido na arquitetura, mas implementa-lo por completo e desproporcional
para o tamanho desta etapa do MVP. Em vez disso, `POST /{hostname}/metrics` aceita um payload
JSON simples com o mesmo proposito pratico — ingestao via push, agentless do lado do backend (o
host, ou um agente rodando nele, envia as metricas periodicamente para ca, em vez do backend
fazer scrape em cada node_exporter/windows_exporter). Isso preserva a intencao original da
decisao (nao exigir scrape configurado/acessivel a partir do backend para cada host) sem o custo
de implementar o protocolo binario completo. Se o `remote_write` real vier a ser necessario, este
endpoint pode ser substituido/complementado sem mudar o restante do pipeline — o destino
continua sendo o mesmo `NormalizedMetricSample`/`MetricSamplesRepository` usado pela coleta de
instancia.

`GET /` lista os hosts ativos conhecidos (auto-registrados no primeiro push, ver
`ServersRepository.get_or_create_by_hostname`).
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, status
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session
from app.normalizers.models import NormalizedMetricSample
from app.repositories.metric_samples_repository import MetricSamplesRepository
from app.repositories.servers_repository import ServersRepository

router = APIRouter()


class HostMetricPoint(BaseModel):
    """Um ponto de metrica de host no payload de ingestao."""

    name: str
    value: float
    labels: dict[str, str] = Field(default_factory=dict)


class HostMetricsIngestRequest(BaseModel):
    """Corpo de `POST /{hostname}/metrics` — lote de metricas de um host num instante."""

    os_type: str = Field(pattern="^(linux|windows)$")
    metrics: list[HostMetricPoint]


class HostMetricsIngestResponse(BaseModel):
    """Resposta de ingestao — servidor resolvido/criado e quantidade de amostras persistidas."""

    server_id: UUID
    ingested: int


class HostResponse(BaseModel):
    """Linha resumida de host para listagem."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    hostname: str
    os_type: str
    active: bool


# --- Dependency providers -----------------------------------------------------------------


def get_servers_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> ServersRepository:
    return ServersRepository(session)


def get_metric_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> MetricSamplesRepository:
    return MetricSamplesRepository(session)


# --- Endpoints -----------------------------------------------------------------------------


@router.post(
    "/{hostname}/metrics",
    response_model=HostMetricsIngestResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def ingest_host_metrics(
    hostname: str,
    payload: HostMetricsIngestRequest,
    servers_repo: Annotated[ServersRepository, Depends(get_servers_repository)],
    metrics_repo: Annotated[MetricSamplesRepository, Depends(get_metric_samples_repository)],
) -> HostMetricsIngestResponse:
    """Ingesta um lote de metricas de host, auto-registrando o `Server` no primeiro push."""
    server = await servers_repo.get_or_create_by_hostname(hostname, payload.os_type)
    now = datetime.now(UTC)
    samples = [
        NormalizedMetricSample(
            time=now,
            target_type="server",
            target_id=server.id,
            metric_name=metric.name,
            value=metric.value,
            labels=metric.labels,
        )
        for metric in payload.metrics
    ]
    await metrics_repo.insert_batch(samples)
    return HostMetricsIngestResponse(server_id=server.id, ingested=len(samples))


@router.get("", response_model=list[HostResponse])
async def list_hosts(
    servers_repo: Annotated[ServersRepository, Depends(get_servers_repository)],
) -> list[HostResponse]:
    """Lista servidores/hosts ativos conhecidos."""
    servers = await servers_repo.list_active()
    return [HostResponse.model_validate(server) for server in servers]
