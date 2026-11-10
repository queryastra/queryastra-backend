"""Varreduras de manutencao on-demand: saude de indices, estatisticas desatualizadas,
concessoes de memoria, estimativa de compressao e particionamento.

Cada endpoint conecta no servidor monitorado a cada requisicao (sem persistencia), igual a
`health_stats.py`. A logica por motor fica nos adaptadores (`Collector.collect_index_health`
etc.); motores sem suporte devolvem `meta.supported=False` — o frontend mostra o aviso.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Annotated, Literal, TypeVar
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import check_instance_access, get_db_session
from app.api.v1.health_stats import _build_collector
from app.collectors.base import Collector
from app.core.config import get_settings
from app.core.logging import get_logger
from app.normalizers.models import (
    NormalizedCompressionReport,
    NormalizedIndexHealthReport,
    NormalizedMemoryGrantReport,
    NormalizedPartitionReport,
    NormalizedStatisticsHealthReport,
)
from app.security.credential_reference import CredentialResolver

logger = get_logger(__name__)

router = APIRouter()

T = TypeVar("T")

_DatabaseParam = Annotated[
    str | None,
    Query(max_length=256, description="Banco especifico; omitido = todos os bancos acessiveis"),
]


async def _run_scan(
    instance_id: UUID,
    session: AsyncSession,
    credential_resolver: CredentialResolver,
    label: str,
    call: Callable[[Collector], Awaitable[T]],
) -> T:
    collector = await _build_collector(instance_id, session, credential_resolver)
    timeout = get_settings().maintenance_scan_timeout_seconds
    try:
        return await asyncio.wait_for(call(collector), timeout=timeout)
    except TimeoutError as exc:
        logger.warning("Timeout na varredura %s instance_id=%s", label, instance_id)
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Varredura de {label} excedeu {int(timeout)}s. Selecione um banco especifico.",
        ) from exc
    except Exception as exc:
        logger.exception("Erro na varredura %s instance_id=%s", label, instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Erro ao consultar {label} na instancia.",
        ) from exc
    finally:
        await collector.close()


@router.get("/{instance_id}/databases", response_model=list[str])
async def list_databases(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[str]:
    """Bancos de usuario acessiveis — alimenta o seletor das telas de manutencao.
    Lista vazia = motor de banco unico (Oracle PDB, DB2) ou sem suporte."""
    return await _run_scan(
        instance_id, session, credential_resolver, "bancos", lambda c: c.list_databases()
    )


@router.get("/{instance_id}/index-health", response_model=NormalizedIndexHealthReport)
async def get_index_health(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    database: _DatabaseParam = None,
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> NormalizedIndexHealthReport:
    """Fragmentacao, indices sem uso, duplicados e sobrepostos."""
    return await _run_scan(
        instance_id,
        session,
        credential_resolver,
        "saude de indices",
        lambda c: c.collect_index_health(database),
    )


@router.get("/{instance_id}/statistics-health", response_model=NormalizedStatisticsHealthReport)
async def get_statistics_health(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    database: _DatabaseParam = None,
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> NormalizedStatisticsHealthReport:
    """Ultima atualizacao de estatisticas e % de linhas modificadas desde entao."""
    return await _run_scan(
        instance_id,
        session,
        credential_resolver,
        "estatisticas",
        lambda c: c.collect_statistics_health(database),
    )


@router.get("/{instance_id}/memory-grants", response_model=NormalizedMemoryGrantReport)
async def get_memory_grants(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> NormalizedMemoryGrantReport:
    """Concessoes de memoria ativas/em espera e consultas que mais pedem memoria/derramam."""
    return await _run_scan(
        instance_id,
        session,
        credential_resolver,
        "concessoes de memoria",
        lambda c: c.collect_memory_grants(),
    )


@router.get("/{instance_id}/compression-estimate", response_model=NormalizedCompressionReport)
async def get_compression_estimate(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    database: _DatabaseParam = None,
    compression: Literal["PAGE", "ROW"] = Query(default="PAGE"),
    max_objects: int | None = Query(default=None, ge=1, le=200),
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> NormalizedCompressionReport:
    """Estimativa de ganho com compressao (SQL Server/DB2) ou inventario de compressao atual.

    A estimativa amostra dados reais no servidor monitorado — so roda sob demanda (botao na
    tela), limitada aos `max_objects` maiores objetos."""
    limit = max_objects or get_settings().compression_estimate_max_objects
    return await _run_scan(
        instance_id,
        session,
        credential_resolver,
        "compressao",
        lambda c: c.estimate_compression(database, compression, limit),
    )


@router.get("/{instance_id}/partitions", response_model=NormalizedPartitionReport)
async def get_partitions(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    database: _DatabaseParam = None,
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> NormalizedPartitionReport:
    """Tabelas/indices particionados com o ultimo limite criado e linhas alem dele."""
    return await _run_scan(
        instance_id,
        session,
        credential_resolver,
        "particoes",
        lambda c: c.collect_partitions(database),
    )
