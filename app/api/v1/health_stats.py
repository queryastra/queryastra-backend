"""Endpoints on-demand de saude do banco — Fase 5 (T55, T57, T58, T59, T61, T62, T63, T64, T65, T68, T69).

Todos os endpoints consultam diretamente o servidor monitorado a cada requisicao (sem
persistencia), exceto T62 e os novos endpoints locais que agregam dados do `query_samples`/
`blocking_events`/`metric_samples` local.

T55: index bloat (PostgreSQL only)
T57: bgwriter/checkpoints (PostgreSQL only)
T58: user functions (PostgreSQL only)
T59: replication slots (PostgreSQL only)
T61: TempDB contention (SQL Server only)
T62: wait events heatmap (PostgreSQL + SQL Server; agrega query_samples local)
T63: database sizes (PostgreSQL + SQL Server)
T64: config compliance (PostgreSQL + SQL Server)
T65: connection consumers (PostgreSQL + SQL Server)
schema-health: indices duplicados, tabelas sem PK, FKs sem indice (PostgreSQL + SQL Server)
buffer-cache: cache de buffer por tabela + tempfiles + file I/O (PostgreSQL/SQL Server)
hot-objects: objetos mais bloqueados por total_wait_ms (local, blocking_events)
concurrency-trend: tendencia de sessoes ativas por hora (local, metric_samples)
workload-classification: distribuicao OLTP/analytics/maintenance/other (local, query_samples)
explain-plan: EXPLAIN (FORMAT JSON) sob demanda (PostgreSQL only)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import check_instance_access, get_db_session
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.logging import get_logger
from app.repositories.instances_repository import InstancesRepository
from app.repositories.query_samples_repository import QuerySamplesRepository
from app.repositories.query_stats_trend_repository import QueryStatsTrendRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

router = APIRouter()


# ── Response models ────────────────────────────────────────────────────────────


class IndexBloatEntryResponse(BaseModel):
    schema_name: str
    table_name: str
    index_name: str
    index_size_bytes: int
    idx_scan: int
    idx_tup_read: int
    idx_tup_fetch: int
    bloat_pct: float


class BgwriterStatsResponse(BaseModel):
    checkpoints_timed: int
    checkpoints_req: int
    checkpoint_write_time: float
    checkpoint_sync_time: float
    buffers_checkpoint: int
    buffers_clean: int
    maxwritten_clean: int
    buffers_backend: int
    buffers_backend_fsync: int
    buffers_alloc: int
    stats_reset: datetime | None


class UserFunctionResponse(BaseModel):
    schema_name: str
    func_name: str
    calls: int
    total_time_ms: float
    self_time_ms: float
    avg_time_ms: float


class ReplicationSlotResponse(BaseModel):
    slot_name: str
    plugin: str | None
    slot_type: str
    database_name: str | None
    active: bool
    active_pid: int | None
    lag_bytes: int | None
    wal_status: str | None


class ConfigComplianceItemResponse(BaseModel):
    param_name: str
    current_value: str
    recommended_value: str | None
    unit: str | None
    status: Literal["ok", "warn", "error"]
    description: str
    recommendation: str | None


class ConnectionConsumerResponse(BaseModel):
    application_name: str
    client_addr: str | None
    username: str
    total: int
    active: int
    idle: int
    idle_in_transaction: int
    waiting: int


# ── Shared helper ─────────────────────────────────────────────────────────────


async def _build_collector(
    instance_id: UUID,
    session: AsyncSession,
    credential_resolver: CredentialResolver,
):
    instance = await InstancesRepository(session).get_by_id(instance_id)
    if instance is None or not instance.active:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Instancia nao encontrada ou inativa.",
        )

    username: str | None = None
    secret_value: str | None = None
    if instance.auth_method == "encrypted_fallback":
        if instance.encrypted_credential_ref is None:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail="Referencia de credencial ausente na instancia.",
            )
        try:
            resolved = await credential_resolver.resolve(
                CredentialReference(instance.encrypted_credential_ref)
            )
            username, secret_value = resolved.username, resolved.secret_value
        except CredentialResolutionError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Falha ao resolver credencial da instancia.",
            ) from exc

    connection = ConnectionParams(
        host=instance.host,
        port=instance.port,
        auth_method=instance.auth_method,
        username=username,
        secret_value=secret_value,
    )
    return create_collector(instance.db_type, connection=connection, instance_id=instance.id)


# ── Endpoints ─────────────────────────────────────────────────────────────────


@router.get("/{instance_id}/index-bloat", response_model=list[IndexBloatEntryResponse])
async def get_index_bloat(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[IndexBloatEntryResponse]:
    """Indices com estimativa de bloat ordenados por tamanho (PostgreSQL only).

    Consulta on-demand. Retorna lista vazia para engines sem suporte."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        entries = await collector.collect_index_bloat()
    except Exception as exc:
        logger.exception("Erro ao coletar index bloat para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar index bloat na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [IndexBloatEntryResponse(**e.model_dump()) for e in entries]


@router.get("/{instance_id}/bgwriter", response_model=BgwriterStatsResponse | None)
async def get_bgwriter_stats(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> BgwriterStatsResponse | None:
    """Estatisticas do bgwriter e checkpoints (PostgreSQL only).

    Consulta on-demand. Retorna null para engines sem suporte."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        stats = await collector.collect_bgwriter_stats()
    except Exception as exc:
        logger.exception("Erro ao coletar bgwriter stats para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar bgwriter stats na instancia.",
        ) from exc
    finally:
        await collector.close()

    if stats is None:
        return None
    return BgwriterStatsResponse(**stats.model_dump())


@router.get("/{instance_id}/user-functions", response_model=list[UserFunctionResponse])
async def get_user_functions(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[UserFunctionResponse]:
    """Top funcoes PL/pgSQL por tempo total (PostgreSQL only, requer track_functions=all).

    Consulta on-demand. Retorna lista vazia para engines sem suporte ou track_functions desabilitado."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        funcs = await collector.collect_user_functions()
    except Exception as exc:
        logger.exception("Erro ao coletar user functions para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar funcoes de usuario na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [UserFunctionResponse(**f.model_dump()) for f in funcs]


@router.get("/{instance_id}/replication-slots", response_model=list[ReplicationSlotResponse])
async def get_replication_slots(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[ReplicationSlotResponse]:
    """Slots de replicacao com lag em bytes (PostgreSQL only).

    Consulta on-demand. Retorna lista vazia para engines sem suporte ou sem slots configurados."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        slots = await collector.collect_replication_slots()
    except Exception as exc:
        logger.exception("Erro ao coletar replication slots para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar slots de replicacao na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [ReplicationSlotResponse(**s.model_dump()) for s in slots]


@router.get("/{instance_id}/config-compliance", response_model=list[ConfigComplianceItemResponse])
async def get_config_compliance(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[ConfigComplianceItemResponse]:
    """Checklist de parametros de configuracao vs boas praticas (PostgreSQL + SQL Server).

    Consulta on-demand. Retorna lista vazia para engines sem suporte."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        items = await collector.collect_config_compliance()
    except Exception as exc:
        logger.exception("Erro ao coletar config compliance para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar configuracao da instancia.",
        ) from exc
    finally:
        await collector.close()

    return [ConfigComplianceItemResponse(**i.model_dump()) for i in items]


@router.get("/{instance_id}/connection-consumers", response_model=list[ConnectionConsumerResponse])
async def get_connection_consumers(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[ConnectionConsumerResponse]:
    """Conexoes agrupadas por (application_name, client_addr, username) — Fase 5, T65.

    Consulta on-demand. Util para identificar connection leaks (muitas idle) ou
    apps que abrem idle-in-transaction. Suportado em PostgreSQL e SQL Server."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        consumers = await collector.collect_connection_consumers()
    except Exception as exc:
        logger.exception("Erro ao coletar connection consumers para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar consumidores de conexao na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [ConnectionConsumerResponse(**c.model_dump()) for c in consumers]


class TempDbStatsResponse(BaseModel):
    file_count: int
    total_size_mb: float
    user_objects_mb: float
    internal_objects_mb: float
    version_store_mb: float
    free_space_mb: float
    session_count: int
    pagelatch_wait_ms: int


class UnusedIndexResponse(BaseModel):
    schema_name: str
    table_name: str
    index_name: str
    index_size_bytes: int
    idx_scan: int


class TableIoStatsResponse(BaseModel):
    schema_name: str
    table_name: str
    seq_scan: int
    idx_scan: int
    n_live_tup: int
    n_dead_tup: int
    table_size_bytes: int
    last_analyze: datetime | None
    last_autoanalyze: datetime | None
    last_vacuum: datetime | None
    last_autovacuum: datetime | None


class WaitHeatmapBucketResponse(BaseModel):
    bucket: str
    wait_type: str
    sample_count: int
    total_wait_ms: int


class DbSizeResponse(BaseModel):
    database_name: str
    size_bytes: int


@router.get("/{instance_id}/tempdb", response_model=TempDbStatsResponse | None)
async def get_tempdb_stats(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> TempDbStatsResponse | None:
    """Snapshot de uso do TempDB — Fase 5, T61. SQL Server only.

    Agrega file count/size, space usage breakdown e PAGELATCH wait time.
    Retorna null para engines sem TempDB (PostgreSQL)."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        stats = await collector.collect_tempdb_stats()
    except Exception as exc:
        logger.exception("Erro ao coletar tempdb stats para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar TempDB na instancia.",
        ) from exc
    finally:
        await collector.close()

    if stats is None:
        return None
    return TempDbStatsResponse(**stats.model_dump())


@router.get("/{instance_id}/wait-heatmap", response_model=list[WaitHeatmapBucketResponse])
async def get_wait_heatmap(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    window: int = Query(default=24, ge=1, le=168, description="Janela em horas (1-168)"),
) -> list[WaitHeatmapBucketResponse]:
    """Heatmap de wait events agrupado por (hora, categoria) — Fase 5, T62.

    Agrega `query_samples` local por `time_bucket` de 1h e `wait_type` (categoria canonica).
    Nao consulta o servidor monitorado — apenas os dados historicos coletados.
    PostgreSQL + SQL Server."""
    until = datetime.now(timezone.utc)
    since = until - timedelta(hours=window)

    repo = QuerySamplesRepository(session)
    try:
        rows = await repo.wait_heatmap(instance_id, since, until)
    except Exception as exc:
        logger.exception("Erro ao agregar wait heatmap para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao agregar wait heatmap.",
        ) from exc

    return [
        WaitHeatmapBucketResponse(
            bucket=row.bucket.isoformat() if hasattr(row.bucket, "isoformat") else str(row.bucket),
            wait_type=str(row.wait_type),
            sample_count=int(row.sample_count),
            total_wait_ms=int(row.total_wait_ms),
        )
        for row in rows
    ]


@router.get("/{instance_id}/db-sizes", response_model=list[DbSizeResponse])
async def get_db_sizes(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[DbSizeResponse]:
    """Tamanho atual de cada banco de dados — Fase 5, T63.

    PostgreSQL: `pg_database_size(datname)`; SQL Server: `sys.master_files` (data files).
    Retorna lista vazia para engines sem suporte."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        sizes = await collector.collect_db_sizes()
    except Exception as exc:
        logger.exception("Erro ao coletar db sizes para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar tamanhos de banco na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [DbSizeResponse(**s.model_dump()) for s in sizes]


# ── T60 — Query Trends ────────────────────────────────────────────────────────


class QueryTrendBucketResponse(BaseModel):
    bucket: str
    calls: int
    mean_exec_ms: float


class QueryTrendSeriesResponse(BaseModel):
    queryid: str
    query_text: str | None
    buckets: list[QueryTrendBucketResponse]
    baseline_mean_ms: float | None
    current_mean_ms: float | None
    regression_ratio: float | None


@router.get("/{instance_id}/query-trends", response_model=list[QueryTrendSeriesResponse])
async def get_query_trends(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    days: int = Query(default=7, ge=1, le=30),
) -> list[QueryTrendSeriesResponse]:
    """Tendencia de performance das top-10 queries (pg_stat_statements) — Fase 6, T60.

    Agrega snapshots horarios armazenados pelo QueryStatsSupervisor. Retorna lista vazia se
    nenhum snapshot foi coletado ainda. PostgreSQL only; SQL Server retorna lista vazia."""
    until = datetime.now(timezone.utc)
    since = until - timedelta(days=days)

    repo = QueryStatsTrendRepository(session)
    try:
        rows = await repo.top_query_trends(instance_id, since, until, limit=10)
    except Exception as exc:
        logger.exception("Erro ao consultar query trends para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao consultar query trends.",
        ) from exc

    # Agrupa por queryid e calcula regression_ratio
    series_map: dict[int, QueryTrendSeriesResponse] = {}
    cutoff = until - timedelta(hours=24)

    for row in rows:
        qid = int(row.queryid)
        bucket_str = row.bucket.isoformat() if hasattr(row.bucket, "isoformat") else str(row.bucket)
        bucket_entry = QueryTrendBucketResponse(
            bucket=bucket_str,
            calls=int(row.calls),
            mean_exec_ms=float(row.mean_exec_ms),
        )

        if qid not in series_map:
            series_map[qid] = QueryTrendSeriesResponse(
                queryid=str(qid),
                query_text=row.query_text,
                buckets=[],
                baseline_mean_ms=None,
                current_mean_ms=None,
                regression_ratio=None,
            )
        series_map[qid].buckets.append(bucket_entry)

    # Calcula stats de regressao para cada query
    result = []
    for series in series_map.values():
        baseline_buckets = [b for b in series.buckets if b.bucket < cutoff.isoformat()]
        current_buckets = [b for b in series.buckets if b.bucket >= cutoff.isoformat()]

        def weighted_mean(buckets: list[QueryTrendBucketResponse]) -> float | None:
            total_calls = sum(b.calls for b in buckets)
            if total_calls == 0:
                return None
            return sum(b.mean_exec_ms * b.calls for b in buckets) / total_calls

        baseline_mean = weighted_mean(baseline_buckets)
        current_mean = weighted_mean(current_buckets)
        ratio: float | None = None
        if baseline_mean and current_mean and baseline_mean > 0:
            ratio = round(current_mean / baseline_mean, 2)

        result.append(
            QueryTrendSeriesResponse(
                queryid=series.queryid,
                query_text=series.query_text,
                buckets=series.buckets,
                baseline_mean_ms=round(baseline_mean, 3) if baseline_mean else None,
                current_mean_ms=round(current_mean, 3) if current_mean else None,
                regression_ratio=ratio,
            )
        )

    # Ordena: maior regression_ratio primeiro, depois por current_mean_ms desc
    result.sort(key=lambda s: (-(s.regression_ratio or 0), -(s.current_mean_ms or 0)))
    return result


@router.get("/{instance_id}/unused-indexes", response_model=list[UnusedIndexResponse])
async def get_unused_indexes(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[UnusedIndexResponse]:
    """Indices com zero scans desde o ultimo stats reset (PostgreSQL only) — Fase 5, T69.

    Indices nunca usados desperdicam espaco e penalizam escritas. Consulta on-demand."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        entries = await collector.collect_unused_indexes()
    except Exception as exc:
        logger.exception("Erro ao coletar unused indexes para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar unused indexes na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [UnusedIndexResponse(**e.model_dump()) for e in entries]


@router.get("/{instance_id}/table-io-stats", response_model=list[TableIoStatsResponse])
async def get_table_io_stats(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[TableIoStatsResponse]:
    """Estatisticas de I/O de tabelas ordenadas por seq_scan (PostgreSQL only) — Fase 5, T68.

    Tabelas com seq_scan alto e idx_scan baixo sao candidatas a novos indices. Consulta on-demand."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        entries = await collector.collect_table_io_stats()
    except Exception as exc:
        logger.exception("Erro ao coletar table IO stats para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar table IO stats na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [TableIoStatsResponse(**e.model_dump()) for e in entries]


# ── Schema Health ──────────────────────────────────────────────────────────────


class SchemaHealthItemResponse(BaseModel):
    category: str
    schema_name: str
    table_name: str
    object_name: str | None = None
    detail: dict = {}
    severity: str


@router.get("/{instance_id}/schema-health", response_model=list[SchemaHealthItemResponse])
async def get_schema_health(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> list[SchemaHealthItemResponse]:
    """Saude do schema: indices duplicados, tabelas sem PK, FKs sem indice.

    PostgreSQL: detecta as tres categorias. SQL Server: missing_pk e duplicate_index.
    Consulta on-demand no servidor monitorado."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        items = await collector.collect_schema_health()
    except Exception as exc:
        logger.exception("Erro ao coletar schema health para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar schema health na instancia.",
        ) from exc
    finally:
        await collector.close()

    return [SchemaHealthItemResponse(**i.model_dump()) for i in items]


# ── Buffer Cache ───────────────────────────────────────────────────────────────


class BufferTableStatsResponse(BaseModel):
    schema_name: str
    table_name: str
    heap_blks_read: int
    heap_blks_hit: int
    hit_ratio: float
    idx_blks_read: int
    idx_blks_hit: int
    idx_hit_ratio: float


class TempfileStatsResponse(BaseModel):
    database_name: str
    temp_files: int
    temp_bytes: int


class FileIoStatsResponse(BaseModel):
    database_name: str
    file_name: str
    io_stall_read_ms: int
    io_stall_write_ms: int
    num_of_reads: int
    num_of_writes: int
    avg_read_ms: float
    avg_write_ms: float


class BufferCacheResponse(BaseModel):
    table_stats: list[BufferTableStatsResponse]
    tempfile_stats: list[TempfileStatsResponse]
    file_io_stats: list[FileIoStatsResponse]


@router.get("/{instance_id}/buffer-cache", response_model=BufferCacheResponse)
async def get_buffer_cache(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
) -> BufferCacheResponse:
    """Cache de buffer por tabela, tempfiles e file I/O stats.

    PostgreSQL: table_stats e tempfile_stats preenchidos; file_io_stats vazio.
    SQL Server: file_io_stats preenchido; table_stats e tempfile_stats vazios.
    Consulta on-demand no servidor monitorado."""
    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        table_stats, tempfile_stats, file_io_stats = await collector.collect_buffer_cache_stats()
    except Exception as exc:
        logger.exception("Erro ao coletar buffer cache stats para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao consultar buffer cache stats na instancia.",
        ) from exc
    finally:
        await collector.close()

    return BufferCacheResponse(
        table_stats=[BufferTableStatsResponse(**t.model_dump()) for t in table_stats],
        tempfile_stats=[TempfileStatsResponse(**t.model_dump()) for t in tempfile_stats],
        file_io_stats=[FileIoStatsResponse(**f.model_dump()) for f in file_io_stats],
    )


# ── Hot Objects ────────────────────────────────────────────────────────────────


class HotObjectResponse(BaseModel):
    object_name: str
    incident_count: int
    total_wait_ms: int


@router.get("/{instance_id}/hot-objects", response_model=list[HotObjectResponse])
async def get_hot_objects(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    window: int = Query(default=24, ge=1, le=168, description="Janela em horas (1-168)"),
) -> list[HotObjectResponse]:
    """Top 20 objetos mais bloqueados por total_wait_ms na janela especificada.

    Consulta a hypertable local `blocking_events` — nao conecta ao servidor monitorado.
    PostgreSQL + SQL Server."""
    stmt = text("""
        SELECT blocked_object, COUNT(*) AS incident_count, SUM(wait_time_ms) AS total_wait_ms
        FROM blocking_events
        WHERE instance_id = :instance_id
          AND time > NOW() - INTERVAL ':window hours'
          AND blocked_object IS NOT NULL
        GROUP BY blocked_object
        ORDER BY total_wait_ms DESC
        LIMIT 20
    """.replace(":window", str(window)))

    try:
        result = await session.execute(stmt, {"instance_id": str(instance_id)})
        rows = result.fetchall()
    except Exception as exc:
        logger.exception("Erro ao consultar hot objects para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao consultar hot objects.",
        ) from exc

    return [
        HotObjectResponse(
            object_name=str(row.blocked_object),
            incident_count=int(row.incident_count),
            total_wait_ms=int(row.total_wait_ms),
        )
        for row in rows
    ]


# ── Concurrency Trend ──────────────────────────────────────────────────────────


class ConcurrencyPointResponse(BaseModel):
    bucket: str
    max_sessions: int
    avg_sessions: float
    min_sessions: int


@router.get("/{instance_id}/concurrency-trend", response_model=list[ConcurrencyPointResponse])
async def get_concurrency_trend(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    window: int = Query(default=24, ge=1, le=168, description="Janela em horas (1-168)"),
) -> list[ConcurrencyPointResponse]:
    """Tendencia de sessoes ativas (metric_name='active_sessions') por hora.

    Consulta a hypertable local `metric_samples` — nao conecta ao servidor monitorado.
    PostgreSQL + SQL Server."""
    stmt = text("""
        SELECT time_bucket('1 hour', time) AS bucket,
               MAX(value) AS max_sessions,
               AVG(value) AS avg_sessions,
               MIN(value) AS min_sessions
        FROM metric_samples
        WHERE target_id = :instance_id
          AND metric_name = 'active_sessions'
          AND time > NOW() - INTERVAL ':window hours'
        GROUP BY bucket
        ORDER BY bucket
    """.replace(":window", str(window)))

    try:
        result = await session.execute(stmt, {"instance_id": str(instance_id)})
        rows = result.fetchall()
    except Exception as exc:
        logger.exception("Erro ao consultar concurrency trend para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao consultar concurrency trend.",
        ) from exc

    return [
        ConcurrencyPointResponse(
            bucket=row.bucket.isoformat() if hasattr(row.bucket, "isoformat") else str(row.bucket),
            max_sessions=int(row.max_sessions),
            avg_sessions=float(row.avg_sessions),
            min_sessions=int(row.min_sessions),
        )
        for row in rows
    ]


# ── Workload Classification ────────────────────────────────────────────────────


class WorkloadClassificationResponse(BaseModel):
    oltp_pct: float
    analytics_pct: float
    maintenance_pct: float
    other_pct: float
    oltp_count: int
    analytics_count: int
    maintenance_count: int
    other_count: int
    period_hours: int


@router.get("/{instance_id}/workload-classification", response_model=WorkloadClassificationResponse)
async def get_workload_classification(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    window: int = Query(default=24, ge=1, le=168, description="Janela em horas (1-168)"),
) -> WorkloadClassificationResponse:
    """Distribuicao do workload em OLTP / analytics / maintenance / other.

    Classifica amostras de `query_samples` locais — nao conecta ao servidor monitorado.
    Regras: oltp = duration_ms < 1000 sem wait ou CPU wait; analytics = duration_ms >= 5000
    ou wait I/O; maintenance = wait Lock ou programa com 'maintenance'/'backup'/'vacuum'.
    PostgreSQL + SQL Server."""
    stmt = text("""
        SELECT
            SUM(CASE WHEN COALESCE(duration_ms, 0) < 1000 AND wait_type NOT IN ('Lock','IO') THEN 1 ELSE 0 END) AS oltp_count,
            SUM(CASE WHEN COALESCE(duration_ms, 0) >= 5000 OR wait_type = 'IO' THEN 1 ELSE 0 END) AS analytics_count,
            SUM(CASE WHEN wait_type = 'Lock' OR lower(program) LIKE ANY(ARRAY['%maintenance%','%backup%','%vacuum%']) THEN 1 ELSE 0 END) AS maintenance_count,
            COUNT(*) AS total_count
        FROM query_samples
        WHERE instance_id = :instance_id
          AND time > NOW() - INTERVAL ':window hours'
    """.replace(":window", str(window)))

    try:
        result = await session.execute(stmt, {"instance_id": str(instance_id)})
        row = result.fetchone()
    except Exception as exc:
        logger.exception("Erro ao classificar workload para instance_id=%s", instance_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao classificar workload.",
        ) from exc

    if row is None or int(row.total_count or 0) == 0:
        return WorkloadClassificationResponse(
            oltp_pct=0.0, analytics_pct=0.0, maintenance_pct=0.0, other_pct=0.0,
            oltp_count=0, analytics_count=0, maintenance_count=0, other_count=0,
            period_hours=window,
        )

    total = int(row.total_count)
    oltp = int(row.oltp_count or 0)
    analytics = int(row.analytics_count or 0)
    maintenance = int(row.maintenance_count or 0)
    other = max(0, total - oltp - analytics - maintenance)

    def pct(n: int) -> float:
        return round(n / total * 100, 2) if total > 0 else 0.0

    return WorkloadClassificationResponse(
        oltp_pct=pct(oltp),
        analytics_pct=pct(analytics),
        maintenance_pct=pct(maintenance),
        other_pct=pct(other),
        oltp_count=oltp,
        analytics_count=analytics,
        maintenance_count=maintenance,
        other_count=other,
        period_hours=window,
    )


# ── Explain Plan ───────────────────────────────────────────────────────────────


class ExplainPlanResponse(BaseModel):
    engine: str
    sql_hash: str
    plan: dict | None = None
    cached_plan_available: bool


@router.get("/{instance_id}/explain-plan", response_model=ExplainPlanResponse)
async def get_explain_plan(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    session: Annotated[AsyncSession, Depends(get_db_session)],
    credential_resolver: CredentialResolver = Depends(CredentialResolver),  # noqa: B008
    sql_hash: str = Query(..., description="Hash da query (sql_hash de query_samples)"),
) -> ExplainPlanResponse:
    """EXPLAIN (FORMAT JSON) sob demanda para o texto mascarado associado ao sql_hash.

    Busca o sql_text_masked mais recente associado ao sql_hash nos query_samples locais e
    executa EXPLAIN contra o servidor monitorado. PostgreSQL only — SQL Server retorna
    cached_plan_available=false. O texto mascarado e seguro pois EXPLAIN apenas planeja,
    nunca executa o statement."""
    # Lookup sql_text from local query_samples
    lookup_stmt = text("""
        SELECT sql_text_masked
        FROM query_samples
        WHERE instance_id = :instance_id AND sql_hash = :sql_hash
        ORDER BY time DESC
        LIMIT 1
    """)
    try:
        lookup_result = await session.execute(
            lookup_stmt, {"instance_id": str(instance_id), "sql_hash": sql_hash}
        )
        lookup_row = lookup_result.fetchone()
    except Exception as exc:
        logger.exception("Erro ao buscar sql_text para sql_hash=%s instance_id=%s", sql_hash, instance_id)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Erro ao buscar texto da query.",
        ) from exc

    if lookup_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="sql_hash nao encontrado nos query_samples desta instancia.",
        )

    sql_text_masked: str = str(lookup_row.sql_text_masked)

    collector = await _build_collector(instance_id, session, credential_resolver)
    try:
        plan = await collector.explain_plan_for_hash(sql_text_masked)
    except Exception as exc:
        logger.exception("Erro ao executar EXPLAIN para instance_id=%s sql_hash=%s", instance_id, sql_hash)
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail="Erro ao executar EXPLAIN na instancia.",
        ) from exc
    finally:
        await collector.close()

    # Resolve engine name from instance record
    instance_repo_result = await session.execute(
        text("SELECT db_type FROM instances WHERE id = :id"),
        {"id": str(instance_id)},
    )
    instance_row = instance_repo_result.fetchone()
    engine_name = str(instance_row.db_type) if instance_row else "unknown"

    return ExplainPlanResponse(
        engine=engine_name,
        sql_hash=sql_hash,
        plan=plan,
        cached_plan_available=plan is not None,
    )
