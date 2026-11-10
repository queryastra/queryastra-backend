"""Adaptador IBM DB2. Fonte de verdade: `Docs/collector-queries.md`.

Usa ibm_db_dbi (DBAPI-2 síncrono) — cada chamada roda em thread separada via
asyncio.get_event_loop().run_in_executor(), seguindo o mesmo padrão do adaptador SQL Server.

`check_capabilities()` abre conexão e checa acesso a MON_GET_ACTIVITY e MON_GET_STATEMENT.

`collect()` executa um ciclo somente leitura:
    - MON_GET_ACTIVITY — sessão/query ativa, normalizada em NormalizedQuerySample.
    - MON_GET_DATABASE — métricas de recurso (deadlocks, lock_waits).
    - MON_GET_BUFFERPOOL — hit ratio de buffer pool.

Permissão mínima: EXECUTE on MON_GET_ACTIVITY (requer DBADM ou DATAACCESS).

Formato do host em ConnectionParams: "hostname/DBNAME" (ex.: "mydb2server/SAMPLE").
Se não houver "/", usa banco padrão "SAMPLE".
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
import time
from datetime import UTC, datetime
from uuid import UUID

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    CONNECTIVITY,
    DB2_QUERY_STATS,
    DB2_SESSION_ACTIVITY,
)
from app.collectors.collector_mixin import CollectorNoOpMixin
from app.collectors.connection import ConnectionParams
from app.collectors.db2 import maintenance as db2_maintenance
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals, mask_sql_literals_for_display
from app.normalizers.metric_names import (
    DB2_ACTIVE_SESSION_COUNT,
    DB2_BP_HIT_RATIO,
    DB2_DEADLOCKS_TOTAL,
    DB2_LOCK_WAITS_TOTAL,
)
from app.normalizers.models import (
    NormalizedCompressionReport,
    NormalizedIndexHealthReport,
    NormalizedMemoryGrantReport,
    NormalizedMetricSample,
    NormalizedPartitionReport,
    NormalizedQuerySample,
    NormalizedStatisticsHealthReport,
)
from app.normalizers.wait_taxonomy import map_wait_event

logger = get_logger(__name__)

_SESSION_ACTIVITY_QUERY = """
    SELECT application_handle, application_name, session_auth_id,
           client_hostname, activity_state, activity_type,
           stmt_text, elapsed_time_sec, lock_wait_time
    FROM TABLE(MON_GET_ACTIVITY(NULL, -2)) AS t
    WHERE activity_state IN ('EXECUTING', 'LOCK_WAIT')
      AND activity_type IN ('READ_DML', 'WRITE_DML', 'DDL', 'CALL')
    FETCH FIRST 500 ROWS ONLY
"""

_DATABASE_STATS_QUERY = """
    SELECT deadlocks, lock_waits
    FROM TABLE(MON_GET_DATABASE(-2)) AS t
    FETCH FIRST 1 ROWS ONLY
"""

_BUFFERPOOL_QUERY = """
    SELECT SUM(pool_data_l_reads + pool_index_l_reads) AS logical_reads,
           SUM(pool_data_p_reads + pool_index_p_reads) AS physical_reads
    FROM TABLE(MON_GET_BUFFERPOOL('', -2)) AS t
"""


def _parse_db2_host(params: ConnectionParams) -> tuple[str, str]:
    """Extrai (hostname, database) de ConnectionParams.host.

    Formatos suportados:
    - "hostname" → ("hostname", "SAMPLE")
    - "hostname/DBNAME" → ("hostname", "DBNAME")
    """
    if "/" in params.host:
        host, database = params.host.split("/", 1)
        return host.strip(), database.strip().upper()
    return params.host, "SAMPLE"


def _build_conn_str(params: ConnectionParams) -> str:
    db2_host, database = _parse_db2_host(params)
    parts = [
        f"DATABASE={database}",
        f"HOSTNAME={db2_host}",
        f"PORT={params.port}",
        "PROTOCOL=TCPIP",
    ]
    if params.username:
        parts.append(f"UID={params.username}")
    if params.secret_value:
        parts.append(f"PWD={params.secret_value}")
    return ";".join(parts) + ";"


def _connect_sync(conn_str: str):
    try:
        import ibm_db_dbi  # lazy — so instalado quando DB2 esta em uso
    except ImportError as exc:
        raise RuntimeError(
            "ibm-db não está instalado nesta plataforma (sem wheel para esta arquitetura). "
            "Suporte a IBM Db2 requer x86_64 Linux ou Windows. "
            "Em ARM Linux use Docker com --platform linux/amd64."
        ) from exc
    return ibm_db_dbi.connect(conn_str, "", "")


def _execute_sync(conn, query: str) -> list[dict]:
    cursor = conn.cursor()
    cursor.execute(query)
    if cursor.description is None:
        cursor.close()
        return []
    cols = [d[0].lower() for d in cursor.description]
    rows = cursor.fetchall()
    cursor.close()
    return [dict(zip(cols, row, strict=False)) for row in rows]


def _close_sync(conn) -> None:
    try:
        conn.close()
    except Exception:
        pass


class DB2Collector(CollectorNoOpMixin):
    """Implementa `app.collectors.base.Collector` para instâncias IBM DB2."""

    def __init__(self, connection: ConnectionParams, instance_id: UUID | None = None) -> None:
        self._connection_params = connection
        self._instance_id = instance_id

    async def _connect(self):
        conn_str = _build_conn_str(self._connection_params)
        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(None, functools.partial(_connect_sync, conn_str)),
            timeout=self._connection_params.connect_timeout_seconds,
        )

    async def _run_query(self, conn, query: str, timeout_seconds: float) -> list[dict]:
        loop = asyncio.get_event_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(None, functools.partial(_execute_sync, conn, query)),
            timeout=timeout_seconds,
        )

    async def check_capabilities(self) -> list[CapabilityResult]:
        try:
            conn = await self._connect()
        except Exception as exc:
            return [
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=f"Falha ao conectar: {type(exc).__name__}",
                )
            ]

        settings = get_settings()
        results: list[CapabilityResult] = []
        loop = asyncio.get_event_loop()
        try:
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        functools.partial(
                            _execute_sync,
                            conn,
                            "SELECT COUNT(*) AS cnt FROM TABLE(MON_GET_ACTIVITY(NULL, -2)) AS t FETCH FIRST 1 ROWS ONLY",
                        ),
                    ),
                    timeout=settings.collection_query_timeout_seconds,
                )
                results.append(
                    CapabilityResult(
                        capability=DB2_SESSION_ACTIVITY,
                        status=CapabilityStatus.FULL,
                    )
                )
            except Exception as exc:
                results.append(
                    CapabilityResult(
                        capability=DB2_SESSION_ACTIVITY,
                        status=CapabilityStatus.UNAVAILABLE,
                        reason=f"Sem acesso a MON_GET_ACTIVITY: {type(exc).__name__}",
                    )
                )

            try:
                await asyncio.wait_for(
                    loop.run_in_executor(
                        None,
                        functools.partial(
                            _execute_sync,
                            conn,
                            "SELECT COUNT(*) AS cnt FROM TABLE(MON_GET_STATEMENT(NULL, -2)) AS t FETCH FIRST 1 ROWS ONLY",
                        ),
                    ),
                    timeout=settings.collection_query_timeout_seconds,
                )
                results.append(
                    CapabilityResult(
                        capability=DB2_QUERY_STATS,
                        status=CapabilityStatus.FULL,
                    )
                )
            except Exception as exc:
                results.append(
                    CapabilityResult(
                        capability=DB2_QUERY_STATS,
                        status=CapabilityStatus.DEGRADED,
                        reason=f"Sem acesso a MON_GET_STATEMENT: {type(exc).__name__}",
                    )
                )
        finally:
            await loop.run_in_executor(None, functools.partial(_close_sync, conn))

        return results

    async def collect(
        self,
        *,
        sleeping_min_duration_ms: int | None = None,
        sleeping_database_overrides_ms: dict[str, int] | None = None,
    ) -> CollectionResult:
        if self._instance_id is None:
            raise ValueError(
                "DB2Collector.collect requer instance_id de uma instância já cadastrada."
            )

        start = time.perf_counter()
        settings = get_settings()
        loop = asyncio.get_event_loop()

        conn = await self._connect()
        try:
            collected_at = datetime.now(UTC)
            session_rows = await self._run_query(
                conn, _SESSION_ACTIVITY_QUERY, settings.collection_query_timeout_seconds
            )
            session_rows = session_rows[: settings.collection_max_rows]

            try:
                db_rows = await self._run_query(
                    conn, _DATABASE_STATS_QUERY, settings.collection_query_timeout_seconds
                )
            except Exception:
                db_rows = []
            try:
                bp_rows = await self._run_query(
                    conn, _BUFFERPOOL_QUERY, settings.collection_query_timeout_seconds
                )
            except Exception:
                bp_rows = []
        finally:
            await loop.run_in_executor(None, functools.partial(_close_sync, conn))

        query_samples = self._build_query_samples(session_rows, collected_at)
        metric_samples = self._build_metric_samples(session_rows, db_rows, bp_rows, collected_at)

        duration_ms = int((time.perf_counter() - start) * 1000)
        return CollectionResult(
            instance_id=self._instance_id,
            collected_at=collected_at,
            duration_ms=duration_ms,
            metric_samples=metric_samples,
            query_samples=query_samples,
        )

    def _build_query_samples(
        self, session_rows: list[dict], collected_at: datetime
    ) -> list[NormalizedQuerySample]:
        assert self._instance_id is not None
        instance_id: UUID = self._instance_id
        samples: list[NormalizedQuerySample] = []
        for row in session_rows:
            sql_text = str(row.get("stmt_text") or "").strip()
            if not sql_text:
                continue
            sql_hash = hashlib.sha256(mask_sql_literals(sql_text).encode()).hexdigest()
            masked_display = mask_sql_literals_for_display(sql_text)
            is_lock_wait = row.get("activity_state") == "LOCK_WAIT"
            elapsed_sec = float(row.get("elapsed_time_sec") or 0)
            samples.append(
                NormalizedQuerySample(
                    time=collected_at,
                    instance_id=instance_id,
                    engine="db2",
                    sql_hash=sql_hash,
                    sql_text_masked=masked_display,
                    sql_text_full_masked=masked_display,
                    wait_type=map_wait_event("db2", "LOCK" if is_lock_wait else "CPU"),
                    wait_event_native=row.get("activity_state"),
                    wait_time_ms=int(row.get("lock_wait_time") or 0),
                    duration_ms=int(elapsed_sec * 1000),
                    session_id=str(row.get("application_handle", "")),
                    db_user=row.get("session_auth_id"),
                    program=row.get("application_name"),
                    database_name=None,
                    machine=row.get("client_hostname"),
                )
            )
        return samples

    def _build_metric_samples(
        self,
        session_rows: list[dict],
        db_rows: list[dict],
        bp_rows: list[dict],
        collected_at: datetime,
    ) -> list[NormalizedMetricSample]:
        assert self._instance_id is not None
        instance_id: UUID = self._instance_id

        def sample(metric_name: str, value: float | None) -> NormalizedMetricSample | None:
            if value is None:
                return None
            return NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=metric_name,
                value=float(value),
                labels={},
            )

        candidates: list[NormalizedMetricSample | None] = [
            sample(DB2_ACTIVE_SESSION_COUNT, len(session_rows)),
        ]
        if db_rows:
            row = db_rows[0]
            candidates.extend([
                sample(DB2_DEADLOCKS_TOTAL, row.get("deadlocks")),
                sample(DB2_LOCK_WAITS_TOTAL, row.get("lock_waits")),
            ])
        if bp_rows:
            row = bp_rows[0]
            logical = float(row.get("logical_reads") or 0)
            physical = float(row.get("physical_reads") or 0)
            if logical > 0:
                candidates.append(sample(DB2_BP_HIT_RATIO, (logical - physical) / logical * 100.0))

        return [c for c in candidates if c is not None]

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        conn = await self._connect()
        loop = asyncio.get_event_loop()
        try:
            rows = await asyncio.wait_for(
                loop.run_in_executor(None, functools.partial(_execute_sync, conn, sql)),
                timeout=timeout_seconds,
            )
        finally:
            await loop.run_in_executor(None, functools.partial(_close_sync, conn))
        if not rows:
            return None
        try:
            return float(next(iter(rows[0].values())))
        except (TypeError, ValueError, StopIteration):
            return None

    # ── Varreduras de manutencao on-demand (app/collectors/db2/maintenance.py) ────────────
    # Uma conexao = um banco: `list_databases` vazio e `database` e ignorado.

    async def _with_runner(self, call):  # noqa: ANN001, ANN202
        conn = await self._connect()
        runner = db2_maintenance.Db2Runner(
            conn, get_settings().maintenance_per_database_timeout_seconds
        )
        _host, db_name = _parse_db2_host(self._connection_params)
        try:
            return await call(runner, db_name)
        finally:
            await asyncio.get_running_loop().run_in_executor(
                None, functools.partial(_close_sync, conn)
            )

    async def list_databases(self) -> list[str]:
        return []

    async def collect_index_health(
        self, database: str | None = None
    ) -> NormalizedIndexHealthReport:
        return await self._with_runner(db2_maintenance.collect_index_health)

    async def collect_statistics_health(
        self, database: str | None = None
    ) -> NormalizedStatisticsHealthReport:
        return await self._with_runner(db2_maintenance.collect_statistics_health)

    async def collect_memory_grants(self) -> NormalizedMemoryGrantReport:
        return await self._with_runner(db2_maintenance.collect_memory_grants)

    async def estimate_compression(
        self, database: str | None, compression: str, max_objects: int
    ) -> NormalizedCompressionReport:
        return await self._with_runner(
            lambda runner, db_name: db2_maintenance.estimate_compression(
                runner, db_name, compression, max_objects
            )
        )

    async def collect_partitions(self, database: str | None = None) -> NormalizedPartitionReport:
        return await self._with_runner(db2_maintenance.collect_partitions)

    async def close(self) -> None:
        logger.info("close chamado para instância db2 (instance_id=%s)", self._instance_id)
