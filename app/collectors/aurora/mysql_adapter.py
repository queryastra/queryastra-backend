"""Adaptador Aurora MySQL — compartilha o MySQLCollector.

Aurora MySQL e compativel com MySQL padrao, entao delega tudo ao `MySQLCollector`.
A diferenca e a deteccao da capability `AURORA_MYSQL_ENGINE` via:
    SHOW VARIABLES LIKE 'aurora_version'

Se nao houver resultado (MySQL vanilla), a capability e DEGRADED.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import AURORA_MYSQL_ENGINE
from app.collectors.connection import ConnectionParams
from app.collectors.mysql.adapter import MySQLCollector
from app.core.config import get_settings
from app.core.logging import get_logger
from app.normalizers.models import (
    NormalizedBackupStatus,
    NormalizedCompressionReport,
    NormalizedConfirmedTransactionError,
    NormalizedDeadlockEvent,
    NormalizedIndexHealthReport,
    NormalizedMemoryGrantReport,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedPartitionReport,
    NormalizedPlanCompileTimeoutFinding,
    NormalizedPlanSnapshot,
    NormalizedStatisticsHealthReport,
)

logger = get_logger(__name__)


class AuroraMySQLCollector:
    """Implementa `app.collectors.base.Collector` para instancias Aurora MySQL.

    Delega tudo ao `MySQLCollector` e adiciona deteccao de `aurora_version`.
    """

    def __init__(
        self,
        connection: ConnectionParams,
        instance_id: UUID | None = None,
    ) -> None:
        self._connection = connection
        self._instance_id = instance_id
        self._mysql = MySQLCollector(connection=connection, instance_id=instance_id)

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Delega ao MySQL collector e adiciona capability AURORA_MYSQL_ENGINE."""
        settings = get_settings()
        results = await self._mysql.check_capabilities()

        # Tenta detectar aurora_version via SHOW VARIABLES
        try:
            import asyncio

            import aiomysql

            conn = await asyncio.wait_for(
                aiomysql.connect(
                    host=self._connection.host,
                    port=self._connection.port,
                    user=self._connection.username or "",
                    password=self._connection.secret_value or "",
                    connect_timeout=int(self._connection.connect_timeout_seconds),
                    autocommit=True,
                ),
                timeout=self._connection.connect_timeout_seconds,
            )
            try:
                async with conn.cursor() as cur:
                    await asyncio.wait_for(
                        cur.execute("SHOW VARIABLES LIKE 'aurora_version'"),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                    row = await cur.fetchone()
                    if row:
                        aurora_ver = str(row[1])
                        results.append(
                            CapabilityResult(
                                capability=AURORA_MYSQL_ENGINE,
                                status=CapabilityStatus.FULL,
                                reason=aurora_ver,
                            )
                        )
                    else:
                        # MySQL vanilla — sem aurora_version
                        results.append(
                            CapabilityResult(
                                capability=AURORA_MYSQL_ENGINE,
                                status=CapabilityStatus.DEGRADED,
                                reason="aurora_version nao encontrada (pode ser MySQL vanilla)",
                            )
                        )
            finally:
                conn.close()
        except Exception as exc:
            logger.debug("Erro ao detectar aurora_version MySQL: %s", exc)
            results.append(
                CapabilityResult(
                    capability=AURORA_MYSQL_ENGINE,
                    status=CapabilityStatus.DEGRADED,
                    reason=str(exc),
                )
            )

        return results

    async def collect(
        self,
        *,
        sleeping_min_duration_ms: int | None = None,
        sleeping_database_overrides_ms: dict[str, int] | None = None,
    ) -> CollectionResult:
        return await self._mysql.collect(
            sleeping_min_duration_ms=sleeping_min_duration_ms,
            sleeping_database_overrides_ms=sleeping_database_overrides_ms,
        )

    async def close(self) -> None:
        await self._mysql.close()

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        return await self._mysql.collect_missing_index_suggestions()

    async def get_engine_start_time(self) -> datetime | None:
        return await self._mysql.get_engine_start_time()

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        return await self._mysql.collect_backup_status()

    async def collect_deadlock_events(self) -> list[NormalizedDeadlockEvent]:
        return await self._mysql.collect_deadlock_events()

    async def ensure_transaction_error_capture(self) -> bool:
        return await self._mysql.ensure_transaction_error_capture()

    async def collect_transaction_error_events(self) -> list[NormalizedConfirmedTransactionError]:
        return await self._mysql.collect_transaction_error_events()

    async def collect_freespace_metrics(self) -> list[NormalizedMetricSample]:
        return await self._mysql.collect_freespace_metrics()

    async def collect_replication_lag_metrics(self) -> list[NormalizedMetricSample]:
        return await self._mysql.collect_replication_lag_metrics()

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        return await self._mysql.run_scalar_query(sql, timeout_seconds)

    async def scan_plan_compile_timeouts(self) -> list[NormalizedPlanCompileTimeoutFinding]:
        return await self._mysql.scan_plan_compile_timeouts()

    async def scan_plan_history(self) -> list[NormalizedPlanSnapshot]:
        return await self._mysql.scan_plan_history()

    async def explain_queries(
        self, queries: list[tuple[str, str]]
    ) -> list[NormalizedPlanSnapshot]:
        return await self._mysql.explain_queries(queries)

    async def list_databases(self) -> list[str]:
        return await self._mysql.list_databases()

    async def collect_index_health(
        self, database: str | None = None
    ) -> NormalizedIndexHealthReport:
        return await self._mysql.collect_index_health(database)

    async def collect_statistics_health(
        self, database: str | None = None
    ) -> NormalizedStatisticsHealthReport:
        return await self._mysql.collect_statistics_health(database)

    async def collect_memory_grants(self) -> NormalizedMemoryGrantReport:
        return await self._mysql.collect_memory_grants()

    async def estimate_compression(
        self, database: str | None, compression: str, max_objects: int
    ) -> NormalizedCompressionReport:
        return await self._mysql.estimate_compression(database, compression, max_objects)

    async def collect_partitions(self, database: str | None = None) -> NormalizedPartitionReport:
        return await self._mysql.collect_partitions(database)
