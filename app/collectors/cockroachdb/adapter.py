"""Adaptador CockroachDB — delega ao PostgreSQLCollector.

CockroachDB e compativel com o protocolo de wire do PostgreSQL, entao usa asyncpg
internamente via `PostgreSQLCollector`. Este wrapper adiciona apenas a deteccao
da versao do CockroachDB e a capability `COCKROACHDB_ENGINE`.

Detecta versao via `SELECT version()` — retorna string como:
    "CockroachDB CCL v22.2.0 (..."
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import COCKROACHDB_ENGINE
from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.adapter import PostgreSQLCollector
from app.core.config import get_settings
from app.core.logging import get_logger
from app.normalizers.models import (
    NormalizedBackupStatus,
    NormalizedBlockingEvent,
    NormalizedConfirmedTransactionError,
    NormalizedDeadlockEvent,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedPlanCompileTimeoutFinding,
    NormalizedPlanSnapshot,
    NormalizedQuerySample,
    NormalizedTransactionEvent,
)

logger = get_logger(__name__)


class CockroachDBCollector:
    """Implementa `app.collectors.base.Collector` para instancias CockroachDB.

    Delega tudo ao `PostgreSQLCollector` exceto a deteccao da versao
    e a adicao da capability `COCKROACHDB_ENGINE`.
    """

    def __init__(
        self,
        connection: ConnectionParams,
        instance_id: UUID | None = None,
    ) -> None:
        self._connection = connection
        self._instance_id = instance_id
        self._pg = PostgreSQLCollector(connection=connection, instance_id=instance_id)
        self._crdb_version: str | None = None

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Delega ao PG collector e adiciona capability COCKROACHDB_ENGINE."""
        settings = get_settings()
        results = await self._pg.check_capabilities()

        # Detecta versao do CockroachDB
        try:
            import asyncpg

            conn = await asyncpg.connect(
                host=self._connection.host,
                port=self._connection.port,
                user=self._connection.username or "root",
                password=self._connection.secret_value or "",
                timeout=self._connection.connect_timeout_seconds,
            )
            try:
                import asyncio

                row = await asyncio.wait_for(
                    conn.fetchrow("SELECT version()"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                if row:
                    version_str = str(row[0])
                    self._crdb_version = version_str
                    results.append(
                        CapabilityResult(
                            capability=COCKROACHDB_ENGINE,
                            status=CapabilityStatus.FULL,
                            reason=None,
                        )
                    )
            finally:
                await conn.close()
        except Exception as exc:
            logger.debug("Nao foi possivel detectar versao CockroachDB: %s", exc)
            results.append(
                CapabilityResult(
                    capability=COCKROACHDB_ENGINE,
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
        """Delega ao PG collector e sobrescreve engine_version se detectada."""
        result = await self._pg.collect(
            sleeping_min_duration_ms=sleeping_min_duration_ms,
            sleeping_database_overrides_ms=sleeping_database_overrides_ms,
        )
        if self._crdb_version is not None:
            import dataclasses

            result = dataclasses.replace(result, engine_version=self._crdb_version)
        return result

    async def close(self) -> None:
        await self._pg.close()

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        return await self._pg.collect_missing_index_suggestions()

    async def get_engine_start_time(self) -> datetime | None:
        return await self._pg.get_engine_start_time()

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        return await self._pg.collect_backup_status()

    async def collect_deadlock_events(self) -> list[NormalizedDeadlockEvent]:
        return await self._pg.collect_deadlock_events()

    async def ensure_transaction_error_capture(self) -> bool:
        return await self._pg.ensure_transaction_error_capture()

    async def collect_transaction_error_events(self) -> list[NormalizedConfirmedTransactionError]:
        return await self._pg.collect_transaction_error_events()

    async def collect_freespace_metrics(self) -> list[NormalizedMetricSample]:
        return await self._pg.collect_freespace_metrics()

    async def collect_replication_lag_metrics(self) -> list[NormalizedMetricSample]:
        return await self._pg.collect_replication_lag_metrics()

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        return await self._pg.run_scalar_query(sql, timeout_seconds)

    async def scan_plan_compile_timeouts(self) -> list[NormalizedPlanCompileTimeoutFinding]:
        return await self._pg.scan_plan_compile_timeouts()

    async def scan_plan_history(self) -> list[NormalizedPlanSnapshot]:
        return await self._pg.scan_plan_history()
