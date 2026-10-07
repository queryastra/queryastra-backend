"""Adaptador Aurora PostgreSQL — estende o PostgreSQLCollector.

Aurora PostgreSQL e compativel com PostgreSQL padrao, entao delega tudo ao
`PostgreSQLCollector`. A diferenca e a deteccao da capability `AURORA_POSTGRESQL_ENGINE`
via `SELECT aurora_version()`.

Se `aurora_version()` falhar (instancia PostgreSQL vanilla), a capability e DEGRADED
— nao reprova o cadastro.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import AURORA_POSTGRESQL_ENGINE
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


class AuroraPostgreSQLCollector:
    """Implementa `app.collectors.base.Collector` para instancias Aurora PostgreSQL.

    Delega tudo ao `PostgreSQLCollector` e adiciona deteccao de `aurora_version()`.
    """

    def __init__(
        self,
        connection: ConnectionParams,
        instance_id: UUID | None = None,
    ) -> None:
        self._connection = connection
        self._instance_id = instance_id
        self._pg = PostgreSQLCollector(connection=connection, instance_id=instance_id)

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Delega ao PG collector e adiciona capability AURORA_POSTGRESQL_ENGINE."""
        settings = get_settings()
        results = await self._pg.check_capabilities()

        # Tenta detectar aurora_version()
        try:
            import asyncio

            import asyncpg

            conn = await asyncpg.connect(
                host=self._connection.host,
                port=self._connection.port,
                user=self._connection.username or "postgres",
                password=self._connection.secret_value or "",
                timeout=self._connection.connect_timeout_seconds,
            )
            try:
                row = await asyncio.wait_for(
                    conn.fetchrow("SELECT aurora_version()"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                aurora_ver = str(row[0]) if row else None
                results.append(
                    CapabilityResult(
                        capability=AURORA_POSTGRESQL_ENGINE,
                        status=CapabilityStatus.FULL,
                        reason=aurora_ver,
                    )
                )
            finally:
                await conn.close()
        except Exception as exc:
            # Pode ser PostgreSQL vanilla — degrada graciosamente
            logger.debug("aurora_version() nao disponivel (talvez PG vanilla): %s", exc)
            results.append(
                CapabilityResult(
                    capability=AURORA_POSTGRESQL_ENGINE,
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
        return await self._pg.collect(
            sleeping_min_duration_ms=sleeping_min_duration_ms,
            sleeping_database_overrides_ms=sleeping_database_overrides_ms,
        )

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
