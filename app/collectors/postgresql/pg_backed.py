"""Base class for collectors that delegate to PostgreSQLCollector.

CockroachDB, Aurora PostgreSQL and Redshift are wire-compatible with PostgreSQL,
so their adapters only customise check_capabilities() while delegating every
collection method to an inner PostgreSQLCollector. This class holds the shared
delegation logic once, so subclasses stay focused on their specific capability
detection.

Usage:
    class MyCollector(PostgreSQLBackedCollector):
        async def check_capabilities(self) -> list[CapabilityResult]:
            results = await self._pg.check_capabilities()
            # add engine-specific capabilities ...
            return results
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

from app.collectors.base import CapabilityResult, CollectionResult
from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.adapter import PostgreSQLCollector
from app.normalizers.models import (
    NormalizedBackupStatus,
    NormalizedConfirmedTransactionError,
    NormalizedDeadlockEvent,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedPlanCompileTimeoutFinding,
    NormalizedPlanSnapshot,
)


class PostgreSQLBackedCollector:
    """Mixin base for collectors that fully delegate to PostgreSQLCollector.

    Subclasses must:
    - call super().__init__(connection, instance_id) OR set self._pg themselves
    - implement check_capabilities()

    Subclasses may override collect() to patch the returned CollectionResult
    (e.g. to inject a vendor-specific engine_version string).
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
        raise NotImplementedError

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

    async def explain_queries(
        self, queries: list[tuple[str, str]]
    ) -> list[NormalizedPlanSnapshot]:
        return await self._pg.explain_queries(queries)
