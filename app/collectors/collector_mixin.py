"""CollectorNoOpMixin — padrões no-op para métodos opcionais do protocolo Collector.

Engines que não implementam certas capacidades (deadlocks, planos, freespace, etc.)
herdam este mixin para evitar repetição de stubs idênticos. Cada método retorna o
valor semanticamente correto para "não disponível": lista vazia, None ou True.

Uso:
    class MyCollector(CollectorNoOpMixin):
        # implementa apenas collect(), close(), check_capabilities() e os métodos
        # que este engine realmente suporta; o resto vem do mixin.
        ...
"""

from __future__ import annotations

from datetime import datetime

from app.normalizers.models import (
    NormalizedAutovacuumWorker,
    NormalizedBackupStatus,
    NormalizedBgwriterStats,
    NormalizedBufferTableStats,
    NormalizedConfirmedTransactionError,
    NormalizedConfigComplianceItem,
    NormalizedConnectionConsumer,
    NormalizedDbSize,
    NormalizedFileIoStats,
    NormalizedQueryStatsSnapshot,
    NormalizedDeadlockEvent,
    NormalizedIdleInTransactionSession,
    NormalizedIndexBloatEntry,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedPlanCompileTimeoutFinding,
    NormalizedPlanSnapshot,
    NormalizedReplicationSlot,
    NormalizedSchemaHealthItem,
    NormalizedTableIoStats,
    NormalizedTempDbStats,
    NormalizedTempfileStats,
    NormalizedUnusedIndex,
    NormalizedUserFunction,
    NormalizedVacuumTableStats,
)


class CollectorNoOpMixin:
    """No-op defaults for Collector protocol methods not implemented by an engine."""

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        return []

    async def get_engine_start_time(self) -> datetime | None:
        return None

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        return []

    async def collect_deadlock_events(self) -> list[NormalizedDeadlockEvent]:
        return []

    async def ensure_transaction_error_capture(self) -> bool:
        return True

    async def collect_transaction_error_events(self) -> list[NormalizedConfirmedTransactionError]:
        return []

    async def collect_freespace_metrics(self) -> list[NormalizedMetricSample]:
        return []

    async def collect_replication_lag_metrics(self) -> list[NormalizedMetricSample]:
        return []

    async def scan_plan_compile_timeouts(self) -> list[NormalizedPlanCompileTimeoutFinding]:
        return []

    async def scan_plan_history(self) -> list[NormalizedPlanSnapshot]:
        return []

    async def explain_queries(
        self, queries: list[tuple[str, str]]
    ) -> list[NormalizedPlanSnapshot]:
        return []

    async def collect_vacuum_stats(
        self,
    ) -> tuple[list[NormalizedVacuumTableStats], list[NormalizedAutovacuumWorker]]:
        return [], []

    async def collect_idle_in_transaction(self) -> list[NormalizedIdleInTransactionSession]:
        return []

    async def collect_index_bloat(self) -> list[NormalizedIndexBloatEntry]:
        return []

    async def collect_bgwriter_stats(self) -> NormalizedBgwriterStats | None:
        return None

    async def collect_user_functions(self) -> list[NormalizedUserFunction]:
        return []

    async def collect_replication_slots(self) -> list[NormalizedReplicationSlot]:
        return []

    async def collect_config_compliance(self) -> list[NormalizedConfigComplianceItem]:
        return []

    async def collect_connection_consumers(self) -> list[NormalizedConnectionConsumer]:
        return []

    async def collect_tempdb_stats(self) -> NormalizedTempDbStats | None:
        return None

    async def collect_db_sizes(self) -> list[NormalizedDbSize]:
        return []

    async def collect_query_stats_snapshot(self) -> list[NormalizedQueryStatsSnapshot]:
        return []

    async def collect_unused_indexes(self) -> list[NormalizedUnusedIndex]:
        return []

    async def collect_table_io_stats(self) -> list[NormalizedTableIoStats]:
        return []

    async def collect_schema_health(self) -> list[NormalizedSchemaHealthItem]:
        return []

    async def collect_buffer_cache_stats(
        self,
    ) -> tuple[list[NormalizedBufferTableStats], list[NormalizedTempfileStats], list[NormalizedFileIoStats]]:
        return [], [], []

    async def explain_plan_for_hash(self, sql_text: str) -> dict | None:
        return None
