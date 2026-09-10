"""Nomes canonicos de metric_name usados por ambos adaptadores em metric_samples."""

from __future__ import annotations

PG_ACTIVE_SESSION_COUNT = "postgresql_active_session_count"
PG_NUMBACKENDS = "postgresql_connections_total"
PG_XACT_COMMIT = "postgresql_xact_commit_total"
PG_XACT_ROLLBACK = "postgresql_xact_rollback_total"
PG_BLKS_READ = "postgresql_blocks_read_total"
PG_BLKS_HIT = "postgresql_blocks_hit_total"
PG_TUP_RETURNED = "postgresql_tuples_returned_total"
PG_TUP_FETCHED = "postgresql_tuples_fetched_total"
PG_TUP_INSERTED = "postgresql_tuples_inserted_total"
PG_TUP_UPDATED = "postgresql_tuples_updated_total"
PG_TUP_DELETED = "postgresql_tuples_deleted_total"
PG_DEADLOCKS = "postgresql_deadlocks_total"
PG_TEMP_FILES = "postgresql_temp_files_total"
PG_TEMP_BYTES = "postgresql_temp_bytes_total"

PG_CHECKPOINTS_TIMED = "postgresql_checkpoints_timed_total"
PG_CHECKPOINTS_REQUESTED = "postgresql_checkpoints_requested_total"
PG_CHECKPOINT_WRITE_TIME_MS = "postgresql_checkpoint_write_time_ms_total"
PG_CHECKPOINT_SYNC_TIME_MS = "postgresql_checkpoint_sync_time_ms_total"
PG_CHECKPOINT_BUFFERS_WRITTEN = "postgresql_checkpoint_buffers_written_total"

SQLSERVER_ACTIVE_SESSION_COUNT = "sqlserver_active_session_count"
SQLSERVER_PAGE_LIFE_EXPECTANCY = "sqlserver_page_life_expectancy_seconds"
SQLSERVER_PROCESSES_BLOCKED = "sqlserver_processes_blocked"
SQLSERVER_TARGET_SERVER_MEMORY_KB = "sqlserver_target_server_memory_kb"
SQLSERVER_TOTAL_SERVER_MEMORY_KB = "sqlserver_total_server_memory_kb"

COLLECTOR_CYCLE_DURATION_MS = "collector_cycle_duration_ms"
COLLECTOR_CYCLE_ERROR_COUNT = "collector_cycle_error_count"

SQLSERVER_RAW_COUNTER_TO_METRIC_NAME: dict[str, str] = {
    "Page life expectancy": SQLSERVER_PAGE_LIFE_EXPECTANCY,
    "Processes blocked": SQLSERVER_PROCESSES_BLOCKED,
    "Target Server Memory (KB)": SQLSERVER_TARGET_SERVER_MEMORY_KB,
    "Total Server Memory (KB)": SQLSERVER_TOTAL_SERVER_MEMORY_KB,
}
