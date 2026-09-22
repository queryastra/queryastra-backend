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
# Contador CUMULATIVO de deadlocks desde o start do servico (`sys.dm_os_performance_counters`,
# "Number of Deadlocks/sec", cntr_type=272696 — NAO faz parte de
# SQLSERVER_RAW_COUNTER_TO_METRIC_NAME, tipo de contador diferente). Comparado ciclo a ciclo pelo
# `PollingWorker` (que ja roda a cada `polling_interval_seconds`, ate 1s numa instancia critica)
# para detectar QUE um deadlock aconteceu na hora, antes mesmo do `system_health` publicar o
# relatorio completo (que pode demorar dezenas de minutos) — ver
# `app.repositories.deadlock_events_repository.DeadlockEventsRepository.insert_pending`.
SQLSERVER_DEADLOCKS_TOTAL = "sqlserver_deadlocks_total"
SQLSERVER_PAGE_LIFE_EXPECTANCY = "sqlserver_page_life_expectancy_seconds"
SQLSERVER_PROCESSES_BLOCKED = "sqlserver_processes_blocked"
SQLSERVER_TARGET_SERVER_MEMORY_KB = "sqlserver_target_server_memory_kb"
SQLSERVER_TOTAL_SERVER_MEMORY_KB = "sqlserver_total_server_memory_kb"
# `sys.dm_os_ring_buffers` (RING_BUFFER_SCHEDULER_MONITOR) — sem permissao especial alem de VIEW
# SERVER STATE, ja mandatoria. So SQL Server: PostgreSQL nao expoe CPU do processo nem do SO via
# SQL (sem view equivalente). Ambas em % (0-100).
SQLSERVER_INSTANCE_CPU_PERCENT = "sqlserver_instance_cpu_percent"
SQLSERVER_OS_CPU_PERCENT = "sqlserver_os_cpu_percent"

# Fase 2, item 2 (catalogo de alertas expandido) — coletados por um worker de baixa frequencia
# proprio (`app.workers.extended_health_supervisor`, nao o ciclo de poll rapido), ver
# `Docs/product-vision-and-roadmap.md`. Uma linha por volume/replica, com `labels` identificando
# qual (ex. `{"volume": "C:\\"}`, `{"replica": "AUTKLINUX20-SECONDARY"}`).
#
# Freespace: so SQL Server (`sys.dm_os_volume_stats`) — PostgreSQL nao expoe espaco livre de
# disco via SQL puro sem acesso ao SO (mesma classe de limitacao ja documentada pra CPU/deadlock
# neste projeto: "SQL Server pode, Postgres nao pode").
SQLSERVER_VOLUME_FREE_PERCENT = "sqlserver_volume_free_percent"
# Replicacao: SQL Server (Always On AG, fila de log/redo em KB — quanto maior, mais atrasada a
# replica) e PostgreSQL (streaming replication, atraso de replay em segundos). Sem replicacao
# configurada, a metrica simplesmente nunca aparece (mesmo padrao de degradacao graciosa).
SQLSERVER_REPLICA_LOG_SEND_QUEUE_KB = "sqlserver_replica_log_send_queue_kb"
SQLSERVER_REPLICA_REDO_QUEUE_KB = "sqlserver_replica_redo_queue_kb"
PG_REPLICA_REPLAY_LAG_SECONDS = "postgresql_replica_replay_lag_seconds"
# Backup atrasado: coletado pelo `BackupStatusSupervisor` (ja existente, Etapa 10/Q16) a partir
# do proprio `NormalizedBackupStatus` — 1 valor por INSTANCIA (pior caso entre os bancos, quando
# ha mais de um), nao por banco. "Nunca fez backup" vira um valor bem alto
# (`NEVER_BACKED_UP_SENTINEL_DAYS`, nao infinito) para que qualquer threshold razoavel dispare
# critico sem exigir tratamento especial de "sem dado" no worker de alertas. Mesma engine
# threshold comum de metrica (sem rule_type proprio), mesmo padrao de freespace/replicacao acima.
SQLSERVER_BACKUP_DAYS_SINCE_LAST = "sqlserver_backup_days_since_last"
PG_BACKUP_DAYS_SINCE_LAST = "postgresql_backup_days_since_last"

COLLECTOR_CYCLE_DURATION_MS = "collector_cycle_duration_ms"
COLLECTOR_CYCLE_ERROR_COUNT = "collector_cycle_error_count"

SQLSERVER_RAW_COUNTER_TO_METRIC_NAME: dict[str, str] = {
    "Page life expectancy": SQLSERVER_PAGE_LIFE_EXPECTANCY,
    "Processes blocked": SQLSERVER_PROCESSES_BLOCKED,
    "Target Server Memory (KB)": SQLSERVER_TARGET_SERVER_MEMORY_KB,
    "Total Server Memory (KB)": SQLSERVER_TOTAL_SERVER_MEMORY_KB,
}
