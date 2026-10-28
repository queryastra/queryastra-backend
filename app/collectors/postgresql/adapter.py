"""Adaptador PostgreSQL. Fonte de verdade: `Docs/collector-queries.md`.

Usa `asyncpg` (Docs/database-schema.md, ARCHITECTURE_SPEC.md §4) como driver assincrono.

`check_capabilities()` abre uma conexao curta (timeout configuravel) e checa, via
`pg_has_role`, se o usuario de coleta tem as roles minimas confirmadas em
`Docs/collector-queries.md` §7 (`pg_read_all_stats`, `pg_read_all_settings`,
`pg_signal_backend`) — mandatorias — e a disponibilidade opcional de `pg_stat_statements`
("Top SQL"), que so degrada, nunca reprova o cadastro sozinha.

`collect()` executa um ciclo real de coleta somente leitura, com timeout por query
(`settings.collection_query_timeout_seconds`) e limite de linhas truncado em Python apos o
fetch (`settings.collection_max_rows`) — nunca DDL/DML:
    - `pg_stat_activity` (WHERE state != 'idle') — sessao/query ativa (collector-queries.md §1),
      normalizada em `NormalizedQuerySample`. `sql_hash` usa `mask_sql_literals` (mascaramento
      total, identidade estavel pro agrupamento de Top SQL); `sql_text_masked`/
      `sql_text_full_masked` (texto exibido) usam `mask_sql_literals_for_display` (mascaramento
      seletivo — so PII, ver `app/masking/pii_classification.py` e `app/masking/sql_masking.py`).
    - `pg_blocking_pids(pid)` — deteccao de bloqueio (collector-queries.md §2); devolve o fecho
      TRANSITIVO de bloqueadores, reduzido pro(s) bloqueador(es) DIRETO(s) por
      `_reduce_to_direct_edges` antes de virar `NormalizedBlockingEvent` — a arvore completa
      (Etapa 9) e reconstruida no app encadeando essas arestas diretas de um mesmo snapshot
      (`app.api.v1.waits_blocking`), nao via CTE recursiva ao vivo. `wait_time_ms` e uma
      aproximacao (`collected_at - query_start` da sessao bloqueada, ou `0`) — Postgres nao expoe
      duracao nativa de espera de lock. `_ALL_SESSIONS_QUERY` identifica bloqueadores OCIOSOS
      (sem `state` ativo); `_BLOCKED_OBJECT_QUERY` (`pg_locks`) resolve o objeto bloqueado.
    - `pg_stat_database` (agregado via SUM, cluster inteiro — `ConnectionParams` nao carrega
      banco especifico) e `pg_stat_bgwriter` (< PG17) / `pg_stat_checkpointer` (>= PG17) —
      metricas de recurso/instancia (collector-queries.md §5). Versao e detectada a cada ciclo
      dentro de `collect()` (via `server_version_num`), nunca persistida em `Instance` (corte de
      escopo desta feature — `engine_version*` fica para o health-check de cadastro).
    - `pg_stat_statements` (extensao opcional) — Top SQL agregado: **nao implementado** em
      `collect()` nesta feature (so a capacidade e checada em `check_capabilities()`).
    - Detalhe de deadlock: **nao disponivel via SQL** neste motor — so
      `pg_stat_database.deadlocks` (contador agregado, coletado como metrica); detalhe fica em
      log do servidor (collector-queries.md §4) — `deadlock_events` fora de escopo (corte).

Permissao minima esperada: role `pg_read_all_stats`/`pg_read_all_settings`/`pg_signal_backend`
(ou superuser) — collector-queries.md §7.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import UTC, datetime
from uuid import UUID

import asyncpg

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    CONNECTIVITY,
    POSTGRESQL_CONFIGURATION_VISIBILITY,
    POSTGRESQL_QUALSTATS,
    POSTGRESQL_SESSION_ACTIVITY,
    POSTGRESQL_SESSION_TERMINATION,
    POSTGRESQL_TOP_SQL,
)
from app.collectors.connection import ConnectionParams
from app.collectors.transaction_flagging import compute_flag_reason
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals, mask_sql_literals_for_display
from app.normalizers.metric_names import (
    PG_ACTIVE_SESSION_COUNT,
    PG_BLKS_HIT,
    PG_BLKS_READ,
    PG_CHECKPOINT_BUFFERS_WRITTEN,
    PG_CHECKPOINT_SYNC_TIME_MS,
    PG_CHECKPOINT_WRITE_TIME_MS,
    PG_CHECKPOINTS_REQUESTED,
    PG_CHECKPOINTS_TIMED,
    PG_DEADLOCKS,
    PG_NUMBACKENDS,
    PG_REPLICA_REPLAY_LAG_SECONDS,
    PG_TEMP_BYTES,
    PG_TEMP_FILES,
    PG_TUP_DELETED,
    PG_TUP_FETCHED,
    PG_TUP_INSERTED,
    PG_TUP_RETURNED,
    PG_TUP_UPDATED,
    PG_XACT_COMMIT,
    PG_XACT_ROLLBACK,
)
from app.normalizers.models import (
    NormalizedAutovacuumWorker,
    NormalizedBackupStatus,
    NormalizedBgwriterStats,
    NormalizedBlockingEvent,
    NormalizedBufferTableStats,
    NormalizedConfirmedTransactionError,
    NormalizedConfigComplianceItem,
    NormalizedConnectionConsumer,
    NormalizedDbSize,
    NormalizedQueryStatsSnapshot,
    NormalizedDeadlockEvent,
    NormalizedIdleInTransactionSession,
    NormalizedIndexBloatEntry,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedPlanCompileTimeoutFinding,
    NormalizedPlanSnapshot,
    NormalizedQuerySample,
    NormalizedReplicationSlot,
    NormalizedSchemaHealthItem,
    NormalizedTableIoStats,
    NormalizedTempfileStats,
    NormalizedTransactionEvent,
    NormalizedUnusedIndex,
    NormalizedUserFunction,
    NormalizedVacuumTableStats,
)
from app.normalizers.wait_taxonomy import map_wait_event

logger = get_logger(__name__)

# `track_activity_query_size` minimo para nao truncar query capturada demais cedo
# (default do Postgres e 1024 — Docs/collector-queries.md §7).
_MIN_TRACK_ACTIVITY_QUERY_SIZE = 4096

# PG17 substitui `pg_stat_bgwriter.checkpoints_*` por `pg_stat_checkpointer` (colunas renomeadas
# `num_timed`/`num_requested`/`write_time`/`sync_time`/`buffers_written`).
_PG17_VERSION_NUM = 170000

_SESSION_ACTIVITY_QUERY = """
    SELECT pid, usename, datname, application_name, client_addr, backend_start, xact_start,
           query_start, state_change, wait_event_type, wait_event, state, backend_type, query
    FROM pg_stat_activity
    WHERE state != 'idle'
"""

_BLOCKING_QUERY = """
    SELECT pid, pg_blocking_pids(pid) AS blocked_by, query
    FROM pg_stat_activity
    WHERE cardinality(pg_blocking_pids(pid)) > 0
"""

# Etapa 9 (arvore completa de bloqueio) — identidade de TODAS as sessoes conectadas, sem o filtro
# `state != 'idle'` de `_SESSION_ACTIVITY_QUERY`. Necessaria pro "bloqueador ocioso": uma sessao
# com transacao aberta segurando lock sem estar rodando nada (`state = 'idle in transaction'` ou,
# no caso raro de lock preservado por outro motivo, mesmo `'idle'`) nao aparece em
# `_SESSION_ACTIVITY_QUERY` — sem isso, ficaria sem usuario/programa/maquina na arvore.
# `datname`/`xact_start`/`state` (Etapa 10 — rastreamento de transacoes) reaproveitados daqui:
# uma transacao aberta pode estar em QUALQUER estado (inclusive "idle in transaction", o caso
# mais comum de transacao orfa), nao so nas sessoes ativas de `_SESSION_ACTIVITY_QUERY`.
_ALL_SESSIONS_QUERY = """
    SELECT pid, usename, application_name, client_addr, datname, xact_start, state
    FROM pg_stat_activity
"""

# Etapa 9 — recurso/objeto bloqueado. `pg_locks` e uma view do sistema (sem role especial
# exigida) — `NOT granted` filtra so os PEDIDOS de lock ainda esperando (o pid bloqueado). `mode`
# e o modo de lock pedido (ex. "ShareLock"); `relation::regclass` resolve o nome qualificado da
# tabela quando o lock e sobre uma relacao (NULL pra locktype sem relacao associada, ex.
# transactionid/tuple/advisory — `blocked_object` fica NULL nesse caso, nao e erro).
_BLOCKED_OBJECT_QUERY = """
    SELECT pid, locktype, mode, relation::regclass::text AS blocked_object
    FROM pg_locks
    WHERE NOT granted
"""

_DATABASE_STATS_QUERY = """
    SELECT sum(numbackends) AS numbackends, sum(xact_commit) AS xact_commit,
           sum(xact_rollback) AS xact_rollback, sum(blks_read) AS blks_read,
           sum(blks_hit) AS blks_hit, sum(tup_returned) AS tup_returned,
           sum(tup_fetched) AS tup_fetched, sum(tup_inserted) AS tup_inserted,
           sum(tup_updated) AS tup_updated, sum(tup_deleted) AS tup_deleted,
           sum(deadlocks) AS deadlocks, sum(temp_files) AS temp_files,
           sum(temp_bytes) AS temp_bytes
    FROM pg_stat_database
"""

_BGWRITER_QUERY = """
    SELECT checkpoints_timed, checkpoints_req, checkpoint_write_time, checkpoint_sync_time,
           buffers_checkpoint
    FROM pg_stat_bgwriter
"""

_CHECKPOINTER_QUERY = """
    SELECT num_timed, num_requested, write_time, sync_time, buffers_written
    FROM pg_stat_checkpointer
"""

# `datallowconn=false` (ex.: templates) nunca aceitaria conexao; `datistemplate=false` exclui
# template0/template1 mesmo que `datallowconn` estivesse true por engano de configuracao.
_LIST_DATABASES_QUERY = """
    SELECT datname FROM pg_database
    WHERE datistemplate = false AND datallowconn = true
"""

# Heuristica (Docs/collector-queries.md §10) — sem DMV nativa equivalente ao
# `sys.dm_db_missing_index_details` do SQL Server. `pg_stat_user_tables` e escopada ao banco
# CONECTADO (ao contrario de `pg_stat_activity`/`pg_stat_database`, cluster-wide) — por isso
# `collect_missing_index_suggestions()` itera por banco em vez de rodar uma unica vez.
_MISSING_INDEX_HEURISTIC_QUERY = """
    SELECT schemaname, relname AS table_name,
           seq_scan, idx_scan, n_live_tup,
           pg_relation_size(relid) AS table_size_bytes
    FROM pg_stat_user_tables
    WHERE seq_scan > COALESCE(idx_scan, 0)
      AND pg_relation_size(relid) > $1
      AND seq_scan > $2
    ORDER BY seq_scan - COALESCE(idx_scan, 0) DESC
"""

# pg_qualstats_index_advisor() retorna sugestoes de indice com indicacao de coluna.
# Requer a extensao `pg_qualstats` instalada (shared_preload_libraries) no servidor monitorado.
# A funcao aceita parametros opcionais (min_filter, min_selectivity) — chamada sem argumentos usa
# os defaults do pg_qualstats (min_filter=1000, min_selectivity=0.1 em versoes recentes).
_QUALSTATS_INDEX_ADVISOR_QUERY = """
    SELECT
        nspname || '.' || relname AS table_name,
        attnames,
        ddl
    FROM pg_qualstats_index_advisor() AS t(relid oid, nspname text, relname text, attnames text[], indexes text[], ddl text)
    WHERE ddl IS NOT NULL
    ORDER BY nspname, relname
"""

# Fase 5 T54 — estatisticas de vacuum por tabela. Exclui tabelas < 1MB para reduzir ruido.
# Ordenadas por dead tuples DESC para destacar candidatos urgentes primeiro.
_VACUUM_TABLE_STATS_QUERY = """
    SELECT
        s.schemaname,
        s.relname AS table_name,
        COALESCE(s.n_dead_tup, 0) AS n_dead_tup,
        COALESCE(s.n_live_tup, 0) AS n_live_tup,
        CASE
            WHEN COALESCE(s.n_live_tup, 0) + COALESCE(s.n_dead_tup, 0) > 0
            THEN round(100.0 * COALESCE(s.n_dead_tup, 0)
                 / (COALESCE(s.n_live_tup, 0) + COALESCE(s.n_dead_tup, 0)), 2)
            ELSE 0
        END AS dead_tup_pct,
        s.last_vacuum,
        s.last_autovacuum,
        s.last_analyze,
        s.last_autoanalyze,
        COALESCE(s.vacuum_count, 0) AS vacuum_count,
        COALESCE(s.autovacuum_count, 0) AS autovacuum_count,
        pg_relation_size(s.relid) AS table_size_bytes,
        age(c.relfrozenxid) AS xid_age
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.oid = s.relid
    WHERE pg_relation_size(s.relid) > 1048576
    ORDER BY COALESCE(s.n_dead_tup, 0) DESC
    LIMIT 200
"""

# Workers de autovacuum atualmente em execucao.
_AUTOVACUUM_WORKERS_QUERY = """
    SELECT
        pid,
        COALESCE(datname, '') AS datname,
        COALESCE(query, '') AS query,
        EXTRACT(EPOCH FROM (now() - query_start))::int AS running_for_seconds
    FROM pg_stat_activity
    WHERE application_name = 'autovacuum worker'
      AND state != 'idle'
    ORDER BY running_for_seconds DESC
"""

# Fase 5 T56 — sessoes com transacao aberta mas sem query em execucao.
_IDLE_IN_TRANSACTION_QUERY = """
    SELECT
        pid AS session_id,
        COALESCE(usename, '') AS username,
        COALESCE(application_name, '') AS application_name,
        COALESCE(client_addr::text, '') AS client_info,
        state,
        EXTRACT(EPOCH FROM (now() - state_change))::int AS idle_seconds,
        COALESCE(query, '') AS last_query
    FROM pg_stat_activity
    WHERE state IN ('idle in transaction', 'idle in transaction (aborted)')
      AND state_change IS NOT NULL
    ORDER BY idle_seconds DESC
"""

# Fase 5 T55 — bloat de indices. Usa dead_tup% da tabela como proxy de fragmentacao do indice.
# Apenas indices > 1MB. Ordenado por tamanho DESC para priorizar os que ocupam mais espaco.
_INDEX_BLOAT_QUERY = """
    SELECT
        s.schemaname AS schema_name,
        s.relname AS table_name,
        s.indexrelname AS index_name,
        pg_relation_size(s.indexrelid) AS index_size_bytes,
        COALESCE(s.idx_scan, 0) AS idx_scan,
        COALESCE(s.idx_tup_read, 0) AS idx_tup_read,
        COALESCE(s.idx_tup_fetch, 0) AS idx_tup_fetch,
        CASE
            WHEN COALESCE(t.n_live_tup, 0) + COALESCE(t.n_dead_tup, 0) > 0
            THEN round(100.0 * COALESCE(t.n_dead_tup, 0)
                 / (COALESCE(t.n_live_tup, 0) + COALESCE(t.n_dead_tup, 0)), 1)::float
            ELSE 0.0
        END AS bloat_pct
    FROM pg_stat_user_indexes s
    JOIN pg_stat_user_tables t ON t.schemaname = s.schemaname AND t.relname = s.relname
    WHERE pg_relation_size(s.indexrelid) > 1048576
    ORDER BY pg_relation_size(s.indexrelid) DESC
    LIMIT 100
"""

# Fase 5 T57 — bgwriter + checkpoints. `pg_stat_bgwriter` e cluster-wide, sempre 1 linha.
_BGWRITER_STATS_QUERY = """
    SELECT
        checkpoints_timed,
        checkpoints_req,
        checkpoint_write_time,
        checkpoint_sync_time,
        buffers_checkpoint,
        buffers_clean,
        maxwritten_clean,
        buffers_backend,
        buffers_backend_fsync,
        buffers_alloc,
        stats_reset
    FROM pg_stat_bgwriter
"""

# PG17+ split checkpoint columns para pg_stat_checkpointer; buffers_backend removido tbm.
_BGWRITER_STATS_QUERY_PG17 = """
    SELECT
        c.num_timed        AS checkpoints_timed,
        c.num_requested    AS checkpoints_req,
        c.write_time       AS checkpoint_write_time,
        c.sync_time        AS checkpoint_sync_time,
        c.buffers_written  AS buffers_checkpoint,
        b.buffers_clean,
        b.maxwritten_clean,
        0                  AS buffers_backend,
        0                  AS buffers_backend_fsync,
        b.buffers_alloc,
        b.stats_reset
    FROM pg_stat_bgwriter b CROSS JOIN pg_stat_checkpointer c
"""

# Fase 5 T58 — funcoes PL/pgSQL por tempo total. So retorna dados se track_functions != 'none'.
_USER_FUNCTIONS_QUERY = """
    SELECT
        schemaname AS schema_name,
        funcname AS func_name,
        calls,
        total_time AS total_time_ms,
        self_time AS self_time_ms,
        CASE WHEN calls > 0 THEN round((total_time / calls)::numeric, 3)::float ELSE 0 END AS avg_time_ms
    FROM pg_stat_user_functions
    ORDER BY total_time DESC
    LIMIT 50
"""

# Fase 5 T59 — slots de replicacao com lag em bytes. Slots inativos com lag > 1GB sao risco critico.
# wal_status disponivel apenas em PG 13+ — COALESCE cobre versoes anteriores.
_REPLICATION_SLOTS_QUERY = """
    SELECT
        rs.slot_name,
        rs.plugin,
        rs.slot_type,
        COALESCE(d.datname, '') AS database_name,
        rs.active,
        rs.active_pid,
        CASE
            WHEN rs.restart_lsn IS NOT NULL
            THEN (pg_current_wal_lsn() - rs.restart_lsn)::bigint
            ELSE NULL
        END AS lag_bytes,
        CAST(NULL AS text) AS wal_status
    FROM pg_replication_slots rs
    LEFT JOIN pg_database d ON d.oid = rs.datoid
    ORDER BY CASE WHEN rs.restart_lsn IS NOT NULL
                  THEN (pg_current_wal_lsn() - rs.restart_lsn)::bigint
                  ELSE 0
             END DESC
"""

# Mesma query com wal_status (PG 13+) — fallback dinamico no metodo collect_replication_slots.
_REPLICATION_SLOTS_QUERY_PG13 = """
    SELECT
        rs.slot_name,
        rs.plugin,
        rs.slot_type,
        COALESCE(d.datname, '') AS database_name,
        rs.active,
        rs.active_pid,
        CASE
            WHEN rs.restart_lsn IS NOT NULL
            THEN (pg_current_wal_lsn() - rs.restart_lsn)::bigint
            ELSE NULL
        END AS lag_bytes,
        rs.wal_status
    FROM pg_replication_slots rs
    LEFT JOIN pg_database d ON d.oid = rs.datoid
    ORDER BY CASE WHEN rs.restart_lsn IS NOT NULL
                  THEN (pg_current_wal_lsn() - rs.restart_lsn)::bigint
                  ELSE 0
             END DESC
"""

# Fase 5 T65 — consumidores de conexao agrupados por (application_name, client_addr, usename).
_CONNECTION_CONSUMERS_QUERY = """
    SELECT
        COALESCE(application_name, '') AS application_name,
        client_addr::text             AS client_addr,
        COALESCE(usename, '')         AS username,
        COUNT(*)                      AS total,
        COUNT(*) FILTER (WHERE state = 'active')                      AS active,
        COUNT(*) FILTER (WHERE state = 'idle')                        AS idle,
        COUNT(*) FILTER (WHERE state = 'idle in transaction')         AS idle_in_transaction,
        COUNT(*) FILTER (WHERE wait_event_type IS NOT NULL
                            AND state = 'active')                     AS waiting
    FROM pg_stat_activity
    WHERE pid <> pg_backend_pid()
    GROUP BY application_name, client_addr, usename
    ORDER BY total DESC, idle_in_transaction DESC
"""

# Fase 5 T69 — indices com zero scans. Exclui PKs (indisprimary). Minimo 128KB para ignorar
# indices triviais em tabelas pequenas. Ordenado por tamanho DESC para priorizar economia de espaco.
_UNUSED_INDEXES_QUERY = """
    SELECT
        s.schemaname AS schema_name,
        s.relname AS table_name,
        s.indexrelname AS index_name,
        pg_relation_size(s.indexrelid) AS index_size_bytes,
        COALESCE(s.idx_scan, 0) AS idx_scan
    FROM pg_stat_user_indexes s
    JOIN pg_index i ON i.indexrelid = s.indexrelid
    WHERE COALESCE(s.idx_scan, 0) = 0
      AND NOT i.indisprimary
      AND pg_relation_size(s.indexrelid) > 131072
    ORDER BY pg_relation_size(s.indexrelid) DESC
    LIMIT 50
"""

# Fase 5 T68 — I/O de tabelas do pg_stat_user_tables. Tabelas com seq_scan alto e idx_scan
# baixo sao candidatas a criacao de indice. Dead tuples altos indicam necessidade de VACUUM.
_TABLE_IO_STATS_QUERY = """
    SELECT
        schemaname AS schema_name,
        relname AS table_name,
        COALESCE(seq_scan, 0) AS seq_scan,
        COALESCE(idx_scan, 0) AS idx_scan,
        COALESCE(n_live_tup, 0) AS n_live_tup,
        COALESCE(n_dead_tup, 0) AS n_dead_tup,
        pg_relation_size(relid) AS table_size_bytes,
        last_analyze,
        last_autoanalyze,
        last_vacuum,
        last_autovacuum
    FROM pg_stat_user_tables
    WHERE COALESCE(seq_scan, 0) > 0
    ORDER BY seq_scan DESC
    LIMIT 50
"""

# Fase 6 T60 — snapshot periodico de pg_stat_statements (top-200 por total_exec_time).
# Valores acumulativos; deltas calculados via LAG() no repositorio.
# LEFT(query, 500): textos longos nao agregam valor para identificacao e enchem o banco.
_QUERY_STATS_SNAPSHOT_QUERY = """
    SELECT
        queryid,
        LEFT(query, 500)   AS query_text,
        calls,
        total_exec_time,
        rows               AS total_rows
    FROM pg_stat_statements
    WHERE calls > 0
    ORDER BY total_exec_time DESC
    LIMIT 200
"""

# Schema health — duplicate indexes, tables without PK, FKs without supporting index.
_SCHEMA_HEALTH_DUPLICATE_INDEXES_QUERY = """
SELECT
    n.nspname AS schema_name,
    t.relname AS table_name,
    i1.relname AS index_name,
    i2.relname AS duplicate_of,
    pg_get_indexdef(ix1.indexrelid) AS index_def
FROM pg_index ix1
JOIN pg_index ix2 ON ix1.indrelid = ix2.indrelid
    AND ix1.indexrelid < ix2.indexrelid
    AND ix1.indkey::text = ix2.indkey::text
JOIN pg_class i1 ON i1.oid = ix1.indexrelid
JOIN pg_class i2 ON i2.oid = ix2.indexrelid
JOIN pg_class t ON t.oid = ix1.indrelid
JOIN pg_namespace n ON n.oid = t.relnamespace
WHERE n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
ORDER BY n.nspname, t.relname
"""

_SCHEMA_HEALTH_MISSING_PK_QUERY = """
SELECT n.nspname AS schema_name, c.relname AS table_name
FROM pg_class c
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relkind = 'r'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
  AND NOT EXISTS (
      SELECT 1 FROM pg_constraint con
      WHERE con.conrelid = c.oid AND con.contype = 'p'
  )
ORDER BY n.nspname, c.relname
LIMIT 50
"""

_SCHEMA_HEALTH_FK_WITHOUT_INDEX_QUERY = """
SELECT n.nspname AS schema_name, c.relname AS table_name,
       con.conname AS fk_name,
       array_to_string(ARRAY(SELECT a.attname FROM pg_attribute a
           WHERE a.attrelid = con.conrelid AND a.attnum = ANY(con.conkey)
           ORDER BY a.attnum), ', ') AS fk_columns
FROM pg_constraint con
JOIN pg_class c ON c.oid = con.conrelid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE con.contype = 'f'
  AND n.nspname NOT IN ('pg_catalog', 'information_schema')
  AND NOT EXISTS (
      SELECT 1 FROM pg_index i
      WHERE i.indrelid = con.conrelid
        AND (i.indkey::smallint[])[0:array_length(con.conkey,1)-1] @> con.conkey::smallint[]
  )
ORDER BY n.nspname, c.relname
LIMIT 50
"""

# Buffer cache stats — pg_statio_user_tables.
_BUFFER_TABLE_STATS_QUERY = """
SELECT n.nspname AS schema_name, c.relname AS table_name,
       COALESCE(s.heap_blks_read, 0) AS heap_blks_read,
       COALESCE(s.heap_blks_hit, 0) AS heap_blks_hit,
       CASE WHEN COALESCE(s.heap_blks_read,0) + COALESCE(s.heap_blks_hit,0) > 0
            THEN COALESCE(s.heap_blks_hit,0)::float / (COALESCE(s.heap_blks_read,0) + COALESCE(s.heap_blks_hit,0))
            ELSE 1.0 END AS hit_ratio,
       COALESCE(s.idx_blks_read, 0) AS idx_blks_read,
       COALESCE(s.idx_blks_hit, 0) AS idx_blks_hit,
       CASE WHEN COALESCE(s.idx_blks_read,0) + COALESCE(s.idx_blks_hit,0) > 0
            THEN COALESCE(s.idx_blks_hit,0)::float / (COALESCE(s.idx_blks_read,0) + COALESCE(s.idx_blks_hit,0))
            ELSE 1.0 END AS idx_hit_ratio
FROM pg_statio_user_tables s
JOIN pg_class c ON c.oid = s.relid
JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname NOT IN ('pg_catalog','information_schema','pg_toast')
ORDER BY (COALESCE(s.heap_blks_read,0) + COALESCE(s.idx_blks_read,0)) DESC
LIMIT 50
"""

_TEMPFILE_STATS_QUERY = """
SELECT datname AS database_name, temp_files, temp_bytes
FROM pg_stat_database
WHERE datname NOT IN ('template0','template1','postgres')
ORDER BY temp_bytes DESC
"""

# Fase 5 T63 — tamanho de cada banco de dados (on-demand, nao template).
_DB_SIZES_QUERY = """
    SELECT
        datname AS database_name,
        pg_database_size(datname) AS size_bytes
    FROM pg_database
    WHERE datistemplate = false
      AND datallowconn = true
    ORDER BY size_bytes DESC
"""

# Fase 5 T64 — compliance de configuracao. Apenas parametros com impacto significativo de performance.
_CONFIG_COMPLIANCE_QUERY = """
    SELECT name, setting, unit, short_desc AS description
    FROM pg_settings
    WHERE name IN (
        'work_mem', 'max_connections', 'shared_buffers', 'effective_cache_size',
        'maintenance_work_mem', 'autovacuum_vacuum_scale_factor',
        'autovacuum_analyze_scale_factor', 'autovacuum_max_workers', 'autovacuum_naptime',
        'checkpoint_completion_target', 'max_wal_size', 'wal_buffers',
        'log_min_duration_statement', 'random_page_cost'
    )
    ORDER BY name
"""

# `pg_stat_archiver` sempre devolve exatamente 1 linha (cluster-wide, nao por banco) — mesmo com
# archiving desabilitado (colunas ficam NULL nesse caso). Docs/collector-queries.md §6.
_ARCHIVER_STATUS_QUERY = """
    SELECT archived_count, last_archived_wal, last_archived_time,
           failed_count, last_failed_wal, last_failed_time, stats_reset
    FROM pg_stat_archiver
"""

# Fase 2, item 2 (replicacao) — streaming replication. Sem replica conectada devolve 0 linhas
# (nao e erro, mesmo padrao de degradacao graciosa ja usado no resto do projeto).
_REPLICATION_LAG_QUERY = """
    SELECT
        application_name,
        EXTRACT(EPOCH FROM replay_lag) AS replay_lag_seconds
    FROM pg_stat_replication
    WHERE replay_lag IS NOT NULL
"""


def _evaluate_pg_config_param(
    name: str, setting: str, unit: str | None, description: str
) -> NormalizedConfigComplianceItem:
    """Avalia um parametro do pg_settings contra boas praticas e retorna item de compliance."""

    def _to_mb(value: str, u: str | None) -> float | None:
        try:
            v = float(value)
        except (ValueError, TypeError):
            return None
        if u == "kB":
            return v / 1024
        if u == "8kB":
            return v * 8 / 1024
        if u == "MB":
            return v
        if u == "GB":
            return v * 1024
        return v

    rules: dict[str, tuple[str, str, str]] = {}

    if name == "work_mem":
        mb = _to_mb(setting, unit)
        status = "ok" if mb is not None and 4 <= mb <= 512 else "warn"
        rules[name] = ("4 MB – 512 MB", "Acima de 512 MB pode causar OOM sob carga paralela; abaixo de 4 MB prejudica sorts e hash joins.", status)
    elif name == "max_connections":
        try:
            v = int(setting)
            status = "error" if v > 500 else ("warn" if v > 200 else "ok")
        except ValueError:
            status = "warn"
        rules[name] = ("≤ 200 (com PgBouncer)", "Muitas conexoes diretas consomem shared memory e context switches; use um pooler acima de 200.", status)
    elif name == "autovacuum_vacuum_scale_factor":
        try:
            v = float(setting)
            status = "warn" if v > 0.1 else "ok"
        except ValueError:
            status = "warn"
        rules[name] = ("0.01 – 0.05", "O valor padrao (0.2) adia vacuum em tabelas grandes (ex.: 1M linhas * 0.2 = 200k dead tuples antes de vacuum).", status)
    elif name == "autovacuum_analyze_scale_factor":
        try:
            v = float(setting)
            status = "warn" if v > 0.1 else "ok"
        except ValueError:
            status = "warn"
        rules[name] = ("0.01 – 0.05", "Mesmo problema do vacuum_scale_factor — estatisticas ficam desatualizadas em tabelas grandes.", status)
    elif name == "checkpoint_completion_target":
        try:
            v = float(setting)
            status = "warn" if v < 0.7 else "ok"
        except ValueError:
            status = "warn"
        rules[name] = ("0.9", "Valores baixos (< 0.7) causam I/O em rajadas ao final do checkpoint; 0.9 distribui o flush uniformemente.", status)
    elif name == "log_min_duration_statement":
        try:
            v = int(setting)
            status = "error" if v == -1 else ("warn" if v == 0 else "ok")
        except ValueError:
            status = "warn"
        rules[name] = ("500 – 2000 ms", "-1 desabilita completamente o log de queries lentas; 0 loga tudo (ruidoso); 500-2000ms e um bom ponto de partida.", status)
    elif name == "random_page_cost":
        try:
            v = float(setting)
            status = "warn" if v >= 4.0 else "ok"
        except ValueError:
            status = "warn"
        rules[name] = ("1.1 – 2.0 (SSD)", "O padrao (4.0) assume disco rotativo; em SSD o planner subestima index scans e escolhe seq scans desnecessarios.", status)
    elif name == "autovacuum_max_workers":
        try:
            v = int(setting)
            status = "warn" if v < 3 else "ok"
        except ValueError:
            status = "warn"
        rules[name] = ("3 – 6", "O padrao (3) pode ser insuficiente em bancos com muitas tabelas de alta escrita.", status)

    rec_val, recommendation, status_val = rules.get(name, (None, None, "ok"))

    return NormalizedConfigComplianceItem(
        param_name=name,
        current_value=setting,
        recommended_value=rec_val,
        unit=unit,
        status=status_val,
        description=description,
        recommendation=recommendation,
    )


def _reduce_to_direct_edges(blocked_by_map: dict[int, set[int]]) -> dict[int, set[int]]:
    """Reduz o fecho transitivo de `pg_blocking_pids()` pro(s) bloqueador(es) DIRETO(s).

    `pg_blocking_pids(pid)` documenta devolver todo mundo "a frente" de `pid` na fila de espera,
    nao so quem bloqueia `pid` imediatamente — se A bloqueia B e B bloqueia C,
    `blocked_by_map[C] == {A, B}`, nao so `{B}`. Um bloqueador `b` e mantido em `blockers(w)` so
    se `b` NAO estiver, ele mesmo, no conjunto de bloqueadores de outro `b2` tambem presente em
    `blockers(w)` — nesse caso `b` so bloqueia `w` INDIRETAMENTE (via `b2`), entao e removido,
    sobrando so o(s) bloqueador(es) mais proximo(s). Sem essa reducao, a arvore reconstruida em
    `app.api.v1.waits_blocking` ligaria C tanto em A quanto em B, achatando/duplicando a cadeia
    real A -> B -> C.
    """
    direct: dict[int, set[int]] = {}
    for waiter, blockers in blocked_by_map.items():
        direct[waiter] = {
            b
            for b in blockers
            if not any(b in blocked_by_map.get(b2, ()) for b2 in blockers if b2 != b)
        }
    return direct


class PostgreSQLCollector:
    """Implementa `app.collectors.base.Collector` para instancias PostgreSQL."""

    def __init__(self, connection: ConnectionParams, instance_id: UUID | None = None) -> None:
        self._connection_params = connection
        self._instance_id = instance_id
        self.skip_blocking_queries = False
        self.skip_pg_catalog_metrics = False

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Conecta e checa roles (`pg_has_role`) e extensao `pg_stat_statements`.

        Conexao com timeout (`Docs/rules/02-safe-collection.md`); qualquer erro de
        conectividade vira uma unica `CapabilityResult(CONNECTIVITY, UNAVAILABLE, ...)` — nunca
        a mensagem bruta da excecao, so o nome da classe (pode conter host/senha no texto
        default do driver).
        """
        try:
            conn = await asyncio.wait_for(
                self._connect(), timeout=self._connection_params.connect_timeout_seconds
            )
        except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
            return [
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=f"Falha ao conectar: {type(exc).__name__}",
                )
            ]

        try:
            return await self._check_roles_and_extensions(conn)
        finally:
            await conn.close()

    async def _connect(self, database: str = "postgres") -> asyncpg.Connection:
        """`database` default ("postgres", banco de manutencao) serve pra `check_capabilities()`/
        `collect()`, que so leem views cluster-wide (`pg_stat_activity`/`pg_stat_database`) — o
        banco conectado nao importa pra elas. `collect_missing_index_suggestions()` passa um
        banco real explicito (`pg_stat_user_tables` e escopada ao banco conectado)."""
        params = self._connection_params
        connect_kwargs: dict[str, object] = {
            "host": params.host,
            "port": params.port,
            "user": params.username,
            "database": database,
            "timeout": params.connect_timeout_seconds,
        }
        # `auth_method == "integrated"`: nao passamos `password` — a conexao depende de
        # confianca do ambiente (ex.: `pg_hba.conf` com `trust`/peer, ou certificado de cliente
        # ja provisionado fora deste adaptador). Caminho ainda nao totalmente resolvido no MVP
        # (nao ha integracao Kerberos/GSSAPI aqui) mas nao deve quebrar: o `asyncpg.connect`
        # segue sem senha, e o servidor decide se aceita.
        if params.auth_method != "integrated":
            connect_kwargs["password"] = params.secret_value
        return await asyncpg.connect(**connect_kwargs)

    async def _check_roles_and_extensions(self, conn: asyncpg.Connection) -> list[CapabilityResult]:
        results: list[CapabilityResult] = []

        has_session_activity = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_read_all_stats', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_SESSION_ACTIVITY,
                status=CapabilityStatus.FULL
                if has_session_activity
                else CapabilityStatus.UNAVAILABLE,
                reason=None if has_session_activity else "role pg_read_all_stats ausente",
            )
        )

        has_configuration_visibility = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_read_all_settings', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_CONFIGURATION_VISIBILITY,
                status=(
                    CapabilityStatus.FULL
                    if has_configuration_visibility
                    else CapabilityStatus.UNAVAILABLE
                ),
                reason=None
                if has_configuration_visibility
                else "role pg_read_all_settings ausente",
            )
        )

        has_session_termination = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_signal_backend', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_SESSION_TERMINATION,
                status=(
                    CapabilityStatus.FULL
                    if has_session_termination
                    else CapabilityStatus.UNAVAILABLE
                ),
                reason=None if has_session_termination else "role pg_signal_backend ausente",
            )
        )

        results.append(await self._check_top_sql(conn))
        results.append(await self._check_qualstats(conn))
        return results

    async def _check_qualstats(self, conn: asyncpg.Connection) -> CapabilityResult:
        has_extension = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_qualstats')"
        )
        if has_extension:
            return CapabilityResult(
                capability=POSTGRESQL_QUALSTATS, status=CapabilityStatus.FULL
            )
        return CapabilityResult(
            capability=POSTGRESQL_QUALSTATS,
            status=CapabilityStatus.DEGRADED,
            reason="extensao pg_qualstats nao instalada (requer shared_preload_libraries)",
        )

    async def _check_top_sql(self, conn: asyncpg.Connection) -> CapabilityResult:
        has_extension = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements')"
        )
        # `current_setting()` devolve o valor "bonito" com unidade (ex.: "4kB") para GUCs com
        # unidade de exibicao — `pg_settings.setting` sempre devolve o numero cru (bytes),
        # seguro para castar direto em inteiro.
        query_size_ok = await conn.fetchval(
            "SELECT setting::int >= $1 FROM pg_settings WHERE name = 'track_activity_query_size'",
            _MIN_TRACK_ACTIVITY_QUERY_SIZE,
        )

        if has_extension and query_size_ok:
            return CapabilityResult(capability=POSTGRESQL_TOP_SQL, status=CapabilityStatus.FULL)

        missing = []
        if not has_extension:
            missing.append("extensao pg_stat_statements nao instalada")
        if not query_size_ok:
            missing.append(f"track_activity_query_size menor que {_MIN_TRACK_ACTIVITY_QUERY_SIZE}")
        return CapabilityResult(
            capability=POSTGRESQL_TOP_SQL,
            status=CapabilityStatus.DEGRADED,
            reason="; ".join(missing),
        )

    async def collect(
        self,
        *,
        sleeping_min_duration_ms: int | None = None,
        sleeping_database_overrides_ms: dict[str, int] | None = None,
    ) -> CollectionResult:
        """Executa um ciclo de coleta: sessao ativa, bloqueio (nivel direto) e metricas de recurso.

        Ver `Docs/collector-queries.md` §1 (sessao ativa), §2 (bloqueio), §5 (metricas de
        recurso). Versao do engine e detectada a cada ciclo pra decidir entre `pg_stat_bgwriter`
        (< PG17) e `pg_stat_checkpointer` (>= PG17); tambem exposta em
        `CollectionResult.engine_version*` pra `PollingWorker` persistir de volta em
        `instances.engine_version*`.

        Requer `instance_id` (levanta `ValueError` se `None` — usado em teste de conexao
        pre-cadastro, onde `collect()` nao se aplica).
        """
        if self._instance_id is None:
            raise ValueError(
                "PostgreSQLCollector.collect requer instance_id de uma instancia ja cadastrada."
            )

        start = time.perf_counter()
        settings = get_settings()

        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            version_num = await self._fetch_version(conn, settings.collection_query_timeout_seconds)
            # `server_version_num` (PG10+, unico suportado formalmente - Q20): major*10000+minor
            # direto, sem componente de patch separado (ex.: 170000 -> "17.0", 160003 -> "16.3") —
            # confirmado contra pgpedia.info/documentacao oficial. Derivado do que ja foi
            # buscado, sem round-trip extra.
            engine_version = f"{version_num // 10000}.{version_num % 10000}"

            session_rows = await self._fetch_rows(
                conn, _SESSION_ACTIVITY_QUERY, settings.collection_query_timeout_seconds
            )
            # Carimbado logo apos o fetch de `pg_stat_activity` (nao antes de abrir a conexao,
            # como era antes) - e o instante que corresponde de fato ao `query_start` que acabamos
            # de ler dali. Carimbar antes da conexao/`_fetch_version` (que juntos levam ~100-300ms)
            # fazia `duration_ms = collected_at - query_start` sair negativo sempre que uma sessao
            # comecava uma query nesse intervalo (query_start ficava depois de collected_at).
            collected_at = self._now()
            session_rows = session_rows[: settings.collection_max_rows]

            if self.skip_blocking_queries:
                blocking_rows = []
                all_session_rows = []
                blocked_object_rows = []
            else:
                blocking_rows = await self._fetch_rows(
                    conn, _BLOCKING_QUERY, settings.collection_query_timeout_seconds
                )
                blocking_rows = blocking_rows[: settings.collection_max_rows]

                all_session_rows = await self._fetch_rows(
                    conn, _ALL_SESSIONS_QUERY, settings.collection_query_timeout_seconds
                )
                all_session_rows = all_session_rows[: settings.collection_max_rows]

                blocked_object_rows = await self._fetch_rows(
                    conn, _BLOCKED_OBJECT_QUERY, settings.collection_query_timeout_seconds
                )
                blocked_object_rows = blocked_object_rows[: settings.collection_max_rows]

            if self.skip_pg_catalog_metrics:
                database_stats_row = None
                checkpoint_row = None
            else:
                database_stats_row = await asyncio.wait_for(
                    conn.fetchrow(_DATABASE_STATS_QUERY),
                    timeout=settings.collection_query_timeout_seconds,
                )

                if version_num >= _PG17_VERSION_NUM:
                    checkpoint_row = await asyncio.wait_for(
                        conn.fetchrow(_CHECKPOINTER_QUERY),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                else:
                    checkpoint_row = await asyncio.wait_for(
                        conn.fetchrow(_BGWRITER_QUERY),
                        timeout=settings.collection_query_timeout_seconds,
                    )
        finally:
            await conn.close()

        query_samples = self._build_query_samples(session_rows, collected_at)
        blocking_events = self._build_blocking_events(
            session_rows, blocking_rows, all_session_rows, blocked_object_rows, collected_at
        )
        metric_samples = self._build_metric_samples(
            session_rows,
            database_stats_row,
            checkpoint_row,
            version_num,
            collected_at,
        )
        transaction_events = self._build_transaction_events(
            all_session_rows,
            blocking_events,
            collected_at,
            settings.transaction_orphan_threshold_minutes,
            sleeping_min_duration_ms,
            sleeping_database_overrides_ms or {},
        )
        open_transaction_identifiers = [
            str(row["pid"]) for row in all_session_rows if row["xact_start"] is not None
        ]

        duration_ms = int((time.perf_counter() - start) * 1000)
        return CollectionResult(
            instance_id=self._instance_id,
            collected_at=collected_at,
            duration_ms=duration_ms,
            metric_samples=metric_samples,
            query_samples=query_samples,
            blocking_events=blocking_events,
            transaction_events=transaction_events,
            open_transaction_identifiers=open_transaction_identifiers,
            engine_version=engine_version,
            engine_version_major=version_num // 10000,
        )

    async def _fetch_version(self, conn: asyncpg.Connection, timeout_seconds: float) -> int:
        return await asyncio.wait_for(
            conn.fetchval("SELECT current_setting('server_version_num')::int"),
            timeout=timeout_seconds,
        )

    async def _fetch_rows(
        self, conn: asyncpg.Connection, query: str, timeout_seconds: float
    ) -> list[asyncpg.Record]:
        return await asyncio.wait_for(conn.fetch(query), timeout=timeout_seconds)

    def _build_query_samples(
        self, session_rows: list[asyncpg.Record], collected_at: datetime
    ) -> list[NormalizedQuerySample]:
        assert self._instance_id is not None  # garantido por collect()
        instance_id: UUID = self._instance_id
        samples: list[NormalizedQuerySample] = []
        for row in session_rows:
            query_text = row["query"]
            if not query_text:
                continue
            # `sql_hash` precisa continuar vindo do mascaramento BLANKET (`mask_sql_literals`) —
            # identidade estavel usada pra agrupar execucoes da mesma query (Top SQL/wait time).
            # O texto exibido (`sql_text_masked`/`sql_text_full_masked`) usa o mascaramento
            # seletivo (so PII), calculado a partir do MESMO `query_text` original.
            sql_hash = hashlib.sha256(mask_sql_literals(query_text).encode("utf-8")).hexdigest()
            masked_display = mask_sql_literals_for_display(query_text)
            query_start = row["query_start"]
            duration_ms = (
                # `max(0, ...)`: rede residual entre o servidor executar o snapshot de
                # `pg_stat_activity` e este processo carimbar `collected_at` ainda pode deixar
                # uma query iniciada nesse intervalo alguns ms "no futuro" - clamp evita duracao
                # negativa nesse caso raro (a causa principal do problema, o carimbo tirado antes
                # de sequer abrir a conexao, foi corrigida acima, movendo `collected_at` pra logo
                # apos este fetch).
                max(0, int((collected_at - query_start).total_seconds() * 1000))
                if query_start is not None
                else None
            )
            client_addr = row["client_addr"]
            samples.append(
                NormalizedQuerySample(
                    time=collected_at,
                    instance_id=instance_id,
                    engine="postgresql",
                    sql_hash=sql_hash,
                    sql_text_masked=masked_display,
                    # `pg_stat_activity.query` ja e so o statement atual (sem a distincao
                    # batch/procedure do SQL Server) — os dois campos coincidem aqui.
                    sql_text_full_masked=masked_display,
                    wait_type=map_wait_event("postgresql", row["wait_event_type"]),
                    wait_event_native=row["wait_event"],
                    # Postgres nao expoe duracao de espera do backend diretamente.
                    wait_time_ms=None,
                    duration_ms=duration_ms,
                    session_id=str(row["pid"]),
                    db_user=row["usename"],
                    program=row["application_name"],
                    database_name=row["datname"],
                    machine=str(client_addr) if client_addr is not None else None,
                )
            )
        return samples

    def _build_blocking_events(
        self,
        session_rows: list[asyncpg.Record],
        blocking_rows: list[asyncpg.Record],
        all_session_rows: list[asyncpg.Record],
        blocked_object_rows: list[asyncpg.Record],
        collected_at: datetime,
    ) -> list[NormalizedBlockingEvent]:
        """Constroi um evento por par (bloqueador DIRETO, bloqueado).

        `pg_blocking_pids(pid)` devolve o fecho TRANSITIVO de bloqueadores (toda a fila a frente,
        nao so o bloqueador imediato — confirmado na documentacao oficial: se A bloqueia B e B
        bloqueia C, `pg_blocking_pids(C)` devolve `{A, B}`, nao so `{B}`). `_reduce_to_direct_edges`
        reduz cada `blocked_by` pro(s) bloqueador(es) mais proximo(s) antes de gerar eventos — sem
        isso, a arvore reconstruida em `app.api.v1.waits_blocking` ficaria achatada/duplicada (C
        apareceria bloqueado por A E por B, em vez de A -> B -> C).

        `wait_time_ms` e uma APROXIMACAO: Postgres nao expoe duracao nativa de espera de lock,
        entao usamos `collected_at - query_start` da sessao bloqueada quando disponivel, ou `0`
        quando `query_start` e `None`. Identidade (usuario/programa/maquina) vem PRIMEIRO de
        `session_rows` (sessao ativa) e cai pra `all_session_rows` (qualquer sessao conectada)
        pro caso do bloqueador ocioso — mesmo padrao do adaptador SQL Server. Banco de dados
        (`datname`) vem do mesmo jeito — ao contrario do SQL Server, `datname` esta disponivel em
        `all_session_rows` pra QUALQUER sessao conectada, ativa ou ociosa, sem precisar de
        fallback via transacao.
        """
        assert self._instance_id is not None  # garantido por collect()
        instance_id: UUID = self._instance_id
        sessions_by_pid = {row["pid"]: row for row in session_rows}
        identity_by_pid = {row["pid"]: row for row in all_session_rows}
        blocked_object_by_pid = {row["pid"]: row for row in blocked_object_rows}

        def _identity(pid: int) -> tuple[str | None, str | None, str | None, str | None]:
            active_row = sessions_by_pid.get(pid)
            row = active_row if active_row is not None else identity_by_pid.get(pid)
            if row is None:
                return None, None, None, None
            client_addr = row["client_addr"]
            return (
                row["usename"],
                row["application_name"],
                str(client_addr) if client_addr is not None else None,
                row["datname"],
            )

        def _xact_start(pid: int) -> datetime | None:
            active_row = sessions_by_pid.get(pid)
            row = active_row if active_row is not None else identity_by_pid.get(pid)
            return row["xact_start"] if row is not None else None

        blocked_by_map = {row["pid"]: set(row["blocked_by"]) for row in blocking_rows}
        direct_edges = _reduce_to_direct_edges(blocked_by_map)

        events: list[NormalizedBlockingEvent] = []
        for row in blocking_rows:
            blocked_pid = row["pid"]
            blocked_session = sessions_by_pid.get(blocked_pid)
            blocked_sql_hash = self._hash_query(
                blocked_session["query"] if blocked_session is not None else None
            )
            query_start = blocked_session["query_start"] if blocked_session is not None else None
            wait_time_ms = (
                # `max(0, ...)`: mesma razao do clamp em `_build_query_samples` — residual de rede
                # entre o snapshot no servidor e o carimbo de `collected_at` neste processo.
                max(0, int((collected_at - query_start).total_seconds() * 1000))
                if query_start is not None
                else 0
            )
            blocked_login, blocked_program, blocked_machine, blocked_database_name = _identity(
                blocked_pid
            )
            blocked_object_row = blocked_object_by_pid.get(blocked_pid)
            for blocker_pid in direct_edges.get(blocked_pid, set()):
                blocker_session = sessions_by_pid.get(blocker_pid)
                blocker_sql_hash = self._hash_query(
                    blocker_session["query"] if blocker_session is not None else None
                )
                blocker_login, blocker_program, blocker_machine, blocker_database_name = _identity(
                    blocker_pid
                )
                events.append(
                    NormalizedBlockingEvent(
                        time=collected_at,
                        instance_id=instance_id,
                        blocker_session_id=str(blocker_pid),
                        blocked_session_id=str(blocked_pid),
                        blocker_sql_hash=blocker_sql_hash,
                        blocked_sql_hash=blocked_sql_hash,
                        wait_time_ms=wait_time_ms,
                        blocker_db_user=blocker_login,
                        blocker_program=blocker_program,
                        blocker_machine=blocker_machine,
                        blocker_wait_event_native=(
                            blocker_session["wait_event"] if blocker_session is not None else None
                        ),
                        blocked_db_user=blocked_login,
                        blocked_program=blocked_program,
                        blocked_machine=blocked_machine,
                        blocked_wait_event_native=(
                            blocked_session["wait_event"] if blocked_session is not None else None
                        ),
                        blocked_wait_resource=(
                            f"{blocked_object_row['locktype']}/{blocked_object_row['mode']}"
                            if blocked_object_row is not None
                            else None
                        ),
                        blocked_object=(
                            blocked_object_row["blocked_object"]
                            if blocked_object_row is not None
                            else None
                        ),
                        # Postgres nao tem transacao nomeada separada da sessao — o proprio pid
                        # ja identifica a transacao (ver NormalizedTransactionEvent).
                        blocker_transaction_id=str(blocker_pid),
                        blocked_transaction_id=str(blocked_pid),
                        blocker_database_name=blocker_database_name,
                        blocked_database_name=blocked_database_name,
                        blocker_transaction_begin_time=_xact_start(blocker_pid),
                        blocked_transaction_begin_time=_xact_start(blocked_pid),
                    )
                )
        return events

    def _build_transaction_events(
        self,
        all_session_rows: list[asyncpg.Record],
        blocking_events: list[NormalizedBlockingEvent],
        collected_at: datetime,
        orphan_threshold_minutes: int,
        sleeping_min_duration_ms: int | None,
        sleeping_database_overrides_ms: dict[str, int],
    ) -> list[NormalizedTransactionEvent]:
        """Transacoes DIGNAS DE NOTA neste ciclo — qualquer sessao com `xact_start IS NOT NULL`
        em `all_session_rows` (todas as sessoes conectadas, nao so as ativas — pega tambem "idle
        in transaction", o caso mais comum de transacao orfa) que `compute_flag_reason` marcar
        (bloqueio ou orfa — Postgres nao tem `last_error_code`, sempre `None`, entao o criterio
        de erro nunca se aplica aqui). Redesenho pos-uso real: log de incidente retido, nao
        espelho de toda transacao aberta (ver `app.collectors.transaction_flagging`).
        `transaction_identifier` = o proprio `pid` (Postgres nao tem conceito de transacao
        nomeada separada da sessao — ao contrario do SQL Server, so pode haver 1 transacao
        aberta por backend por vez).

        `sleeping_min_duration_ms`/`sleeping_database_overrides_ms` (novas tarefas do usuario,
        itens 1/2): `state` ja vem de `_ALL_SESSIONS_QUERY` (sem filtro `!= 'idle'`) — "idle in
        transaction"/"idle in transaction (aborted)" e o sinal direto de sessao ociosa, sem
        precisar de query nova."""
        assert self._instance_id is not None  # garantido por collect()
        instance_id: UUID = self._instance_id
        blocking_involved_transaction_ids = {
            tx_id
            for e in blocking_events
            for tx_id in (e.blocker_transaction_id, e.blocked_transaction_id)
            if tx_id is not None
        }
        events: list[NormalizedTransactionEvent] = []
        for row in all_session_rows:
            xact_start = row["xact_start"]
            if xact_start is None:
                continue
            transaction_identifier = str(row["pid"])
            database_name = row["datname"]
            effective_sleeping_threshold_ms = sleeping_database_overrides_ms.get(
                database_name if database_name is not None else "", sleeping_min_duration_ms
            )
            flag_reason = compute_flag_reason(
                involved_in_blocking=transaction_identifier in blocking_involved_transaction_ids,
                has_error=False,
                begin_time=xact_start,
                now=collected_at,
                orphan_threshold_minutes=orphan_threshold_minutes,
                is_idle=row["state"] in ("idle in transaction", "idle in transaction (aborted)"),
                sleeping_threshold_ms=effective_sleeping_threshold_ms,
            )
            if flag_reason is None:
                continue
            client_addr = row["client_addr"]
            events.append(
                NormalizedTransactionEvent(
                    instance_id=instance_id,
                    transaction_identifier=transaction_identifier,
                    session_id=transaction_identifier,
                    database_name=row["datname"],
                    transaction_type=None,
                    transaction_uow=None,
                    begin_time=xact_start,
                    state=row["state"],
                    last_error_code=None,
                    flag_reason=flag_reason,
                    machine=str(client_addr) if client_addr is not None else None,
                )
            )
        return events

    @staticmethod
    def _hash_query(query_text: str | None) -> str | None:
        if not query_text:
            return None
        return hashlib.sha256(mask_sql_literals(query_text).encode("utf-8")).hexdigest()

    def _build_metric_samples(
        self,
        session_rows: list[asyncpg.Record],
        database_stats_row: asyncpg.Record | None,
        checkpoint_row: asyncpg.Record | None,
        version_num: int,
        collected_at: datetime,
    ) -> list[NormalizedMetricSample]:
        assert self._instance_id is not None  # garantido por collect()
        instance_id: UUID = self._instance_id
        labels: dict[str, str] = {}

        def sample(metric_name: str, value: float | None) -> NormalizedMetricSample | None:
            if value is None:
                return None
            return NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=metric_name,
                value=float(value),
                labels=labels,
            )

        candidates: list[NormalizedMetricSample | None] = [
            sample(PG_ACTIVE_SESSION_COUNT, len(session_rows)),
        ]

        if database_stats_row is not None:
            candidates.extend(
                [
                    sample(PG_NUMBACKENDS, database_stats_row["numbackends"]),
                    sample(PG_XACT_COMMIT, database_stats_row["xact_commit"]),
                    sample(PG_XACT_ROLLBACK, database_stats_row["xact_rollback"]),
                    sample(PG_BLKS_READ, database_stats_row["blks_read"]),
                    sample(PG_BLKS_HIT, database_stats_row["blks_hit"]),
                    sample(PG_TUP_RETURNED, database_stats_row["tup_returned"]),
                    sample(PG_TUP_FETCHED, database_stats_row["tup_fetched"]),
                    sample(PG_TUP_INSERTED, database_stats_row["tup_inserted"]),
                    sample(PG_TUP_UPDATED, database_stats_row["tup_updated"]),
                    sample(PG_TUP_DELETED, database_stats_row["tup_deleted"]),
                    sample(PG_DEADLOCKS, database_stats_row["deadlocks"]),
                    sample(PG_TEMP_FILES, database_stats_row["temp_files"]),
                    sample(PG_TEMP_BYTES, database_stats_row["temp_bytes"]),
                ]
            )

        if checkpoint_row is not None:
            if version_num >= _PG17_VERSION_NUM:
                candidates.extend(
                    [
                        sample(PG_CHECKPOINTS_TIMED, checkpoint_row["num_timed"]),
                        sample(PG_CHECKPOINTS_REQUESTED, checkpoint_row["num_requested"]),
                        sample(PG_CHECKPOINT_WRITE_TIME_MS, checkpoint_row["write_time"]),
                        sample(PG_CHECKPOINT_SYNC_TIME_MS, checkpoint_row["sync_time"]),
                        sample(PG_CHECKPOINT_BUFFERS_WRITTEN, checkpoint_row["buffers_written"]),
                    ]
                )
            else:
                candidates.extend(
                    [
                        sample(PG_CHECKPOINTS_TIMED, checkpoint_row["checkpoints_timed"]),
                        sample(PG_CHECKPOINTS_REQUESTED, checkpoint_row["checkpoints_req"]),
                        sample(PG_CHECKPOINT_WRITE_TIME_MS, checkpoint_row["checkpoint_write_time"]),
                        sample(PG_CHECKPOINT_SYNC_TIME_MS, checkpoint_row["checkpoint_sync_time"]),
                        sample(PG_CHECKPOINT_BUFFERS_WRITTEN, checkpoint_row["buffers_checkpoint"]),
                    ]
                )

        return [c for c in candidates if c is not None]

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        """Heuristica de indice ausente + sugestoes pg_qualstats quando disponivel.

        Heuristica (sempre): itera `pg_stat_user_tables` por banco do cluster.
        pg_qualstats (opcional): se a extensao estiver instalada, chama
        `pg_qualstats_index_advisor()` no banco de manutencao para obter sugestoes com
        indicacao de coluna especifica. As duas fontes sao mescladas na lista retornada."""
        if self._instance_id is None:
            raise ValueError(
                "PostgreSQLCollector.collect_missing_index_suggestions requer instance_id de "
                "uma instancia ja cadastrada."
            )
        instance_id: UUID = self._instance_id
        settings = get_settings()

        bootstrap_conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            database_rows = await self._fetch_rows(
                bootstrap_conn, _LIST_DATABASES_QUERY, settings.collection_query_timeout_seconds
            )
        finally:
            await bootstrap_conn.close()

        suggestions: list[NormalizedMissingIndexSuggestion] = []
        for db_row in database_rows:
            database_name = db_row["datname"]
            try:
                db_conn = await asyncio.wait_for(
                    self._connect(database=database_name),
                    timeout=self._connection_params.connect_timeout_seconds,
                )
            except (OSError, asyncpg.PostgresError, TimeoutError):
                logger.warning(
                    "Falha ao conectar ao banco %s (instance_id=%s) para checagem de indice "
                    "ausente - pulando",
                    database_name,
                    instance_id,
                )
                continue
            try:
                rows = await asyncio.wait_for(
                    db_conn.fetch(
                        _MISSING_INDEX_HEURISTIC_QUERY,
                        settings.missing_index_pg_min_table_size_bytes,
                        settings.missing_index_pg_min_seq_scans,
                    ),
                    timeout=settings.collection_query_timeout_seconds,
                )
            except (asyncpg.PostgresError, TimeoutError):
                logger.warning(
                    "Falha ao rodar heuristica de indice ausente no banco %s "
                    "(instance_id=%s) - pulando",
                    database_name,
                    instance_id,
                )
                continue
            finally:
                await db_conn.close()

            for row in rows[: settings.collection_max_rows]:
                seq_scan = row["seq_scan"]
                idx_scan = row["idx_scan"] or 0
                suggestions.append(
                    NormalizedMissingIndexSuggestion(
                        instance_id=instance_id,
                        database_name=database_name,
                        table_name=f"{row['schemaname']}.{row['table_name']}",
                        suggestion=(
                            f"Candidato a indice (heuristica, nao indica coluna): "
                            f"{seq_scan} sequential scans vs {idx_scan} index scans"
                        ),
                        impact_score=float(seq_scan - idx_scan),
                        detail={
                            "seq_scan": seq_scan,
                            "idx_scan": row["idx_scan"],
                            "n_live_tup": row["n_live_tup"],
                            "table_size_bytes": row["table_size_bytes"],
                        },
                    )
                )
        # pg_qualstats — coleta adicional com indicacao de coluna especifica.
        # Roda em banco separado (banco de manutencao); erros sao logados e ignorados.
        qualstats_conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            has_qualstats = await qualstats_conn.fetchval(
                "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_qualstats')"
            )
            if has_qualstats:
                try:
                    qualstats_rows = await asyncio.wait_for(
                        qualstats_conn.fetch(_QUALSTATS_INDEX_ADVISOR_QUERY),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                    for row in qualstats_rows[: settings.collection_max_rows]:
                        attnames: list[str] = list(row["attnames"] or [])
                        ddl: str = row["ddl"]
                        suggestions.append(
                            NormalizedMissingIndexSuggestion(
                                instance_id=instance_id,
                                database_name=None,
                                table_name=row["table_name"],
                                suggestion=f"pg_qualstats: {ddl}",
                                impact_score=None,
                                detail={"source": "pg_qualstats", "columns": attnames, "ddl": ddl},
                            )
                        )
                except (asyncpg.PostgresError, TimeoutError):
                    logger.warning(
                        "Falha ao rodar pg_qualstats_index_advisor() (instance_id=%s) - pulando",
                        instance_id,
                    )
        finally:
            await qualstats_conn.close()

        return suggestions

    async def get_engine_start_time(self) -> datetime | None:
        """`pg_postmaster_start_time()` - ver docstring do Protocol. Cluster-wide, sempre no
        banco de manutencao (`collect()`), sem precisar iterar por banco."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            return await asyncio.wait_for(
                conn.fetchval("SELECT pg_postmaster_start_time()"),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        """`pg_stat_archiver` (Docs/collector-queries.md §6) — cluster-wide, 1 unica linha,
        `database_name=None`. `last_backup_at = last_archived_time`: PROXY de saude de WAL
        archiving, NAO um backup de fato (Postgres nao tem catalogo nativo de backup completo
        como o `msdb` do SQL Server)."""
        if self._instance_id is None:
            raise ValueError(
                "PostgreSQLCollector.collect_backup_status requer instance_id de uma instancia "
                "ja cadastrada."
            )
        instance_id: UUID = self._instance_id
        settings = get_settings()

        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            row = await asyncio.wait_for(
                conn.fetchrow(_ARCHIVER_STATUS_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedBackupStatus(
                instance_id=instance_id,
                database_name=None,
                last_backup_at=row["last_archived_time"],
                detail={
                    "archived_count": row["archived_count"],
                    "last_archived_wal": row["last_archived_wal"],
                    "failed_count": row["failed_count"],
                    "last_failed_wal": row["last_failed_wal"],
                    "last_failed_time": row["last_failed_time"].isoformat()
                    if row["last_failed_time"] is not None
                    else None,
                    "stats_reset": row["stats_reset"].isoformat()
                    if row["stats_reset"] is not None
                    else None,
                },
            )
        ]

    async def collect_deadlock_events(self) -> list[NormalizedDeadlockEvent]:
        """PostgreSQL nao expoe detalhe de deadlock via SQL (so o contador agregado
        `pg_stat_database.deadlocks`, ja coletado como metrica em `collect()`) — sempre lista
        vazia, sem tentar conectar (mesmo padrao ja usado pro plano de execucao sob demanda no
        PostgreSQL, Etapa 11)."""
        return []

    async def ensure_transaction_error_capture(self) -> bool:
        """PostgreSQL nao tem Extended Events — sempre `True` sem fazer nada (nunca ha o que
        criar/iniciar neste motor)."""
        return True

    async def collect_transaction_error_events(self) -> list[NormalizedConfirmedTransactionError]:
        """PostgreSQL nao tem Extended Events — sempre lista vazia, sem tentar conectar (mesma
        classe de limitacao ja documentada pra CPU/deadlock neste projeto: "SQL Server pode,
        Postgres nao pode")."""
        return []

    async def collect_freespace_metrics(self) -> list[NormalizedMetricSample]:
        """PostgreSQL nao expoe espaco livre de disco via SQL puro sem acesso ao SO — sempre
        lista vazia, sem tentar conectar (mesma classe de limitacao ja documentada pra CPU/
        deadlock neste projeto: "SQL Server pode, Postgres nao pode")."""
        return []

    async def collect_replication_lag_metrics(self) -> list[NormalizedMetricSample]:
        """Fase 2, item 2 (replicacao) — `pg_stat_replication.replay_lag`, em segundos. Sem
        streaming replication configurada devolve lista vazia (mesma degradacao graciosa)."""
        if self._instance_id is None:
            raise ValueError(
                "PostgreSQLCollector.collect_replication_lag_metrics requer instance_id de uma "
                "instancia ja cadastrada."
            )
        instance_id: UUID = self._instance_id
        settings = get_settings()

        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_REPLICATION_LAG_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        collected_at = datetime.now(UTC)
        return [
            NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=PG_REPLICA_REPLAY_LAG_SECONDS,
                value=float(row["replay_lag_seconds"]),
                labels={"replica": str(row["application_name"])},
            )
            for row in rows
        ]

    async def scan_plan_compile_timeouts(self) -> list[NormalizedPlanCompileTimeoutFinding]:
        """PostgreSQL nao tem cache de plano exposto do mesmo jeito que o SQL Server — sempre
        lista vazia, sem tentar conectar (mesma classe de limitacao ja documentada)."""
        return []

    async def scan_plan_history(self) -> list[NormalizedPlanSnapshot]:
        """PostgreSQL não tem cache de plano exposto via SQL — use explain_queries()."""
        return []

    async def explain_queries(
        self, queries: list[tuple[str, str]]
    ) -> list[NormalizedPlanSnapshot]:
        """Gera planos estimados via EXPLAIN (FORMAT JSON) para as queries informadas.

        `queries` é uma lista de (sql_hash, sql_text_masked). Substitui literais mascarados
        (`?`) por NULL antes de executar o EXPLAIN — plano estimado, sem executar a query.
        Silencia erros por query (EXPLAIN pode falhar em CTEs complexas, DML sem RETURNING,
        etc.) e continua para as demais.
        """
        import hashlib
        import json
        import re

        if not queries or self._instance_id is None:
            return []

        try:
            conn = await asyncio.wait_for(
                self._connect(), timeout=self._connection_params.connect_timeout_seconds
            )
        except Exception:
            return []

        results: list[NormalizedPlanSnapshot] = []
        try:
            for sql_hash, sql_text in queries[:40]:
                if not sql_text or not sql_text.strip():
                    continue
                # Substitui literais mascarados por NULL para tornar o SQL EXPLAINável
                explainable = re.sub(r"\?", "NULL", sql_text)
                try:
                    rows = await asyncio.wait_for(
                        conn.fetch(f"EXPLAIN (FORMAT JSON) {explainable}"),
                        timeout=5.0,
                    )
                    if not rows:
                        continue
                    plan_obj = rows[0][0]
                    plan_json = json.dumps(plan_obj, ensure_ascii=False)
                    plan_hash = hashlib.sha256(plan_json.encode()).hexdigest()
                    results.append(
                        NormalizedPlanSnapshot(
                            instance_id=self._instance_id,
                            sql_hash=sql_hash,
                            plan_hash=plan_hash,
                            plan_xml_masked=plan_json,
                            plan_format="postgresql_json",
                        )
                    )
                except Exception:
                    continue
        finally:
            await conn.close()

        return results

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        """Fase 2, item 2 (`rule_type="custom_sql"`) — `sql` ja deve ter passado por
        `app.services.custom_sql_guard` ANTES de chegar aqui; este metodo so executa, nunca
        valida. Espera a PRIMEIRA coluna da PRIMEIRA linha como numero; qualquer outro formato
        (0 linhas, valor nao numerico) devolve `None`."""
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            value = await asyncio.wait_for(
                conn.fetchval(sql),
                timeout=timeout_seconds,
            )
        finally:
            await conn.close()

        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    async def collect_vacuum_stats(
        self,
    ) -> tuple[list[NormalizedVacuumTableStats], list[NormalizedAutovacuumWorker]]:
        """Fase 5 T54 — vacuum/autovacuum por tabela + workers em execucao (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            table_rows, worker_rows = await asyncio.gather(
                asyncio.wait_for(
                    conn.fetch(_VACUUM_TABLE_STATS_QUERY),
                    timeout=settings.collection_query_timeout_seconds,
                ),
                asyncio.wait_for(
                    conn.fetch(_AUTOVACUUM_WORKERS_QUERY),
                    timeout=settings.collection_query_timeout_seconds,
                ),
            )
        finally:
            await conn.close()

        tables = [
            NormalizedVacuumTableStats(
                schema_name=str(row["schemaname"]),
                table_name=str(row["table_name"]),
                n_dead_tup=int(row["n_dead_tup"]),
                n_live_tup=int(row["n_live_tup"]),
                dead_tup_pct=float(row["dead_tup_pct"]),
                last_vacuum=row["last_vacuum"],
                last_autovacuum=row["last_autovacuum"],
                last_analyze=row["last_analyze"],
                last_autoanalyze=row["last_autoanalyze"],
                vacuum_count=int(row["vacuum_count"]),
                autovacuum_count=int(row["autovacuum_count"]),
                table_size_bytes=int(row["table_size_bytes"]),
                xid_age=int(row["xid_age"]),
            )
            for row in table_rows
        ]
        workers = [
            NormalizedAutovacuumWorker(
                pid=int(row["pid"]),
                datname=str(row["datname"]),
                query=str(row["query"]),
                running_for_seconds=int(row["running_for_seconds"] or 0),
            )
            for row in worker_rows
        ]
        return tables, workers

    async def collect_idle_in_transaction(self) -> list[NormalizedIdleInTransactionSession]:
        """Fase 5 T56 — sessoes idle in transaction (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_IDLE_IN_TRANSACTION_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedIdleInTransactionSession(
                session_id=int(row["session_id"]),
                username=str(row["username"]) or None,
                application_name=str(row["application_name"]) or None,
                client_info=str(row["client_info"]) or None,
                idle_seconds=int(row["idle_seconds"] or 0),
                last_query=str(row["last_query"]) or None,
                state=str(row["state"]),
            )
            for row in rows
        ]

    async def collect_index_bloat(self) -> list[NormalizedIndexBloatEntry]:
        """Fase 5 T55 — indices com estimativa de bloat (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_INDEX_BLOAT_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedIndexBloatEntry(
                schema_name=str(row["schema_name"]),
                table_name=str(row["table_name"]),
                index_name=str(row["index_name"]),
                index_size_bytes=int(row["index_size_bytes"] or 0),
                idx_scan=int(row["idx_scan"] or 0),
                idx_tup_read=int(row["idx_tup_read"] or 0),
                idx_tup_fetch=int(row["idx_tup_fetch"] or 0),
                bloat_pct=float(row["bloat_pct"] or 0.0),
            )
            for row in rows
        ]

    async def collect_bgwriter_stats(self) -> NormalizedBgwriterStats | None:
        """Fase 5 T57 — bgwriter + checkpoints (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            version_num = await self._fetch_version(conn, settings.collection_query_timeout_seconds)
            query = _BGWRITER_STATS_QUERY_PG17 if version_num >= _PG17_VERSION_NUM else _BGWRITER_STATS_QUERY
            row = await asyncio.wait_for(
                conn.fetchrow(query),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        if row is None:
            return None
        return NormalizedBgwriterStats(
            checkpoints_timed=int(row["checkpoints_timed"] or 0),
            checkpoints_req=int(row["checkpoints_req"] or 0),
            checkpoint_write_time=float(row["checkpoint_write_time"] or 0),
            checkpoint_sync_time=float(row["checkpoint_sync_time"] or 0),
            buffers_checkpoint=int(row["buffers_checkpoint"] or 0),
            buffers_clean=int(row["buffers_clean"] or 0),
            maxwritten_clean=int(row["maxwritten_clean"] or 0),
            buffers_backend=int(row["buffers_backend"] or 0),
            buffers_backend_fsync=int(row["buffers_backend_fsync"] or 0),
            buffers_alloc=int(row["buffers_alloc"] or 0),
            stats_reset=row["stats_reset"],
        )

    async def collect_user_functions(self) -> list[NormalizedUserFunction]:
        """Fase 5 T58 — funcoes PL/pgSQL por tempo total (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_USER_FUNCTIONS_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedUserFunction(
                schema_name=str(row["schema_name"]),
                func_name=str(row["func_name"]),
                calls=int(row["calls"] or 0),
                total_time_ms=float(row["total_time_ms"] or 0),
                self_time_ms=float(row["self_time_ms"] or 0),
                avg_time_ms=float(row["avg_time_ms"] or 0),
            )
            for row in rows
        ]

    async def collect_replication_slots(self) -> list[NormalizedReplicationSlot]:
        """Fase 5 T59 — slots de replicacao com lag (on-demand).

        Tenta a query com `wal_status` (PG 13+) primeiro; em caso de erro de coluna
        (asyncpg.UndefinedColumnError) cai na query sem `wal_status`."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            try:
                rows = await asyncio.wait_for(
                    conn.fetch(_REPLICATION_SLOTS_QUERY_PG13),
                    timeout=settings.collection_query_timeout_seconds,
                )
            except Exception:
                rows = await asyncio.wait_for(
                    conn.fetch(_REPLICATION_SLOTS_QUERY),
                    timeout=settings.collection_query_timeout_seconds,
                )
        finally:
            await conn.close()

        return [
            NormalizedReplicationSlot(
                slot_name=str(row["slot_name"]),
                plugin=str(row["plugin"]) if row["plugin"] else None,
                slot_type=str(row["slot_type"]),
                database_name=str(row["database_name"]) or None,
                active=bool(row["active"]),
                active_pid=int(row["active_pid"]) if row["active_pid"] is not None else None,
                lag_bytes=int(row["lag_bytes"]) if row["lag_bytes"] is not None else None,
                wal_status=str(row["wal_status"]) if row.get("wal_status") else None,
            )
            for row in rows
        ]

    async def collect_config_compliance(self) -> list[NormalizedConfigComplianceItem]:
        """Fase 5 T64 — compliance de configuracao vs boas praticas (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_CONFIG_COMPLIANCE_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [_evaluate_pg_config_param(r["name"], r["setting"], r["unit"], r["description"]) for r in rows]

    async def collect_connection_consumers(self) -> list[NormalizedConnectionConsumer]:
        """Fase 5 T65 — consumidores de conexao agrupados por app/host/usuario (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_CONNECTION_CONSUMERS_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedConnectionConsumer(
                application_name=r["application_name"],
                client_addr=r["client_addr"],
                username=r["username"],
                total=r["total"],
                active=r["active"],
                idle=r["idle"],
                idle_in_transaction=r["idle_in_transaction"],
                waiting=r["waiting"],
            )
            for r in rows
        ]

    async def collect_db_sizes(self) -> list[NormalizedDbSize]:
        """Fase 5 T63 — tamanho de cada banco PostgreSQL (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_DB_SIZES_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedDbSize(
                database_name=r["database_name"],
                size_bytes=int(r["size_bytes"]),
            )
            for r in rows
        ]

    async def collect_query_stats_snapshot(self) -> list[NormalizedQueryStatsSnapshot]:
        """Fase 6 T60 — snapshot do pg_stat_statements para trending de queries (periodico)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_QUERY_STATS_SNAPSHOT_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()
        return [
            NormalizedQueryStatsSnapshot(
                queryid=r["queryid"],
                query_text=r["query_text"],
                calls=r["calls"],
                total_exec_time=r["total_exec_time"],
                total_rows=r["total_rows"],
            )
            for r in rows
            if r["queryid"] is not None
        ]

    async def collect_unused_indexes(self) -> list[NormalizedUnusedIndex]:
        """Fase 5 T69 — indices com zero scans (on-demand). Exclui PKs."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_UNUSED_INDEXES_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedUnusedIndex(
                schema_name=str(row["schema_name"]),
                table_name=str(row["table_name"]),
                index_name=str(row["index_name"]),
                index_size_bytes=int(row["index_size_bytes"] or 0),
                idx_scan=int(row["idx_scan"] or 0),
            )
            for row in rows
        ]

    async def collect_table_io_stats(self) -> list[NormalizedTableIoStats]:
        """Fase 5 T68 — I/O de tabelas por seq_scan (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            rows = await asyncio.wait_for(
                conn.fetch(_TABLE_IO_STATS_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        return [
            NormalizedTableIoStats(
                schema_name=str(row["schema_name"]),
                table_name=str(row["table_name"]),
                seq_scan=int(row["seq_scan"] or 0),
                idx_scan=int(row["idx_scan"] or 0),
                n_live_tup=int(row["n_live_tup"] or 0),
                n_dead_tup=int(row["n_dead_tup"] or 0),
                table_size_bytes=int(row["table_size_bytes"] or 0),
                last_analyze=row["last_analyze"],
                last_autoanalyze=row["last_autoanalyze"],
                last_vacuum=row["last_vacuum"],
                last_autovacuum=row["last_autovacuum"],
            )
            for row in rows
        ]

    async def collect_schema_health(self) -> list[NormalizedSchemaHealthItem]:
        """Schema health — duplicate indexes, missing PKs, FKs without index (on-demand)."""
        import json as _json  # noqa: PLC0415

        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        results: list[NormalizedSchemaHealthItem] = []
        try:
            dup_rows = await asyncio.wait_for(
                conn.fetch(_SCHEMA_HEALTH_DUPLICATE_INDEXES_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
            for row in dup_rows:
                results.append(
                    NormalizedSchemaHealthItem(
                        category="duplicate_index",
                        schema_name=str(row["schema_name"]),
                        table_name=str(row["table_name"]),
                        object_name=str(row["index_name"]),
                        detail={
                            "duplicate_of": str(row["duplicate_of"]),
                            "index_def": str(row["index_def"]),
                        },
                        severity="warning",
                    )
                )

            pk_rows = await asyncio.wait_for(
                conn.fetch(_SCHEMA_HEALTH_MISSING_PK_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
            for row in pk_rows:
                results.append(
                    NormalizedSchemaHealthItem(
                        category="missing_pk",
                        schema_name=str(row["schema_name"]),
                        table_name=str(row["table_name"]),
                        severity="warning",
                    )
                )

            fk_rows = await asyncio.wait_for(
                conn.fetch(_SCHEMA_HEALTH_FK_WITHOUT_INDEX_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
            for row in fk_rows:
                results.append(
                    NormalizedSchemaHealthItem(
                        category="fk_without_index",
                        schema_name=str(row["schema_name"]),
                        table_name=str(row["table_name"]),
                        object_name=str(row["fk_name"]),
                        detail={"fk_columns": str(row["fk_columns"])},
                        severity="warning",
                    )
                )
        finally:
            await conn.close()

        return results

    async def collect_buffer_cache_stats(
        self,
    ) -> tuple[list[NormalizedBufferTableStats], list[NormalizedTempfileStats], list]:
        """Buffer cache e tempfile stats — PostgreSQL only (on-demand)."""
        settings = get_settings()
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            buf_rows = await asyncio.wait_for(
                conn.fetch(_BUFFER_TABLE_STATS_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
            tmp_rows = await asyncio.wait_for(
                conn.fetch(_TEMPFILE_STATS_QUERY),
                timeout=settings.collection_query_timeout_seconds,
            )
        finally:
            await conn.close()

        table_stats = [
            NormalizedBufferTableStats(
                schema_name=str(r["schema_name"]),
                table_name=str(r["table_name"]),
                heap_blks_read=int(r["heap_blks_read"] or 0),
                heap_blks_hit=int(r["heap_blks_hit"] or 0),
                hit_ratio=float(r["hit_ratio"] or 1.0),
                idx_blks_read=int(r["idx_blks_read"] or 0),
                idx_blks_hit=int(r["idx_blks_hit"] or 0),
                idx_hit_ratio=float(r["idx_hit_ratio"] or 1.0),
            )
            for r in buf_rows
        ]
        tempfile_stats = [
            NormalizedTempfileStats(
                database_name=str(r["database_name"]),
                temp_files=int(r["temp_files"] or 0),
                temp_bytes=int(r["temp_bytes"] or 0),
            )
            for r in tmp_rows
        ]
        return table_stats, tempfile_stats, []

    async def explain_plan_for_hash(self, sql_text: str) -> dict | None:
        """Executa EXPLAIN (FORMAT JSON) no texto mascarado — PostgreSQL only (on-demand)."""
        import json as _json  # noqa: PLC0415

        try:
            conn = await asyncio.wait_for(
                self._connect(), timeout=self._connection_params.connect_timeout_seconds
            )
            try:
                rows = await conn.fetch(
                    f"EXPLAIN (FORMAT JSON, VERBOSE true, COSTS true) {sql_text}"
                )
            finally:
                await conn.close()
            if rows:
                return _json.loads(rows[0][0])
        except Exception:
            return None
        return None

    async def close(self) -> None:
        """Sem estado persistente entre chamadas (cada `check_capabilities` fecha sua conexao)."""
        logger.info("close chamado para instancia postgresql (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
