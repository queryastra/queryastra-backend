"""Adaptador SQL Server. Fonte de verdade: `Docs/collector-queries.md`.

Usa `pymssql` (sincrono) — cada chamada roda em thread separada, sob `asyncio.wait_for` para
respeitar o timeout de conexao configurado (Docs/rules/02-safe-collection.md). A partir do
thread pool DEDICADO por instancia (`app.core.instance_executors.run_in_instance_executor`, nao
o executor padrao compartilhado do event loop) — mitigacao pra uma chamada travada (pymssql nao
tem timeout confiavel de socket/SO, ver docstring daquele modulo) nunca esgotar a capacidade de
OUTRAS instancias.

`check_capabilities()` conecta e checa, via `fn_my_permissions(NULL, 'SERVER')` +
`IS_SRVROLEMEMBER`/`IS_ROLEMEMBER`, a permissao mandatoria (`VIEW SERVER STATE` ou, no SQL
Server 2022+, `VIEW SERVER PERFORMANCE STATE` — Docs/collector-queries.md §7, §9) e as
permissoes opcionais que so degradam funcionalidades especificas quando ausentes.

`collect()` executa um ciclo real de coleta somente leitura (mesmo padrao de thread + timeout
usado em `check_capabilities()`), com timeout de execucao de query
(`settings.collection_query_timeout_seconds`, via `pymssql.connect(timeout=...)`) e limite de
linhas truncado em Python apos o fetch (`settings.collection_max_rows`):
    - `sys.dm_exec_requests` + `sys.dm_exec_sessions` + `sys.dm_exec_sql_text()` — sessao/query
      ativa (collector-queries.md §1), normalizada em `NormalizedQuerySample`. Cuidado com
      queries paralelas (dop>1): so a coordinator thread aparece, `reads`/`writes`/
      `logical_reads` nao atualizam nessa linha (nao tratado especificamente nesta feature).
      Texto fatiado por `statement_start_offset`/`statement_end_offset` (padrao oficial da
      Microsoft) em `sql_text_masked` — sem isso, uma sessao dentro de uma stored procedure
      mostraria a `CREATE PROCEDURE ...` inteira em vez do statement em execucao; o texto
      completo do batch/procedure fica em `sql_text_full_masked`, so pra tela de detalhe.
    - `sys.dm_exec_requests.blocking_session_id` — deteccao de bloqueio de nivel direto
      (collector-queries.md §2); a arvore completa (Etapa 9) e reconstruida no app encadeando as
      arestas de um mesmo snapshot (`app.api.v1.waits_blocking`), nao via CTE recursiva ao vivo
      contra o servidor monitorado. Sentinela negativo (-2 a -5) = bloqueador nao determinavel —
      NAO tratado como erro, so ignorado (nenhum `NormalizedBlockingEvent` gerado para essa
      linha). `_ALL_SESSIONS_QUERY` identifica bloqueadores OCIOSOS (sem request ativa);
      `_BLOCKED_LOCKS_QUERY`/`_resolve_hobt_object_names` (`sys.dm_tran_locks`/`sys.partitions`,
      esta última com uma conexão curta por banco quando necessário) resolve o objeto bloqueado,
      melhor esforço.
    - `sys.dm_os_wait_stats`/`sys.dm_exec_session_wait_stats` (cumulativo, via `LAG()`): **nao
      implementado** (corte de escopo) — so o `wait_type`/`wait_time` instantaneo de
      `sys.dm_exec_requests` por sessao ativa.
    - `sys.dm_xe_session_targets`/`sys.dm_xe_sessions` (deadlock detalhado): **nao
      implementado** (corte de escopo, sem tabela `deadlock_events`).
    - `sys.dm_os_performance_counters` — so os 4 contadores RAW (`cntr_type=65792`) descritos em
      `app.normalizers.metric_names.SQLSERVER_RAW_COUNTER_TO_METRIC_NAME` (collector-queries.md
      §5); contadores delta/razao **nao implementados** (corte de escopo).
    - `sys.dm_io_virtual_file_stats`, `sys.dm_db_file_space_usage`, backup
      (`msdb.dbo.backupset`): **nao implementados** nesta feature.

Versao (`SERVERPROPERTY('ProductVersion')`) e detectada a cada ciclo dentro de `collect()` e
exposta em `CollectionResult.engine_version*` — `PollingWorker` persiste de volta em
`instances.engine_version*` apos um ciclo bem-sucedido.

Permissao minima esperada: `VIEW SERVER STATE` (< 2022) / `VIEW SERVER PERFORMANCE STATE`
(>= 2022) — collector-queries.md §7, §9.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from uuid import UUID
from xml.etree import ElementTree

import pymssql

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    CONNECTIVITY,
    SQLSERVER_DATABASE_VISIBILITY,
    SQLSERVER_DEFINITION_VISIBILITY,
    SQLSERVER_SERVER_STATE,
    SQLSERVER_SESSION_TERMINATION,
    SQLSERVER_TABLE_READ_ACCESS,
)
from app.collectors.connection import ConnectionParams
from app.collectors.transaction_flagging import compute_flag_reason
from app.core.config import get_settings
from app.core.instance_executors import run_in_instance_executor
from app.core.logging import get_logger
from app.masking.sql_masking import (
    mask_showplan_xml_literals,
    mask_sql_literals,
    mask_sql_literals_for_display,
)
from app.normalizers.metric_names import (
    SQLSERVER_ACTIVE_SESSION_COUNT,
    SQLSERVER_DEADLOCKS_TOTAL,
    SQLSERVER_INSTANCE_CPU_PERCENT,
    SQLSERVER_OS_CPU_PERCENT,
    SQLSERVER_RAW_COUNTER_TO_METRIC_NAME,
    SQLSERVER_REPLICA_LOG_SEND_QUEUE_KB,
    SQLSERVER_REPLICA_REDO_QUEUE_KB,
    SQLSERVER_VOLUME_FREE_PERCENT,
)
from app.normalizers.models import (
    NormalizedBackupStatus,
    NormalizedBlockingEvent,
    NormalizedConfirmedTransactionError,
    NormalizedConfigComplianceItem,
    NormalizedConnectionConsumer,
    NormalizedDbSize,
    NormalizedDeadlockEvent,
    NormalizedIdleInTransactionSession,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedPlanCompileTimeoutFinding,
    NormalizedPlanSnapshot,
    NormalizedQuerySample,
    NormalizedTempDbStats,
    NormalizedTransactionEvent,
)
from app.normalizers.wait_taxonomy import map_wait_event

logger = get_logger(__name__)

_SERVER_STATE_PERMISSIONS = {"VIEW SERVER STATE", "VIEW SERVER PERFORMANCE STATE"}

# Fase 5 T56 — sessoes com transacao aberta mas sem request ativa. Requer VIEW SERVER STATE.
# `NOT EXISTS (SELECT 1 FROM sys.dm_exec_requests r WHERE r.session_id = s.session_id)`
# garante que so sessoes OCIOSAS (sem request em execucao) sao retornadas — sessoes com
# request ativa mas transacao aberta sao cobertas pela coleta normal de sessoes.
_IDLE_IN_TRANSACTION_QUERY = """
    SELECT
        s.session_id,
        ISNULL(s.login_name, '') AS username,
        ISNULL(s.host_name, '') AS host_name,
        ISNULL(s.program_name, '') AS program_name,
        s.open_transaction_count,
        DATEDIFF(SECOND, s.last_request_end_time, GETDATE()) AS idle_seconds,
        ISNULL(t.text, '') AS last_query
    FROM sys.dm_exec_sessions s
    LEFT JOIN sys.dm_exec_connections c ON c.session_id = s.session_id
    OUTER APPLY sys.dm_exec_sql_text(c.most_recent_sql_handle) t
    WHERE s.is_user_process = 1
      AND s.open_transaction_count > 0
      AND NOT EXISTS (
          SELECT 1 FROM sys.dm_exec_requests r WHERE r.session_id = s.session_id
      )
    ORDER BY idle_seconds DESC
"""

# Sentinela negativo de `blocking_session_id` (-2 a -5): bloqueador nao determinavel — NAO
# significa "sem bloqueio" nem e um erro (collector-queries.md §2).
_BLOCKING_SESSION_REAL_BLOCK_MIN = 1

# `sys.dm_exec_sql_text(r.sql_handle)` devolve o texto do BATCH/PROCEDURE inteiro associado ao
# sql_handle, nao so o statement em execucao agora — para uma sessao dentro de uma stored
# procedure, isso e a `CREATE PROCEDURE ...` inteira (Docs/collector-queries.md §1, nuance de
# texto). `statement_start_offset`/`statement_end_offset` (em bytes, texto e nvarchar -> dividir
# por 2) fatiam so o statement atual - padrao oficial da Microsoft para "o que esta sessao esta
# executando agora". `query_text` (fatiado) vira `sql_hash`/`sql_text_masked`; `full_query_text`
# (batch/procedure inteiro) vira `sql_text_full_masked`, so pra tela de detalhe.
# `r.cpu_time` (ms cumulativos consumidos pela request atual, zera numa request nova) vira
# `cpu_time_ms` em `NormalizedQuerySample` — antes so era buscado (como `cpu_seconds`) e
# descartado sem uso; agora fecha o gap de CPU nao coberto por esperas nomeadas.
_SESSION_ACTIVITY_QUERY = """
    SELECT r.session_id, r.status, r.command, r.start_time,
           r.total_elapsed_time / 1000.00 AS elapsed_seconds,
           r.total_elapsed_time,
           r.cpu_time AS cpu_time_ms,
           r.reads, r.writes, r.logical_reads, r.row_count,
           r.blocking_session_id, r.wait_type, r.wait_time, r.wait_resource,
           r.transaction_id, r.prev_error,
           s.login_name, s.host_name, s.program_name,
           DB_NAME(r.database_id) AS database_name,
           t.text AS full_query_text,
           SUBSTRING(
               t.text,
               (r.statement_start_offset / 2) + 1,
               (
                   (CASE r.statement_end_offset
                        WHEN -1 THEN DATALENGTH(t.text)
                        ELSE r.statement_end_offset
                    END - r.statement_start_offset) / 2
               ) + 1
           ) AS query_text
    FROM sys.dm_exec_requests AS r
    INNER JOIN sys.dm_exec_sessions AS s ON r.session_id = s.session_id
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
    WHERE r.session_id <> @@SPID
    ORDER BY r.start_time DESC
"""

# Etapa 9 (arvore completa de bloqueio) — identidade de TODAS as sessoes conectadas, nao so as
# com request ativa. Necessaria pro caso do "bloqueador ocioso": uma sessao que abriu transacao e
# segura lock sem estar rodando nada agora nunca aparece em `_SESSION_ACTIVITY_QUERY` (que faz
# INNER JOIN com `sys.dm_exec_requests`), entao sem isso ela ficaria sem usuario/programa/maquina
# na arvore — so o `session_id`. `is_user_process = 1` exclui sessoes de sistema/background.
_ALL_SESSIONS_QUERY = """
    SELECT session_id, login_name, host_name, program_name
    FROM sys.dm_exec_sessions
    WHERE is_user_process = 1
"""

# Etapa 9 — resolve o OBJETO bloqueado (nao so o recurso bruto de `wait_resource`, ja selecionado
# em `_SESSION_ACTIVITY_QUERY`) pra sessoes atualmente esperando por um lock (`request_status =
# 'WAIT'`). Padrao oficial da Microsoft: pra `resource_type = 'OBJECT'`,
# `resource_associated_entity_id` JA E o `object_id` (`OBJECT_NAME` resolve cross-database direto,
# passando `resource_database_id` — sem depender do banco da conexao). Pra `'PAGE'/'KEY'/'RID'`,
# `resource_associated_entity_id` e o `hobt_id` — so resolvivel via `sys.partitions`, uma view POR
# BANCO (sem variante cross-database) - BUG REAL CORRIGIDO (relatado pelo usuario ao vivo,
# `blocked_object` sempre None mesmo pra um lock comum de RID/heap): a versao anterior desta
# query juntava `sys.partitions` (sempre escopada ao banco ATUAL DA CONEXAO) contra
# `resource_database_id = DB_ID()` - `DB_ID()` e o banco da conexao (tipicamente o banco padrao
# do login, nao necessariamente o banco onde o lock realmente esta), entao o join praticamente
# NUNCA batia pra bancos de usuario reais. Corrigido: esta query so devolve o dado bruto
# (resolve OBJECT direto, que nao precisa de sys.partitions); PAGE/KEY/RID sao resolvidos depois,
# em Python, com uma conexao curta NO BANCO CERTO (`_resolve_hobt_object_names`) - so pros bancos
# que de fato tem um hobt_id pendente agora (tipicamente 0-1 banco por ciclo, nao todos).
_BLOCKED_LOCKS_QUERY = """
    SELECT
        l.request_session_id AS session_id,
        l.resource_type,
        l.resource_database_id,
        DB_NAME(l.resource_database_id) AS resource_database_name,
        l.resource_associated_entity_id,
        CASE WHEN l.resource_type = 'OBJECT'
             THEN OBJECT_NAME(l.resource_associated_entity_id, l.resource_database_id)
             ELSE NULL
        END AS resolved_object_name
    FROM sys.dm_tran_locks AS l
    WHERE l.request_status = 'WAIT'
      AND l.resource_type IN ('OBJECT', 'PAGE', 'KEY', 'RID')
"""


def _resolve_hobt_object_names(
    connection: ConnectionParams,
    query_timeout_seconds: float,
    hobt_ids_by_database: dict[str, set[int]],
) -> dict[tuple[str, int], str | None]:
    """Resolve nome de objeto pra locks PAGE/KEY/RID (chave = `hobt_id`) — `sys.partitions` so
    enxerga o banco ATUAL da conexao, sem equivalente cross-database, entao conecta uma vez por
    banco com pelo menos 1 `hobt_id` pendente agora (nao itera todos os bancos da instancia -
    normalmente 0-1 conexao extra por ciclo, so quando ha bloqueio de verdade acontecendo). Melhor
    esforco: falha de conexao/permissao num banco especifico so deixa aquele lock sem
    `blocked_object` (o `wait_resource` bruto continua disponivel), nao derruba o ciclo inteiro.

    Verificado ao vivo (Autklinux20/21): resolve corretamente o nome real da tabela pra locks
    KEY/RID em bancos de usuario de verdade — o bug antigo (join contra `DB_ID()`, banco da
    conexao, nunca o banco do lock) tornava isso impossivel pra qualquer banco != o da conexao.
    `blocked_object` ainda pode sair `None` ocasionalmente mesmo com a correcao: a resolucao roda
    numa SEGUNDA consulta (`sys.dm_tran_locks` primeiro, esta depois), e um lock pode mudar/ser
    liberado no intervalo entre as duas — limitacao inerente de qualquer abordagem por snapshot
    contra estado volatil (mesma familia de ressalva ja documentada pra arvore de bloqueio, "foto
    do ultimo ciclo de poll"), nao um bug desta funcao.
    """
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    result: dict[tuple[str, int], str | None] = {}
    for database_name, hobt_ids in hobt_ids_by_database.items():
        if not hobt_ids:
            continue
        try:
            conn = pymssql.connect(
                server=connection.host,
                port=str(connection.port),
                user=connection.username,
                password=connection.secret_value,
                database=database_name,
                timeout=int(query_timeout_seconds),
                login_timeout=login_timeout_seconds,
            )
        except Exception:
            logger.warning(
                "Falha ao conectar no banco %s pra resolver objeto bloqueado - melhor esforco, "
                "wait_resource bruto continua disponivel",
                database_name,
                exc_info=True,
            )
            continue
        try:
            cursor = conn.cursor(as_dict=True)
            placeholders = ",".join(["%s"] * len(hobt_ids))
            cursor.execute(
                f"SELECT hobt_id, OBJECT_NAME(object_id) AS object_name "
                f"FROM sys.partitions WHERE hobt_id IN ({placeholders})",
                tuple(hobt_ids),
            )
            for row in cursor.fetchall():
                object_name = row["object_name"]
                result[(database_name, cast(int, row["hobt_id"]))] = (
                    str(object_name) if object_name is not None else None
                )
        except Exception:
            logger.warning(
                "Falha ao resolver nome de objeto (hobt_id) no banco %s - melhor esforco, "
                "wait_resource bruto continua disponivel",
                database_name,
                exc_info=True,
            )
        finally:
            conn.close()
    return result


# Etapa 10 (rastreamento de transacoes) — toda transacao ABERTA no ciclo, join documentado pela
# propria Microsoft (`sys.dm_tran_session_transactions` linka pra `sys.dm_tran_active_
# transactions` por `transaction_id`). Cobre sessoes ativas E ociosas (uma sessao "idle in
# transaction" ainda aparece aqui, mesmo sem request ativa) — usado tanto pra popular
# `transaction_events` quanto pra enriquecer `blocking_events` com `blocker_transaction_id`/
# `blocked_transaction_id` (join por `session_id`).
_TRANSACTION_QUERY = """
    SELECT
        tst.transaction_id,
        tst.session_id,
        tat.transaction_begin_time,
        tat.transaction_type,
        tat.transaction_state,
        tat.transaction_uow
    FROM sys.dm_tran_session_transactions AS tst
    INNER JOIN sys.dm_tran_active_transactions AS tat
        ON tst.transaction_id = tat.transaction_id
"""

# Banco de dados de uma transacao — `sys.dm_tran_database_transactions` tem 1 linha por
# (transacao, banco tocado); uma transacao pode tocar mais de um banco (ex.: via linked server),
# entao agregamos pro banco de MENOR database_id como melhor esforco (nao ha um "banco principal"
# formalmente definido pela DMV). `DB_NAME()` resolve pelo `database_id` diretamente, sem
# depender do banco atual da conexao (funciona pra qualquer banco da instancia).
_TRANSACTION_DATABASE_QUERY = """
    SELECT transaction_id, DB_NAME(MIN(database_id)) AS database_name
    FROM sys.dm_tran_database_transactions
    GROUP BY transaction_id
"""

# So contadores RAW (`cntr_type=65792`) — delta/razao (§5, corte de escopo) nao implementados.
# `RTRIM(instance_name) = ''` evita duplicacao por node NUMA.
_PERFORMANCE_COUNTERS_QUERY = """
    SELECT RTRIM(counter_name) AS counter_name, cntr_value
    FROM sys.dm_os_performance_counters
    WHERE cntr_type = 65792
      AND RTRIM(counter_name) IN (
          'Page life expectancy', 'Processes blocked',
          'Target Server Memory (KB)', 'Total Server Memory (KB)'
      )
      AND RTRIM(instance_name) = ''
"""

# Contador CUMULATIVO (nunca zera, so cresce) de deadlocks desde que o servico do SQL Server
# subiu — `cntr_type=272696` (PERF_COUNTER_BULK_COUNT), tipo diferente dos 4 contadores RAW
# acima (`cntr_type=65792`), por isso query separada. Usado pelo `PollingWorker` (que ja roda a
# cada `polling_interval_seconds`, ate 1s numa instancia critica) para detectar um deadlock NA
# HORA em que acontece, comparando este valor contra o ciclo anterior — sem isso, so saberiamos
# depois que o SQL Server publicasse o relatorio completo em `system_health`, o que na pratica
# demorou 25-35min num teste real do usuario (ver `_DEADLOCK_FILE_TARGET_QUERY`/
# `_DEADLOCK_RING_BUFFER_QUERY`). Deteccao instantanea != detalhe instantaneo: aqui so sabemos
# QUE aconteceu, os detalhes (vitima/sobrevivente/SQL/recurso) continuam vindo depois, do
# `system_health`, via `DeadlockEventsRepository` reconciliando o registro "pendente" criado por
# esta deteccao com o registro completo assim que ele aparecer.
_DEADLOCK_COUNTER_QUERY = """
    SELECT cntr_value
    FROM sys.dm_os_performance_counters
    WHERE RTRIM(counter_name) = 'Number of Deadlocks/sec'
      AND RTRIM(instance_name) = '_Total'
"""

# Consulta oficial da Microsoft pra CPU via o ring buffer de scheduler monitor (atualizado a cada
# ~1 minuto pelo proprio motor, independente do intervalo de poll) — sem permissao alem de VIEW
# SERVER STATE (ja mandatoria). `SystemIdle` = % ocioso do SO inteiro; `SQLProcessUtilization` = %
# do SO consumido pelo PROCESSO do SQL Server (a instancia). "CPU do SO" (gráfico 2 pedido pelo
# usuário) = 100 - SystemIdle (todo consumo, SQL Server + outros processos) — o ring buffer só
# guarda um histórico curto (ring buffer, não persistente), por isso este valor precisa ser
# amostrado a cada ciclo de poll como qualquer outra métrica, não recuperado retroativamente.
_CPU_UTILIZATION_QUERY = """
    SELECT TOP (1)
        SQLProcessUtilization,
        SystemIdle
    FROM (
        SELECT
            record.value('(./Record/@id)[1]', 'int') AS record_id,
            record.value(
                '(./Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', 'int'
            ) AS SystemIdle,
            record.value(
                '(./Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', 'int'
            ) AS SQLProcessUtilization
        FROM (
            SELECT CONVERT(xml, record) AS record
            FROM sys.dm_os_ring_buffers
            WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
              AND record LIKE '%<SystemHealth>%'
        ) AS raw_records
    ) AS parsed
    ORDER BY record_id DESC
"""

# Baseado no exemplo oficial da Microsoft para este exato caso de uso (Docs/collector-queries.md
# §10) — `mid.statement` ja vem qualificado pela propria DMV (ex.: "[DB].[schema].[tabela]"), sem
# precisar de `OBJECT_NAME()`. Ao contrario de `sys.dm_exec_requests` (sempre "banco atual"),
# `sys.dm_db_missing_index_details` cobre a INSTANCIA INTEIRA numa unica chamada — sem iterar por
# banco (confirmado contra a documentacao oficial da Microsoft).
_MISSING_INDEX_QUERY = """
    SELECT
        DB_NAME(mid.database_id) AS database_name,
        mid.statement AS table_name,
        migs.avg_total_user_cost * migs.avg_user_impact
            * (migs.user_seeks + migs.user_scans) AS impact_score,
        migs.avg_total_user_cost, migs.avg_user_impact,
        mid.equality_columns, mid.inequality_columns, mid.included_columns,
        migs.user_seeks, migs.user_scans
    FROM sys.dm_db_missing_index_group_stats AS migs
    INNER JOIN sys.dm_db_missing_index_groups AS mig
        ON migs.group_handle = mig.index_group_handle
    INNER JOIN sys.dm_db_missing_index_details AS mid
        ON mig.index_handle = mid.index_handle
    ORDER BY impact_score DESC
"""

# Docs/collector-queries.md §6 — so backup COMPLETO (`type='D'`, "Database"); diferencial
# (`type='I'`) e log (`type='L'`) ficam de fora desta primeira versao. `MAX(backup_finish_date)`
# ja agrega pra "ultimo backup completo por banco" direto na query, sem pos-processamento.
_BACKUP_STATUS_QUERY = """
    SELECT database_name, MAX(backup_finish_date) AS last_backup_at
    FROM msdb.dbo.backupset
    WHERE type = 'D'
    GROUP BY database_name
"""

# Fase 2, item 2 (freespace) — `sys.dm_os_volume_stats` devolve espaco por VOLUME (nao por
# banco); `DISTINCT` porque varios arquivos de bancos diferentes costumam compartilhar o mesmo
# volume fisico (ex. todos os .mdf no mesmo drive D:) — sem isso a mesma linha repetiria uma vez
# por arquivo. So SQL Server: nao ha equivalente PostgreSQL sem acesso ao SO (ver
# `Docs/product-vision-and-roadmap.md`).
_VOLUME_FREESPACE_QUERY = """
    SELECT DISTINCT
        vs.volume_mount_point,
        vs.total_bytes,
        vs.available_bytes
    FROM sys.master_files AS mf
    CROSS APPLY sys.dm_os_volume_stats(mf.database_id, mf.file_id) AS vs
"""

# Fase 2, item 2 (replicacao) — Always On Availability Groups. Sem AG configurado, devolve 0
# linhas (nao e um erro) - degradacao graciosa, mesmo padrao ja usado em toda capacidade
# especifica de motor deste projeto.
_REPLICA_STATE_QUERY = """
    SELECT
        ars.replica_server_name,
        DB_NAME(drs.database_id) AS database_name,
        drs.log_send_queue_size,
        drs.redo_queue_size
    FROM sys.dm_hadr_database_replica_states AS drs
    INNER JOIN sys.availability_replicas AS ars ON ars.replica_id = drs.replica_id
"""

# Etapa 10, item 7 (Deadlocks) — sessao Extended Events `system_health`, nativa e sempre ativa
# por padrao desde o SQL Server 2008 (nunca precisa ser criada).
#
# Historico real desta query (2026-09-21, confirmado ao vivo pelo usuario, 2 rodadas de teste):
# o target `ring_buffer` (usado aqui originalmente) e um buffer em MEMORIA de poucos MB, com um
# problema documentado em varias versoes do SQL Server de NAO refletir `xml_deadlock_report` de
# forma confiavel — no teste do usuario, o evento nunca apareceu ali (nem via query direta dele
# no SSMS). Trocado para o target `event_file` (`.xel` no disco, `sys.fn_xe_file_target_read_file`)
# por ser a alternativa documentada pela comunidade SQL Server — mas o mesmo teste ao vivo revelou
# que, NESTA instancia, o `event_file` estava BEM MAIS lento a refletir o evento que o proprio
# `ring_buffer` (o `ring_buffer`, com o codigo antigo, achou o mesmo deadlock ~24min depois do
# evento; o `event_file` ainda nao tinha o mesmo deadlock 35+min depois). Ou seja: nenhum dos 2
# targets e consistentemente mais rapido — o atraso de dispatch e imprevisivel e varia por
# instancia/carga. Decisao final: consultar OS DOIS e mesclar (deduplicado em
# `_build_deadlock_events`, ja que o mesmo deadlock frequentemente aparece nos dois eventualmente)
# — maximiza a chance de pegar o evento assim que QUALQUER um dos dois o tiver, sem trocar
# confiabilidade por velocidade ou vice-versa.
_DEADLOCK_RING_BUFFER_QUERY = """
    SELECT CAST(xet.target_data AS NVARCHAR(MAX)) AS target_data
    FROM sys.dm_xe_session_targets AS xet
    INNER JOIN sys.dm_xe_sessions AS xes ON xes.address = xet.event_session_address
    WHERE xes.name = 'system_health' AND xet.target_name = 'ring_buffer'
"""

# `'system_health*.xel'` (sem caminho) resolve para a pasta padrao `MSSQL\\Log` da propria
# instancia desde o SQL Server 2016 — sem precisar descobrir/hardcodar o caminho. Mesma permissao
# ja exigida (`VIEW SERVER STATE` no SQL Server ate 2019; 2022+ pede
# `VIEW SERVER PERFORMANCE STATE`, fora de escopo tratar aqui). Janela de 1h (bem mais que os
# 5min do ciclo do supervisor) da margem de sobra pra qualquer atraso de flush do arquivo.
_DEADLOCK_FILE_TARGET_QUERY = """
    SELECT event_data, timestamp_utc
    FROM sys.fn_xe_file_target_read_file('system_health*.xel', NULL, NULL, NULL)
    WHERE object_name = 'xml_deadlock_report'
      AND timestamp_utc >= DATEADD(HOUR, -1, SYSUTCDATETIME())
"""

# Etapa 10, item 3 (captura confiavel de erro por transacao) — ao contrario do deadlock (que
# reaproveita a sessao `system_health` ja sempre ativa), aqui precisamos de uma sessao Extended
# Events DEDICADA e NOVA (`sqlserver.error_reported` nao e capturado por `system_health` por
# padrao) - `_ensure_transaction_error_session` cria/inicia de forma idempotente (checa
# `sys.server_event_sessions`/`sys.dm_xe_sessions` antes). Exige a permissao de servidor
# `ALTER ANY EVENT SESSION`; sem ela, a criacao falha e o supervisor loga e pula esta instancia
# neste ciclo (nunca propaga excecao) - nao tratado como capacidade formal de cadastro (mesmo
# racional de "degrada em runtime" ja usado por outras checagens de baixa frequencia).
#
# `WHERE severity >= 16` (bug real ja documentado em `_build_transaction_events`: severidade
# sozinha NAO distingue um erro real de ruido recuperavel do mesmo nivel - ex. "The cursor was
# not declared", severidade 16, nao aborta a transacao) - severidade >= 16 e' o corte real entre
# "erro de usuario que precisa de atencao" e mensagens informativas (severidade < 11), mas
# CONTINUA sem confirmar causalidade de rollback (isso exigiria correlacionar com um evento de
# rollback - fora de escopo aqui, nao implementado por falta de forma segura de verificar o nome
# exato do evento contra uma instancia real neste momento). A melhoria real e concreta desta
# feature: o `prev_error` (`sys.dm_exec_requests`) SOME assim que a sessao fica idle apos o erro -
# a sessao XE retem o evento independente do estado atual da sessao.
_TRANSACTION_ERROR_SESSION_NAME = "ab_xql_pm_transaction_errors"

_TRANSACTION_ERROR_SESSION_EXISTS_QUERY = f"""
    SELECT name FROM sys.server_event_sessions WHERE name = '{_TRANSACTION_ERROR_SESSION_NAME}'
"""

_TRANSACTION_ERROR_SESSION_RUNNING_QUERY = f"""
    SELECT s.name FROM sys.dm_xe_sessions AS s WHERE s.name = '{_TRANSACTION_ERROR_SESSION_NAME}'
"""

_CREATE_TRANSACTION_ERROR_SESSION_SQL = f"""
    CREATE EVENT SESSION [{_TRANSACTION_ERROR_SESSION_NAME}] ON SERVER
    ADD EVENT sqlserver.error_reported(
        ACTION (sqlserver.session_id, sqlserver.database_name)
        WHERE severity >= 16
    )
    ADD TARGET package0.event_file(
        SET filename = N'{_TRANSACTION_ERROR_SESSION_NAME}.xel',
        max_file_size = 10,
        max_rollover_files = 2
    )
    WITH (
        MAX_MEMORY = 4096 KB,
        EVENT_RETENTION_MODE = ALLOW_SINGLE_EVENT_LOSS,
        MAX_DISPATCH_LATENCY = 5 SECONDS,
        STARTUP_STATE = ON
    )
"""

_START_TRANSACTION_ERROR_SESSION_SQL = f"""
    ALTER EVENT SESSION [{_TRANSACTION_ERROR_SESSION_NAME}] ON SERVER STATE = START
"""

_TRANSACTION_ERROR_FILE_TARGET_QUERY = f"""
    SELECT event_data, timestamp_utc
    FROM sys.fn_xe_file_target_read_file('{_TRANSACTION_ERROR_SESSION_NAME}*.xel', NULL, NULL, NULL)
    WHERE object_name = 'error_reported'
      AND timestamp_utc >= DATEADD(HOUR, -1, SYSUTCDATETIME())
"""

# Etapa 11 (plano de execucao sob demanda) — busca o `plan_handle` cujo statement isolado
# (mesmo padrao statement_start_offset/end_offset de `_SESSION_ACTIVITY_QUERY`, mas contra
# `sys.dm_exec_query_stats`, que tambem tem essas colunas) mascarado+hasheado bate com o
# `sql_hash` pedido. `TOP (2000)` por `last_execution_time DESC` — acao sob demanda (nao parte do
# ciclo de poll), scan limitado e aceitavel (~1-2s medido ao vivo); prioriza planos usados
# recentemente, mais provaveis de corresponder a uma query vista ha pouco em `query_samples`.
#
# Limitacao real confirmada ao vivo (Autklinux20/autklinux70, instancias com CDC/replicacao
# ativos): em instancias muito movimentadas, o TOP do cache por recencia pode ser dominado
# inteiramente por chatter de fundo (CDC, replicacao, o proprio polling deste app) executando
# multiplas vezes por segundo — mesmo TOP (2000) as vezes cobre so poucos segundos de atividade
# real, empurrando pra fora do scan uma query que rodou ha mais de poucos segundos mesmo que
# ainda esteja tecnicamente ativa. Aceito como limitacao conhecida da abordagem "sob demanda,
# sem captura continua" escolhida explicitamente pelo usuario (alternativa seria captura
# continua/persistida, escopo maior, item futuro do roadmap) — a resposta `available=false`
# sempre vem com motivo honesto, nunca finge ter encontrado o plano.
#
# So devolve o XML (segunda query, so quando ha match) via `sys.dm_exec_query_plan(plan_handle)`
# — pode ser NULL se o plano ja saiu do cache (memoria, recompilacao) entre a amostra e esta
# busca; XML e o plano CACHEADO/compilado (estimado), nao o "actual" com contagens reais de
# execucao — ver `_ACTIVE_STATEMENT_SEARCH_QUERY`/`_ACTUAL_PLAN_XML_QUERY` abaixo pro caminho
# "real", tentado primeiro pelo service antes de cair pra este (`fetch_execution_plan`).
_EXECUTION_PLAN_SEARCH_QUERY = """
    SELECT TOP (2000)
        qs.plan_handle,
        qs.last_execution_time,
        SUBSTRING(
            st.text,
            (qs.statement_start_offset / 2) + 1,
            (
                (CASE qs.statement_end_offset
                     WHEN -1 THEN DATALENGTH(st.text)
                     ELSE qs.statement_end_offset
                 END - qs.statement_start_offset) / 2
            ) + 1
        ) AS statement_text
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
    WHERE st.text IS NOT NULL
    ORDER BY qs.last_execution_time DESC
"""

_EXECUTION_PLAN_XML_QUERY = "SELECT query_plan FROM sys.dm_exec_query_plan(%s)"

# Item 10 (Query Store) — fallback quando o plano ja saiu do cache de planos. Query Store persiste
# planos em disco (ate o limite de retencao configurado na instancia, tipicamente 30 dias) mesmo
# depois de uma recompilacao ou pressao de memoria que expulsaria o plano do cache. So disponivel
# quando o banco alvo tem Query Store habilitado (`is_query_store_on = 1` em `sys.databases`).
#
# Usa 3-part naming `[{db}].sys.query_store_query_text` — permitido sem `USE [db]` desde SQL
# Server 2016. `db` e sanitizado em `_connect_and_fetch_plan_from_query_store` (remove `]` antes
# de interpolar) — nunca e input direto do usuario, vem da nossa tabela `query_samples`.
#
# Limite de 2000 textos por banco (mesmo limite do scan do cache de plano): acao sob demanda,
# nao parte do ciclo de coleta, scan pontual e aceitavel.
_QUERY_STORE_TEXT_QUERY_TMPL = """
    SELECT TOP (2000)
        CAST(qsqt.query_sql_text AS nvarchar(max)) AS statement_text,
        qsqt.query_text_id
    FROM [{db}].sys.query_store_query_text AS qsqt
    ORDER BY qsqt.query_text_id DESC
"""

# Plano REAL (com contagens de linha/CPU/IO reais de execucao, nao so estimativa de
# compilacao) — so possivel sem Query Store (desligado por padrao on-premises) quando a mesma
# sessao que rodou `sql_hash` AINDA esta em execucao no momento da busca
# (`sys.dm_exec_query_statistics_xml`, SQL Server 2016 SP1+). Mesmo padrao de fatiamento de
# statement de `_SESSION_ACTIVITY_QUERY`, mas contra `sys.dm_exec_requests` (sessoes ativas
# agora, nao o cache de plano). Tentado PRIMEIRO pelo service; se nao achar sessao ativa
# correspondente (o caso comum — a maioria das queries olhadas no detalhe ja terminou ha
# tempo), cai pro plano cacheado/estimado de `_EXECUTION_PLAN_SEARCH_QUERY` acima.
_ACTIVE_STATEMENT_SEARCH_QUERY = """
    SELECT
        r.session_id,
        SUBSTRING(
            t.text,
            (r.statement_start_offset / 2) + 1,
            (
                (CASE r.statement_end_offset
                     WHEN -1 THEN DATALENGTH(t.text)
                     ELSE r.statement_end_offset
                 END - r.statement_start_offset) / 2
            ) + 1
        ) AS statement_text
    FROM sys.dm_exec_requests AS r
    CROSS APPLY sys.dm_exec_sql_text(r.sql_handle) AS t
    WHERE r.session_id <> @@SPID AND t.text IS NOT NULL
"""

_ACTUAL_PLAN_XML_QUERY = "SELECT query_plan FROM sys.dm_exec_query_statistics_xml(%s)"

# Fase 2, item 5 (plan_compile_timeout) — mesmo padrao de scan do cache de plano de
# `_EXECUTION_PLAN_SEARCH_QUERY`, mas ja trazendo o XML do plano numa unica query (nao precisa
# de um `sql_hash` alvo — varre o cache inteiro procurando o motivo de aborto do otimizador).
# `TOP (500)`: worker de baixa frequencia (1x/dia por padrao), nao acao sob demanda — escopo
# menor que os 2000 do plano sob demanda e suficiente pra essa varredura periodica.
_PLAN_COMPILE_TIMEOUT_SCAN_QUERY = """
    SELECT TOP (500)
        qp.query_plan,
        SUBSTRING(
            st.text,
            (qs.statement_start_offset / 2) + 1,
            (
                (CASE qs.statement_end_offset
                     WHEN -1 THEN DATALENGTH(st.text)
                     ELSE qs.statement_end_offset
                 END - qs.statement_start_offset) / 2
            ) + 1
        ) AS statement_text
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
    CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
    WHERE st.text IS NOT NULL AND qp.query_plan IS NOT NULL
    ORDER BY qs.last_execution_time DESC
"""

# Historico de plano de execucao (item priorizado pelo usuario apos a Fase 2) — mesma forma de
# `_PLAN_COMPILE_TIMEOUT_SCAN_QUERY` (varre o cache inteiro, TOP 500 por recencia), mas sem
# filtrar por motivo de aborto do otimizador: aqui queremos TODO plano em cache, pra detectar
# quando ele MUDA em relacao ao ultimo conhecido (deduplicacao fica no repositorio). Inclui
# `last_execution_time` — ajuda a correlacionar quando aquele plano estava de fato em uso, nao
# so quando o worker aconteceu de varrer.
_PLAN_HISTORY_SCAN_QUERY = """
    SELECT TOP (500)
        qp.query_plan,
        qs.last_execution_time,
        SUBSTRING(
            st.text,
            (qs.statement_start_offset / 2) + 1,
            (
                (CASE qs.statement_end_offset
                     WHEN -1 THEN DATALENGTH(st.text)
                     ELSE qs.statement_end_offset
                 END - qs.statement_start_offset) / 2
            ) + 1
        ) AS statement_text
    FROM sys.dm_exec_query_stats AS qs
    CROSS APPLY sys.dm_exec_sql_text(qs.sql_handle) AS st
    CROSS APPLY sys.dm_exec_query_plan(qs.plan_handle) AS qp
    WHERE st.text IS NOT NULL AND qp.query_plan IS NOT NULL
    ORDER BY qs.last_execution_time DESC
"""

# `StatementOptmEarlyAbortReason` em `StmtSimple` — os 2 motivos que indicam o otimizador abortou
# a busca de plano cedo, nao um "timeout de execucao" (isso seria outra coisa, wait/blocking).
_PLAN_COMPILE_TIMEOUT_REASONS = frozenset({"TimeOutReached", "MemoryLimitExceeded"})


def _connect_and_check(connection: ConnectionParams) -> list[CapabilityResult]:
    """Helper sincrono: conecta via `pymssql` e roda as checagens de permissao.

    Roda em thread separada (pool dedicado por instancia, ver `app.core.instance_executors`) —
    `pymssql` nao e assincrono.
    """
    timeout_seconds = int(connection.connect_timeout_seconds)
    try:
        conn = pymssql.connect(
            server=connection.host,
            port=str(connection.port),
            user=connection.username,
            password=connection.secret_value,
            timeout=timeout_seconds,
            login_timeout=timeout_seconds,
        )
    except (pymssql.OperationalError, OSError, TimeoutError) as exc:
        return [
            CapabilityResult(
                capability=CONNECTIVITY,
                status=CapabilityStatus.UNAVAILABLE,
                reason=f"Falha ao conectar: {type(exc).__name__}",
            )
        ]

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT permission_name FROM fn_my_permissions(NULL, 'SERVER')")
        granted_permissions = {str(row[0]) for row in cursor.fetchall()}

        cursor.execute(
            "SELECT IS_SRVROLEMEMBER('processadmin') AS is_processadmin, "
            "IS_ROLEMEMBER('db_datareader') AS is_db_datareader"
        )
        role_membership_row = cursor.fetchone()
        if role_membership_row is None:  # pragma: no cover - SELECT literal sempre devolve 1 linha
            raise pymssql.OperationalError("fn_my_permissions/IS_ROLEMEMBER nao retornou linha")
        is_processadmin_row, is_db_datareader_row = role_membership_row
        cursor.close()

        return _build_capability_results(
            granted_permissions=granted_permissions,
            is_processadmin=bool(is_processadmin_row),
            is_db_datareader=bool(is_db_datareader_row),
        )
    finally:
        conn.close()


def _parse_major_version(product_version: str) -> int:
    """Extrai a versao major de `SERVERPROPERTY('ProductVersion')` (ex.: "16.0.1000.6" -> 16).

    So usado para log/deteccao a cada ciclo (`Instance.engine_version*` nao e persistido nesta
    feature — corte de escopo). NAO usa `ProductMajorVersion` (so existe a partir do 2017).
    """
    return int(product_version.split(".")[0])


def _as_utc(value: datetime | None, server_utc_offset: timedelta) -> datetime | None:
    """`pymssql` devolve `datetime`/`datetime2` do SQL Server como datetime NAIVE (sem tzinfo) —
    e, confirmado ao vivo, no horario LOCAL do servidor (ex.: America/Sao_Paulo, UTC-3), NAO em
    UTC. So anexar `tzinfo=UTC` sem converter (primeira tentativa desta correcao) produzia uma
    diferenca de ~3h em relacao ao `datetime.now(UTC)` deste processo, fazendo TODA transacao
    parecer "orfa" instantaneamente (idade aparente inflada pela diferenca de fuso). Correcao:
    soma `server_utc_offset` (= `GETUTCDATE() - GETDATE()`, calculado uma vez por ciclo em
    `_connect_and_collect`) antes de anexar `tzinfo=UTC` — converte de verdade pro instante UTC
    correto, nao so relabela.

    Bug relacionado tambem encontrado ao vivo: SEM nenhum tratamento (nem relabel), comparar este
    valor naive contra `datetime.now(UTC)` (aware) em `compute_flag_reason`/`_is_orphan_candidate`
    levantava `TypeError: can't subtract offset-naive and offset-aware datetimes` a cada ciclo,
    derrubando o ciclo de coleta INTEIRO (capturado pelo try/except do `PollingWorker`, sem
    persistir NADA naquele ciclo — nao so transacoes)."""
    if value is None or value.tzinfo is not None:
        return value
    return (value + server_utc_offset).replace(tzinfo=UTC)


def _connect_and_collect(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
    max_rows: int,
    collected_at: datetime,
    transaction_orphan_threshold_minutes: int,
    sleeping_min_duration_ms: int | None,
    sleeping_database_overrides_ms: dict[str, int],
) -> tuple[
    list[NormalizedMetricSample],
    list[NormalizedQuerySample],
    list[NormalizedBlockingEvent],
    list[NormalizedTransactionEvent],
    list[str],
    str,
    int,
]:
    """Helper sincrono: conecta via `pymssql`, roda as queries de coleta e normaliza.

    Roda em thread separada (pool dedicado por instancia, mesmo padrao de `_connect_and_check`) —
    `pymssql` nao e assincrono. `timeout` (login) usa `connection.connect_timeout_seconds`,
    `login_timeout`... na verdade o parametro `timeout` do `pymssql.connect` controla o timeout
    de EXECUCAO de query (`query_timeout_seconds`); `login_timeout` controla o timeout do
    proprio login (`connection.connect_timeout_seconds`).
    """
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)

        cursor.execute("SELECT SERVERPROPERTY('ProductVersion') AS product_version")
        version_row = cursor.fetchone()
        raw_product_version = version_row["product_version"] if version_row else "0"
        # pymssql pode devolver essa coluna como bytes dependendo da configuracao de charset
        # do driver/servidor (ex.: b'15.0.2000.5') — `str()` direto sobre bytes produz o repr
        # literal "b'15.0.2000.5'", nao o texto decodificado, quebrando o parse da versao major.
        product_version = (
            raw_product_version.decode("utf-8", errors="replace")
            if isinstance(raw_product_version, bytes)
            else str(raw_product_version)
        )
        version_major = _parse_major_version(product_version)
        logger.debug(
            "SQL Server instance_id=%s versao detectada product_version=%s major=%s",
            instance_id,
            product_version,
            version_major,
        )

        # Offset entre o horario LOCAL do servidor (o que `transaction_begin_time`/demais
        # colunas `datetime` naive de fato representam, confirmado ao vivo) e UTC de verdade —
        # ver docstring de `_as_utc`. Calculado 1x por ciclo (a diferenca de fuso nao muda dentro
        # de um mesmo ciclo de poll).
        cursor.execute("SELECT GETUTCDATE() AS utc_now, GETDATE() AS local_now")
        offset_row = cursor.fetchone()
        server_utc_now = offset_row["utc_now"] if offset_row is not None else None
        server_local_now = offset_row["local_now"] if offset_row is not None else None
        server_utc_offset = (
            server_utc_now - server_local_now
            if isinstance(server_utc_now, datetime) and isinstance(server_local_now, datetime)
            else timedelta(0)
        )

        cursor.execute(_SESSION_ACTIVITY_QUERY)
        session_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_ALL_SESSIONS_QUERY)
        all_session_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_BLOCKED_LOCKS_QUERY)
        blocked_lock_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_TRANSACTION_QUERY)
        transaction_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_TRANSACTION_DATABASE_QUERY)
        transaction_database_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_PERFORMANCE_COUNTERS_QUERY)
        counter_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_CPU_UTILIZATION_QUERY)
        cpu_row = cursor.fetchone()

        cursor.execute(_DEADLOCK_COUNTER_QUERY)
        deadlock_counter_row = cursor.fetchone()

        cursor.close()
    finally:
        conn.close()

    # Resolve PAGE/KEY/RID (hobt_id) por banco — so pros bancos que de fato tem 1+ lock pendente
    # agora (ver docstring de `_resolve_hobt_object_names`). OBJECT ja vem resolvido direto na
    # query principal (`resolved_object_name`), sem precisar de conexao extra.
    hobt_ids_by_database: dict[str, set[int]] = {}
    for row in blocked_lock_rows:
        database_name_raw = row["resource_database_name"]
        if row["resource_type"] == "OBJECT" or database_name_raw is None:
            continue
        hobt_ids_by_database.setdefault(str(database_name_raw), set()).add(
            cast(int, row["resource_associated_entity_id"])
        )
    hobt_object_names = (
        _resolve_hobt_object_names(connection, query_timeout_seconds, hobt_ids_by_database)
        if hobt_ids_by_database
        else {}
    )
    blocked_object_rows = [
        {
            "session_id": row["session_id"],
            "blocked_object": (
                row["resolved_object_name"]
                if row["resource_type"] == "OBJECT"
                else hobt_object_names.get(
                    (
                        str(row["resource_database_name"]),
                        cast(int, row["resource_associated_entity_id"]),
                    )
                )
            ),
        }
        for row in blocked_lock_rows
    ]

    transaction_id_by_session_id = {
        str(row["session_id"]): str(row["transaction_id"]) for row in transaction_rows
    }
    database_name_by_transaction_id: dict[str, str | None] = {
        str(row["transaction_id"]): (
            str(row["database_name"]) if row["database_name"] is not None else None
        )
        for row in transaction_database_rows
    }
    begin_time_by_transaction_id: dict[str, datetime | None] = {
        str(row["transaction_id"]): (
            _as_utc(row["transaction_begin_time"], server_utc_offset)
            if isinstance(row["transaction_begin_time"], datetime)
            else None
        )
        for row in transaction_rows
    }
    query_samples = _build_query_samples(session_rows, instance_id, collected_at)
    blocking_events = _build_blocking_events(
        session_rows,
        all_session_rows,
        blocked_object_rows,
        transaction_id_by_session_id,
        database_name_by_transaction_id,
        begin_time_by_transaction_id,
        instance_id,
        collected_at,
    )
    metric_samples = _build_metric_samples(
        session_rows, counter_rows, cpu_row, deadlock_counter_row, instance_id, collected_at
    )
    transaction_events = _build_transaction_events(
        transaction_rows,
        transaction_database_rows,
        session_rows,
        all_session_rows,
        blocking_events,
        instance_id,
        collected_at,
        transaction_orphan_threshold_minutes,
        server_utc_offset,
        sleeping_min_duration_ms,
        sleeping_database_overrides_ms,
    )
    open_transaction_identifiers = [str(row["transaction_id"]) for row in transaction_rows]
    return (
        metric_samples,
        query_samples,
        blocking_events,
        transaction_events,
        open_transaction_identifiers,
        product_version,
        version_major,
    )


def _connect_and_get_engine_start_time(
    connection: ConnectionParams, query_timeout_seconds: float
) -> datetime | None:
    """Helper sincrono (mesmo padrao de `_connect_and_collect_missing_indexes`) - quando o
    servico do SQL Server foi iniciado pela ultima vez (`sqlserver_start_time`, sempre 1 linha).
    Usado pra informar desde quando as estatisticas de indice ausente acumulam."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT sqlserver_start_time FROM sys.dm_os_sys_info")
        row = cursor.fetchone()
        # `sqlserver_start_time` vem NAIVE, no horario local do servidor - mesma correcao ja
        # documentada em `_as_utc` (sem isso, o valor exibido fica deslocado pelo fuso do
        # servidor, ex. ~3h adiantado/atrasado em relacao ao instante UTC real).
        cursor.execute("SELECT GETUTCDATE() - GETDATE()")
        offset_row = cursor.fetchone()
        cursor.close()
    finally:
        conn.close()
    if row is None or row[0] is None:
        return None
    server_utc_offset = cast(timedelta, offset_row[0]) if offset_row is not None else timedelta(0)
    return _as_utc(cast(datetime, row[0]), server_utc_offset)


def _connect_and_collect_missing_indexes(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
    max_rows: int,
) -> list[NormalizedMissingIndexSuggestion]:
    """Helper sincrono (mesmo padrao de `_connect_and_collect`): conecta via `pymssql` e roda a
    query de indice ausente, instancia inteira numa unica chamada."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_MISSING_INDEX_QUERY)
        rows = list(cursor.fetchall())[:max_rows]
        cursor.close()
    finally:
        conn.close()

    return _build_missing_index_suggestions(rows, instance_id)


def _build_missing_index_suggestions(
    rows: list[dict], instance_id: UUID
) -> list[NormalizedMissingIndexSuggestion]:
    suggestions: list[NormalizedMissingIndexSuggestion] = []
    for row in rows:
        equality = row["equality_columns"] or "-"
        inequality = row["inequality_columns"] or "-"
        included = row["included_columns"] or "-"
        suggestions.append(
            NormalizedMissingIndexSuggestion(
                instance_id=instance_id,
                database_name=row["database_name"],
                table_name=row["table_name"],
                suggestion=(
                    f"Igualdade: {equality}; Desigualdade: {inequality}; Incluidas: {included}"
                ),
                impact_score=(
                    float(row["impact_score"]) if row["impact_score"] is not None else None
                ),
                detail={
                    "equality_columns": row["equality_columns"],
                    "inequality_columns": row["inequality_columns"],
                    "included_columns": row["included_columns"],
                    "user_seeks": row["user_seeks"],
                    "user_scans": row["user_scans"],
                    "avg_total_user_cost": (
                        float(row["avg_total_user_cost"])
                        if row.get("avg_total_user_cost") is not None
                        else None
                    ),
                    "avg_user_impact": (
                        float(row["avg_user_impact"])
                        if row.get("avg_user_impact") is not None
                        else None
                    ),
                },
            )
        )
    return suggestions


def _connect_and_collect_backup_status(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
    max_rows: int,
) -> list[NormalizedBackupStatus]:
    """Helper sincrono (mesmo padrao de `_connect_and_collect_missing_indexes`): conecta via
    `pymssql` e roda a query de backup, instancia inteira numa unica chamada."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_BACKUP_STATUS_QUERY)
        rows = list(cursor.fetchall())[:max_rows]
        cursor.close()
    finally:
        conn.close()

    return [
        NormalizedBackupStatus(
            instance_id=instance_id,
            database_name=row["database_name"],
            last_backup_at=row["last_backup_at"],
            detail={"backup_type": "D"},
        )
        for row in rows
    ]


def _connect_and_collect_freespace(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
) -> list[NormalizedMetricSample]:
    """Helper sincrono (mesmo padrao de `_connect_and_collect_missing_indexes`) — Fase 2, item 2
    (freespace). `labels={"volume": ...}` identifica o volume dentro da metrica generica
    (`metric_samples` ja tem uma coluna `labels` JSONB, sem migracao nova pra isso)."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_VOLUME_FREESPACE_QUERY)
        rows: list[dict] = list(cursor.fetchall())
        cursor.close()
    finally:
        conn.close()

    collected_at = datetime.now(UTC)
    samples: list[NormalizedMetricSample] = []
    for row in rows:
        total_bytes = row["total_bytes"]
        available_bytes = row["available_bytes"]
        if not total_bytes:  # pragma: no cover - defensivo, nao deveria acontecer
            continue
        free_percent = (available_bytes / total_bytes) * 100
        samples.append(
            NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=SQLSERVER_VOLUME_FREE_PERCENT,
                value=float(free_percent),
                # `volume_mount_point` pode vir NULL de `sys.dm_os_volume_stats` (Microsoft
                # documenta esse caso quando o arquivo nao tem ponto de montagem associado) -
                # `str(None)` viraria a string literal "None", pior que so rotular como
                # desconhecido (confirmado ao vivo contra Autklinux20).
                labels={"volume": row["volume_mount_point"] or "desconhecido"},
            )
        )
    return samples


def _connect_and_collect_replication_lag(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
) -> list[NormalizedMetricSample]:
    """Helper sincrono (mesmo padrao de `_connect_and_collect_missing_indexes`) — Fase 2, item 2
    (replicacao, Always On AG). Sem AG configurado, `_REPLICA_STATE_QUERY` devolve 0 linhas (nao
    e erro) e este helper devolve lista vazia — a metrica simplesmente nunca aparece."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_REPLICA_STATE_QUERY)
        rows: list[dict] = list(cursor.fetchall())
        cursor.close()
    finally:
        conn.close()

    collected_at = datetime.now(UTC)
    samples: list[NormalizedMetricSample] = []
    for row in rows:
        labels = {
            "replica": str(row["replica_server_name"]),
            "database": str(row["database_name"]),
        }
        samples.append(
            NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=SQLSERVER_REPLICA_LOG_SEND_QUEUE_KB,
                value=float(row["log_send_queue_size"]),
                labels=labels,
            )
        )
        samples.append(
            NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=SQLSERVER_REPLICA_REDO_QUEUE_KB,
                value=float(row["redo_queue_size"]),
                labels=labels,
            )
        )
    return samples


def _connect_and_collect_idle_in_transaction(
    connection: ConnectionParams,
    query_timeout_seconds: float,
) -> list[NormalizedIdleInTransactionSession]:
    """Fase 5 T56 — helper sincrono: sessoes idle in transaction no SQL Server."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_IDLE_IN_TRANSACTION_QUERY)
        rows: list[dict] = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    result = []
    for row in rows:
        idle_seconds = row.get("idle_seconds")
        result.append(
            NormalizedIdleInTransactionSession(
                session_id=int(row["session_id"]),
                username=str(row["username"]) or None,
                application_name=str(row["program_name"]) or None,
                client_info=str(row["host_name"]) or None,
                idle_seconds=int(idle_seconds) if idle_seconds is not None else 0,
                open_transaction_count=int(row.get("open_transaction_count") or 0),
                last_query=str(row.get("last_query") or "") or None,
                state=None,
            )
        )
    return result


_SS_CONFIG_COMPLIANCE_QUERY = """
    SELECT
        name,
        CAST(value_in_use AS varchar(100)) AS setting,
        description
    FROM sys.configurations
    WHERE name IN (
        'max degree of parallelism',
        'cost threshold for parallelism',
        'optimize for ad hoc workloads',
        'max server memory (MB)',
        'min server memory (MB)',
        'fill factor (%)',
        'recovery interval (min)'
    )
    ORDER BY name
"""

# Fase 5 T65 — consumidores de conexao SQL Server.
_SS_CONNECTION_CONSUMERS_QUERY = """
    SELECT
        ISNULL(program_name, '')           AS application_name,
        ISNULL(client_net_address, '')     AS client_addr,
        ISNULL(login_name, '')             AS username,
        COUNT(*)                           AS total,
        SUM(CASE WHEN s.status = 'running' THEN 1 ELSE 0 END)  AS active,
        SUM(CASE WHEN s.status = 'sleeping'
                  AND (r.session_id IS NULL OR r.blocking_session_id = 0) THEN 1 ELSE 0 END) AS idle,
        SUM(CASE WHEN s.open_transaction_count > 0
                  AND s.status = 'sleeping' THEN 1 ELSE 0 END) AS idle_in_transaction,
        SUM(CASE WHEN r.blocking_session_id > 0 THEN 1 ELSE 0 END) AS waiting
    FROM sys.dm_exec_sessions s
    LEFT JOIN sys.dm_exec_requests r ON r.session_id = s.session_id
    LEFT JOIN sys.dm_exec_connections c ON c.session_id = s.session_id
    WHERE s.is_user_process = 1
    GROUP BY s.program_name, c.client_net_address, s.login_name
    ORDER BY total DESC, idle_in_transaction DESC
"""


def _evaluate_ss_config_param(name: str, setting: str, description: str) -> NormalizedConfigComplianceItem:
    """Avalia um parametro do sys.configurations e retorna item de compliance SQL Server."""
    try:
        v = float(setting)
    except (ValueError, TypeError):
        v = None

    rules: dict[str, tuple[str | None, str | None, str]] = {}

    if name == "max degree of parallelism":
        status = "error" if v == 0 else ("warn" if v and v > 16 else "ok")
        rules[name] = ("1 – num_vcpus/2", "0 permite paralelismo irrestrito — pode saturar CPUs em queries longas e prejudicar workloads OLTP concorrentes.", status)
    elif name == "cost threshold for parallelism":
        status = "warn" if v is not None and v < 25 else "ok"
        rules[name] = ("25 – 50", "O padrao (5) faz o planner paralelizar queries simples desnecessariamente; 25-50 e mais adequado para OLTP.", status)
    elif name == "optimize for ad hoc workloads":
        status = "error" if v == 0 else "ok"
        rules[name] = ("1 (habilitado)", "Desabilitado (0) faz o plan cache guardar planos completos de single-use queries, inflando a memoria.", status)
    elif name == "max server memory (MB)":
        status = "warn" if v is not None and v >= 2147483647 else "ok"
        rules[name] = ("RAM total - 4 GB (mínimo)", "Sem limite (2147483647) — o SQL Server pode consumir toda a RAM e causar paginacao do OS.", status)
    elif name == "fill factor (%)":
        status = "warn" if v == 0 or v == 100 else "ok"
        rules[name] = ("70 – 90", "0 ou 100 = paginas de indice 100% cheias; inserts futuras causam page splits e fragmentacao.", status)
    elif name == "recovery interval (min)":
        status = "warn" if v is not None and v == 0 else "ok"
        rules[name] = ("0 (auto)", "0 e o padrao automatico e e correto; valores altos aumentam tempo de recovery apos crash.", status)
    else:
        status = "ok"

    rec_val, recommendation, status_val = rules.get(name, (None, None, "ok"))

    return NormalizedConfigComplianceItem(
        param_name=name,
        current_value=setting,
        recommended_value=rec_val,
        unit=None,
        status=status_val,
        description=description,
        recommendation=recommendation,
    )


def _connect_and_collect_config_compliance(
    connection: ConnectionParams,
    query_timeout_seconds: float,
) -> list[NormalizedConfigComplianceItem]:
    """Fase 5 T64 — helper sincrono: compliance de configuracao SQL Server."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_SS_CONFIG_COMPLIANCE_QUERY)
        rows: list[dict] = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    return [_evaluate_ss_config_param(r["name"], str(r["setting"]), str(r["description"] or "")) for r in rows]


def _connect_and_collect_connection_consumers(
    connection: ConnectionParams,
    query_timeout_seconds: float,
) -> list[NormalizedConnectionConsumer]:
    """Fase 5 T65 — helper sincrono: consumidores de conexao SQL Server."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_SS_CONNECTION_CONSUMERS_QUERY)
        rows: list[dict] = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    return [
        NormalizedConnectionConsumer(
            application_name=str(r["application_name"]),
            client_addr=str(r["client_addr"]) if r["client_addr"] else None,
            username=str(r["username"]),
            total=int(r["total"]),
            active=int(r["active"]),
            idle=int(r["idle"]),
            idle_in_transaction=int(r["idle_in_transaction"]),
            waiting=int(r["waiting"]),
        )
        for r in rows
    ]


_SS_TEMPDB_FILE_QUERY = """
SELECT
    COUNT(*) AS file_count,
    CAST(SUM(CAST(size AS BIGINT) * 8) AS FLOAT) / 1024.0 AS total_size_mb
FROM tempdb.sys.database_files
WHERE type = 0
"""

_SS_TEMPDB_SPACE_QUERY = """
SELECT
    SUM(CAST(internal_object_reserved_page_count AS BIGINT)) * 8.0 / 1024.0 AS internal_objects_mb,
    SUM(CAST(user_object_reserved_page_count AS BIGINT)) * 8.0 / 1024.0 AS user_objects_mb,
    SUM(CAST(version_store_reserved_page_count AS BIGINT)) * 8.0 / 1024.0 AS version_store_mb,
    SUM(CAST(unallocated_extent_page_count AS BIGINT)) * 8.0 / 1024.0 AS free_space_mb
FROM tempdb.sys.dm_db_file_space_usage
"""

_SS_TEMPDB_SESSIONS_QUERY = """
SELECT COUNT(DISTINCT session_id) AS session_count
FROM tempdb.sys.dm_db_task_space_usage
WHERE internal_objects_alloc_page_count > 0
   OR user_objects_alloc_page_count > 0
"""

_SS_TEMPDB_PAGELATCH_QUERY = """
SELECT ISNULL(SUM(wait_time_ms), 0) AS pagelatch_wait_ms
FROM sys.dm_os_wait_stats
WHERE wait_type LIKE 'PAGELATCH_%'
"""

_SS_DB_SIZES_QUERY = """
SELECT
    name AS database_name,
    SUM(CAST(size AS BIGINT)) * 8 * 1024 AS size_bytes
FROM sys.master_files
WHERE type = 0
GROUP BY name
ORDER BY size_bytes DESC
"""


def _connect_and_collect_tempdb_stats(
    connection: ConnectionParams,
    query_timeout_seconds: float,
) -> NormalizedTempDbStats:
    """Fase 5 T61 — helper sincrono: snapshot de uso do TempDB."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)

        cursor.execute(_SS_TEMPDB_FILE_QUERY)
        file_row = cursor.fetchone() or {}

        cursor.execute(_SS_TEMPDB_SPACE_QUERY)
        space_row = cursor.fetchone() or {}

        cursor.execute(_SS_TEMPDB_SESSIONS_QUERY)
        session_row = cursor.fetchone() or {}

        cursor.execute(_SS_TEMPDB_PAGELATCH_QUERY)
        pagelatch_row = cursor.fetchone() or {}

        cursor.close()
    finally:
        conn.close()

    return NormalizedTempDbStats(
        file_count=int(file_row.get("file_count") or 0),
        total_size_mb=float(file_row.get("total_size_mb") or 0.0),
        internal_objects_mb=float(space_row.get("internal_objects_mb") or 0.0),
        user_objects_mb=float(space_row.get("user_objects_mb") or 0.0),
        version_store_mb=float(space_row.get("version_store_mb") or 0.0),
        free_space_mb=float(space_row.get("free_space_mb") or 0.0),
        session_count=int(session_row.get("session_count") or 0),
        pagelatch_wait_ms=int(pagelatch_row.get("pagelatch_wait_ms") or 0),
    )


def _connect_and_collect_db_sizes_ss(
    connection: ConnectionParams,
    query_timeout_seconds: float,
) -> list[NormalizedDbSize]:
    """Fase 5 T63 — helper sincrono: tamanho de cada banco SQL Server."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_SS_DB_SIZES_QUERY)
        rows: list[dict] = cursor.fetchall()
        cursor.close()
    finally:
        conn.close()

    return [
        NormalizedDbSize(
            database_name=str(r["database_name"]),
            size_bytes=int(r["size_bytes"]),
        )
        for r in rows
    ]


def _connect_and_run_scalar_query(
    connection: ConnectionParams,
    sql: str,
    query_timeout_seconds: float,
) -> float | None:
    """Helper sincrono pro alerta `custom_sql` (Fase 2, item 2) — roda uma query JA VALIDADA
    (ver `app.services.custom_sql_guard`, chamado antes desta funcao, nunca depois) e espera a
    PRIMEIRA coluna da PRIMEIRA linha como um numero. Qualquer outro formato (0 linhas, valor nao
    numerico) devolve `None` — o worker trata como "sem avaliar este ciclo", nao inventa valor."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(sql)
        row: tuple | None = cursor.fetchone()
        cursor.close()
    finally:
        conn.close()

    if row is None or row[0] is None:
        return None
    try:
        return float(row[0])
    except (TypeError, ValueError):
        return None


def _connect_and_collect_deadlocks(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
) -> list[NormalizedDeadlockEvent]:
    """Helper sincrono (mesmo padrao de `_connect_and_collect_missing_indexes`): le AMBOS os
    targets da sessao `system_health` (`event_file` em disco e `ring_buffer` em memoria) e
    mescla os eventos `xml_deadlock_report` de cada um antes de normalizar — nenhum dos dois se
    mostrou consistentemente mais rapido ao vivo (ver docstring de `_DEADLOCK_RING_BUFFER_QUERY`),
    entao consultar os dois maximiza a chance de capturar o evento assim que QUALQUER um dos dois
    o tiver. Duplicatas entre os dois targets (o mesmo deadlock aparecendo em ambos) sao
    resolvidas depois, na reconciliacao de `DeadlockEventsRepository.insert_new`."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_DEADLOCK_FILE_TARGET_QUERY)
        rows = list(cursor.fetchall())
        cursor.execute(_DEADLOCK_RING_BUFFER_QUERY)
        ring_buffer_rows = list(cursor.fetchall())
        cursor.close()
    finally:
        conn.close()

    rows.extend(_extract_ring_buffer_event_rows(ring_buffer_rows, instance_id))
    return _build_deadlock_events(rows, instance_id)


def _extract_ring_buffer_event_rows(
    ring_buffer_rows: list[dict], instance_id: UUID
) -> list[dict[str, Any]]:
    """O target `ring_buffer` devolve 1 linha com um XML-envelope (`target_data`) contendo VARIOS
    `<event>` acumulados recentemente — diferente do target `event_file`, que ja filtra/devolve 1
    linha por evento. Isola cada `<event name="xml_deadlock_report">` do envelope e devolve no
    MESMO formato de linha que `_build_deadlock_events` ja espera (`event_data` = XML do `<event>`
    isolado, `timestamp_utc` ausente — `_build_deadlock_events` cai pro atributo `timestamp` do
    proprio `<event>` nesse caso)."""
    event_rows: list[dict[str, Any]] = []
    for row in ring_buffer_rows:
        target_data = row.get("target_data")
        if not target_data:
            continue
        try:
            root = ElementTree.fromstring(target_data)
        except ElementTree.ParseError:
            logger.warning(
                "Ring buffer system_health com XML invalido para instance_id=%s - ignorado",
                instance_id,
            )
            continue
        for event_el in root.findall(".//event[@name='xml_deadlock_report']"):
            event_rows.append({"event_data": ElementTree.tostring(event_el, encoding="unicode")})
    return event_rows


def _build_deadlock_events(rows: list[dict], instance_id: UUID) -> list[NormalizedDeadlockEvent]:
    """Faz o parse do XML de cada linha (uma linha por evento `xml_deadlock_report` do target
    `event_file`, ja filtrado na query — `event_data` e o `<event>` inteiro daquele evento).

    O `<deadlock>` de cada evento tem `<victim-list>` (id do processo vitima, escolhido pelo
    motor pra rollback), `<process-list>` (TODOS os processos envolvidos — sessao, banco,
    transacao, comando, pilha de execucao) e `<resource-list>` (recursos em disputa, cada um com
    owner-list/waiter-list referenciando process ids). A pedido explicito do usuario, o grafo
    COMPLETO (nao so vitima+1 sobrevivente) vai pro campo `detail` de cada evento — os campos
    `victim_*`/`survivor_*`/`resource_description` continuam existindo como resumo de leitura
    rapida (melhor esforco: sobrevivente = o primeiro processo que nao e vitima; um deadlock de
    3+ processos tem mais de um sobrevivente de verdade, todos aparecem em `detail.processes`).
    """
    events: list[NormalizedDeadlockEvent] = []
    for row in rows:
        event_data = row.get("event_data")
        if not event_data:
            continue
        try:
            event_el = ElementTree.fromstring(event_data)
        except ElementTree.ParseError:
            logger.warning(
                "Evento xml_deadlock_report com XML invalido para instance_id=%s - ignorado",
                instance_id,
            )
            continue

        deadlock_el = event_el.find("./data[@name='xml_report']/value/deadlock")
        if deadlock_el is not None:
            timestamp_value = row.get("timestamp_utc")
            time_value = (
                timestamp_value
                if isinstance(timestamp_value, datetime)
                else _parse_xe_timestamp(event_el.get("timestamp"))
            )

            process_elements = deadlock_el.findall("./process-list/process")
            victim_ids = {
                v.get("id")
                for v in deadlock_el.findall("./victim-list/victimProcess")
                if v.get("id")
            }
            # Mapa process id ("process1") -> session_id ("62") - resource-list referencia
            # owner/waiter por process id, nao por spid diretamente.
            session_id_by_process_id = {
                p.get("id"): p.get("spid") for p in process_elements if p.get("id")
            }
            processes = [
                _parse_deadlock_process(p, p.get("id") in victim_ids) for p in process_elements
            ]
            resources = [
                _parse_deadlock_resource(r, session_id_by_process_id)
                for r in deadlock_el.findall("./resource-list/*")
            ]

            victim_process = next((p for p in processes if p["is_victim"]), None)
            survivor_process = next((p for p in processes if not p["is_victim"]), None)
            # Banco de dados do evento (melhor esforco): o da vitima, senao o do primeiro
            # processo com currentdbname disponivel.
            database_name = (victim_process or {}).get("database_name") or next(
                (p["database_name"] for p in processes if p["database_name"]), None
            )
            resource_description = None
            if resources:
                first_resource = resources[0]
                resource_description = (
                    f"{first_resource['type']} ({first_resource['object_name']})"
                    if first_resource["object_name"]
                    else first_resource["type"]
                )

            raw_xml = ElementTree.tostring(deadlock_el, encoding="unicode")

            events.append(
                NormalizedDeadlockEvent(
                    instance_id=instance_id,
                    time=time_value,
                    database_name=database_name,
                    victim_session_id=(victim_process or {}).get("session_id"),
                    victim_sql_text_masked=(victim_process or {}).get("sql_text_masked"),
                    survivor_session_id=(survivor_process or {}).get("session_id"),
                    survivor_sql_text_masked=(survivor_process or {}).get("sql_text_masked"),
                    resource_description=resource_description,
                    raw_xml_masked=mask_sql_literals(raw_xml),
                    detail={"processes": processes, "resources": resources},
                )
            )
    return events


def _parse_xe_timestamp(timestamp_raw: str | None) -> datetime:
    """`event/@timestamp` do Extended Events vem em ISO8601 UTC (ex.
    `"2026-09-15T14:23:01.123Z"`) — `datetime.fromisoformat` (Python 3.11+) ja aceita o sufixo
    `Z` diretamente. `now(UTC)` como fallback so no caso (nunca visto na pratica) do atributo
    estar ausente ou mal formado."""
    if timestamp_raw is None:
        return datetime.now(UTC)
    try:
        return datetime.fromisoformat(timestamp_raw)
    except ValueError:
        return datetime.now(UTC)


def _connect_and_ensure_transaction_error_session(
    connection: ConnectionParams, query_timeout_seconds: float
) -> bool:
    """Cria/inicia (idempotente) a sessao Extended Events dedicada `_TRANSACTION_ERROR_SESSION_
    NAME` - helper sincrono (mesmo padrao de `_connect_and_collect_deadlocks`), chamado a cada
    ciclo do `TransactionErrorSupervisor` antes de ler. Devolve `False` (nunca levanta excecao)
    quando a criacao/inicio falha - permissao ausente (`ALTER ANY EVENT SESSION`) e o caso mais
    comum, mas qualquer erro aqui so desativa esta feature para esta instancia neste ciclo, nunca
    derruba o supervisor."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor()
        cursor.execute(_TRANSACTION_ERROR_SESSION_EXISTS_QUERY)
        if cursor.fetchone() is None:
            cursor.execute(_CREATE_TRANSACTION_ERROR_SESSION_SQL)
            conn.commit()
        cursor.execute(_TRANSACTION_ERROR_SESSION_RUNNING_QUERY)
        if cursor.fetchone() is None:
            cursor.execute(_START_TRANSACTION_ERROR_SESSION_SQL)
            conn.commit()
        cursor.close()
        return True
    except Exception:
        logger.exception(
            "Falha ao criar/iniciar a sessao Extended Events %s - "
            "captura confiavel de erro por transacao desativada neste ciclo "
            "(permissao ALTER ANY EVENT SESSION ausente e a causa mais comum)",
            _TRANSACTION_ERROR_SESSION_NAME,
        )
        return False
    finally:
        conn.close()


def _connect_and_collect_transaction_errors(
    connection: ConnectionParams, instance_id: UUID, query_timeout_seconds: float
) -> list[NormalizedConfirmedTransactionError]:
    """Helper sincrono (mesmo padrao de `_connect_and_collect_deadlocks`): le o target `event_file`
    da sessao dedicada e extrai os eventos `error_reported` (severidade >= 16, ja filtrado na
    propria sessao) da ultima hora."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_TRANSACTION_ERROR_FILE_TARGET_QUERY)
        rows = list(cursor.fetchall())
        cursor.close()
    finally:
        conn.close()

    return _build_confirmed_transaction_errors(rows, instance_id)


def _xe_event_field(event_el: ElementTree.Element, name: str) -> str | None:
    """Le `<data name="X"><value>...</value></data>` ou `<action name="X"><value>...</value>
    </action>` — mesma estrutura de `value` filho nos dois casos."""
    field_el = event_el.find(f"./data[@name='{name}']/value")
    if field_el is None:
        field_el = event_el.find(f"./action[@name='{name}']/value")
    return field_el.text if field_el is not None else None


def _build_confirmed_transaction_errors(
    rows: list[dict], instance_id: UUID
) -> list[NormalizedConfirmedTransactionError]:
    """Faz o parse do XML de cada linha (uma linha por evento `error_reported`, ja filtrado na
    query) - `error_number`/`severity` sao numericos, `message` e mascarado por completo
    (`mask_sql_literals`, blanket - texto de erro nao tem contexto de coluna pra mascaramento
    seletivo) antes de sair do coletor."""
    errors: list[NormalizedConfirmedTransactionError] = []
    for row in rows:
        event_data = row.get("event_data")
        if not event_data:
            continue
        try:
            event_el = ElementTree.fromstring(event_data)
        except ElementTree.ParseError:
            logger.warning(
                "Evento error_reported com XML invalido para instance_id=%s - ignorado",
                instance_id,
            )
            continue

        session_id = _xe_event_field(event_el, "session_id")
        error_number = _xe_event_field(event_el, "error_number")
        severity = _xe_event_field(event_el, "severity")
        message = _xe_event_field(event_el, "message")
        if session_id is None or error_number is None or severity is None or message is None:
            continue

        timestamp_value = row.get("timestamp_utc")
        time_value = (
            timestamp_value
            if isinstance(timestamp_value, datetime)
            else _parse_xe_timestamp(event_el.get("timestamp"))
        )
        errors.append(
            NormalizedConfirmedTransactionError(
                instance_id=instance_id,
                session_id=session_id,
                error_number=int(error_number),
                severity=int(severity),
                message_masked=mask_sql_literals(message),
                time=time_value,
            )
        )
    return errors


def _parse_deadlock_process(process: ElementTree.Element, is_victim: bool) -> dict[str, Any]:
    """Extrai um `<process>` do grafo de deadlock — atributos best-effort (a presenca exata
    varia por versao do SQL Server, `.get()` devolve `None` quando ausente, nunca inventa dado).
    `procedures`: cadeia de `procname` de `<executionStack>/<frame>` (ordem de chamada, sem
    duplicatas), quando o comando rodou dentro de uma procedure/function."""
    inputbuf = process.find("./inputbuf")
    sql_text = inputbuf.text if inputbuf is not None else None
    procedures: list[str] = []
    for frame in process.findall("./executionStack/frame"):
        procname = frame.get("procname")
        if procname and procname not in procedures:
            procedures.append(procname)
    session_id = process.get("spid")
    return {
        "id": process.get("id"),
        "session_id": str(session_id) if session_id is not None else None,
        "database_name": process.get("currentdbname"),
        "transaction_id": process.get("xactid") or process.get("ownerId"),
        "transaction_begin_time": process.get("lasttranstarted"),
        "login_name": process.get("loginname"),
        "host_name": process.get("hostname"),
        "program_name": process.get("clientapp"),
        "isolation_level": process.get("isolationlevel"),
        "is_victim": is_victim,
        # Texto de comando exibido na tela de Deadlocks — mascaramento SELETIVO (só PII), nao o
        # blanket usado pra `sql_hash` (aqui nao ha hash nenhum computado, entao nao ha
        # ambiguidade de qual funcao usar).
        "sql_text_masked": mask_sql_literals_for_display(sql_text) if sql_text else None,
        "procedures": procedures,
    }


def _parse_deadlock_resource(
    resource: ElementTree.Element, session_id_by_process_id: dict[str | None, str | None]
) -> dict[str, Any]:
    """Extrai um recurso do `<resource-list>` (`keylock`/`objectlock`/`pagelock`/etc — o proprio
    nome da tag XML e o tipo). `owner-list`/`waiter-list` referenciam process id ("process1"),
    resolvidos pra session_id via `session_id_by_process_id` (mesmo mapa montado a partir de
    `<process-list>` no mesmo evento)."""

    def _session_ids(container_tag: str, item_tag: str) -> list[str]:
        ids = []
        for item in resource.findall(f"./{container_tag}/{item_tag}"):
            process_id = item.get("id")
            session_id = session_id_by_process_id.get(process_id)
            if session_id is not None:
                ids.append(str(session_id))
        return ids

    return {
        "type": resource.tag,
        "object_name": resource.get("objectname"),
        "index_name": resource.get("indexname"),
        "mode": resource.get("mode"),
        "owner_session_ids": _session_ids("owner-list", "owner"),
        "waiter_session_ids": _session_ids("waiter-list", "waiter"),
    }


def _connect_and_fetch_actual_execution_plan(
    connection: ConnectionParams,
    sql_hash: str,
    query_timeout_seconds: float,
) -> str | None:
    """Helper sincrono (mesmo padrao dos demais `_connect_and_*`): procura uma sessao AINDA EM
    EXECUCAO cujo statement isolado+hasheado bate com `sql_hash`; no primeiro match, busca o
    plano REAL (com contagens reais) via `sys.dm_exec_query_statistics_xml(session_id)`. `None`
    quando nenhuma sessao ativa corresponde, OU quando a funcao nao existe/falha nesta versao/
    permissao do SQL Server (2016 SP1+) — em qualquer um dos dois casos, o chamador
    (`fetch_execution_plan`, service) cai pro plano cacheado/estimado, nunca propaga o erro."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_ACTIVE_STATEMENT_SEARCH_QUERY)
        rows: list[dict] = list(cursor.fetchall())
        cursor.close()

        session_id = None
        for row in rows:
            if _hash_query_text(row["statement_text"]) == sql_hash:
                session_id = row["session_id"]
                break
        if session_id is None:
            return None

        try:
            cursor = conn.cursor(as_dict=True)
            cursor.execute(_ACTUAL_PLAN_XML_QUERY, (session_id,))
            plan_row: dict | None = cursor.fetchone()
            cursor.close()
        except Exception:  # noqa: BLE001 - degrada pro plano cacheado, nunca propaga
            return None
        if plan_row is None or not plan_row.get("query_plan"):
            return None
        return str(plan_row["query_plan"])
    finally:
        conn.close()


def _connect_and_fetch_execution_plan(
    connection: ConnectionParams,
    sql_hash: str,
    query_timeout_seconds: float,
) -> str | None:
    """Helper sincrono (mesmo padrao de `_connect_and_collect_missing_indexes`): conecta,
    varre ate 500 planos em cache (`_EXECUTION_PLAN_SEARCH_QUERY`) procurando um `statement_text`
    cujo hash (mesma `_hash_query_text` usada na coleta normal) bate com `sql_hash`; no primeiro
    match, busca o XML do plano via `sys.dm_exec_query_plan(plan_handle)`. `None` quando nenhum
    plano em cache corresponde."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_EXECUTION_PLAN_SEARCH_QUERY)
        rows: list[dict] = list(cursor.fetchall())
        cursor.close()

        plan_handle = None
        for row in rows:
            if _hash_query_text(row["statement_text"]) == sql_hash:
                plan_handle = row["plan_handle"]
                break
        if plan_handle is None:
            return None

        cursor = conn.cursor(as_dict=True)
        cursor.execute(_EXECUTION_PLAN_XML_QUERY, (plan_handle,))
        plan_row: dict | None = cursor.fetchone()
        cursor.close()
        return str(plan_row["query_plan"]) if plan_row is not None else None
    finally:
        conn.close()


def _connect_and_fetch_plan_from_query_store(
    connection: ConnectionParams,
    sql_hash: str,
    database_names: list[str],
    query_timeout_seconds: float,
) -> str | None:
    """Helper sincrono — Item 10 (Query Store). Itera pelos `database_names` (bancos onde o
    `sql_hash` foi visto em `query_samples`) procurando o plano no Query Store de cada banco via
    3-part naming. Ignora silenciosamente bancos sem Query Store habilitado (excecao no cursor).
    Devolve o XML do plano mais recente encontrado, ou `None` se nao achou em nenhum banco."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        for db_name in database_names:
            # Remove `]` para nao escapar o colchete de fechamento no 3-part naming — unico
            # caractere que quebraria `[{db}]`. `db_name` vem da nossa tabela `query_samples`,
            # nao de input arbitrario do usuario.
            safe_db = db_name.replace("]", "")
            try:
                cursor = conn.cursor(as_dict=True)
                cursor.execute(_QUERY_STORE_TEXT_QUERY_TMPL.format(db=safe_db))
                rows: list[dict] = list(cursor.fetchall())
                cursor.close()
            except Exception:  # noqa: BLE001 - QS desabilitado ou banco sem acesso: pula
                continue

            query_text_id: int | None = None
            for row in rows:
                if _hash_query_text(row.get("statement_text")) == sql_hash:
                    query_text_id = int(row["query_text_id"])
                    break
            if query_text_id is None:
                continue

            try:
                cursor = conn.cursor(as_dict=True)
                cursor.execute(
                    f"SELECT TOP (1) CAST(qsp.query_plan AS nvarchar(max)) AS query_plan "
                    f"FROM [{safe_db}].sys.query_store_plan AS qsp "
                    f"JOIN [{safe_db}].sys.query_store_query AS qsq ON qsq.query_id = qsp.query_id "
                    f"WHERE qsq.query_text_id = %s "
                    f"ORDER BY qsp.last_execution_time DESC",
                    (query_text_id,),
                )
                plan_row: dict | None = cursor.fetchone()
                cursor.close()
            except Exception:  # noqa: BLE001
                continue
            if plan_row and plan_row.get("query_plan"):
                return str(plan_row["query_plan"])
        return None
    finally:
        conn.close()


def _hash_query_text(query_text: str | None) -> str | None:
    if not query_text:
        return None
    return hashlib.sha256(mask_sql_literals(query_text).encode("utf-8")).hexdigest()


def _plan_xml_compile_timeout_reason(plan_xml: str) -> str | None:
    """Procura `StatementOptmEarlyAbortReason` em qualquer `StmtSimple` do plano — devolve o
    motivo (`"TimeOutReached"`/`"MemoryLimitExceeded"`) do primeiro achado, ou `None`. Ignora
    namespace do ShowPlanXML (`{http://schemas.microsoft.com/...}StmtSimple`) comparando so o
    nome local da tag - mais simples que registrar o namespace pra uma checagem pontual."""
    try:
        root = ElementTree.fromstring(plan_xml)
    except ElementTree.ParseError:
        return None
    for element in root.iter():
        local_tag = element.tag.rsplit("}", maxsplit=1)[-1]
        if local_tag != "StmtSimple":
            continue
        reason = element.get("StatementOptmEarlyAbortReason")
        if reason in _PLAN_COMPILE_TIMEOUT_REASONS:
            return reason
    return None


def _connect_and_scan_plan_compile_timeouts(
    connection: ConnectionParams,
    query_timeout_seconds: float,
) -> list[NormalizedPlanCompileTimeoutFinding]:
    """Helper sincrono (mesmo padrao de `_connect_and_fetch_execution_plan`) — Fase 2, item 5:
    varre o cache de planos e devolve um achado por `sql_hash` distinto cujo plano sinaliza
    timeout/limite de memoria de compilacao."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_PLAN_COMPILE_TIMEOUT_SCAN_QUERY)
        rows: list[dict] = list(cursor.fetchall())
        cursor.close()
    finally:
        conn.close()

    findings: dict[str, NormalizedPlanCompileTimeoutFinding] = {}
    for row in rows:
        statement_text = row["statement_text"]
        plan_xml = row["query_plan"]
        if not statement_text or not plan_xml:
            continue
        reason = _plan_xml_compile_timeout_reason(str(plan_xml))
        if reason is None:
            continue
        # `sql_hash` precisa vir do mascaramento BLANKET (identidade estavel, igual ao resto do
        # projeto) — o texto exibido (`statement_text_masked`) usa o mascaramento seletivo (so
        # PII), calculado separadamente a partir do MESMO `statement_text` original.
        sql_hash = hashlib.sha256(mask_sql_literals(statement_text).encode("utf-8")).hexdigest()
        findings.setdefault(
            sql_hash,
            NormalizedPlanCompileTimeoutFinding(
                sql_hash=sql_hash,
                reason=reason,
                statement_text_masked=mask_sql_literals_for_display(statement_text),
            ),
        )
    return list(findings.values())


def _connect_and_scan_plan_history(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
) -> list[NormalizedPlanSnapshot]:
    """Helper sincrono (mesmo padrao de `_connect_and_scan_plan_compile_timeouts`) — devolve o
    snapshot atual de TODA query com plano em cache, uma por `sql_hash` distinto. A deduplicacao
    por mudanca de plano (so persistir quando o plano muda de fato) fica no repositorio."""
    login_timeout_seconds = int(connection.connect_timeout_seconds)
    conn = pymssql.connect(
        server=connection.host,
        port=str(connection.port),
        user=connection.username,
        password=connection.secret_value,
        timeout=int(query_timeout_seconds),
        login_timeout=login_timeout_seconds,
    )
    try:
        cursor = conn.cursor(as_dict=True)
        cursor.execute(_PLAN_HISTORY_SCAN_QUERY)
        rows: list[dict] = list(cursor.fetchall())
        cursor.close()
    finally:
        conn.close()

    snapshots: dict[str, NormalizedPlanSnapshot] = {}
    for row in rows:
        statement_text = row["statement_text"]
        plan_xml = row["query_plan"]
        if not statement_text or not plan_xml:
            continue
        sql_hash = hashlib.sha256(mask_sql_literals(statement_text).encode("utf-8")).hexdigest()
        if sql_hash in snapshots:
            continue
        plan_xml_masked = mask_showplan_xml_literals(str(plan_xml))
        plan_hash = hashlib.sha256(plan_xml_masked.encode("utf-8")).hexdigest()
        snapshots[sql_hash] = NormalizedPlanSnapshot(
            instance_id=instance_id,
            sql_hash=sql_hash,
            plan_hash=plan_hash,
            plan_xml_masked=plan_xml_masked,
            last_execution_time=row["last_execution_time"],
        )
    return list(snapshots.values())


def _build_query_samples(
    session_rows: list[dict], instance_id: UUID, collected_at: datetime
) -> list[NormalizedQuerySample]:
    samples: list[NormalizedQuerySample] = []
    for row in session_rows:
        # `query_text` (fatiado por offset) pode sair vazio em cenarios raros (ex.:
        # statement_start_offset/end_offset zerados por um plano em cache atipico) mesmo com
        # full_query_text preenchido — cai pro texto completo em vez de descartar a amostra.
        query_text = row["query_text"] or row["full_query_text"]
        if not query_text:
            continue
        # `sql_hash` precisa continuar vindo do mascaramento BLANKET (`mask_sql_literals`) —
        # identidade estavel usada pra agrupar execucoes da mesma query (Top SQL/wait time), que
        # fragmentaria se dependesse do mascaramento seletivo abaixo. Os campos exibidos
        # (`sql_text_masked`/`sql_text_full_masked`) usam o mascaramento seletivo (so PII),
        # calculado a partir dos MESMOS textos originais, separadamente do hash.
        sql_hash = hashlib.sha256(mask_sql_literals(query_text).encode("utf-8")).hexdigest()
        full_query_text = row["full_query_text"]
        masked_display = mask_sql_literals_for_display(query_text)
        masked_full_display = (
            mask_sql_literals_for_display(full_query_text) if full_query_text else masked_display
        )
        samples.append(
            NormalizedQuerySample(
                time=collected_at,
                instance_id=instance_id,
                engine="sqlserver",
                sql_hash=sql_hash,
                sql_text_masked=masked_display,
                sql_text_full_masked=masked_full_display,
                wait_type=map_wait_event("sqlserver", row["wait_type"]),
                wait_event_native=row["wait_type"],
                wait_time_ms=row["wait_time"],
                cpu_time_ms=row["cpu_time_ms"],
                duration_ms=row["total_elapsed_time"],
                session_id=str(row["session_id"]),
                db_user=row["login_name"],
                program=row["program_name"],
                database_name=row["database_name"],
                machine=row["host_name"],
            )
        )
    return samples


def _build_blocking_events(
    session_rows: list[dict],
    all_session_rows: list[dict],
    blocked_object_rows: list[dict],
    transaction_id_by_session_id: dict[str, str],
    database_name_by_transaction_id: dict[str, str | None],
    begin_time_by_transaction_id: dict[str, datetime | None],
    instance_id: UUID,
    collected_at: datetime,
) -> list[NormalizedBlockingEvent]:
    """Nivel direto apenas (`blocking_session_id`) — a arvore completa (Etapa 9) e reconstruida
    no app encadeando as arestas de um mesmo snapshot (`app.api.v1.waits_blocking`). Sentinela
    negativo (-2 a -5) = bloqueador nao determinavel, tratado como "sem evento" — NUNCA como
    erro.

    Identidade (usuario/programa/maquina) vem PRIMEIRO de `session_rows` (que ja carrega
    login_name/host_name/program_name via join com `sys.dm_exec_sessions` para sessoes com
    request ativa) e cai pra `all_session_rows` (identidade de QUALQUER sessao conectada) quando
    a sessao nao tem request ativa — o caso do bloqueador ocioso, que de outra forma ficaria sem
    nenhuma identidade. `blocked_object`/`blocked_wait_resource` sao melhor esforco (ver
    `_BLOCKED_LOCKS_QUERY`/`_resolve_hobt_object_names`/docstring de `NormalizedBlockingEvent`) —
    so preenchidos pro lado
    BLOQUEADO (e quem esta de fato esperando por um recurso).

    Banco de dados (`blocker_database_name`/`blocked_database_name`, a pedido explicito do
    usuario): `sys.dm_exec_sessions` (`all_session_rows`) nao expoe um "banco atual" fora de uma
    request ativa, entao um bloqueador OCIOSO cai pro banco resolvido via sua transacao aberta
    (`database_name_by_transaction_id`, mesmo lookup ja usado por `_build_transaction_events`) —
    `None` so no caso raro de bloqueador ocioso sem nenhuma transacao rastreavel.
    """
    sessions_by_id = {row["session_id"]: row for row in session_rows}
    identity_by_id = {row["session_id"]: row for row in all_session_rows}
    blocked_object_by_id = {row["session_id"]: row for row in blocked_object_rows}

    def _identity(session_id: int) -> tuple[str | None, str | None, str | None]:
        active_row = sessions_by_id.get(session_id)
        if active_row is not None:
            return active_row["login_name"], active_row["program_name"], active_row["host_name"]
        idle_row = identity_by_id.get(session_id)
        if idle_row is not None:
            return idle_row["login_name"], idle_row["program_name"], idle_row["host_name"]
        return None, None, None

    def _database_name(session_id: int) -> str | None:
        active_row = sessions_by_id.get(session_id)
        if active_row is not None and active_row["database_name"] is not None:
            return active_row["database_name"]
        transaction_id = transaction_id_by_session_id.get(str(session_id))
        if transaction_id is not None:
            return database_name_by_transaction_id.get(transaction_id)
        return None

    def _transaction_begin_time(session_id: int) -> datetime | None:
        transaction_id = transaction_id_by_session_id.get(str(session_id))
        if transaction_id is None:
            return None
        return begin_time_by_transaction_id.get(transaction_id)

    events: list[NormalizedBlockingEvent] = []
    for row in session_rows:
        blocking_session_id = row["blocking_session_id"]
        if blocking_session_id is None or blocking_session_id < _BLOCKING_SESSION_REAL_BLOCK_MIN:
            continue
        blocker_row = sessions_by_id.get(blocking_session_id)
        # Mesmo fallback de `_build_query_samples`: `query_text` (fatiado por offset) pode sair
        # vazio num ciclo especifico (ex.: statement_start_offset/end_offset zerados por um plano
        # em cache atipico) mesmo com `full_query_text` preenchido — bug real relatado pelo
        # usuario ao vivo (transacao/incidente gravados, mas `blocker_sql_hash=None`, mesmo com a
        # MESMA sessao tendo uma amostra valida em `query_samples` no mesmo instante, porque ali o
        # fallback ja existia e aqui nao). Sem isso, o "Comando" da tela de Transacoes/detalhe de
        # sessao nunca resolve pro lado bloqueador nesses ciclos.
        blocker_sql_hash = (
            _hash_query_text(blocker_row["query_text"] or blocker_row["full_query_text"])
            if blocker_row is not None
            else None
        )
        blocker_login, blocker_program, blocker_machine = _identity(blocking_session_id)
        blocked_login, blocked_program, blocked_machine = _identity(row["session_id"])
        blocked_object_row = blocked_object_by_id.get(row["session_id"])
        events.append(
            NormalizedBlockingEvent(
                time=collected_at,
                instance_id=instance_id,
                blocker_session_id=str(blocking_session_id),
                blocked_session_id=str(row["session_id"]),
                blocker_sql_hash=blocker_sql_hash,
                blocked_sql_hash=_hash_query_text(row["query_text"] or row["full_query_text"]),
                wait_time_ms=row["wait_time"] or 0,
                blocker_db_user=blocker_login,
                blocker_program=blocker_program,
                blocker_machine=blocker_machine,
                blocker_wait_event_native=(
                    blocker_row["wait_type"] if blocker_row is not None else None
                ),
                blocked_db_user=blocked_login,
                blocked_program=blocked_program,
                blocked_machine=blocked_machine,
                blocked_wait_event_native=row["wait_type"],
                blocked_wait_resource=row["wait_resource"] or None,
                blocked_object=(
                    blocked_object_row["blocked_object"] if blocked_object_row is not None else None
                ),
                blocker_transaction_id=transaction_id_by_session_id.get(str(blocking_session_id)),
                blocked_transaction_id=transaction_id_by_session_id.get(str(row["session_id"])),
                blocker_database_name=_database_name(blocking_session_id),
                blocked_database_name=_database_name(row["session_id"]),
                blocker_transaction_begin_time=_transaction_begin_time(blocking_session_id),
                blocked_transaction_begin_time=_transaction_begin_time(row["session_id"]),
            )
        )
    return events


_TRANSACTION_TYPE_LABELS = {
    1: "local",
    2: "local",  # read-only
    3: "local",  # system
    4: "distributed",
}

# Ver Microsoft Learn (`sys.dm_tran_active_transactions.transaction_state`) — mapeamento
# best-effort, nao exaustivo (fallback pro codigo bruto quando desconhecido).
_TRANSACTION_STATE_LABELS = {
    0: "nao inicializada",
    1: "inicializada",
    2: "ativa",
    3: "encerrada (read-only)",
    4: "commit iniciado",
    5: "preparada (aguardando resolucao)",
    6: "commitada",
    7: "revertendo (rollback)",
    8: "revertida (rolled back)",
}


def _build_transaction_events(
    transaction_rows: list[dict],
    transaction_database_rows: list[dict],
    session_rows: list[dict],
    all_session_rows: list[dict],
    blocking_events: list[NormalizedBlockingEvent],
    instance_id: UUID,
    collected_at: datetime,
    orphan_threshold_minutes: int,
    server_utc_offset: timedelta,
    sleeping_min_duration_ms: int | None,
    sleeping_database_overrides_ms: dict[str, int],
) -> list[NormalizedTransactionEvent]:
    """Transacoes DIGNAS DE NOTA neste ciclo — `_TRANSACTION_QUERY` (join `sys.dm_tran_session_
    transactions`/`sys.dm_tran_active_transactions`) enumera toda transacao ABERTA, mas so vira
    `NormalizedTransactionEvent` quando `compute_flag_reason` acha pelo menos 1 motivo (bloqueio,
    erro, orfa) — redesenho pos-uso real: log de incidente retido, nao espelho de toda transacao
    aberta (ver `app.collectors.transaction_flagging`). `transaction_uow` (GUID) so preenchido
    quando a transacao e uma distribuida promovida pelo MSDTC (unico caso em que correlacionar 2
    transacoes entre instancias e confiavel — ver `ARCHITECTURE_SPEC.md`). `last_error_code` e
    melhor esforco (`sys.dm_exec_requests.prev_error`, so em `session_rows` — so existe enquanto
    a sessao ainda tem request ativa; ausente quando a transacao ja ficou idle apos o erro).

    BUG REAL corrigido (relatado pelo usuario ao vivo, tela de Transacoes travando com volume
    alto): `prev_error` NAO significa "esta transacao falhou/deu rollback" — e' literalmente "o
    numero do ultimo erro, de qualquer severidade, levantado por QUALQUER statement da sessao",
    incluindo mensagens rotineiras sem nenhum impacto (confirmado ao vivo contra
    `sys.messages`: codigo 16945 = "The cursor was not declared.", severidade 16, tipicamente
    codigo defensivo de limpeza de cursor que nao chegou a ser aberto — nao aborta a transacao,
    nao causa rollback, a transacao seguinte fecha normalmente). Como a severidade sozinha NAO
    distingue isso de erro real (violacao de constraint tambem e' severidade 14-16, mesma faixa),
    nao ha como confirmar de forma confiavel que UM erro especifico derrubou a transacao sem
    Extended Events (`sqlserver.error_reported` correlacionado a rollback - ja registrado como
    trabalho futuro, nao implementado). Por isso `has_error` NUNCA mais alimenta `flag_reason`
    (nao e' mais motivo pra gravar uma transacao sozinho) - `last_error_code` continua capturado
    e exibido, mas so como contexto informativo quando a transacao ja foi gravada por outro
    motivo (bloqueio/orfa/deadlock).

    `sleeping_min_duration_ms`/`sleeping_database_overrides_ms` (novas tarefas do usuario, itens
    1/2): `session_rows` (`_SESSION_ACTIVITY_QUERY`) so tem sessao com REQUEST ATIVA (INNER JOIN
    `sys.dm_exec_requests`) — uma sessao dona de transacao que NAO aparece ali esta ociosa
    ("sleeping", sem request). O limiar efetivo por transacao prioriza o override do banco
    (`sleeping_database_overrides_ms`, resolvido pelo `PollingWorker`) sobre o valor de
    instancia/global (`sleeping_min_duration_ms`) — mesma precedencia de `blocking_min_duration_ms`.
    """
    database_name_by_transaction_id = {
        str(row["transaction_id"]): row["database_name"] for row in transaction_database_rows
    }
    prev_error_by_session_id = {str(row["session_id"]): row["prev_error"] for row in session_rows}
    active_session_ids = {str(row["session_id"]) for row in session_rows}
    machine_by_session_id = {
        str(row["session_id"]): row["host_name"]
        for row in all_session_rows
        if row["host_name"]
    }
    blocking_involved_transaction_ids = {
        tx_id
        for e in blocking_events
        for tx_id in (e.blocker_transaction_id, e.blocked_transaction_id)
        if tx_id is not None
    }
    events: list[NormalizedTransactionEvent] = []
    for row in transaction_rows:
        transaction_id = str(row["transaction_id"])
        session_id = str(row["session_id"])
        transaction_type_code = row["transaction_type"]
        transaction_state_code = row["transaction_state"]
        transaction_uow = row["transaction_uow"]
        prev_error = prev_error_by_session_id.get(session_id)
        last_error_code = int(prev_error) if prev_error is not None and prev_error != 0 else None
        begin_time = _as_utc(row["transaction_begin_time"], server_utc_offset)
        database_name = database_name_by_transaction_id.get(transaction_id)
        effective_sleeping_threshold_ms = sleeping_database_overrides_ms.get(
            database_name if database_name is not None else "", sleeping_min_duration_ms
        )
        flag_reason = compute_flag_reason(
            involved_in_blocking=transaction_id in blocking_involved_transaction_ids,
            # `prev_error` nao e' confiavel como sinal de falha real (ver docstring da funcao) —
            # nunca sozinho motivo pra gravar a transacao. `last_error_code` continua no evento
            # normalizado abaixo, so como contexto quando ha outro motivo de flag.
            has_error=False,
            begin_time=begin_time,
            now=collected_at,
            orphan_threshold_minutes=orphan_threshold_minutes,
            is_idle=session_id not in active_session_ids,
            sleeping_threshold_ms=effective_sleeping_threshold_ms,
        )
        if flag_reason is None:
            continue
        events.append(
            NormalizedTransactionEvent(
                instance_id=instance_id,
                transaction_identifier=transaction_id,
                session_id=session_id,
                database_name=database_name_by_transaction_id.get(transaction_id),
                transaction_type=_TRANSACTION_TYPE_LABELS.get(
                    transaction_type_code, f"desconhecido ({transaction_type_code})"
                ),
                transaction_uow=(
                    str(transaction_uow)
                    if transaction_uow
                    and str(transaction_uow) != "00000000-0000-0000-0000-000000000000"
                    else None
                ),
                begin_time=begin_time,
                state=_TRANSACTION_STATE_LABELS.get(
                    transaction_state_code, f"código {transaction_state_code}"
                ),
                last_error_code=last_error_code,
                flag_reason=flag_reason,
                machine=machine_by_session_id.get(session_id),
            )
        )
    return events


def _build_metric_samples(
    session_rows: list[dict],
    counter_rows: list[dict],
    cpu_row: dict | None,
    deadlock_counter_row: dict | None,
    instance_id: UUID,
    collected_at: datetime,
) -> list[NormalizedMetricSample]:
    samples = [
        NormalizedMetricSample(
            time=collected_at,
            target_type="instance",
            target_id=instance_id,
            metric_name=SQLSERVER_ACTIVE_SESSION_COUNT,
            value=float(len(session_rows)),
        )
    ]
    if cpu_row is not None:
        sql_process_utilization = cpu_row.get("SQLProcessUtilization")
        system_idle = cpu_row.get("SystemIdle")
        if sql_process_utilization is not None:
            samples.append(
                NormalizedMetricSample(
                    time=collected_at,
                    target_type="instance",
                    target_id=instance_id,
                    metric_name=SQLSERVER_INSTANCE_CPU_PERCENT,
                    value=float(sql_process_utilization),
                )
            )
        if system_idle is not None:
            samples.append(
                NormalizedMetricSample(
                    time=collected_at,
                    target_type="instance",
                    target_id=instance_id,
                    metric_name=SQLSERVER_OS_CPU_PERCENT,
                    value=float(100 - system_idle),
                )
            )
    for row in counter_rows:
        counter_name = str(row["counter_name"]).strip()
        metric_name = SQLSERVER_RAW_COUNTER_TO_METRIC_NAME.get(counter_name)
        if metric_name is None:  # pragma: no cover - nao deveria acontecer dado o WHERE da query
            continue
        samples.append(
            NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=metric_name,
                value=float(row["cntr_value"]),
            )
        )
    if deadlock_counter_row is not None:
        samples.append(
            NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=SQLSERVER_DEADLOCKS_TOTAL,
                value=float(deadlock_counter_row["cntr_value"]),
            )
        )
    return samples


def _build_capability_results(
    *, granted_permissions: set[str], is_processadmin: bool, is_db_datareader: bool
) -> list[CapabilityResult]:
    has_server_state = bool(granted_permissions & _SERVER_STATE_PERMISSIONS)
    results = [
        CapabilityResult(
            capability=SQLSERVER_SERVER_STATE,
            status=CapabilityStatus.FULL if has_server_state else CapabilityStatus.UNAVAILABLE,
            reason=(
                None
                if has_server_state
                else "VIEW SERVER STATE / VIEW SERVER PERFORMANCE STATE ausente"
            ),
        )
    ]

    has_database_visibility = "VIEW ANY DATABASE" in granted_permissions
    results.append(
        CapabilityResult(
            capability=SQLSERVER_DATABASE_VISIBILITY,
            status=CapabilityStatus.FULL if has_database_visibility else CapabilityStatus.DEGRADED,
            reason=(
                None
                if has_database_visibility
                else "sem VIEW ANY DATABASE, visibilidade de bancos fica limitada"
            ),
        )
    )

    has_definition_visibility = "VIEW ANY DEFINITION" in granted_permissions
    results.append(
        CapabilityResult(
            capability=SQLSERVER_DEFINITION_VISIBILITY,
            status=(
                CapabilityStatus.FULL if has_definition_visibility else CapabilityStatus.DEGRADED
            ),
            reason=(
                None
                if has_definition_visibility
                else "sem VIEW ANY DEFINITION, definicao de objetos (planos/procs) fica limitada"
            ),
        )
    )

    results.append(
        CapabilityResult(
            capability=SQLSERVER_SESSION_TERMINATION,
            status=CapabilityStatus.FULL if is_processadmin else CapabilityStatus.DEGRADED,
            reason=None if is_processadmin else "sem processadmin, Kill Session nao vai funcionar",
        )
    )

    results.append(
        CapabilityResult(
            capability=SQLSERVER_TABLE_READ_ACCESS,
            status=CapabilityStatus.FULL if is_db_datareader else CapabilityStatus.DEGRADED,
            reason=(
                None
                if is_db_datareader
                else "sem db_datareader, leitura de dados de tabela fica indisponivel"
            ),
        )
    )

    return results


class SQLServerCollector:
    """Implementa `app.collectors.base.Collector` para instancias SQL Server."""

    def __init__(self, connection: ConnectionParams, instance_id: UUID | None = None) -> None:
        self._connection_params = connection
        self._instance_id = instance_id

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Conecta (via thread, `pymssql` e sincrono) e checa permissoes de servidor/banco.

        `asyncio.wait_for` cobre o caso em que a propria conexao trava alem do timeout (a thread
        sincrona nao e cancelavel, mas o resultado deixa de ser aguardado) — tratado igual a
        qualquer outro erro de conectividade.
        """
        try:
            return await asyncio.wait_for(
                run_in_instance_executor(
                    self._instance_id, _connect_and_check, self._connection_params
                ),
                timeout=self._connection_params.connect_timeout_seconds,
            )
        except TimeoutError as exc:
            return [
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=f"Falha ao conectar: {type(exc).__name__}",
                )
            ]

    async def collect(
        self,
        *,
        sleeping_min_duration_ms: int | None = None,
        sleeping_database_overrides_ms: dict[str, int] | None = None,
    ) -> CollectionResult:
        """Executa um ciclo de coleta: sessao/request ativa, bloqueio (nivel direto) e contadores
        de recurso RAW.

        Ver `Docs/collector-queries.md` §1 (sessao ativa), §2 (bloqueio), §5 (metricas de
        recurso, so `cntr_type=65792`). NAO implementa `sys.dm_os_wait_stats` cumulativo via
        `LAG()` nem arvore recursiva de bloqueio (cortes de escopo desta feature).

        Requer `instance_id` (levanta `ValueError` se `None` — usado em teste de conexao
        pre-cadastro, onde `collect()` nao se aplica).
        """
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.collect requer instance_id de uma instancia ja cadastrada."
            )

        start = time.perf_counter()
        collected_at = self._now()
        settings = get_settings()

        # `pymssql` e sincrono e a thread nao e cancelavel — o timeout externo cobre o caso em
        # que a chamada travar alem do esperado (login + execucao de todas as queries do ciclo).
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        (
            metric_samples,
            query_samples,
            blocking_events,
            transaction_events,
            open_transaction_identifiers,
            product_version,
            version_major,
        ) = await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
                settings.collection_max_rows,
                collected_at,
                settings.transaction_orphan_threshold_minutes,
                sleeping_min_duration_ms,
                sleeping_database_overrides_ms or {},
            ),
            timeout=overall_timeout,
        )

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
            engine_version=product_version,
            engine_version_major=version_major,
        )

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        """Sugestao real do otimizador via `sys.dm_db_missing_index_*` (Docs/collector-queries.md
        §10) — instancia inteira numa unica chamada, sem iterar por banco (ao contrario do
        PostgreSQL, que nao tem DMV equivalente)."""
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.collect_missing_index_suggestions requer instance_id de "
                "uma instancia ja cadastrada."
            )
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_missing_indexes,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
                settings.collection_max_rows,
            ),
            timeout=overall_timeout,
        )

    async def get_engine_start_time(self) -> datetime | None:
        """`sys.dm_os_sys_info.sqlserver_start_time` - ver docstring do Protocol."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_get_engine_start_time,
                self._connection_params,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        """`msdb.dbo.backupset` (Docs/collector-queries.md §6) — instancia inteira numa unica
        chamada, uma linha por banco. So backup COMPLETO (`type='D'`) nesta primeira versao."""
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.collect_backup_status requer instance_id de uma instancia "
                "ja cadastrada."
            )
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_backup_status,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
                settings.collection_max_rows,
            ),
            timeout=overall_timeout,
        )

    async def collect_freespace_metrics(self) -> list[NormalizedMetricSample]:
        """Fase 2, item 2 (freespace) — instancia inteira numa unica chamada, uma linha por
        volume de disco usado pelos arquivos de dados/log (`sys.dm_os_volume_stats`)."""
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.collect_freespace_metrics requer instance_id de uma "
                "instancia ja cadastrada."
            )
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_freespace,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def collect_replication_lag_metrics(self) -> list[NormalizedMetricSample]:
        """Fase 2, item 2 (replicacao) — Always On AG. Sem AG configurado devolve lista vazia
        (nao e erro, `sys.dm_hadr_database_replica_states` so tem linhas se houver replica)."""
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.collect_replication_lag_metrics requer instance_id de uma "
                "instancia ja cadastrada."
            )
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_replication_lag,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        """Fase 2, item 2 (`rule_type="custom_sql"`) — `sql` ja deve ter passado por
        `app.services.custom_sql_guard` (bloqueio por palavra-chave) ANTES de chegar aqui; este
        metodo so executa, nunca valida. `timeout_seconds` e o timeout de QUERY (o timeout de
        conexao segue vindo de `self._connection_params`, mesmo padrao dos demais metodos)."""
        overall_timeout = self._connection_params.connect_timeout_seconds + timeout_seconds
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_run_scalar_query,
                self._connection_params,
                sql,
                timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def scan_plan_compile_timeouts(self) -> list[NormalizedPlanCompileTimeoutFinding]:
        """Fase 2, item 5 — varre o cache de planos procurando
        `StatementOptmEarlyAbortReason` (timeout/limite de memoria de compilacao). Roda num
        worker de baixa frequencia (`app/workers/plan_compile_timeout_supervisor.py`),
        instancia inteira numa unica chamada."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_scan_plan_compile_timeouts,
                self._connection_params,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def scan_plan_history(self) -> list[NormalizedPlanSnapshot]:
        """Historico de plano de execucao — varre o cache de planos e devolve o snapshot atual
        de toda query com plano em cache, instancia inteira numa unica chamada. A deduplicacao
        por mudanca de plano fica no repositorio
        (`QueryPlanSnapshotsRepository.insert_if_changed`)."""
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.scan_plan_history requer instance_id de uma instancia ja "
                "cadastrada."
            )
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_scan_plan_history,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def collect_deadlock_events(self) -> list[NormalizedDeadlockEvent]:
        """Target `event_file` da sessao `system_health` (Etapa 10, item 7) — sempre ativa por
        padrao desde o SQL Server 2008, sem precisar criar nenhuma Extended Events session nova.
        Instancia inteira numa unica chamada, retido/deduplicado pelo repositorio (o mesmo
        deadlock pode aparecer em chamadas consecutivas dentro da janela de 1h da query).

        Timeout proprio (`deadlock_query_timeout_seconds`, bem maior que o das coletas de alta
        frequencia) — ler `.xel` em disco e mais lento que o `ring_buffer` em memoria usado antes
        aqui, confirmado ao vivo (timeout generico de poll estourava em instancias reais)."""
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.collect_deadlock_events requer instance_id de uma instancia "
                "ja cadastrada."
            )
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.deadlock_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_deadlocks,
                self._connection_params,
                self._instance_id,
                settings.deadlock_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def ensure_transaction_error_capture(self) -> bool:
        """Cria/inicia (idempotente) a sessao Extended Events dedicada (Etapa 10, item 3) - ver
        `_connect_and_ensure_transaction_error_session`. Devolve `False` (nunca levanta excecao)
        quando a criacao/inicio falha, inclusive por timeout - o chamador (`TransactionError
        Supervisor`) loga e pula a leitura deste ciclo."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.transaction_error_query_timeout_seconds
        )
        try:
            return await asyncio.wait_for(
                run_in_instance_executor(
                    self._instance_id,
                    _connect_and_ensure_transaction_error_session,
                    self._connection_params,
                    settings.transaction_error_query_timeout_seconds,
                ),
                timeout=overall_timeout,
            )
        except TimeoutError:
            logger.warning(
                "Timeout ao criar/iniciar a sessao Extended Events %s para instance_id=%s",
                _TRANSACTION_ERROR_SESSION_NAME,
                self._instance_id,
            )
            return False

    async def collect_transaction_error_events(self) -> list[NormalizedConfirmedTransactionError]:
        """Target `event_file` da sessao dedicada (Etapa 10, item 3) - ver
        `_connect_and_collect_transaction_errors`. Mesmo timeout dedicado de
        `ensure_transaction_error_capture` (ler `.xel` em disco tem o mesmo perfil de custo do
        target de deadlock)."""
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.collect_transaction_error_events requer instance_id de uma "
                "instancia ja cadastrada."
            )
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.transaction_error_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_transaction_errors,
                self._connection_params,
                self._instance_id,
                settings.transaction_error_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def fetch_execution_plan(self, sql_hash: str) -> str | None:
        """Busca sob demanda (Etapa 11) — NAO faz parte do ciclo de coleta, nao precisa de
        `instance_id`. Devolve o XML `ShowPlanXML` CACHEADO/estimado (ainda NAO mascarado —
        mascaramento e responsabilidade do chamador, `app.services.execution_plan_service`) do
        plano cujo statement isolado+hasheado bate com `sql_hash`, ou `None` quando nenhum
        corresponde (pode ter saido do cache desde a ultima execucao). Ver
        `fetch_actual_execution_plan` pro plano REAL, tentado primeiro pelo service."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_fetch_execution_plan,
                self._connection_params,
                sql_hash,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def fetch_actual_execution_plan(self, sql_hash: str) -> str | None:
        """Busca sob demanda — plano REAL (com contagens de linha/CPU/IO reais de execucao),
        so possivel sem Query Store quando a mesma sessao que rodou `sql_hash` AINDA esta em
        execucao agora (`sys.dm_exec_query_statistics_xml`, SQL Server 2016 SP1+). `None`
        quando nao ha sessao ativa correspondente (o caso comum) ou quando a funcao nao esta
        disponivel nesta versao/permissao — o chamador sempre cai pro plano cacheado/estimado
        de `fetch_execution_plan` nesses casos, nunca propaga erro."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_fetch_actual_execution_plan,
                self._connection_params,
                sql_hash,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def fetch_plan_from_query_store(
        self, sql_hash: str, database_names: list[str]
    ) -> str | None:
        """Item 10 — fallback de plano via Query Store quando o cache de plano nao tem o plano.
        Pesquisa os `database_names` fornecidos (bancos onde o sql_hash foi visto em
        `query_samples`). `None` quando nenhum banco tem o plano no Query Store (ou QS
        desabilitado em todos). XML ainda NAO mascarado — mascaramento e responsabilidade do
        chamador (`app.services.execution_plan_service`)."""
        if not database_names:
            return None
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_fetch_plan_from_query_store,
                self._connection_params,
                sql_hash,
                database_names,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def collect_idle_in_transaction(self) -> list[NormalizedIdleInTransactionSession]:
        """Fase 5 T56 — sessoes idle in transaction SQL Server (on-demand)."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_idle_in_transaction,
                self._connection_params,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def collect_config_compliance(self) -> list[NormalizedConfigComplianceItem]:
        """Fase 5 T64 — compliance de configuracao SQL Server (on-demand)."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_config_compliance,
                self._connection_params,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def collect_connection_consumers(self) -> list[NormalizedConnectionConsumer]:
        """Fase 5 T65 — consumidores de conexao SQL Server (on-demand)."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_connection_consumers,
                self._connection_params,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def collect_tempdb_stats(self) -> NormalizedTempDbStats | None:
        """Fase 5 T61 — snapshot de uso do TempDB (SQL Server only, on-demand)."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds * 4
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_tempdb_stats,
                self._connection_params,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def collect_db_sizes(self) -> list[NormalizedDbSize]:
        """Fase 5 T63 — tamanho de cada banco SQL Server (on-demand)."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            run_in_instance_executor(
                self._instance_id,
                _connect_and_collect_db_sizes_ss,
                self._connection_params,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def close(self) -> None:
        """Sem estado persistente entre chamadas (cada `check_capabilities` fecha sua conexao)."""
        logger.info("close chamado para instancia sqlserver (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
