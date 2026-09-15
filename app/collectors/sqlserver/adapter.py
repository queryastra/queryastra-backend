"""Adaptador SQL Server. Fonte de verdade: `Docs/collector-queries.md`.

Usa `pymssql` (sincrono) — cada chamada roda em thread separada (`asyncio.to_thread`) sob
`asyncio.wait_for` para respeitar o timeout de conexao configurado
(Docs/rules/02-safe-collection.md).

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
      `_BLOCKED_OBJECT_QUERY` (`sys.dm_tran_locks`/`sys.partitions`) resolve o objeto bloqueado,
      melhor esforco.
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
from typing import Any
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
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals
from app.normalizers.metric_names import (
    SQLSERVER_ACTIVE_SESSION_COUNT,
    SQLSERVER_RAW_COUNTER_TO_METRIC_NAME,
)
from app.normalizers.models import (
    NormalizedBackupStatus,
    NormalizedBlockingEvent,
    NormalizedDeadlockEvent,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedQuerySample,
    NormalizedTransactionEvent,
)
from app.normalizers.wait_taxonomy import map_wait_event

logger = get_logger(__name__)

_SERVER_STATE_PERMISSIONS = {"VIEW SERVER STATE", "VIEW SERVER PERFORMANCE STATE"}

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
# descartado sem uso; agora fecha o gap real confirmado comparando com o SolarWinds DPA
# (categoria "Memory/CPU" alem das esperas nomeadas).
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
# `resource_associated_entity_id` JA E o `object_id` (usavel direto em `OBJECT_NAME`); pra
# `'PAGE'/'KEY'/'RID'`, e o `hobt_id` — precisa de `sys.partitions` pra achar o `object_id`.
# `sys.partitions` e uma view POR BANCO (so enxerga o banco atual da conexao) — o join com
# `resource_database_id = DB_ID()` deliberadamente so resolve `blocked_object` quando o lock e no
# mesmo banco da conexao; bloqueio cross-database (banco monitorado != banco da conexao) fica com
# `blocked_object = NULL`, so o `wait_resource` bruto (corte de escopo documentado, nao um bug).
_BLOCKED_OBJECT_QUERY = """
    SELECT
        l.request_session_id AS session_id,
        l.resource_type,
        CASE l.resource_type
            WHEN 'OBJECT' THEN OBJECT_NAME(l.resource_associated_entity_id, l.resource_database_id)
            ELSE OBJECT_NAME(p.object_id, l.resource_database_id)
        END AS blocked_object
    FROM sys.dm_tran_locks AS l
    LEFT JOIN sys.partitions AS p
        ON l.resource_type IN ('PAGE', 'KEY', 'RID')
        AND l.resource_database_id = DB_ID()
        AND p.hobt_id = l.resource_associated_entity_id
    WHERE l.request_status = 'WAIT'
"""

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

# Etapa 10, item 7 (Deadlocks) — ring buffer `system_health`, sessao Extended Events nativa e
# sempre ativa por padrao desde o SQL Server 2008 (nunca precisa ser criada). `target_data` e um
# XML com varios tipos de evento acumulados recentemente (buffer circular, poucos MB) — filtrar
# so `xml_deadlock_report` e mais simples/barato fazendo o parse em Python (`_build_deadlock_
# events`) do que via XQuery T-SQL.
_DEADLOCK_RING_BUFFER_QUERY = """
    SELECT CAST(xet.target_data AS NVARCHAR(MAX)) AS target_data
    FROM sys.dm_xe_session_targets AS xet
    INNER JOIN sys.dm_xe_sessions AS xes ON xes.address = xet.event_session_address
    WHERE xes.name = 'system_health' AND xet.target_name = 'ring_buffer'
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
# execucao (isso exigiria Query Store — desligado por padrao on-premises — ou Extended Events,
# caro; fora de escopo).
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


def _connect_and_check(connection: ConnectionParams) -> list[CapabilityResult]:
    """Helper sincrono: conecta via `pymssql` e roda as checagens de permissao.

    Roda em thread separada (`asyncio.to_thread`) — `pymssql` nao e assincrono.
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

    Roda em thread separada (`asyncio.to_thread`, mesmo padrao de `_connect_and_check`) —
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

        cursor.execute(_BLOCKED_OBJECT_QUERY)
        blocked_object_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_TRANSACTION_QUERY)
        transaction_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_TRANSACTION_DATABASE_QUERY)
        transaction_database_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_PERFORMANCE_COUNTERS_QUERY)
        counter_rows = list(cursor.fetchall())[:max_rows]

        cursor.close()
    finally:
        conn.close()

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
    metric_samples = _build_metric_samples(session_rows, counter_rows, instance_id, collected_at)
    transaction_events = _build_transaction_events(
        transaction_rows,
        transaction_database_rows,
        session_rows,
        blocking_events,
        instance_id,
        collected_at,
        transaction_orphan_threshold_minutes,
        server_utc_offset,
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


def _connect_and_collect_deadlocks(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
) -> list[NormalizedDeadlockEvent]:
    """Helper sincrono (mesmo padrao de `_connect_and_collect_missing_indexes`): le o ring buffer
    `system_health` e extrai os eventos `xml_deadlock_report` acumulados recentemente."""
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
        cursor.execute(_DEADLOCK_RING_BUFFER_QUERY)
        rows = list(cursor.fetchall())
        cursor.close()
    finally:
        conn.close()

    return _build_deadlock_events(rows, instance_id)


def _build_deadlock_events(rows: list[dict], instance_id: UUID) -> list[NormalizedDeadlockEvent]:
    """Faz o parse do XML do ring buffer (uma linha por target `ring_buffer` — normalmente 1 só)
    e extrai cada evento `xml_deadlock_report` como um `NormalizedDeadlockEvent`.

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
            deadlock_el = event_el.find("./data[@name='xml_report']/value/deadlock")
            if deadlock_el is None:
                continue

            timestamp_raw = event_el.get("timestamp")
            time_value = _parse_xe_timestamp(timestamp_raw)

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
        "sql_text_masked": mask_sql_literals(sql_text) if sql_text else None,
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


def _hash_query_text(query_text: str | None) -> str | None:
    if not query_text:
        return None
    return hashlib.sha256(mask_sql_literals(query_text).encode("utf-8")).hexdigest()


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
        masked = mask_sql_literals(query_text)
        sql_hash = hashlib.sha256(masked.encode("utf-8")).hexdigest()
        full_query_text = row["full_query_text"]
        masked_full = mask_sql_literals(full_query_text) if full_query_text else masked
        samples.append(
            NormalizedQuerySample(
                time=collected_at,
                instance_id=instance_id,
                engine="sqlserver",
                sql_hash=sql_hash,
                sql_text_masked=masked,
                sql_text_full_masked=masked_full,
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
    `_BLOCKED_OBJECT_QUERY`/docstring de `NormalizedBlockingEvent`) — so preenchidos pro lado
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
        blocker_sql_hash = (
            _hash_query_text(blocker_row["query_text"]) if blocker_row is not None else None
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
                blocked_sql_hash=_hash_query_text(row["query_text"]),
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
    blocking_events: list[NormalizedBlockingEvent],
    instance_id: UUID,
    collected_at: datetime,
    orphan_threshold_minutes: int,
    server_utc_offset: timedelta,
) -> list[NormalizedTransactionEvent]:
    """Transacoes DIGNAS DE NOTA neste ciclo — `_TRANSACTION_QUERY` (join `sys.dm_tran_session_
    transactions`/`sys.dm_tran_active_transactions`) enumera toda transacao ABERTA, mas so vira
    `NormalizedTransactionEvent` quando `compute_flag_reason` acha pelo menos 1 motivo (bloqueio,
    erro, orfa) — redesenho pos-uso real: log de incidente retido, nao espelho de toda transacao
    aberta (ver `app.collectors.transaction_flagging`). `transaction_uow` (GUID) so preenchido
    quando a transacao e uma distribuida promovida pelo MSDTC (unico caso em que correlacionar 2
    transacoes entre instancias e confiavel — ver `ARCHITECTURE_SPEC.md`). `last_error_code` e
    melhor esforco (`sys.dm_exec_requests.prev_error`, so em `session_rows` — so existe enquanto
    a sessao ainda tem request ativa; ausente quando a transacao ja ficou idle apos o erro)."""
    database_name_by_transaction_id = {
        str(row["transaction_id"]): row["database_name"] for row in transaction_database_rows
    }
    prev_error_by_session_id = {str(row["session_id"]): row["prev_error"] for row in session_rows}
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
        flag_reason = compute_flag_reason(
            involved_in_blocking=transaction_id in blocking_involved_transaction_ids,
            has_error=last_error_code is not None,
            begin_time=begin_time,
            now=collected_at,
            orphan_threshold_minutes=orphan_threshold_minutes,
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
            )
        )
    return events


def _build_metric_samples(
    session_rows: list[dict],
    counter_rows: list[dict],
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
                asyncio.to_thread(_connect_and_check, self._connection_params),
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

    async def collect(self) -> CollectionResult:
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
            asyncio.to_thread(
                _connect_and_collect,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
                settings.collection_max_rows,
                collected_at,
                settings.transaction_orphan_threshold_minutes,
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
            asyncio.to_thread(
                _connect_and_collect_missing_indexes,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
                settings.collection_max_rows,
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
            asyncio.to_thread(
                _connect_and_collect_backup_status,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
                settings.collection_max_rows,
            ),
            timeout=overall_timeout,
        )

    async def collect_deadlock_events(self) -> list[NormalizedDeadlockEvent]:
        """Ring buffer `system_health` (Etapa 10, item 7) — sempre ativo por padrao desde o SQL
        Server 2008, sem precisar criar nenhuma Extended Events session nova. Instancia inteira
        numa unica chamada, retido/deduplicado pelo repositorio (o mesmo deadlock pode aparecer
        em chamadas consecutivas enquanto ainda estiver no ring buffer)."""
        if self._instance_id is None:
            raise ValueError(
                "SQLServerCollector.collect_deadlock_events requer instance_id de uma instancia "
                "ja cadastrada."
            )
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            asyncio.to_thread(
                _connect_and_collect_deadlocks,
                self._connection_params,
                self._instance_id,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def fetch_execution_plan(self, sql_hash: str) -> str | None:
        """Busca sob demanda (Etapa 11) — NAO faz parte do ciclo de coleta, nao precisa de
        `instance_id`. Devolve o XML `ShowPlanXML` (ainda NAO mascarado — mascaramento e
        responsabilidade do chamador, `app.services.execution_plan_service`) do plano em cache
        cujo statement isolado+hasheado bate com `sql_hash`, ou `None` quando nenhum corresponde
        (pode ter saido do cache desde a ultima execucao)."""
        settings = get_settings()
        overall_timeout = (
            self._connection_params.connect_timeout_seconds
            + settings.collection_query_timeout_seconds
        )
        return await asyncio.wait_for(
            asyncio.to_thread(
                _connect_and_fetch_execution_plan,
                self._connection_params,
                sql_hash,
                settings.collection_query_timeout_seconds,
            ),
            timeout=overall_timeout,
        )

    async def close(self) -> None:
        """Sem estado persistente entre chamadas (cada `check_capabilities` fecha sua conexao)."""
        logger.info("close chamado para instancia sqlserver (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
