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
    - `sys.dm_exec_requests.blocking_session_id` — deteccao de bloqueio de **nivel direto
      apenas** (collector-queries.md §2); NAO implementa a CTE recursiva completa (corte de
      escopo desta feature). Sentinela negativo (-2 a -5) = bloqueador nao determinavel — NAO
      tratado como erro, so ignorado (nenhum `NormalizedBlockingEvent` gerado para essa linha).
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
from datetime import UTC, datetime
from uuid import UUID

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
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals
from app.normalizers.metric_names import (
    SQLSERVER_ACTIVE_SESSION_COUNT,
    SQLSERVER_RAW_COUNTER_TO_METRIC_NAME,
)
from app.normalizers.models import (
    NormalizedBlockingEvent,
    NormalizedMetricSample,
    NormalizedQuerySample,
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
_SESSION_ACTIVITY_QUERY = """
    SELECT r.session_id, r.status, r.command, r.start_time,
           r.total_elapsed_time / 1000.00 AS elapsed_seconds,
           r.total_elapsed_time,
           r.cpu_time / 1000.00 AS cpu_seconds,
           r.reads, r.writes, r.logical_reads, r.row_count,
           r.blocking_session_id, r.wait_type, r.wait_time,
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


def _connect_and_collect(
    connection: ConnectionParams,
    instance_id: UUID,
    query_timeout_seconds: float,
    max_rows: int,
    collected_at: datetime,
) -> tuple[
    list[NormalizedMetricSample],
    list[NormalizedQuerySample],
    list[NormalizedBlockingEvent],
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

        cursor.execute(_SESSION_ACTIVITY_QUERY)
        session_rows = list(cursor.fetchall())[:max_rows]

        cursor.execute(_PERFORMANCE_COUNTERS_QUERY)
        counter_rows = list(cursor.fetchall())[:max_rows]

        cursor.close()
    finally:
        conn.close()

    query_samples = _build_query_samples(session_rows, instance_id, collected_at)
    blocking_events = _build_blocking_events(session_rows, instance_id, collected_at)
    metric_samples = _build_metric_samples(session_rows, counter_rows, instance_id, collected_at)
    return metric_samples, query_samples, blocking_events, product_version, version_major


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
    session_rows: list[dict], instance_id: UUID, collected_at: datetime
) -> list[NormalizedBlockingEvent]:
    """Nivel direto apenas (`blocking_session_id`), sem arvore recursiva completa (corte de
    escopo). Sentinela negativo (-2 a -5) = bloqueador nao determinavel, tratado como "sem
    evento" — NUNCA como erro. O bloqueador pode nao ter request ativa (sessao ociosa segurando
    lock) — nesse caso `blocker_sql_hash=None`, mas o evento ainda e registrado.
    """
    sessions_by_id = {row["session_id"]: row for row in session_rows}
    events: list[NormalizedBlockingEvent] = []
    for row in session_rows:
        blocking_session_id = row["blocking_session_id"]
        if blocking_session_id is None or blocking_session_id < _BLOCKING_SESSION_REAL_BLOCK_MIN:
            continue
        blocker_row = sessions_by_id.get(blocking_session_id)
        blocker_sql_hash = (
            _hash_query_text(blocker_row["query_text"]) if blocker_row is not None else None
        )
        events.append(
            NormalizedBlockingEvent(
                time=collected_at,
                instance_id=instance_id,
                blocker_session_id=str(blocking_session_id),
                blocked_session_id=str(row["session_id"]),
                blocker_sql_hash=blocker_sql_hash,
                blocked_sql_hash=_hash_query_text(row["query_text"]),
                wait_time_ms=row["wait_time"] or 0,
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
            engine_version=product_version,
            engine_version_major=version_major,
        )

    async def close(self) -> None:
        """Sem estado persistente entre chamadas (cada `check_capabilities` fecha sua conexao)."""
        logger.info("close chamado para instancia sqlserver (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
