"""Adaptador MySQL/MariaDB. Detecta variante via SELECT VERSION().

Usa `aiomysql` como driver assincrono.

`check_capabilities()` conecta e verifica performance_schema, session activity,
wait events, process visibility e variante (MySQL vs MariaDB).

`collect()` executa ciclo de coleta somente leitura:
    - performance_schema.threads + events_waits_current (sessoes ativas + wait events)
    - SHOW GLOBAL STATUS (metricas de instancia)
    - SHOW REPLICA STATUS / SHOW SLAVE STATUS (lag de replicacao, se disponivel)

Permissao minima esperada: SELECT em performance_schema (PROCESS ou REPLICATION CLIENT).
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import UTC, datetime, timedelta
from uuid import UUID

import aiomysql

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    CONNECTIVITY,
    MYSQL_PERFORMANCE_SCHEMA,
    MYSQL_PROCESS_VISIBILITY,
    MYSQL_SESSION_ACTIVITY,
    MYSQL_WAIT_EVENTS,
)
from app.collectors.connection import ConnectionParams
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals, mask_sql_literals_for_display
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
from app.normalizers.wait_taxonomy import map_wait_event

logger = get_logger(__name__)

# Metricas canonicas MySQL
MYSQL_THREADS_CONNECTED = "mysql_threads_connected"
MYSQL_THREADS_RUNNING = "mysql_threads_running"
MYSQL_QUESTIONS_TOTAL = "mysql_questions_total"
MYSQL_INNODB_BUFFER_READS = "mysql_innodb_buffer_reads"
MYSQL_INNODB_BUFFER_READ_REQUESTS = "mysql_innodb_buffer_read_requests"
MYSQL_BYTES_RECEIVED = "mysql_bytes_received"
MYSQL_BYTES_SENT = "mysql_bytes_sent"
MYSQL_SLOW_QUERIES_TOTAL = "mysql_slow_queries_total"
MYSQL_COM_SELECT = "mysql_com_select"
MYSQL_COM_INSERT = "mysql_com_insert"
MYSQL_COM_UPDATE = "mysql_com_update"
MYSQL_COM_DELETE = "mysql_com_delete"
MYSQL_REPLICATION_LAG_SECONDS = "mysql_replication_lag_seconds"

# Mapa STATUS -> metric_name canonico
_STATUS_METRIC_MAP: dict[str, str] = {
    "Threads_connected": MYSQL_THREADS_CONNECTED,
    "Threads_running": MYSQL_THREADS_RUNNING,
    "Questions": MYSQL_QUESTIONS_TOTAL,
    "Innodb_buffer_pool_reads": MYSQL_INNODB_BUFFER_READS,
    "Innodb_buffer_pool_read_requests": MYSQL_INNODB_BUFFER_READ_REQUESTS,
    "Bytes_received": MYSQL_BYTES_RECEIVED,
    "Bytes_sent": MYSQL_BYTES_SENT,
    "Slow_queries": MYSQL_SLOW_QUERIES_TOTAL,
    "Com_select": MYSQL_COM_SELECT,
    "Com_insert": MYSQL_COM_INSERT,
    "Com_update": MYSQL_COM_UPDATE,
    "Com_delete": MYSQL_COM_DELETE,
}

_THREADS_QUERY = """
    SELECT
        t.THREAD_ID,
        t.PROCESSLIST_ID,
        t.PROCESSLIST_USER,
        t.PROCESSLIST_HOST,
        t.PROCESSLIST_DB,
        t.PROCESSLIST_COMMAND,
        t.PROCESSLIST_TIME,
        t.PROCESSLIST_INFO,
        w.EVENT_NAME AS wait_event_name
    FROM performance_schema.threads t
    LEFT JOIN performance_schema.events_waits_current w
        ON t.THREAD_ID = w.THREAD_ID
    WHERE t.PROCESSLIST_COMMAND != 'Sleep'
      AND t.TYPE = 'FOREGROUND'
    LIMIT %s
"""

_PROCESSLIST_FALLBACK_QUERY = """
    SELECT ID, USER, HOST, DB, COMMAND, TIME, INFO
    FROM information_schema.PROCESSLIST
    WHERE COMMAND != 'Sleep'
    LIMIT %s
"""


class MySQLCollector:
    """Implementa `app.collectors.base.Collector` para instancias MySQL e MariaDB."""

    def __init__(
        self,
        connection: ConnectionParams,
        instance_id: UUID | None = None,
    ) -> None:
        self._connection = connection
        self._instance_id = instance_id
        self._conn: aiomysql.Connection | None = None
        self._engine_variant: str = "mysql"  # "mysql" ou "mariadb"

    async def _get_connection(self) -> aiomysql.Connection:
        """Abre (ou reutiliza) conexao aiomysql."""
        if self._conn is not None:
            try:
                self._conn.ping()
                return self._conn
            except Exception:
                self._conn = None

        self._conn = await asyncio.wait_for(
            aiomysql.connect(
                host=self._connection.host,
                port=self._connection.port,
                user=self._connection.username or "",
                password=self._connection.secret_value or "",
                connect_timeout=int(self._connection.connect_timeout_seconds),
                autocommit=True,
            ),
            timeout=self._connection.connect_timeout_seconds,
        )
        return self._conn

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Detecta capacidades da instancia MySQL/MariaDB."""
        settings = get_settings()
        results: list[CapabilityResult] = []

        try:
            conn = await asyncio.wait_for(
                aiomysql.connect(
                    host=self._connection.host,
                    port=self._connection.port,
                    user=self._connection.username or "",
                    password=self._connection.secret_value or "",
                    connect_timeout=int(self._connection.connect_timeout_seconds),
                    autocommit=True,
                ),
                timeout=self._connection.connect_timeout_seconds,
            )
        except Exception as exc:
            results.append(
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=str(exc),
                )
            )
            return results

        try:
            async with conn.cursor() as cur:
                # Detectar variante (MySQL vs MariaDB)
                try:
                    await asyncio.wait_for(
                        cur.execute("SELECT VERSION()"),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                    row = await cur.fetchone()
                    if row:
                        version_str = str(row[0])
                        if "MariaDB" in version_str:
                            self._engine_variant = "mariadb"
                        else:
                            self._engine_variant = "mysql"
                except Exception:
                    pass

                # 1. performance_schema ativa
                try:
                    await asyncio.wait_for(
                        cur.execute("SELECT @@performance_schema"),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                    row = await cur.fetchone()
                    ps_enabled = row and int(row[0]) == 1
                    results.append(
                        CapabilityResult(
                            capability=MYSQL_PERFORMANCE_SCHEMA,
                            status=CapabilityStatus.FULL if ps_enabled else CapabilityStatus.UNAVAILABLE,
                            reason=None if ps_enabled else "performance_schema desabilitado",
                        )
                    )
                except Exception as exc:
                    results.append(
                        CapabilityResult(
                            capability=MYSQL_PERFORMANCE_SCHEMA,
                            status=CapabilityStatus.UNAVAILABLE,
                            reason=str(exc),
                        )
                    )

                # 2. session activity via performance_schema.events_statements_history_long
                try:
                    await asyncio.wait_for(
                        cur.execute(
                            "SELECT COUNT(*) FROM performance_schema.events_statements_history_long LIMIT 0"
                        ),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                    results.append(
                        CapabilityResult(
                            capability=MYSQL_SESSION_ACTIVITY,
                            status=CapabilityStatus.FULL,
                        )
                    )
                except Exception as exc:
                    results.append(
                        CapabilityResult(
                            capability=MYSQL_SESSION_ACTIVITY,
                            status=CapabilityStatus.UNAVAILABLE,
                            reason=str(exc),
                        )
                    )

                # 3. wait events (opcional)
                try:
                    await asyncio.wait_for(
                        cur.execute(
                            "SELECT COUNT(*) FROM performance_schema.events_waits_current LIMIT 0"
                        ),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                    results.append(
                        CapabilityResult(
                            capability=MYSQL_WAIT_EVENTS,
                            status=CapabilityStatus.FULL,
                        )
                    )
                except Exception as exc:
                    results.append(
                        CapabilityResult(
                            capability=MYSQL_WAIT_EVENTS,
                            status=CapabilityStatus.DEGRADED,
                            reason=str(exc),
                        )
                    )

                # 4. visibilidade de processos
                try:
                    await asyncio.wait_for(
                        cur.execute("SELECT USER()"),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                    results.append(
                        CapabilityResult(
                            capability=MYSQL_PROCESS_VISIBILITY,
                            status=CapabilityStatus.FULL,
                        )
                    )
                except Exception as exc:
                    results.append(
                        CapabilityResult(
                            capability=MYSQL_PROCESS_VISIBILITY,
                            status=CapabilityStatus.DEGRADED,
                            reason=str(exc),
                        )
                    )
        finally:
            conn.close()

        return results

    async def collect(
        self,
        *,
        sleeping_min_duration_ms: int | None = None,
        sleeping_database_overrides_ms: dict[str, int] | None = None,
    ) -> CollectionResult:
        """Executa um ciclo completo de coleta MySQL."""
        settings = get_settings()
        start = time.monotonic()
        collected_at = datetime.now(UTC)

        assert self._instance_id is not None, "instance_id obrigatorio para collect()"

        query_samples: list[NormalizedQuerySample] = []
        metric_samples: list[NormalizedMetricSample] = []

        try:
            conn = await self._get_connection()

            # Coleta sessoes ativas
            query_samples = await self._collect_sessions(conn, settings, collected_at)

            # Coleta metricas SHOW GLOBAL STATUS
            metric_samples = await self._collect_metrics(conn, settings, collected_at)

        except Exception as exc:
            logger.warning("Erro na coleta MySQL instance=%s: %s", self._instance_id, exc)

        duration_ms = int((time.monotonic() - start) * 1000)
        return CollectionResult(
            instance_id=self._instance_id,
            collected_at=collected_at,
            duration_ms=duration_ms,
            query_samples=query_samples,
            metric_samples=metric_samples,
        )

    async def _collect_sessions(
        self,
        conn: aiomysql.Connection,
        settings,
        collected_at: datetime,
    ) -> list[NormalizedQuerySample]:
        """Coleta sessoes ativas via performance_schema.threads ou fallback."""
        samples: list[NormalizedQuerySample] = []

        # Tenta via performance_schema.threads
        try:
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute(_THREADS_QUERY, (settings.collection_max_rows,)),
                    timeout=settings.collection_query_timeout_seconds,
                )
                rows = await cur.fetchall()

            for row in rows:
                (
                    _thread_id,
                    processlist_id,
                    user,
                    host,
                    db,
                    _command,
                    proc_time,
                    info,
                    wait_event_name,
                ) = row

                if not info:
                    continue

                sql_masked = mask_sql_literals(info)
                sql_hash = hashlib.sha256(sql_masked.encode()).hexdigest()
                sql_display = mask_sql_literals_for_display(info)
                wait_cat = map_wait_event("mysql", wait_event_name)

                samples.append(
                    NormalizedQuerySample(
                        time=collected_at,
                        instance_id=self._instance_id,
                        engine="mysql",
                        sql_hash=sql_hash,
                        sql_text_masked=sql_display,
                        sql_text_full_masked=sql_display,
                        wait_type=wait_cat,
                        wait_event_native=wait_event_name,
                        duration_ms=int(proc_time * 1000) if proc_time is not None else None,
                        session_id=str(processlist_id) if processlist_id else None,
                        database_name=db,
                        db_user=user,
                        program=host,
                        machine=host,
                    )
                )
            return samples

        except Exception as exc:
            logger.debug("Fallback para information_schema.PROCESSLIST: %s", exc)

        # Fallback: information_schema.PROCESSLIST (sem wait events)
        try:
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute(_PROCESSLIST_FALLBACK_QUERY, (settings.collection_max_rows,)),
                    timeout=settings.collection_query_timeout_seconds,
                )
                rows = await cur.fetchall()

            from app.normalizers.wait_taxonomy import WaitCategory

            for row in rows:
                proc_id, user, host, db, _command, proc_time, info = row
                if not info:
                    continue

                sql_masked = mask_sql_literals(info)
                sql_hash = hashlib.sha256(sql_masked.encode()).hexdigest()
                sql_display = mask_sql_literals_for_display(info)

                samples.append(
                    NormalizedQuerySample(
                        time=collected_at,
                        instance_id=self._instance_id,
                        engine="mysql",
                        sql_hash=sql_hash,
                        sql_text_masked=sql_display,
                        sql_text_full_masked=sql_display,
                        wait_type=WaitCategory.CPU,
                        wait_event_native=None,
                        duration_ms=int(proc_time * 1000) if proc_time is not None else None,
                        session_id=str(proc_id) if proc_id else None,
                        database_name=db,
                        db_user=user,
                        program=host,
                        machine=host,
                    )
                )
        except Exception as exc:
            logger.warning("Erro ao coletar sessoes MySQL (fallback): %s", exc)

        return samples

    async def _collect_metrics(
        self,
        conn: aiomysql.Connection,
        settings,
        collected_at: datetime,
    ) -> list[NormalizedMetricSample]:
        """Coleta metricas via SHOW GLOBAL STATUS."""
        metrics: list[NormalizedMetricSample] = []
        try:
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute("SHOW GLOBAL STATUS"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                rows = await cur.fetchall()

            status_map: dict[str, str] = {row[0]: row[1] for row in rows}
            for status_name, canonical_name in _STATUS_METRIC_MAP.items():
                raw_value = status_map.get(status_name)
                if raw_value is None:
                    continue
                try:
                    value = float(raw_value)
                except (ValueError, TypeError):
                    continue
                metrics.append(
                    NormalizedMetricSample(
                        time=collected_at,
                        target_type="instance",
                        target_id=self._instance_id,
                        metric_name=canonical_name,
                        value=value,
                        labels={"engine": self._engine_variant},
                    )
                )
        except Exception as exc:
            logger.warning("Erro ao coletar SHOW GLOBAL STATUS MySQL: %s", exc)

        return metrics

    async def collect_replication_lag_metrics(self) -> list[NormalizedMetricSample]:
        """Coleta lag de replicacao via SHOW REPLICA STATUS ou SHOW SLAVE STATUS."""
        settings = get_settings()
        metrics: list[NormalizedMetricSample] = []
        collected_at = datetime.now(UTC)

        try:
            conn = await self._get_connection()
            async with conn.cursor(aiomysql.DictCursor) as cur:
                # MySQL 8+ usa SHOW REPLICA STATUS
                try:
                    await asyncio.wait_for(
                        cur.execute("SHOW REPLICA STATUS"),
                        timeout=settings.collection_query_timeout_seconds,
                    )
                except Exception:
                    # Fallback: MySQL < 8 / MariaDB
                    await asyncio.wait_for(
                        cur.execute("SHOW SLAVE STATUS"),
                        timeout=settings.collection_query_timeout_seconds,
                    )

                row = await cur.fetchone()
                if row is None:
                    return []

                lag = row.get("Seconds_Behind_Master") or row.get("Seconds_Behind_Source")
                if lag is None:
                    return []

                metrics.append(
                    NormalizedMetricSample(
                        time=collected_at,
                        target_type="instance",
                        target_id=self._instance_id,
                        metric_name=MYSQL_REPLICATION_LAG_SECONDS,
                        value=float(lag),
                        labels={"engine": self._engine_variant},
                    )
                )
        except Exception as exc:
            logger.debug("Sem replicacao ou erro em SHOW REPLICA STATUS: %s", exc)

        return metrics

    async def get_engine_start_time(self) -> datetime | None:
        """Retorna quando o MySQL foi iniciado, via SHOW STATUS LIKE 'Uptime'."""
        settings = get_settings()
        try:
            conn = await self._get_connection()
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute("SHOW STATUS LIKE 'Uptime'"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                row = await cur.fetchone()
                if row:
                    uptime_seconds = int(row[1])
                    return datetime.now(UTC) - timedelta(seconds=uptime_seconds)
        except Exception as exc:
            logger.warning("Erro ao obter Uptime MySQL: %s", exc)
        return None

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        """Executa query escalar e retorna o primeiro valor como float."""
        try:
            conn = await self._get_connection()
            async with conn.cursor() as cur:
                await asyncio.wait_for(
                    cur.execute(sql),
                    timeout=timeout_seconds,
                )
                row = await cur.fetchone()
                if row and row[0] is not None:
                    return float(row[0])
        except Exception as exc:
            logger.warning("Erro em run_scalar_query MySQL: %s", exc)
        return None

    async def close(self) -> None:
        """Fecha a conexao."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    # Metodos nao aplicaveis ao MySQL — retornam lista vazia ou no-op

    async def collect_deadlock_events(self) -> list[NormalizedDeadlockEvent]:
        return []

    async def collect_transaction_error_events(self) -> list[NormalizedConfirmedTransactionError]:
        return []

    async def ensure_transaction_error_capture(self) -> bool:
        return True

    async def scan_plan_compile_timeouts(self) -> list[NormalizedPlanCompileTimeoutFinding]:
        return []

    async def scan_plan_history(self) -> list[NormalizedPlanSnapshot]:
        return []

    async def collect_freespace_metrics(self) -> list[NormalizedMetricSample]:
        return []

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        return []

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        return []

    # Nao exposto pelo MySQL — retornar sempre vazio
    async def _collect_blocking_events(self) -> list[NormalizedBlockingEvent]:
        return []

    async def _collect_transaction_events(self) -> list[NormalizedTransactionEvent]:
        return []
