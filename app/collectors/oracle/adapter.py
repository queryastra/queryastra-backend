"""Adaptador Oracle. Fonte de verdade: `Docs/collector-queries.md`.

Usa python-oracledb >= 2.0 em thin mode — nenhuma biblioteca cliente Oracle necessária.
Thin mode: oracledb.init_oracle_client() NÃO é chamado.

`check_capabilities()` abre conexão com timeout e checa acesso a V$SESSION e V$SQL.

`collect()` executa um ciclo somente leitura:
    - V$SESSION WHERE status='ACTIVE' AND type='USER' — sessão/query ativa, normalizada
      em NormalizedQuerySample.
    - V$SESSION WHERE blocking_session IS NOT NULL — detecção de bloqueio.
    - V$SYSSTAT / V$SGAINFO / V$PGASTAT — métricas de recurso (sessions, SGA, PGA).

Permissão mínima: CREATE SESSION + SELECT_CATALOG_ROLE (ou GRANT SELECT ON V$SESSION,
V$SQL, V$SYSSTAT, V$SGAINFO, V$PGASTAT).

Formato do host em ConnectionParams: "hostname/service_name" (ex.: "myhost/ORCL").
Se não houver "/", usa service_name "orcl" como padrão.
"""

from __future__ import annotations

import asyncio
import hashlib
import time
from datetime import UTC, datetime
from uuid import UUID

import oracledb

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    CONNECTIVITY,
    ORACLE_QUERY_STATS,
    ORACLE_SESSION_ACTIVITY,
)
from app.collectors.collector_mixin import CollectorNoOpMixin
from app.collectors.connection import ConnectionParams
from app.collectors.oracle import maintenance as oracle_maintenance
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals, mask_sql_literals_for_display
from app.normalizers.metric_names import (
    ORACLE_ACTIVE_SESSION_COUNT,
    ORACLE_DEADLOCKS_TOTAL,
    ORACLE_PGA_AGGREGATE_BYTES,
    ORACLE_SESSIONS_TOTAL,
    ORACLE_SGA_TOTAL_BYTES,
)
from app.normalizers.models import (
    NormalizedBlockingEvent,
    NormalizedCompressionReport,
    NormalizedIndexHealthReport,
    NormalizedMemoryGrantReport,
    NormalizedMetricSample,
    NormalizedPartitionReport,
    NormalizedPlanSnapshot,
    NormalizedQuerySample,
    NormalizedStatisticsHealthReport,
)
from app.normalizers.wait_taxonomy import map_wait_event

logger = get_logger(__name__)

_SESSION_ACTIVITY_QUERY = """
    SELECT s.sid, s.serial#, s.username, s.machine, s.program,
           s.status, s.event, s.wait_class, s.seconds_in_wait,
           s.blocking_session, s.sql_id, s.logon_time,
           NVL(q.sql_fulltext, TO_CLOB('')) AS sql_text,
           s.service_name AS db_name
    FROM v$session s
    LEFT JOIN v$sql q ON s.sql_id = q.sql_id AND s.sql_child_number = q.child_number
    WHERE s.status = 'ACTIVE' AND s.type = 'USER' AND s.username IS NOT NULL
"""

_SGA_QUERY = "SELECT name, bytes FROM v$sgainfo WHERE name = 'Total SGA'"
_PGA_QUERY = "SELECT value FROM v$pgastat WHERE name = 'total PGA allocated'"
_SESSIONS_COUNT_QUERY = "SELECT COUNT(*) AS cnt FROM v$session WHERE type = 'USER'"
_DEADLOCKS_QUERY = "SELECT value FROM v$sysstat WHERE name = 'enqueue deadlocks'"


def _parse_oracle_host(params: ConnectionParams) -> tuple[str, str]:
    """Extrai (oracle_host, service_name) de ConnectionParams.host.

    Formatos suportados:
    - "hostname" → ("hostname", "orcl")
    - "hostname/service" → ("hostname", "service")
    """
    if "/" in params.host:
        host, service = params.host.split("/", 1)
        return host.strip(), service.strip()
    return params.host, "orcl"


class OracleCollector(CollectorNoOpMixin):
    """Implementa `app.collectors.base.Collector` para instâncias Oracle Database."""

    def __init__(self, connection: ConnectionParams, instance_id: UUID | None = None) -> None:
        self._connection_params = connection
        self._instance_id = instance_id

    async def _connect(self) -> oracledb.AsyncConnection:
        params = self._connection_params
        oracle_host, service_name = _parse_oracle_host(params)
        return await oracledb.connect_async(
            user=params.username or "",
            password=params.secret_value or "",
            host=oracle_host,
            port=params.port,
            service_name=service_name,
        )

    async def _fetch_rows(
        self, conn: oracledb.AsyncConnection, query: str, timeout_seconds: float
    ) -> list[dict]:
        cursor = conn.cursor()
        try:
            await asyncio.wait_for(cursor.execute(query), timeout=timeout_seconds)
            cols = [d[0].lower().replace("#", "_num") for d in cursor.description]
            rows = await asyncio.wait_for(cursor.fetchall(), timeout=timeout_seconds)
            return [dict(zip(cols, row, strict=False)) for row in rows]
        finally:
            cursor.close()

    async def check_capabilities(self) -> list[CapabilityResult]:
        try:
            conn = await asyncio.wait_for(
                self._connect(),
                timeout=self._connection_params.connect_timeout_seconds,
            )
        except Exception as exc:
            return [
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=f"Falha ao conectar: {type(exc).__name__}",
                )
            ]

        settings = get_settings()
        results: list[CapabilityResult] = []
        try:
            try:
                cursor = conn.cursor()
                await asyncio.wait_for(
                    cursor.execute("SELECT COUNT(*) FROM v$session WHERE rownum <= 1"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                cursor.close()
                results.append(
                    CapabilityResult(
                        capability=ORACLE_SESSION_ACTIVITY,
                        status=CapabilityStatus.FULL,
                    )
                )
            except Exception as exc:
                results.append(
                    CapabilityResult(
                        capability=ORACLE_SESSION_ACTIVITY,
                        status=CapabilityStatus.UNAVAILABLE,
                        reason=f"Sem acesso a V$SESSION: {type(exc).__name__}",
                    )
                )

            try:
                cursor = conn.cursor()
                await asyncio.wait_for(
                    cursor.execute("SELECT COUNT(*) FROM v$sql WHERE rownum <= 1"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                cursor.close()
                results.append(
                    CapabilityResult(
                        capability=ORACLE_QUERY_STATS,
                        status=CapabilityStatus.FULL,
                    )
                )
            except Exception as exc:
                results.append(
                    CapabilityResult(
                        capability=ORACLE_QUERY_STATS,
                        status=CapabilityStatus.DEGRADED,
                        reason=f"Sem acesso a V$SQL: {type(exc).__name__}",
                    )
                )
        finally:
            await conn.close()

        return results

    async def collect(
        self,
        *,
        sleeping_min_duration_ms: int | None = None,
        sleeping_database_overrides_ms: dict[str, int] | None = None,
    ) -> CollectionResult:
        if self._instance_id is None:
            raise ValueError(
                "OracleCollector.collect requer instance_id de uma instância já cadastrada."
            )

        start = time.perf_counter()
        settings = get_settings()

        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            collected_at = datetime.now(UTC)
            session_rows = await self._fetch_rows(
                conn, _SESSION_ACTIVITY_QUERY, settings.collection_query_timeout_seconds
            )
            session_rows = session_rows[: settings.collection_max_rows]

            try:
                sga_rows = await self._fetch_rows(
                    conn, _SGA_QUERY, settings.collection_query_timeout_seconds
                )
            except Exception:
                sga_rows = []
            try:
                pga_rows = await self._fetch_rows(
                    conn, _PGA_QUERY, settings.collection_query_timeout_seconds
                )
            except Exception:
                pga_rows = []
            try:
                sessions_rows = await self._fetch_rows(
                    conn, _SESSIONS_COUNT_QUERY, settings.collection_query_timeout_seconds
                )
            except Exception:
                sessions_rows = []
            try:
                deadlock_rows = await self._fetch_rows(
                    conn, _DEADLOCKS_QUERY, settings.collection_query_timeout_seconds
                )
            except Exception:
                deadlock_rows = []
        finally:
            await conn.close()

        query_samples = self._build_query_samples(session_rows, collected_at)
        blocking_events = self._build_blocking_events(session_rows, collected_at)
        metric_samples = self._build_metric_samples(
            session_rows, sga_rows, pga_rows, sessions_rows, deadlock_rows, collected_at
        )

        duration_ms = int((time.perf_counter() - start) * 1000)
        return CollectionResult(
            instance_id=self._instance_id,
            collected_at=collected_at,
            duration_ms=duration_ms,
            metric_samples=metric_samples,
            query_samples=query_samples,
            blocking_events=blocking_events,
        )

    def _build_query_samples(
        self, session_rows: list[dict], collected_at: datetime
    ) -> list[NormalizedQuerySample]:
        assert self._instance_id is not None
        instance_id: UUID = self._instance_id
        samples: list[NormalizedQuerySample] = []
        for row in session_rows:
            sql_text_raw = row.get("sql_text") or ""
            if hasattr(sql_text_raw, "read"):
                sql_text_raw = sql_text_raw.read()
            sql_text_raw = str(sql_text_raw).strip()
            if not sql_text_raw:
                continue
            sql_hash = hashlib.sha256(mask_sql_literals(sql_text_raw).encode()).hexdigest()
            masked_display = mask_sql_literals_for_display(sql_text_raw)
            wait_class = row.get("wait_class") or ""
            samples.append(
                NormalizedQuerySample(
                    time=collected_at,
                    instance_id=instance_id,
                    engine="oracle",
                    sql_hash=sql_hash,
                    sql_text_masked=masked_display,
                    sql_text_full_masked=masked_display,
                    wait_type=map_wait_event("oracle", wait_class),
                    wait_event_native=row.get("event"),
                    wait_time_ms=None,
                    duration_ms=None,
                    session_id=f"{row.get('sid', '')}/{row.get('serial_num', '')}",
                    db_user=row.get("username"),
                    program=row.get("program"),
                    database_name=row.get("db_name"),
                    machine=row.get("machine"),
                )
            )
        return samples

    def _build_blocking_events(
        self, session_rows: list[dict], collected_at: datetime
    ) -> list[NormalizedBlockingEvent]:
        assert self._instance_id is not None
        instance_id: UUID = self._instance_id
        session_by_sid = {row["sid"]: row for row in session_rows}
        events: list[NormalizedBlockingEvent] = []
        for row in session_rows:
            blocker_sid = row.get("blocking_session")
            if not blocker_sid:
                continue
            blocker = session_by_sid.get(blocker_sid, {})
            events.append(
                NormalizedBlockingEvent(
                    time=collected_at,
                    instance_id=instance_id,
                    blocker_session_id=str(blocker_sid),
                    blocked_session_id=str(row["sid"]),
                    blocker_sql_hash=None,
                    blocked_sql_hash=None,
                    wait_time_ms=int(row.get("seconds_in_wait") or 0) * 1000,
                    blocker_db_user=blocker.get("username"),
                    blocker_program=blocker.get("program"),
                    blocker_machine=blocker.get("machine"),
                    blocker_wait_event_native=blocker.get("event"),
                    blocked_db_user=row.get("username"),
                    blocked_program=row.get("program"),
                    blocked_machine=row.get("machine"),
                    blocked_wait_event_native=row.get("event"),
                    blocked_wait_resource=None,
                    blocked_object=None,
                    blocker_transaction_id=str(blocker_sid),
                    blocked_transaction_id=str(row["sid"]),
                )
            )
        return events

    def _build_metric_samples(
        self,
        session_rows: list[dict],
        sga_rows: list[dict],
        pga_rows: list[dict],
        sessions_rows: list[dict],
        deadlock_rows: list[dict],
        collected_at: datetime,
    ) -> list[NormalizedMetricSample]:
        assert self._instance_id is not None
        instance_id: UUID = self._instance_id

        def sample(metric_name: str, value: float | None) -> NormalizedMetricSample | None:
            if value is None:
                return None
            return NormalizedMetricSample(
                time=collected_at,
                target_type="instance",
                target_id=instance_id,
                metric_name=metric_name,
                value=float(value),
                labels={},
            )

        candidates: list[NormalizedMetricSample | None] = [
            sample(ORACLE_ACTIVE_SESSION_COUNT, len(session_rows)),
        ]
        if sessions_rows:
            candidates.append(sample(ORACLE_SESSIONS_TOTAL, sessions_rows[0].get("cnt")))
        if sga_rows:
            candidates.append(sample(ORACLE_SGA_TOTAL_BYTES, sga_rows[0].get("bytes")))
        if pga_rows:
            candidates.append(sample(ORACLE_PGA_AGGREGATE_BYTES, pga_rows[0].get("value")))
        if deadlock_rows:
            candidates.append(sample(ORACLE_DEADLOCKS_TOTAL, deadlock_rows[0].get("value")))

        return [c for c in candidates if c is not None]

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            cursor = conn.cursor()
            await asyncio.wait_for(cursor.execute(sql), timeout=timeout_seconds)
            row = await cursor.fetchone()
            cursor.close()
        finally:
            await conn.close()
        if row is None:
            return None
        try:
            return float(row[0])
        except (TypeError, ValueError):
            return None

    async def explain_queries(
        self, queries: list[tuple[str, str]]
    ) -> list[NormalizedPlanSnapshot]:
        """Coleta planos de execução diretamente de V$SQL_PLAN para as queries informadas.

        Para cada sql_hash, procura o sql_id correspondente em V$SQL via hash SHA-256 do texto
        mascarado. Quando encontrado, formata as linhas de V$SQL_PLAN como texto tabular e
        retorna o snapshot. Silencia erros por query e continua para as demais.
        """
        import hashlib

        if not queries or self._instance_id is None:
            return []

        try:
            conn = await asyncio.wait_for(
                self._connect(), timeout=self._connection_params.connect_timeout_seconds
            )
        except Exception:
            return []

        _V_SQL_QUERY = """
            SELECT sql_id, plan_hash_value, sql_text
            FROM V$SQL
            WHERE executions > 0
              AND sql_text IS NOT NULL
            ORDER BY elapsed_time DESC
            FETCH FIRST 200 ROWS ONLY
        """
        _V_SQL_PLAN_QUERY = """
            SELECT id, operation, options, object_name, cost, cardinality, bytes
            FROM V$SQL_PLAN
            WHERE sql_id = '{sql_id}' AND plan_hash_value = {phv}
            ORDER BY id
        """

        results: list[NormalizedPlanSnapshot] = []
        try:
            # Constrói mapa sql_hash -> (sql_id, plan_hash_value) via V$SQL
            from app.masking.sql_masking import mask_sql_literals

            sql_rows = await self._fetch_rows(conn, _V_SQL_QUERY, timeout_seconds=10.0)
            hash_to_plan: dict[str, tuple[str, int]] = {}
            for row in sql_rows:
                raw_text = row.get("sql_text") or ""
                computed_hash = hashlib.sha256(
                    mask_sql_literals(raw_text).encode()
                ).hexdigest()
                if computed_hash not in hash_to_plan:
                    hash_to_plan[computed_hash] = (row["sql_id"], row["plan_hash_value"])

            for sql_hash, sql_text in queries[:40]:
                plan_info = hash_to_plan.get(sql_hash)
                if not plan_info:
                    continue
                sql_id, phv = plan_info
                try:
                    plan_rows = await self._fetch_rows(
                        conn,
                        _V_SQL_PLAN_QUERY.format(sql_id=sql_id, phv=phv),
                        timeout_seconds=5.0,
                    )
                except Exception:
                    continue

                if not plan_rows:
                    continue

                # Formata as linhas como tabela de texto
                header = "| {:>4} | {:<30} | {:<20} | {:<20} | {:>8} | {:>10} | {:>10} |".format(
                    "ID", "OPERATION", "OPTIONS", "OBJECT", "COST", "ROWS", "BYTES"
                )
                sep = "-" * len(header)
                lines = [sep, header, sep]
                for r in plan_rows:
                    lines.append(
                        "| {:>4} | {:<30} | {:<20} | {:<20} | {:>8} | {:>10} | {:>10} |".format(
                            r.get("id") or 0,
                            (r.get("operation") or "")[:30],
                            (r.get("options") or "")[:20],
                            (r.get("object_name") or "")[:20],
                            r.get("cost") or 0,
                            r.get("cardinality") or 0,
                            r.get("bytes") or 0,
                        )
                    )
                lines.append(sep)
                plan_text = "\n".join(lines)
                plan_hash = hashlib.sha256(plan_text.encode()).hexdigest()
                results.append(
                    NormalizedPlanSnapshot(
                        instance_id=self._instance_id,
                        sql_hash=sql_hash,
                        plan_hash=plan_hash,
                        plan_xml_masked=plan_text,
                        plan_format="oracle_text",
                    )
                )
        except Exception:
            pass
        finally:
            try:
                await conn.close()
            except Exception:
                pass

        return results

    # ── Varreduras de manutencao on-demand (app/collectors/oracle/maintenance.py) ─────────
    # Uma conexao = um banco/PDB: `list_databases` vazio e `database` e ignorado.

    async def _with_connection(self, call):  # noqa: ANN001, ANN202
        conn = await asyncio.wait_for(
            self._connect(), timeout=self._connection_params.connect_timeout_seconds
        )
        try:
            return await call(conn)
        finally:
            await conn.close()

    async def list_databases(self) -> list[str]:
        return []

    async def collect_index_health(
        self, database: str | None = None
    ) -> NormalizedIndexHealthReport:
        return await self._with_connection(oracle_maintenance.collect_index_health)

    async def collect_statistics_health(
        self, database: str | None = None
    ) -> NormalizedStatisticsHealthReport:
        return await self._with_connection(oracle_maintenance.collect_statistics_health)

    async def collect_memory_grants(self) -> NormalizedMemoryGrantReport:
        return await self._with_connection(oracle_maintenance.collect_memory_grants)

    async def estimate_compression(
        self, database: str | None, compression: str, max_objects: int
    ) -> NormalizedCompressionReport:
        return await self._with_connection(
            lambda conn: oracle_maintenance.estimate_compression(conn, max_objects)
        )

    async def collect_partitions(self, database: str | None = None) -> NormalizedPartitionReport:
        return await self._with_connection(oracle_maintenance.collect_partitions)

    async def close(self) -> None:
        logger.info("close chamado para instância oracle (instance_id=%s)", self._instance_id)
