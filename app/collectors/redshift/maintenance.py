"""Varreduras de manutencao on-demand para Amazon Redshift.

Redshift nao tem indices (sort keys/dist keys no lugar — `no_indexes_concept`) nem
particionamento declarativo. Estatisticas: `SVV_TABLE_INFO.stats_off` (0 = atual, 100 =
totalmente desatualizada) — e o proprio % de desatualizacao calculado pelo Redshift.
Compressao: inventario de encoding por tabela (`SVV_TABLE_INFO.encoded`). Memoria: passos
de consulta que foram para disco (`SVL_QUERY_SUMMARY.is_diskbased`) nas ultimas 24h + fila
WLM atual (`STV_WLM_QUERY_STATE`).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from typing import Any

import asyncpg

from app.collectors.postgresql.maintenance import (
    ConnectFn,
    DbScan,
    _fetch,
    qualified,
    scan_databases,
    sql_hash,
)
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals_for_display
from app.normalizers.models import (
    NormalizedCompressionItem,
    NormalizedCompressionReport,
    NormalizedMemoryGrantActive,
    NormalizedMemoryGrantReport,
    NormalizedMemoryGrantSummary,
    NormalizedMemoryGrantTopQuery,
    NormalizedScanMeta,
    NormalizedStatisticsHealthItem,
    NormalizedStatisticsHealthReport,
)

logger = get_logger(__name__)

BOOTSTRAP_DB = "dev"
# Redshift dispara auto-analyze acima de ~10% de desatualizacao.
STATS_OFF_STALE = 10.0
STATS_OFF_CRITICAL = 50.0

_LIST_DATABASES_SQL = """
    SELECT datname FROM pg_database
    WHERE datname NOT IN ('padb_harvest', 'template0', 'template1', 'sys:internal')
      AND datallowconn
    ORDER BY datname
"""


def list_fn(connect: ConnectFn) -> Callable[[], Awaitable[list[str]]]:
    async def _list() -> list[str]:
        conn = await connect(BOOTSTRAP_DB)
        try:
            rows = await _fetch(conn, _LIST_DATABASES_SQL)
        finally:
            await conn.close()
        return [str(r["datname"]) for r in rows]

    return _list


def classify_stats_off(stats_off: float | None) -> dict[str, Any]:
    if stats_off is None:
        return {"is_stale": True, "severity": "warning"}
    if stats_off >= STATS_OFF_CRITICAL:
        return {"is_stale": True, "severity": "critical"}
    if stats_off > STATS_OFF_STALE:
        return {"is_stale": True, "severity": "warning"}
    return {"is_stale": False, "severity": "info"}


# ── Estatisticas ───────────────────────────────────────────────────────────────────────────

_STATISTICS_SQL = """
    SELECT "schema" AS schema_name, "table" AS table_name, table_id,
           tbl_rows, estimated_visible_rows, stats_off, size AS size_mb, unsorted
    FROM svv_table_info
    ORDER BY stats_off DESC
    LIMIT $1
"""

_LAST_ANALYZE_SQL = """
    SELECT table_id, max(endtime) AS last_analyze
    FROM stl_analyze
    WHERE status = 'Full'
    GROUP BY table_id
"""


async def redshift_statistics_for_db(conn: asyncpg.Connection, database: str) -> DbScan:
    settings = get_settings()
    scan = DbScan(notes={"stats_off_from_svv_table_info"})
    rows = await _fetch(conn, _STATISTICS_SQL, settings.maintenance_max_rows)
    try:
        last = {int(r["table_id"]): r["last_analyze"] for r in await _fetch(conn, _LAST_ANALYZE_SQL)}
    except asyncpg.PostgresError:
        # STL so mostra tudo para superuser (e retem ~7 dias) — sem ele, so `stats_off`.
        last = {}
    now = datetime.now(UTC)
    for r in rows:
        stats_off = float(r["stats_off"]) if r["stats_off"] is not None else None
        last_updated = last.get(int(r["table_id"]))
        if last_updated is not None and last_updated.tzinfo is None:
            last_updated = last_updated.replace(tzinfo=UTC)
        rows_count = int(r["tbl_rows"] or 0)
        scan.items.append(
            NormalizedStatisticsHealthItem(
                database_name=database,
                schema_name=r["schema_name"],
                table_name=r["table_name"],
                last_updated=last_updated,
                row_count=rows_count,
                modified_pct=stats_off,
                days_since_update=(
                    round((now - last_updated).total_seconds() / 86400.0, 1)
                    if last_updated else None
                ),
                recommendation=f"ANALYZE {qualified(r['schema_name'], r['table_name'])};",
                detail={
                    "stats_off": stats_off,
                    "estimated_visible_rows": r["estimated_visible_rows"],
                    "size_mb": r["size_mb"],
                    "unsorted_pct": r["unsorted"],
                },
                **classify_stats_off(stats_off),
            )
        )
    return scan


async def redshift_statistics(
    connect: ConnectFn, database: str | None
) -> NormalizedStatisticsHealthReport:
    outcome = await scan_databases(
        connect, BOOTSTRAP_DB, database, redshift_statistics_for_db,
        method="svv_table_info.stats_off", list_fn=list_fn(connect),
    )
    items = sorted(outcome.items, key=lambda i: -(i.modified_pct or 0))
    return NormalizedStatisticsHealthReport(items=items, meta=outcome.meta)


# ── Compressao (encoding) ──────────────────────────────────────────────────────────────────

_COMPRESSION_SQL = """
    SELECT "schema" AS schema_name, "table" AS table_name, encoded, size AS size_mb,
           tbl_rows, diststyle
    FROM svv_table_info
    ORDER BY size DESC
    LIMIT $1
"""


async def redshift_compression_for_db(
    conn: asyncpg.Connection, database: str, max_objects: int
) -> DbScan:
    scan = DbScan(notes={"compression_inventory_only"})
    for r in await _fetch(conn, _COMPRESSION_SQL, max_objects):
        encoded = (r["encoded"] or "").strip()
        not_encoded = encoded.upper().startswith("N")
        table = qualified(r["schema_name"], r["table_name"])
        scan.items.append(
            NormalizedCompressionItem(
                database_name=database,
                schema_name=r["schema_name"],
                table_name=r["table_name"],
                current_compression=encoded or None,
                requested_compression="AUTO" if not_encoded else None,
                current_size_kb=int(r["size_mb"] or 0) * 1024,
                # ANALYZE COMPRESSION so produz um relatorio (amostra a tabela, nao altera nada);
                # ALTER ... ENCODE AUTO deixa o Redshift escolher o encoding de cada coluna.
                command=(
                    f"ANALYZE COMPRESSION {table};\nALTER TABLE {table} ALTER ENCODE AUTO;"
                    if not_encoded else None
                ),
                detail={"tbl_rows": r["tbl_rows"], "diststyle": r["diststyle"]},
            )
        )
    return scan


async def redshift_compression(
    connect: ConnectFn, database: str | None, max_objects: int
) -> NormalizedCompressionReport:
    outcome = await scan_databases(
        connect, BOOTSTRAP_DB, database,
        lambda conn, db: redshift_compression_for_db(conn, db, max_objects),
        method="svv_table_info.encoded", list_fn=list_fn(connect),
    )
    items = sorted(outcome.items, key=lambda i: -(i.current_size_kb or 0))
    return NormalizedCompressionReport(items=items, meta=outcome.meta)


# ── Memoria (spill para disco + fila WLM) ──────────────────────────────────────────────────

_DISK_BASED_SQL = """
    SELECT q.query, TRIM(q.querytxt) AS querytxt, TRIM(q.database) AS database_name,
           q.starttime, MAX(s.workmem) AS max_workmem,
           SUM(CASE WHEN s.is_diskbased = 't' THEN 1 ELSE 0 END) AS diskbased_steps
    FROM stl_query q
    JOIN svl_query_summary s ON s.query = q.query
    WHERE q.starttime > dateadd(day, -1, getdate())
    GROUP BY q.query, q.querytxt, q.database, q.starttime
    HAVING SUM(CASE WHEN s.is_diskbased = 't' THEN 1 ELSE 0 END) > 0
    ORDER BY max_workmem DESC
    LIMIT 200
"""

_WLM_STATE_SQL = """
    SELECT w.query, w.state, w.queue_time, w.exec_time, w.service_class,
           TRIM(i.text) AS text, i.pid, TRIM(u.usename) AS usename
    FROM stv_wlm_query_state w
    LEFT JOIN stv_inflight i ON i.query = w.query
    LEFT JOIN pg_user u ON u.usesysid = i.userid
    ORDER BY w.queue_time DESC
    LIMIT 50
"""


def aggregate_disk_based(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    acc: dict[str, dict[str, Any]] = {}
    for r in rows:
        text = r["querytxt"] or ""
        key = sql_hash(text)
        a = acc.setdefault(
            key, {"hash": key, "text": text, "database_name": r["database_name"],
                  "executions": 0, "steps": 0, "workmem_max": 0, "workmem_sum": 0, "last": None},
        )
        mem = int(r["max_workmem"] or 0)
        a["executions"] += 1
        a["steps"] += int(r["diskbased_steps"] or 0)
        a["workmem_max"] = max(a["workmem_max"], mem)
        a["workmem_sum"] += mem
        if a["last"] is None or (r["starttime"] and r["starttime"] > a["last"]):
            a["last"] = r["starttime"]
    out = sorted(acc.values(), key=lambda a: -a["workmem_max"])
    return out


async def redshift_memory_grants(connect: ConnectFn) -> NormalizedMemoryGrantReport:
    meta = NormalizedScanMeta(method="svl_query_summary.is_diskbased")
    report = NormalizedMemoryGrantReport(meta=meta)
    conn = await connect(BOOTSTRAP_DB)
    try:
        try:
            disk_rows = [dict(r) for r in await _fetch(conn, _DISK_BASED_SQL)]
        except asyncpg.InsufficientPrivilegeError:
            meta.notes.append("permission_denied")
            disk_rows = []
        try:
            wlm_rows = await _fetch(conn, _WLM_STATE_SQL)
        except asyncpg.PostgresError:
            meta.notes.append("permission_denied")
            wlm_rows = []
    finally:
        await conn.close()

    waiting = 0
    for r in wlm_rows:
        text = r["text"] or ""
        is_waiting = str(r["state"] or "").strip().lower().startswith("queued")
        waiting += int(is_waiting)
        report.active.append(
            NormalizedMemoryGrantActive(
                session_id=str(r["pid"] if r["pid"] is not None else r["query"]),
                login_name=r["usename"],
                sql_hash=sql_hash(text) if text else None,
                sql_text_masked=mask_sql_literals_for_display(text) if text else None,
                is_waiting=is_waiting,
                # STV_WLM_QUERY_STATE em microssegundos.
                wait_time_ms=int((r["queue_time"] or 0) / 1000),
                detail={"query_id": r["query"], "state": str(r["state"] or "").strip(),
                        "exec_time_ms": int((r["exec_time"] or 0) / 1000),
                        "service_class": r["service_class"]},
            )
        )

    for a in aggregate_disk_based(disk_rows)[:25]:
        report.top_queries.append(
            NormalizedMemoryGrantTopQuery(
                sql_hash=a["hash"],
                sql_text_masked=mask_sql_literals_for_display(a["text"]),
                database_name=a["database_name"],
                executions=a["executions"],
                max_grant_kb=round(a["workmem_max"] / 1024, 1),
                avg_grant_kb=round(a["workmem_sum"] / a["executions"] / 1024, 1),
                spill_count=a["steps"],
                last_execution=a["last"],
                severity="warning",
                detail={"window_hours": 24},
            )
        )
    report.summary = NormalizedMemoryGrantSummary(
        granted_requests=len(report.active) - waiting, waiting_requests=waiting
    )
    meta.notes = sorted(set(meta.notes))
    return report
