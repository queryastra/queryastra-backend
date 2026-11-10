"""Varreduras de manutencao on-demand para CockroachDB (catalogos `crdb_internal`).

CockroachDB armazena dados em LSM (Pebble): nao ha fragmentacao de indice no sentido B-tree —
a compactacao e automatica (`fragmentation_not_applicable`). Indices sem uso vem de
`crdb_internal.index_usage_statistics`, duplicados/sobrepostos de `crdb_internal.index_columns`,
estatisticas de `system.table_statistics` (exige VIEWSYSTEMTABLE/admin — degrada para
`permission_denied`), memoria das estatisticas amostradas de execucao
(`crdb_internal.statement_statistics`) e particoes de `crdb_internal.partitions`.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import asyncpg

from app.collectors.postgresql.maintenance import (
    ConnectFn,
    DbScan,
    IndexDef,
    _fetch,
    quote_ident,
    redundant_items,
    scan_databases,
    sort_index_items,
    sql_hash,
)
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals_for_display
from app.normalizers.models import (
    NormalizedIndexHealthItem,
    NormalizedIndexHealthReport,
    NormalizedMemoryGrantActive,
    NormalizedMemoryGrantReport,
    NormalizedMemoryGrantSummary,
    NormalizedMemoryGrantTopQuery,
    NormalizedPartitionInfo,
    NormalizedPartitionReport,
    NormalizedScanMeta,
    NormalizedStatisticsHealthItem,
    NormalizedStatisticsHealthReport,
)

logger = get_logger(__name__)

BOOTSTRAP_DB = "defaultdb"
STATS_STALE_DAYS = 7
TOP_QUERIES_LIMIT = 25

_LIST_DATABASES_SQL = """
    SELECT database_name FROM [SHOW DATABASES]
    WHERE database_name NOT IN ('system')
    ORDER BY database_name
"""


async def list_databases(connect: ConnectFn) -> list[str]:
    conn = await connect(BOOTSTRAP_DB)
    try:
        rows = await asyncio.wait_for(
            conn.fetch(_LIST_DATABASES_SQL),
            timeout=get_settings().collection_query_timeout_seconds,
        )
    finally:
        await conn.close()
    return [str(r["database_name"]) for r in rows]


# ── Indices ────────────────────────────────────────────────────────────────────────────────

_INDEXES_SQL = """
    SELECT t.table_id, t.schema_name, t.name AS table_name, ti.index_id, ti.index_name,
           ti.index_type, ti.is_unique, ti.is_inverted,
           COALESCE(us.total_reads, 0) AS total_reads, us.last_read,
           pi.indexdef
    FROM crdb_internal.table_indexes ti
    JOIN crdb_internal.tables t ON t.table_id = ti.descriptor_id
    LEFT JOIN crdb_internal.index_usage_statistics us
           ON us.table_id = ti.descriptor_id AND us.index_id = ti.index_id
    LEFT JOIN pg_catalog.pg_indexes pi
           ON pi.schemaname = t.schema_name AND pi.tablename = t.name
          AND pi.indexname = ti.index_name
    WHERE t.database_name = current_database() AND t.drop_time IS NULL
      AND t.schema_name NOT IN ('crdb_internal', 'information_schema', 'pg_catalog',
                                'pg_extension')
"""

_INDEX_COLUMNS_SQL = """
    SELECT ic.descriptor_id AS table_id, ic.index_id, ic.column_type, ic.column_name,
           ic.column_direction, ic.implicit
    FROM crdb_internal.index_columns ic
    JOIN crdb_internal.tables t ON t.table_id = ic.descriptor_id
    WHERE t.database_name = current_database() AND t.drop_time IS NULL
    ORDER BY ic.descriptor_id, ic.index_id, ic.column_id
"""


def build_crdb_index_defs(
    index_rows: list[dict[str, Any]], column_rows: list[dict[str, Any]]
) -> list[IndexDef]:
    """Monta `IndexDef` a partir das linhas de indices + colunas (ordem = ordem da chave)."""
    keys: dict[tuple[int, int], list[str]] = {}
    storing: dict[tuple[int, int], list[str]] = {}
    for c in column_rows:
        if c.get("implicit"):
            continue
        k = (int(c["table_id"]), int(c["index_id"]))
        if c["column_type"] == "key":
            keys.setdefault(k, []).append(f"{c['column_name']}:{c.get('column_direction') or ''}")
        elif c["column_type"] == "storing":
            storing.setdefault(k, []).append(str(c["column_name"]))

    defs: list[IndexDef] = []
    for r in index_rows:
        k = (int(r["table_id"]), int(r["index_id"]))
        indexdef = r.get("indexdef") or ""
        predicate = indexdef.split(" WHERE ", 1)[1] if " WHERE " in indexdef else ""
        key_cols = tuple(keys.get(k, []))
        is_primary = r["index_type"] == "primary"
        defs.append(
            IndexDef(
                table_key=f"{r['schema_name']}.{r['table_name']}",
                name=str(r["index_name"]),
                method="inverted" if r.get("is_inverted") else "prefix",
                keys=key_cols,
                includes=tuple(sorted(storing.get(k, []))),
                predicate=predicate,
                is_unique=bool(r["is_unique"]),
                is_primary=is_primary,
                # UNIQUE no CRDB e sempre uma constraint (DROP INDEX ... CASCADE necessario).
                backs_constraint=bool(r["is_unique"]),
                reads=int(r["total_reads"] or 0),
                definition=indexdef or None,
                extra={
                    "schema_name": str(r["schema_name"]),
                    "table_name": str(r["table_name"]),
                    "key_columns": ", ".join(c.split(":", 1)[0] for c in key_cols),
                    "included_columns": ", ".join(sorted(storing.get(k, []))) or None,
                },
            )
        )
    return defs


def _crdb_drop(schema: str, table: str, index: str) -> str:
    return f"DROP INDEX {quote_ident(schema)}.{quote_ident(table)}@{quote_ident(index)};"


async def crdb_index_health_for_db(conn: asyncpg.Connection, database: str) -> DbScan:
    scan = DbScan(notes={"fragmentation_not_applicable", "usage_stats_since_restart"})
    index_rows = [dict(r) for r in await _fetch(conn, _INDEXES_SQL)]
    column_rows = [dict(r) for r in await _fetch(conn, _INDEX_COLUMNS_SQL)]

    for r in index_rows:
        if r["index_type"] == "primary" or r["is_unique"] or int(r["total_reads"] or 0) > 0:
            continue
        scan.items.append(
            NormalizedIndexHealthItem(
                category="unused",
                database_name=database,
                schema_name=r["schema_name"],
                table_name=r["table_name"],
                index_name=r["index_name"],
                index_type="inverted" if r.get("is_inverted") else "secondary",
                reads=0,
                last_used=r["last_read"],
                severity="info",
                recommendation=_crdb_drop(r["schema_name"], r["table_name"], r["index_name"]),
                detail={"index_def": r.get("indexdef")},
            )
        )

    defs = build_crdb_index_defs(index_rows, column_rows)
    for item in redundant_items(defs, database, "{schema}.{table}@{index}"):
        item.recommendation = f"DROP INDEX {item.recommendation};"
        scan.items.append(item)
    return scan


async def crdb_index_health(
    connect: ConnectFn, database: str | None
) -> NormalizedIndexHealthReport:
    outcome = await scan_databases(
        connect, BOOTSTRAP_DB, database, crdb_index_health_for_db,
        method="crdb_internal.index_usage_statistics",
        list_fn=lambda: list_databases(connect),
    )
    return NormalizedIndexHealthReport(items=sort_index_items(outcome.items), meta=outcome.meta)


# ── Estatisticas ───────────────────────────────────────────────────────────────────────────

_TABLES_SQL = """
    SELECT t.table_id, t.schema_name, t.name AS table_name
    FROM crdb_internal.tables t
    WHERE t.database_name = current_database() AND t.drop_time IS NULL
      AND t.schema_name NOT IN ('crdb_internal', 'information_schema', 'pg_catalog',
                                'pg_extension')
"""

_TABLE_STATISTICS_SQL = """
    SELECT s."tableID" AS table_id, max(s."createdAt") AS created,
           max(s."rowCount") AS row_count
    FROM system.table_statistics s
    JOIN crdb_internal.tables t ON t.table_id = s."tableID"
    WHERE t.database_name = current_database() AND t.drop_time IS NULL
    GROUP BY s."tableID"
"""

_ROW_ESTIMATES_SQL = """
    SELECT table_id, estimated_row_count FROM crdb_internal.table_row_statistics
"""


def classify_crdb_statistics(
    last_updated: datetime | None, row_count: int | None, now: datetime
) -> dict[str, Any]:
    days = round((now - last_updated).total_seconds() / 86400.0, 1) if last_updated else None
    never = last_updated is None and (row_count or 0) > 0
    stale = never or (days is not None and days > STATS_STALE_DAYS and (row_count or 0) > 0)
    return {
        "days_since_update": days,
        "is_stale": stale,
        "severity": "warning" if stale else "info",
    }


async def crdb_statistics_for_db(conn: asyncpg.Connection, database: str) -> DbScan:
    scan = DbScan()
    tables = await _fetch(conn, _TABLES_SQL)
    try:
        stats = {int(r["table_id"]): r for r in await _fetch(conn, _TABLE_STATISTICS_SQL)}
    except asyncpg.InsufficientPrivilegeError:
        scan.notes.add("permission_denied")
        return scan
    try:
        estimates = {
            int(r["table_id"]): r["estimated_row_count"]
            for r in await _fetch(conn, _ROW_ESTIMATES_SQL)
        }
    except asyncpg.PostgresError:
        estimates = {}

    now = datetime.now(UTC)
    for t in tables:
        st = stats.get(int(t["table_id"]))
        last = st["created"] if st else None
        if last is not None and last.tzinfo is None:
            last = last.replace(tzinfo=UTC)
        rows = int(st["row_count"]) if st and st["row_count"] is not None else None
        estimate = estimates.get(int(t["table_id"]))
        info = classify_crdb_statistics(last, rows if rows is not None else estimate, now)
        scan.items.append(
            NormalizedStatisticsHealthItem(
                database_name=database,
                schema_name=t["schema_name"],
                table_name=t["table_name"],
                last_updated=last,
                row_count=rows if rows is not None else estimate,
                recommendation=(
                    f"CREATE STATISTICS __auto__ FROM "
                    f"{quote_ident(t['schema_name'])}.{quote_ident(t['table_name'])};"
                ),
                detail={"estimated_row_count": estimate},
                **info,
            )
        )
    return scan


async def crdb_statistics(
    connect: ConnectFn, database: str | None
) -> NormalizedStatisticsHealthReport:
    outcome = await scan_databases(
        connect, BOOTSTRAP_DB, database, crdb_statistics_for_db,
        method="system.table_statistics", list_fn=lambda: list_databases(connect),
    )
    items = sorted(outcome.items, key=lambda i: (not i.is_stale, -(i.days_since_update or 0)))
    return NormalizedStatisticsHealthReport(items=items, meta=outcome.meta)


# ── Memoria (estatisticas amostradas de execucao) ──────────────────────────────────────────

_STATEMENT_MEMORY_SQL = """
    SELECT encode(fingerprint_id, 'hex') AS fingerprint,
           metadata->>'query' AS query, metadata->>'db' AS database_name,
           COALESCE((statistics->'statistics'->>'cnt')::INT8, 0) AS executions,
           COALESCE((statistics->'execution_statistics'->>'cnt')::INT8, 0) AS sampled,
           (statistics->'execution_statistics'->'maxMemUsage'->>'mean')::FLOAT8 AS mem_mean,
           (statistics->'execution_statistics'->'maxDiskUsage'->>'mean')::FLOAT8 AS disk_mean,
           aggregated_ts
    FROM crdb_internal.statement_statistics
    WHERE aggregated_ts > now() - INTERVAL '1 day'
      AND app_name NOT LIKE '$ internal%'
"""

_ACTIVE_QUERIES_SQL = """
    SELECT query_id, session_id, user_name, start, query, application_name, phase
    FROM crdb_internal.cluster_queries
    WHERE query NOT LIKE '%crdb_internal.cluster_queries%'
    ORDER BY start
    LIMIT 50
"""


def aggregate_crdb_statements(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Agrega buckets de 1h por fingerprint: execucoes somadas, media de memoria ponderada
    pelas execucoes amostradas, maior media de bucket como "maximo" observado."""
    acc: dict[str, dict[str, Any]] = {}
    for r in rows:
        fp = r["fingerprint"]
        a = acc.setdefault(
            fp,
            {"fingerprint": fp, "query": r["query"], "database_name": r["database_name"],
             "executions": 0, "sampled": 0, "mem_sum": 0.0, "mem_max": None,
             "disk_sum": 0.0, "disk_max": None, "last": None},
        )
        a["executions"] += int(r["executions"] or 0)
        sampled = int(r["sampled"] or 0)
        a["sampled"] += sampled
        if r["mem_mean"] is not None:
            a["mem_sum"] += float(r["mem_mean"]) * sampled
            a["mem_max"] = max(a["mem_max"] or 0.0, float(r["mem_mean"]))
        if r["disk_mean"] is not None:
            a["disk_sum"] += float(r["disk_mean"]) * sampled
            a["disk_max"] = max(a["disk_max"] or 0.0, float(r["disk_mean"]))
        ts = r["aggregated_ts"]
        if ts is not None and (a["last"] is None or ts > a["last"]):
            a["last"] = ts
    out = []
    for a in acc.values():
        if a["sampled"] <= 0 or a["mem_max"] is None:
            continue
        a["mem_avg"] = a["mem_sum"] / a["sampled"]
        a["disk_avg"] = a["disk_sum"] / a["sampled"]
        out.append(a)
    out.sort(key=lambda a: -(a["mem_max"] or 0))
    return out


async def crdb_memory_grants(connect: ConnectFn) -> NormalizedMemoryGrantReport:
    meta = NormalizedScanMeta(method="crdb_internal.statement_statistics",
                              notes=["sampled_statistics"])
    report = NormalizedMemoryGrantReport(meta=meta)
    conn = await connect(BOOTSTRAP_DB)
    try:
        try:
            stmt_rows = [dict(r) for r in await _fetch(conn, _STATEMENT_MEMORY_SQL)]
        except asyncpg.InsufficientPrivilegeError:
            meta.notes.append("permission_denied")
            stmt_rows = []
        try:
            active_rows = await _fetch(conn, _ACTIVE_QUERIES_SQL)
        except asyncpg.InsufficientPrivilegeError:
            meta.notes.append("permission_denied")
            active_rows = []
    finally:
        await conn.close()

    now = datetime.now(UTC)
    for r in active_rows:
        text = r["query"] or ""
        start = r["start"]
        if start is not None and start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        report.active.append(
            NormalizedMemoryGrantActive(
                session_id=str(r["session_id"]),
                login_name=r["user_name"],
                sql_hash=sql_hash(text) if text else None,
                sql_text_masked=mask_sql_literals_for_display(text) if text else None,
                detail={
                    "query_id": r["query_id"],
                    "phase": r["phase"],
                    "application_name": r["application_name"],
                    "duration_ms": int((now - start).total_seconds() * 1000) if start else None,
                },
            )
        )

    for a in aggregate_crdb_statements(stmt_rows)[:TOP_QUERIES_LIMIT]:
        text = a["query"] or ""
        disk_kb = int((a["disk_max"] or 0) / 1024)
        mem_max_kb = a["mem_max"] / 1024
        report.top_queries.append(
            NormalizedMemoryGrantTopQuery(
                sql_hash=sql_hash(text) if text else None,
                sql_text_masked=mask_sql_literals_for_display(text),
                database_name=a["database_name"],
                executions=a["executions"],
                avg_used_kb=round(a["mem_avg"] / 1024, 1),
                max_used_kb=round(mem_max_kb, 1),
                spill_kb=disk_kb or None,
                last_execution=a["last"],
                severity=(
                    "warning" if disk_kb > 0 or mem_max_kb >= 1024 * 1024 else "info"
                ),
                detail={"fingerprint_id": a["fingerprint"], "sampled_executions": a["sampled"],
                        "avg_disk_kb": round(a["disk_avg"] / 1024, 1)},
            )
        )
    report.summary = NormalizedMemoryGrantSummary(
        granted_requests=len(report.active),
        total_used_kb=None,
        detail={"window_hours": 24},
    )
    meta.notes = sorted(set(meta.notes))
    return report


# ── Particoes ──────────────────────────────────────────────────────────────────────────────

_PARTITIONS_SQL = """
    SELECT t.schema_name, t.name AS table_name, ti.index_name, ti.index_type,
           p.name AS partition_name, p.parent_name, p.column_names,
           p.list_value, p.range_value
    FROM crdb_internal.partitions p
    JOIN crdb_internal.tables t ON t.table_id = p.table_id
    LEFT JOIN crdb_internal.table_indexes ti
           ON ti.descriptor_id = p.table_id AND ti.index_id = p.index_id
    WHERE t.database_name = current_database() AND t.drop_time IS NULL
    ORDER BY t.schema_name, t.name, ti.index_name, p.name
"""


def group_crdb_partitions(rows: list[dict[str, Any]], database: str) -> list[NormalizedPartitionInfo]:
    groups: dict[tuple[str, str, str | None], list[dict[str, Any]]] = {}
    for r in rows:
        groups.setdefault((r["schema_name"], r["table_name"], r.get("index_name")), []).append(r)
    items: list[NormalizedPartitionInfo] = []
    for (schema, table, index), parts in groups.items():
        is_range = any(p.get("range_value") for p in parts)
        last = parts[-1].get("range_value") if is_range else None
        unbounded = [p["partition_name"] for p in parts
                     if "MAXVALUE" in str(p.get("range_value") or "").upper()
                     or "DEFAULT" in str(p.get("list_value") or "").upper()]
        items.append(
            NormalizedPartitionInfo(
                database_name=database,
                schema_name=schema,
                table_name=table,
                index_name=index,
                index_type=parts[0].get("index_type"),
                partition_method="RANGE" if is_range else "LIST",
                partition_key=parts[0].get("column_names"),
                partition_count=len(parts),
                last_boundary_value=str(last) if last else None,
                severity="info",
                detail={
                    "partitions": [
                        {"number": i + 1, "name": p["partition_name"],
                         "boundary": p.get("range_value") or p.get("list_value"), "rows": None}
                        for i, p in enumerate(parts)
                    ][-24:],
                    "catch_all_partitions": unbounded or None,
                },
            )
        )
    return items


async def crdb_partitions_for_db(conn: asyncpg.Connection, database: str) -> DbScan:
    rows = [dict(r) for r in await _fetch(conn, _PARTITIONS_SQL)]
    return DbScan(items=group_crdb_partitions(rows, database))


async def crdb_partitions(connect: ConnectFn, database: str | None) -> NormalizedPartitionReport:
    outcome = await scan_databases(
        connect, BOOTSTRAP_DB, database, crdb_partitions_for_db,
        method="crdb_internal.partitions", list_fn=lambda: list_databases(connect),
    )
    return NormalizedPartitionReport(items=outcome.items, meta=outcome.meta)
