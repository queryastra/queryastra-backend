"""Varreduras de manutencao on-demand — Oracle Database.

Escopo = o banco/PDB da conexao (`database_scope_current_only`). Usa as views DBA_* quando ha
permissao (SELECT_CATALOG_ROLE) e cai para ALL_* (`permission_denied`) quando nao ha. Schemas
mantidos pela Oracle (`ALL_USERS.ORACLE_MAINTAINED='Y'`, ou lista fixa em versoes < 12c) ficam
fora. Somente leitura — nunca chama DBMS_STATS.FLUSH_DATABASE_MONITORING_INFO nem ANALYZE.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from collections.abc import Callable
from datetime import datetime
from typing import Any
from uuid import UUID

import oracledb

from app.collectors.mysql.maintenance_helpers import (
    IndexDefinition,
    find_redundant_indexes,
    modified_percentage,
    quote_double,
    severity_rank,
    statistics_assessment,
)
from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals, mask_sql_literals_for_display
from app.normalizers.models import (
    NormalizedCompressionItem,
    NormalizedCompressionReport,
    NormalizedIndexHealthItem,
    NormalizedIndexHealthReport,
    NormalizedMemoryGrantActive,
    NormalizedMemoryGrantReport,
    NormalizedMemoryGrantSummary,
    NormalizedMemoryGrantTopQuery,
    NormalizedMissingIndexSuggestion,
    NormalizedPartitionInfo,
    NormalizedPartitionReport,
    NormalizedScanMeta,
    NormalizedStatisticsHealthItem,
    NormalizedStatisticsHealthReport,
)

logger = get_logger(__name__)

_FALLBACK_SYSTEM_SCHEMAS = (
    "SYS",
    "SYSTEM",
    "OUTLN",
    "DBSNMP",
    "APPQOSSYS",
    "AUDSYS",
    "CTXSYS",
    "DVSYS",
    "DVF",
    "GSMADMIN_INTERNAL",
    "GSMCATUSER",
    "GSMUSER",
    "LBACSYS",
    "MDSYS",
    "MDDATA",
    "OJVMSYS",
    "OLAPSYS",
    "ORDDATA",
    "ORDSYS",
    "ORDPLUGINS",
    "SI_INFORMTN_SCHEMA",
    "WMSYS",
    "XDB",
    "GGSYS",
    "REMOTE_SCHEDULER_AGENT",
    "DBSFWUSER",
    "ANONYMOUS",
    "XS$NULL",
    "SYSBACKUP",
    "SYSDG",
    "SYSKM",
    "SYSRAC",
    "SYSMAN",
    "FLOWS_FILES",
    "APEX_PUBLIC_USER",
    "EXFSYS",
)
_MAX_PARTITIONS_IN_DETAIL = 24
_UNUSED_STALE_DAYS = 30

OwnerFilter = Callable[[str], str]


async def fetch(
    conn: oracledb.AsyncConnection,
    sql: str,
    binds: dict[str, Any] | list[Any] | None = None,
    *,
    timeout: float,
) -> list[dict[str, Any]]:
    cursor = conn.cursor()
    try:
        await asyncio.wait_for(cursor.execute(sql, binds or {}), timeout=timeout)
        cols = [d[0].lower() for d in cursor.description]
        rows = await asyncio.wait_for(cursor.fetchall(), timeout=timeout)
    finally:
        cursor.close()  # AsyncCursor.close() NAO e coroutine no oracledb 2.x+
    return [dict(zip(cols, row, strict=False)) for row in rows]


async def try_fetch(
    conn: oracledb.AsyncConnection,
    sql: str,
    binds: dict[str, Any] | list[Any] | None = None,
    *,
    timeout: float,
) -> list[dict[str, Any]] | None:
    try:
        return await fetch(conn, sql, binds, timeout=timeout)
    except oracledb.DatabaseError as exc:
        logger.debug("Query opcional Oracle falhou: %s", exc)
        return None


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _text(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "read"):
        value = value.read()
    return str(value)


async def owner_filter(conn: oracledb.AsyncConnection, timeout: float) -> OwnerFilter:
    """Filtro SQL de schemas de usuario. Os nomes da lista de fallback sao constantes internas."""
    probe = await try_fetch(
        conn, "SELECT COUNT(*) AS n FROM all_users WHERE oracle_maintained = 'Y'", timeout=timeout
    )
    if probe is not None:
        return lambda col: (
            f"{col} IN (SELECT username FROM all_users WHERE oracle_maintained = 'N')"
        )
    excluded = ", ".join(f"'{s}'" for s in _FALLBACK_SYSTEM_SCHEMAS)
    return lambda col: f"{col} NOT IN ({excluded}) AND {col} NOT LIKE 'APEX%'"


async def view_prefix(conn: oracledb.AsyncConnection, timeout: float) -> str:
    """ "dba" com SELECT_CATALOG_ROLE; "all" (so objetos visiveis ao usuario) caso contrario."""
    probe = await try_fetch(
        conn, "SELECT 1 AS ok FROM dba_tables WHERE ROWNUM = 1", timeout=timeout
    )
    return "dba" if probe is not None else "all"


async def current_service(conn: oracledb.AsyncConnection, timeout: float) -> str | None:
    rows = await try_fetch(
        conn, "SELECT SYS_CONTEXT('USERENV', 'DB_NAME') AS db FROM dual", timeout=timeout
    )
    return str(rows[0]["db"]) if rows and rows[0].get("db") else None


def _base_meta(method: str, db_name: str | None, v: str, notes: list[str]) -> NormalizedScanMeta:
    final_notes = ["database_scope_current_only", *notes]
    if v == "all":
        final_notes.append("permission_denied")
    return NormalizedScanMeta(
        method=method,
        databases_scanned=[db_name] if db_name else [],
        notes=list(dict.fromkeys(final_notes)),
    )


def _qualified(owner: str, name: str) -> str:
    return f"{quote_double(owner)}.{quote_double(name)}"


# ── Saude de indices ─────────────────────────────────────────────────────────────────────────


def build_oracle_index_definitions(
    index_rows: list[dict[str, Any]], column_rows: list[dict[str, Any]]
) -> list[IndexDefinition]:
    columns: dict[tuple[str, str], list[tuple[int, str]]] = defaultdict(list)
    for row in column_rows:
        name = str(row["column_name"])
        if str(row.get("descend") or "ASC").upper() == "DESC":
            name += " DESC"
        columns[(str(row["index_owner"]), str(row["index_name"]))].append(
            (_int(row["column_position"]) or 0, name)
        )
    definitions: list[IndexDefinition] = []
    for row in index_rows:
        key = (str(row["owner"]), str(row["index_name"]))
        cols = [name for _pos, name in sorted(columns.get(key, []))]
        definitions.append(
            IndexDefinition(
                schema_name=str(row["table_owner"]),
                table_name=str(row["table_name"]),
                index_name=str(row["index_name"]),
                key_columns=tuple(cols),
                is_unique=str(row.get("uniqueness") or "") == "UNIQUE",
                is_primary_key=bool(_int(row.get("is_pk"))),
                index_type=str(row.get("index_type") or "") or None,
            )
        )
    return definitions


def oracle_fragmentation_finding(
    row: dict[str, Any], min_leaf_blocks: int
) -> tuple[str, str] | None:
    """`(severity, reason)` pela heuristica sobre estatisticas do otimizador (sem ANALYZE)."""
    if str(row.get("status") or "").upper() == "UNUSABLE":
        return ("critical", "unusable")
    blevel = _int(row.get("blevel"))
    if blevel is not None and blevel >= 4:
        return ("warning", "blevel")
    leaf_blocks = _int(row.get("leaf_blocks")) or 0
    num_rows = _int(row.get("num_rows"))
    if leaf_blocks >= min_leaf_blocks and num_rows is not None and num_rows / leaf_blocks < 10:
        return ("warning", "low_density")
    return None


async def collect_index_health(conn: oracledb.AsyncConnection) -> NormalizedIndexHealthReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    owners = await owner_filter(conn, timeout)
    v = await view_prefix(conn, timeout)
    db_name = await current_service(conn, timeout)
    notes = ["fragmentation_estimated"]

    index_rows = await fetch(
        conn,
        f"""
        SELECT i.owner, i.index_name, i.table_owner, i.table_name, i.index_type, i.uniqueness,
               i.blevel, i.leaf_blocks, i.num_rows, i.distinct_keys, i.clustering_factor,
               i.last_analyzed, i.status, i.partitioned,
               CASE WHEN EXISTS (SELECT 1 FROM {v}_constraints c
                                 WHERE c.owner = i.table_owner AND c.table_name = i.table_name
                                   AND c.index_name = i.index_name AND c.constraint_type = 'P')
                    THEN 1 ELSE 0 END AS is_pk
        FROM {v}_indexes i
        WHERE {owners("i.table_owner")}
          AND i.index_type NOT IN ('LOB', 'IOT - TOP', 'CLUSTER')
          AND i.table_name NOT LIKE 'BIN$%'
        """,
        timeout=timeout,
    )
    column_rows = await fetch(
        conn,
        f"""
        SELECT ic.index_owner, ic.index_name, ic.column_name, ic.column_position, ic.descend
        FROM {v}_ind_columns ic
        WHERE {owners("ic.table_owner")}
        """,
        timeout=timeout,
    )
    sizes: dict[tuple[str, str], int] = {}
    if v == "dba":
        for row in (
            await try_fetch(
                conn,
                f"""
            SELECT owner, segment_name, SUM(bytes) AS bytes FROM dba_segments
            WHERE segment_type LIKE 'INDEX%' AND {owners("owner")}
            GROUP BY owner, segment_name
            """,
                timeout=timeout,
            )
            or []
        ):
            sizes[(str(row["owner"]), str(row["segment_name"]))] = _int(row["bytes"]) or 0

    by_name = {(str(r["owner"]), str(r["index_name"])): r for r in index_rows}
    index_owner = {
        (str(r["table_owner"]), str(r["index_name"])): str(r["owner"]) for r in index_rows
    }
    definitions = build_oracle_index_definitions(index_rows, column_rows)
    items: list[NormalizedIndexHealthItem] = []

    def size_of(definition: IndexDefinition) -> int | None:
        owner = index_owner.get(
            (definition.schema_name, definition.index_name), definition.schema_name
        )
        return sizes.get((owner, definition.index_name))

    def drop_cmd(definition: IndexDefinition) -> str:
        owner = index_owner.get(
            (definition.schema_name, definition.index_name), definition.schema_name
        )
        return f"DROP INDEX {_qualified(owner, definition.index_name)};"

    for redundant, covered_by, kind in find_redundant_indexes(definitions):
        items.append(
            NormalizedIndexHealthItem(
                category=kind,
                database_name=db_name,
                schema_name=redundant.schema_name,
                table_name=redundant.table_name,
                index_name=redundant.index_name,
                index_type=redundant.index_type,
                size_bytes=size_of(redundant),
                duplicate_of=covered_by.index_name,
                key_columns=", ".join(redundant.key_columns),
                is_unique=redundant.is_unique,
                severity="warning" if kind == "duplicate" else "info",
                recommendation=drop_cmd(redundant),
                detail={"covering_key_columns": ", ".join(covered_by.key_columns)},
            )
        )

    # Uso: DBA_INDEX_USAGE (12.2+, persistente entre restarts; rastreio amostrado por padrao).
    usage_rows = (
        await try_fetch(
            conn,
            f"""
            SELECT owner, name, total_access_count, last_used FROM dba_index_usage
            WHERE {owners("owner")}
            """,
            timeout=timeout,
        )
        if v == "dba"
        else None
    )
    now_rows = await fetch(conn, "SELECT SYSDATE AS now_ts FROM dual", timeout=timeout)
    db_now: datetime = now_rows[0]["now_ts"]
    usage: dict[tuple[str, str], tuple[int, datetime | None]] | None = None
    if usage_rows:
        usage = {
            (str(r["owner"]), str(r["name"])): (
                _int(r["total_access_count"]) or 0,
                r.get("last_used"),
            )
            for r in usage_rows
        }
    else:
        monitored = await try_fetch(
            conn, "SELECT index_name, table_name, used FROM v$object_usage", timeout=timeout
        )
        if monitored:
            usage = {}
            for row in monitored:
                # V$OBJECT_USAGE so cobre indices do proprio usuario com MONITORING USAGE ligado.
                for key in by_name:
                    if key[1] == str(row["index_name"]):
                        usage[key] = (0 if str(row["used"]) == "NO" else 1, None)

    if usage is None:
        notes.append("index_usage_tracking_unavailable")
    else:
        writes: dict[tuple[str, str], int] = {}
        for row in (
            await try_fetch(
                conn,
                f"""
            SELECT table_owner, table_name, inserts + updates + deletes AS dml
            FROM {v}_tab_modifications
            WHERE partition_name IS NULL AND {owners("table_owner")}
            """,
                timeout=timeout,
            )
            or []
        ):
            writes[(str(row["table_owner"]), str(row["table_name"]))] = _int(row["dml"]) or 0
        defs_by_key = {(d.schema_name, d.index_name): d for d in definitions}
        for key, row in by_name.items():
            definition = defs_by_key.get((str(row["table_owner"]), key[1]))
            if definition is None or definition.is_unique or definition.is_primary_key:
                continue
            access, last_used = usage.get(key, (0, None))
            stale_use = last_used is not None and (db_now - last_used).days >= _UNUSED_STALE_DAYS
            if access > 0 and not stale_use:
                continue
            table_key = (definition.schema_name, definition.table_name)
            table_writes = writes.get(table_key)
            items.append(
                NormalizedIndexHealthItem(
                    category="unused",
                    database_name=db_name,
                    schema_name=definition.schema_name,
                    table_name=definition.table_name,
                    index_name=definition.index_name,
                    index_type=definition.index_type,
                    size_bytes=size_of(definition),
                    reads=access,
                    writes=table_writes,
                    last_used=last_used,
                    key_columns=", ".join(definition.key_columns),
                    severity="warning" if (table_writes or 0) > 0 and access == 0 else "info",
                    recommendation=drop_cmd(definition),
                    detail={"source": "dba_index_usage" if usage_rows else "v$object_usage"},
                )
            )

    min_leaf = settings.maintenance_fragmentation_min_pages
    missing_stats = False
    for key, row in by_name.items():
        if row.get("last_analyzed") is None:
            missing_stats = True
        finding = oracle_fragmentation_finding(row, min_leaf)
        if finding is None:
            continue
        severity, reason = finding
        items.append(
            NormalizedIndexHealthItem(
                category="fragmentation",
                database_name=db_name,
                schema_name=str(row["table_owner"]),
                table_name=str(row["table_name"]),
                index_name=key[1],
                index_type=str(row.get("index_type") or "") or None,
                size_bytes=sizes.get(key),
                page_count=_int(row.get("leaf_blocks")),
                severity=severity,  # type: ignore[arg-type]
                recommendation=f"ALTER INDEX {_qualified(*key)} REBUILD ONLINE;",
                detail={
                    "reason": reason,
                    "blevel": _int(row.get("blevel")),
                    "num_rows": _int(row.get("num_rows")),
                    "last_analyzed": (
                        row["last_analyzed"].isoformat() if row.get("last_analyzed") else None
                    ),
                },
            )
        )
    if missing_stats:
        notes.append("requires_runstats")

    items.sort(key=lambda i: (severity_rank(i.severity), -(i.size_bytes or 0)))
    return NormalizedIndexHealthReport(
        items=items[: settings.maintenance_max_rows],
        meta=_base_meta(f"{v}_indexes+dba_index_usage", db_name, v, notes),
    )


# ── Estatisticas ─────────────────────────────────────────────────────────────────────────────


async def collect_statistics_health(
    conn: oracledb.AsyncConnection,
) -> NormalizedStatisticsHealthReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    owners = await owner_filter(conn, timeout)
    v = await view_prefix(conn, timeout)
    db_name = await current_service(conn, timeout)
    rows = await fetch(
        conn,
        f"""
        SELECT s.owner, s.table_name, s.num_rows, s.last_analyzed, s.stale_stats,
               s.stattype_locked, ROUND(SYSDATE - s.last_analyzed, 2) AS days_since,
               m.inserts, m.updates, m.deletes, m.truncated, m.timestamp AS modified_at
        FROM {v}_tab_statistics s
        LEFT JOIN {v}_tab_modifications m
               ON m.table_owner = s.owner AND m.table_name = s.table_name
              AND m.partition_name IS NULL AND m.subpartition_name IS NULL
        WHERE s.object_type = 'TABLE' AND {owners("s.owner")}
          AND s.table_name NOT LIKE 'BIN$%'
        """,
        timeout=timeout,
    )
    items: list[NormalizedStatisticsHealthItem] = []
    for row in rows:
        num_rows = _int(row.get("num_rows"))
        mods = [_int(row.get(k)) for k in ("inserts", "updates", "deletes")]
        modified = sum(m or 0 for m in mods) if any(m is not None for m in mods) else 0
        pct = modified_percentage(modified, num_rows)
        days = float(row["days_since"]) if row.get("days_since") is not None else None
        assessment = statistics_assessment(
            modified_pct=pct,
            days_since_update=days,
            never_updated=row.get("last_analyzed") is None,
            row_count=num_rows,
            engine_stale_flag=str(row.get("stale_stats") or "") == "YES",
        )
        if assessment is None:
            continue
        is_stale, severity = assessment
        owner, table = str(row["owner"]), str(row["table_name"])
        items.append(
            NormalizedStatisticsHealthItem(
                database_name=db_name,
                schema_name=owner,
                table_name=table,
                last_updated=row.get("last_analyzed"),
                row_count=num_rows,
                modified_rows=modified,
                modified_pct=pct,
                days_since_update=days,
                is_stale=is_stale,
                severity=severity,
                recommendation=(
                    "EXEC DBMS_STATS.GATHER_TABLE_STATS(ownname => '"
                    + owner.replace("'", "''")
                    + "', tabname => '"
                    + table.replace("'", "''")
                    + "');"
                ),
                detail={
                    "stale_stats": row.get("stale_stats"),
                    "stats_locked": row.get("stattype_locked"),
                    "truncated": row.get("truncated"),
                    "modifications_recorded_at": (
                        row["modified_at"].isoformat() if row.get("modified_at") else None
                    ),
                },
            )
        )
    items.sort(key=lambda i: (severity_rank(i.severity), -(i.modified_pct or 0)))
    return NormalizedStatisticsHealthReport(
        items=items[: settings.maintenance_max_rows],
        meta=_base_meta(
            f"{v}_tab_statistics+{v}_tab_modifications",
            db_name,
            v,
            ["oracle_monitoring_info_not_flushed"],
        ),
    )


# ── Memoria (PGA / work areas) ───────────────────────────────────────────────────────────────

_ACTIVE_WORKAREA_QUERY = """
    SELECT wa.sid, wa.sql_id, wa.operation_type, wa.policy, wa.work_area_size,
           wa.expected_size, wa.actual_mem_used, wa.max_mem_used, wa.number_passes,
           wa.tempseg_size, s.username, s.service_name, q.sql_text
    FROM v$sql_workarea_active wa
    LEFT JOIN v$session s ON s.sid = wa.sid
    LEFT JOIN v$sqlarea q ON q.sql_id = wa.sql_id
"""

_TOP_WORKAREA_QUERY = """
    SELECT w.sql_id, SUM(w.total_executions) AS wa_executions,
           SUM(w.optimal_executions) AS optimal, SUM(w.onepass_executions) AS onepass,
           SUM(w.multipasses_executions) AS multipass,
           MAX(w.estimated_optimal_size) AS est_optimal, MAX(w.last_memory_used) AS last_memory,
           MAX(w.max_tempseg_size) AS max_tempseg
    FROM v$sql_workarea w
    GROUP BY w.sql_id
    HAVING SUM(w.onepass_executions + w.multipasses_executions) > 0
        OR MAX(w.estimated_optimal_size) >= 10485760
    ORDER BY SUM(w.multipasses_executions) DESC, SUM(w.onepass_executions) DESC,
             MAX(w.estimated_optimal_size) DESC
    FETCH FIRST 50 ROWS ONLY
"""


def workarea_severity(onepass: int, multipass: int) -> str:
    if multipass > 0:
        return "critical"
    if onepass > 0:
        return "warning"
    return "info"


def aggregate_active_workareas(rows: list[dict[str, Any]]) -> list[NormalizedMemoryGrantActive]:
    """Uma sessao pode ter varias work areas (sort + hash join) — soma por (sid, sql_id)."""
    grouped: dict[tuple[str, str | None], dict[str, Any]] = {}
    for row in rows:
        key = (str(row["sid"]), row.get("sql_id"))
        entry = grouped.setdefault(
            key,
            {
                "row": row,
                "used": 0,
                "max_used": 0,
                "expected": 0,
                "granted": 0,
                "temp": 0,
                "passes": 0,
                "operations": [],
            },
        )
        entry["used"] += _int(row.get("actual_mem_used")) or 0
        entry["max_used"] += _int(row.get("max_mem_used")) or 0
        entry["expected"] += _int(row.get("expected_size")) or 0
        entry["granted"] += _int(row.get("work_area_size")) or 0
        entry["temp"] += _int(row.get("tempseg_size")) or 0
        entry["passes"] = max(entry["passes"], _int(row.get("number_passes")) or 0)
        entry["operations"].append(row.get("operation_type"))
    result: list[NormalizedMemoryGrantActive] = []
    for (sid, sql_id), entry in grouped.items():
        text = _text(entry["row"].get("sql_text"))
        result.append(
            NormalizedMemoryGrantActive(
                session_id=sid,
                database_name=entry["row"].get("service_name"),
                login_name=entry["row"].get("username"),
                sql_hash=hashlib.sha256(mask_sql_literals(text).encode()).hexdigest()
                if text
                else None,
                sql_text_masked=mask_sql_literals_for_display(text) if text else None,
                requested_kb=entry["expected"] // 1024,
                granted_kb=entry["granted"] // 1024,
                used_kb=entry["used"] // 1024,
                max_used_kb=entry["max_used"] // 1024,
                spill_kb=entry["temp"] // 1024 if entry["temp"] else None,
                detail={
                    "sql_id": sql_id,
                    "number_passes": entry["passes"],
                    "operations": entry["operations"],
                },
            )
        )
    result.sort(key=lambda a: -(a.used_kb or 0))
    return result


async def collect_memory_grants(conn: oracledb.AsyncConnection) -> NormalizedMemoryGrantReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    db_name = await current_service(conn, timeout)
    notes: list[str] = ["database_scope_current_only"]

    active_rows = await try_fetch(conn, _ACTIVE_WORKAREA_QUERY, timeout=timeout)
    if active_rows is None:
        notes.append("permission_denied")
    active = aggregate_active_workareas(active_rows or [])

    top: list[NormalizedMemoryGrantTopQuery] = []
    top_rows = await try_fetch(conn, _TOP_WORKAREA_QUERY, timeout=timeout) or []
    sql_ids = [str(r["sql_id"]) for r in top_rows if r.get("sql_id")]
    texts: dict[str, dict[str, Any]] = {}
    if sql_ids:
        binds = {f"id{i}": sid for i, sid in enumerate(sql_ids)}
        placeholders = ", ".join(f":{name}" for name in binds)
        for row in (
            await try_fetch(
                conn,
                "SELECT sql_id, sql_text, executions, last_active_time, parsing_schema_name "
                f"FROM v$sqlarea WHERE sql_id IN ({placeholders})",
                binds,
                timeout=timeout,
            )
            or []
        ):
            texts[str(row["sql_id"])] = row
    for row in top_rows:
        sql_id = str(row.get("sql_id"))
        info = texts.get(sql_id, {})
        text = _text(info.get("sql_text"))
        if not text:
            continue
        onepass, multipass = _int(row.get("onepass")) or 0, _int(row.get("multipass")) or 0
        est_optimal = _int(row.get("est_optimal"))
        last_memory = _int(row.get("last_memory"))
        max_tempseg = _int(row.get("max_tempseg"))
        top.append(
            NormalizedMemoryGrantTopQuery(
                sql_hash=hashlib.sha256(mask_sql_literals(text).encode()).hexdigest(),
                sql_text_masked=mask_sql_literals_for_display(text),
                database_name=info.get("parsing_schema_name"),
                executions=_int(info.get("executions")) or 0,
                max_grant_kb=est_optimal / 1024 if est_optimal is not None else None,
                max_used_kb=last_memory / 1024 if last_memory is not None else None,
                spill_count=onepass + multipass,
                spill_kb=max_tempseg // 1024 if max_tempseg is not None else None,
                last_execution=info.get("last_active_time"),
                severity=workarea_severity(onepass, multipass),  # type: ignore[arg-type]
                detail={
                    "sql_id": sql_id,
                    "optimal_executions": _int(row.get("optimal")),
                    "onepass_executions": onepass,
                    "multipass_executions": multipass,
                },
            )
        )

    pga = {
        str(r["name"]): _int(r.get("value"))
        for r in await try_fetch(conn, "SELECT name, value FROM v$pgastat", timeout=timeout) or []
    }
    target = pga.get("aggregate PGA target parameter")
    allocated = pga.get("total PGA allocated")
    summary = NormalizedMemoryGrantSummary(
        granted_requests=len(active),
        total_granted_kb=allocated // 1024 if allocated is not None else None,
        total_used_kb=(pga["total PGA inuse"] // 1024)
        if pga.get("total PGA inuse") is not None
        else None,
        target_kb=target // 1024 if target else None,
        available_kb=(
            max(target - allocated, 0) // 1024 if target and allocated is not None else None
        ),
        detail={
            "over_allocation_count": pga.get("over allocation count"),
            "cache_hit_percentage": pga.get("cache hit percentage"),
            "maximum_pga_allocated_kb": (
                pga["maximum PGA allocated"] // 1024
                if pga.get("maximum PGA allocated") is not None
                else None
            ),
        },
    )
    return NormalizedMemoryGrantReport(
        summary=summary,
        active=active,
        top_queries=top,
        meta=NormalizedScanMeta(
            method="v$sql_workarea+v$pgastat",
            databases_scanned=[db_name] if db_name else [],
            notes=notes,
        ),
    )


# ── Compressao (inventario) ──────────────────────────────────────────────────────────────────


def oracle_current_compression(
    compression: str | None, compress_for: str | None, partition_summary: dict[str, int] | None
) -> str:
    if partition_summary:
        total = sum(partition_summary.values())
        compressed = sum(n for kind, n in partition_summary.items() if kind != "DISABLED")
        if compressed == 0:
            return "DISABLED"
        kinds = sorted(k for k in partition_summary if k != "DISABLED")
        return f"{'/'.join(kinds)} ({compressed}/{total})"
    if str(compression or "").upper() == "ENABLED":
        return str(compress_for or "ENABLED")
    return "DISABLED"


async def estimate_compression(
    conn: oracledb.AsyncConnection, max_objects: int
) -> NormalizedCompressionReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    owners = await owner_filter(conn, timeout)
    v = await view_prefix(conn, timeout)
    db_name = await current_service(conn, timeout)
    notes = ["compression_inventory_only"]

    tables = await fetch(
        conn,
        f"""
        SELECT t.owner, t.table_name, t.compression, t.compress_for, t.partitioned, t.blocks
        FROM {v}_tables t
        WHERE {owners("t.owner")} AND t.table_name NOT LIKE 'BIN$%' AND t.temporary = 'N'
        """,
        timeout=timeout,
    )
    sizes: dict[tuple[str, str], int] = {}
    if v == "dba":
        for row in (
            await try_fetch(
                conn,
                f"""
            SELECT owner, segment_name, SUM(bytes) AS bytes FROM dba_segments
            WHERE segment_type IN ('TABLE', 'TABLE PARTITION', 'TABLE SUBPARTITION')
              AND {owners("owner")}
            GROUP BY owner, segment_name
            """,
                timeout=timeout,
            )
            or []
        ):
            sizes[(str(row["owner"]), str(row["segment_name"]))] = _int(row["bytes"]) or 0
    block_rows = await try_fetch(
        conn, "SELECT value FROM v$parameter WHERE name = 'db_block_size'", timeout=timeout
    )
    block_size = (_int(block_rows[0]["value"]) if block_rows else None) or 8192

    partition_summary: dict[tuple[str, str], dict[str, int]] = defaultdict(dict)
    for row in (
        await try_fetch(
            conn,
            f"""
        SELECT table_owner, table_name,
               CASE WHEN compression = 'ENABLED' THEN NVL(compress_for, 'ENABLED')
                    ELSE 'DISABLED' END AS kind,
               COUNT(*) AS n
        FROM {v}_tab_partitions
        WHERE {owners("table_owner")}
        GROUP BY table_owner, table_name,
                 CASE WHEN compression = 'ENABLED' THEN NVL(compress_for, 'ENABLED')
                      ELSE 'DISABLED' END
        """,
            timeout=timeout,
        )
        or []
    ):
        partition_summary[(str(row["table_owner"]), str(row["table_name"]))][str(row["kind"])] = (
            _int(row["n"]) or 0
        )

    items: list[NormalizedCompressionItem] = []
    for row in tables:
        owner, table = str(row["owner"]), str(row["table_name"])
        is_partitioned = str(row.get("partitioned") or "") == "YES"
        size_bytes = sizes.get((owner, table))
        if size_bytes is None:
            size_bytes = (_int(row.get("blocks")) or 0) * block_size
        current = oracle_current_compression(
            row.get("compression"),
            row.get("compress_for"),
            partition_summary.get((owner, table)) if is_partitioned else None,
        )
        command: str | None = None
        if current == "DISABLED":
            command = (
                f"ALTER TABLE {_qualified(owner, table)} MODIFY DEFAULT ATTRIBUTES "
                "ROW STORE COMPRESS ADVANCED;"
                if is_partitioned
                else f"ALTER TABLE {_qualified(owner, table)} MOVE ONLINE "
                "ROW STORE COMPRESS ADVANCED;"
            )
        items.append(
            NormalizedCompressionItem(
                database_name=db_name,
                schema_name=owner,
                table_name=table,
                current_compression=current,
                requested_compression="ADVANCED" if command else None,
                current_size_kb=size_bytes // 1024,
                command=command,
                detail={
                    "partitioned": is_partitioned,
                    "license": "advanced_compression_option",
                    "size_source": "dba_segments" if (owner, table) in sizes else "blocks",
                },
            )
        )
    items.sort(key=lambda i: -(i.current_size_kb or 0))
    if len(items) > max_objects:
        notes.append("max_objects_limited")
    return NormalizedCompressionReport(
        items=items[:max_objects],
        meta=_base_meta(f"{v}_tables+{v}_tab_partitions", db_name, v, notes),
    )


# ── Particoes ────────────────────────────────────────────────────────────────────────────────


def _is_open_bound(high_value: str | None, partition_type: str) -> bool:
    if high_value is None:
        return False
    tokens = [t.strip().upper() for t in high_value.split(",")]
    if partition_type.upper().startswith("LIST"):
        return tokens == ["DEFAULT"]
    return bool(tokens) and all(t == "MAXVALUE" for t in tokens)


def build_oracle_partition_info(
    *,
    db_name: str | None,
    owner: str,
    table: str,
    partitioning_type: str,
    subpartitioning_type: str | None,
    interval: str | None,
    partition_key: str | None,
    partition_count: int,
    total_rows: int | None,
    tail: list[dict[str, Any]],
) -> NormalizedPartitionInfo:
    """`tail` = ultimas particoes (ordenadas por posicao) com high_value ja como texto."""
    is_interval = bool(interval and str(interval).strip() and str(interval).upper() != "NO")
    method = partitioning_type + (" INTERVAL" if is_interval else "")
    if subpartitioning_type and subpartitioning_type.upper() != "NONE":
        method += f"-{subpartitioning_type}"
    qualified = _qualified(owner, table)

    partitions = [
        {
            "number": _int(p.get("partition_position")),
            "name": p.get("partition_name"),
            "boundary": _text(p.get("high_value")),
            "rows": _int(p.get("num_rows")),
            "compression": p.get("compress_for") or p.get("compression"),
        }
        for p in tail
    ]
    last_boundary: str | None = None
    rows_beyond: int | None = None
    severity = "info"
    recommendation: str | None = None
    if partitions and partitioning_type.upper() in ("RANGE", "LIST"):
        bounded = [p for p in partitions if not _is_open_bound(p["boundary"], partitioning_type)]
        last_boundary = bounded[-1]["boundary"] if bounded else None
        last = partitions[-1]
        if _is_open_bound(last["boundary"], partitioning_type):
            rows_beyond = last["rows"]
            if not is_interval and (rows_beyond or 0) > 0:
                severity = "warning"
                name = quote_double(str(last["name"]))
                if partitioning_type.upper() == "LIST":
                    recommendation = (
                        f"ALTER TABLE {qualified} SPLIT PARTITION {name} VALUES (<novos_valores>) "
                        f"INTO (PARTITION p_novo, PARTITION {name});"
                    )
                else:
                    recommendation = (
                        f"ALTER TABLE {qualified} SPLIT PARTITION {name} AT (<novo_limite>) "
                        f"INTO (PARTITION p_novo, PARTITION {name});"
                    )
        elif not is_interval and partitioning_type.upper() == "RANGE":
            recommendation = (
                f"ALTER TABLE {qualified} ADD PARTITION p_novo VALUES LESS THAN (<novo_limite>);"
            )
    elif partitions:
        last_boundary = partitions[-1]["boundary"]

    return NormalizedPartitionInfo(
        database_name=db_name,
        schema_name=owner,
        table_name=table,
        partition_method=method,
        partition_key=partition_key,
        partition_count=partition_count,
        last_boundary_value=last_boundary,
        rows_beyond_last_boundary=rows_beyond,
        total_rows=total_rows,
        data_compression=partitions[-1]["compression"] if partitions else None,
        severity=severity,  # type: ignore[arg-type]
        recommendation=recommendation,
        detail={"partitions": partitions[-_MAX_PARTITIONS_IN_DETAIL:], "interval": interval},
    )


async def collect_partitions(conn: oracledb.AsyncConnection) -> NormalizedPartitionReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    owners = await owner_filter(conn, timeout)
    v = await view_prefix(conn, timeout)
    db_name = await current_service(conn, timeout)
    notes = ["partition_rows_estimated"]

    tables = await fetch(
        conn,
        f"""
        SELECT pt.owner, pt.table_name, pt.partitioning_type, pt.subpartitioning_type,
               pt.interval
        FROM {v}_part_tables pt
        WHERE {owners("pt.owner")} AND pt.table_name NOT LIKE 'BIN$%'
        """,
        timeout=timeout,
    )
    keys: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in await fetch(
        conn,
        f"""
        SELECT owner, name, column_name FROM {v}_part_key_columns
        WHERE object_type = 'TABLE' AND {owners("owner")}
        ORDER BY owner, name, column_position
        """,
        timeout=timeout,
    ):
        keys[(str(row["owner"]), str(row["name"]))].append(str(row["column_name"]))
    aggregates = {
        (str(r["table_owner"]), str(r["table_name"])): r
        for r in await fetch(
            conn,
            f"""
            SELECT table_owner, table_name, COUNT(*) AS cnt, SUM(num_rows) AS total_rows
            FROM {v}_tab_partitions WHERE {owners("table_owner")}
            GROUP BY table_owner, table_name
            """,
            timeout=timeout,
        )
    }
    tails: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in await fetch(
        conn,
        f"""
        SELECT tp.table_owner, tp.table_name, tp.partition_name, tp.partition_position,
               tp.high_value, tp.num_rows, tp.compression, tp.compress_for
        FROM {v}_tab_partitions tp
        JOIN (SELECT table_owner, table_name, MAX(partition_position) AS maxpos
              FROM {v}_tab_partitions GROUP BY table_owner, table_name) a
          ON a.table_owner = tp.table_owner AND a.table_name = tp.table_name
        WHERE tp.partition_position > a.maxpos - {_MAX_PARTITIONS_IN_DETAIL}
          AND {owners("tp.table_owner")}
        ORDER BY tp.table_owner, tp.table_name, tp.partition_position
        """,
        timeout=timeout,
    ):
        tails[(str(row["table_owner"]), str(row["table_name"]))].append(row)

    items: list[NormalizedPartitionInfo] = []
    for row in tables:
        key = (str(row["owner"]), str(row["table_name"]))
        agg = aggregates.get(key, {})
        info = build_oracle_partition_info(
            db_name=db_name,
            owner=key[0],
            table=key[1],
            partitioning_type=str(row.get("partitioning_type") or ""),
            subpartitioning_type=row.get("subpartitioning_type"),
            interval=row.get("interval"),
            partition_key=", ".join(keys.get(key, [])) or None,
            partition_count=_int(agg.get("cnt")) or 0,
            total_rows=_int(agg.get("total_rows")),
            tail=tails.get(key, []),
        )
        if (
            "INTERVAL" in (info.partition_method or "")
            and "interval_partitioning_auto" not in notes
        ):
            notes.append("interval_partitioning_auto")
        items.append(info)

    items.sort(key=lambda i: (severity_rank(i.severity), i.schema_name, i.table_name))
    return NormalizedPartitionReport(
        items=items[: settings.maintenance_max_rows],
        meta=_base_meta(f"{v}_part_tables+{v}_tab_partitions", db_name, v, notes),
    )


async def collect_missing_index_suggestions(
    conn: "oracledb.AsyncConnection", instance_id: "UUID | None"
) -> "list[NormalizedMissingIndexSuggestion]":
    """Heuristica de indice ausente: tabelas sem nenhum indice com alto numero de linhas.

    Usa all_tables LEFT JOIN all_indexes. Nao indica coluna especifica — serve para
    priorizar candidatos; avaliacao detalhada exige EXPLAIN PLAN."""
    if instance_id is None:
        return []
    from app.core.config import get_settings as _gs
    settings = _gs()
    timeout = settings.collection_query_timeout_seconds
    min_rows = 1000
    db_name = await current_service(conn, timeout)
    v = await _dba_or_all(conn, timeout)
    owners = await owner_filter(conn, timeout)
    rows = await try_fetch(
        conn,
        f"""
        SELECT t.owner, t.table_name, t.num_rows
        FROM   {v}_tables t
        WHERE  {owners("t.owner")}
          AND  t.num_rows > :min_rows
          AND  NOT EXISTS (
                 SELECT 1 FROM {v}_indexes i
                 WHERE  i.table_owner = t.owner
                   AND  i.table_name  = t.table_name
               )
        ORDER  BY t.num_rows DESC NULLS LAST
        """,
        {"min_rows": min_rows},
        timeout=timeout,
    )
    if not rows:
        return []
    suggestions: list[NormalizedMissingIndexSuggestion] = []
    for row in rows[: settings.collection_max_rows]:
        owner = str(row["owner"])
        table = str(row["table_name"])
        num_rows = int(row["num_rows"] or 0)
        suggestions.append(
            NormalizedMissingIndexSuggestion(
                instance_id=instance_id,
                database_name=db_name,
                table_name=f"{owner}.{table}",
                suggestion=(
                    f"Tabela sem nenhum indice com {num_rows:,} linhas: {owner}.{table}"
                ),
                impact_score=float(num_rows),
                detail={"num_rows": num_rows, "source": f"{v}_tables", "owner": owner},
            )
        )
    return suggestions


async def get_engine_start_time(
    conn: "oracledb.AsyncConnection",
) -> "datetime | None":
    """Hora de startup da instancia Oracle via v$instance.startup_time."""
    from app.core.config import get_settings as _gs
    timeout = _gs().collection_query_timeout_seconds
    rows = await try_fetch(conn, "SELECT startup_time FROM v$instance", timeout=timeout)
    if rows and rows[0].get("startup_time"):
        val = rows[0]["startup_time"]
        if isinstance(val, datetime):
            return val
        try:
            return datetime.fromisoformat(str(val))
        except ValueError:
            pass
    return None
