"""Varreduras de manutencao on-demand — MySQL/MariaDB (e Aurora MySQL via delegacao).

"Banco" = schema. Toda query e somente leitura contra information_schema/performance_schema/
`mysql.innodb_*_stats`; cada schema roda com timeout proprio e falha vira `databases_skipped`.
Comandos sugeridos sao so texto.
"""

from __future__ import annotations

import asyncio
import hashlib
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Any

import aiomysql

from app.collectors.connection import ConnectionParams
from app.collectors.mysql.maintenance_helpers import (
    IndexDefinition,
    find_redundant_indexes,
    fragmentation_severity,
    modified_percentage,
    quote_mysql,
    severity_rank,
    statistics_assessment,
    usage_notes,
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
    NormalizedPartitionInfo,
    NormalizedPartitionReport,
    NormalizedScanMeta,
    NormalizedStatisticsHealthItem,
    NormalizedStatisticsHealthReport,
)

logger = get_logger(__name__)

SYSTEM_SCHEMAS = ("mysql", "sys", "performance_schema", "information_schema")
_INNODB_DEFAULT_PAGE_SIZE = 16384
_MAX_PARTITIONS_IN_DETAIL = 24


class _ConnHolder:
    """Conexao dedicada a varredura; descartada apos qualquer erro (um timeout no meio de uma
    query deixa o protocolo aiomysql em estado indefinido — reconecta no proximo schema)."""

    def __init__(self, params: ConnectionParams) -> None:
        self._params = params
        self._conn: aiomysql.Connection | None = None

    async def get(self) -> aiomysql.Connection:
        if self._conn is None:
            self._conn = await asyncio.wait_for(
                aiomysql.connect(
                    host=self._params.host,
                    port=self._params.port,
                    user=self._params.username or "",
                    password=self._params.secret_value or "",
                    connect_timeout=int(self._params.connect_timeout_seconds),
                    autocommit=True,
                ),
                timeout=self._params.connect_timeout_seconds,
            )
        return self._conn

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
            self._conn = None


async def _fetch(
    holder: _ConnHolder, sql: str, params: tuple | None = None, *, timeout: float
) -> list[dict[str, Any]]:
    conn = await holder.get()
    try:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await asyncio.wait_for(cur.execute(sql, params), timeout=timeout)
            rows = await cur.fetchall()
        return [dict(r) for r in rows]
    except BaseException:
        holder.close()
        raise


async def _try_fetch(
    holder: _ConnHolder, sql: str, params: tuple | None = None, *, timeout: float
) -> list[dict[str, Any]] | None:
    """`None` = view/tabela inacessivel (permissao, versao) — o chamador degrada."""
    try:
        return await _fetch(holder, sql, params, timeout=timeout)
    except (TimeoutError, asyncio.CancelledError):
        raise
    except Exception as exc:  # noqa: BLE001
        logger.debug("Query opcional MySQL falhou: %s", exc)
        return None


def _int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _sql_hash(text: str) -> str:
    return hashlib.sha256(mask_sql_literals(text).encode()).hexdigest()


async def list_schemas(holder: _ConnHolder, timeout: float) -> list[str]:
    placeholders = ",".join(["%s"] * len(SYSTEM_SCHEMAS))
    rows = await _fetch(
        holder,
        f"SELECT SCHEMA_NAME AS schema_name FROM information_schema.SCHEMATA "
        f"WHERE SCHEMA_NAME NOT IN ({placeholders}) ORDER BY SCHEMA_NAME",
        SYSTEM_SCHEMAS,
        timeout=timeout,
    )
    return [str(r["schema_name"]) for r in rows]


async def _resolve_schemas(
    holder: _ConnHolder, database: str | None
) -> tuple[list[str], list[str]]:
    settings = get_settings()
    available = await list_schemas(holder, settings.collection_query_timeout_seconds)
    if database is not None:
        return ([database], []) if database in available else ([], [database])
    cap = settings.maintenance_max_databases
    return available[:cap], available[cap:]


async def _server_clock(holder: _ConnHolder, timeout: float) -> tuple[datetime, datetime | None]:
    """`(agora no relogio do servidor, inicio do servidor)` — datas de information_schema vem no
    fuso do servidor, entao "dias desde" e calculado contra o relogio dele, nao o nosso."""
    now_rows = await _fetch(holder, "SELECT NOW() AS now_ts", timeout=timeout)
    server_now: datetime = now_rows[0]["now_ts"]
    uptime_rows = await _try_fetch(holder, "SHOW GLOBAL STATUS LIKE 'Uptime'", timeout=timeout)
    started: datetime | None = None
    if uptime_rows:
        seconds = _int(uptime_rows[0].get("Value"))
        if seconds is not None:
            started = datetime.now(UTC) - timedelta(seconds=seconds)
    return server_now, started


async def _performance_schema_on(holder: _ConnHolder, timeout: float) -> bool:
    rows = await _try_fetch(holder, "SELECT @@performance_schema AS ps", timeout=timeout)
    return bool(rows) and _int(rows[0].get("ps")) == 1


def _usage_since_notes(started: datetime | None) -> list[str]:
    return usage_notes(started, datetime.now(UTC))


# ── Saude de indices ─────────────────────────────────────────────────────────────────────────

_STATISTICS_QUERY = """
    SELECT TABLE_NAME AS table_name, INDEX_NAME AS index_name, NON_UNIQUE AS non_unique,
           SEQ_IN_INDEX AS seq, COLUMN_NAME AS column_name, SUB_PART AS sub_part,
           INDEX_TYPE AS index_type
    FROM information_schema.STATISTICS
    WHERE TABLE_SCHEMA = %s
    ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX
"""

_INDEX_SIZE_QUERY = """
    SELECT table_name, index_name, stat_value AS pages
    FROM mysql.innodb_index_stats
    WHERE database_name = %s AND stat_name = 'size'
"""

_UNUSED_INDEX_QUERY = """
    SELECT OBJECT_NAME AS table_name, INDEX_NAME AS index_name, COUNT_READ AS count_read,
           COUNT_WRITE AS count_write
    FROM performance_schema.table_io_waits_summary_by_index_usage
    WHERE OBJECT_SCHEMA = %s AND INDEX_NAME IS NOT NULL AND INDEX_NAME <> 'PRIMARY'
      AND COUNT_READ = 0
"""

_TABLE_WRITES_QUERY = """
    SELECT OBJECT_NAME AS table_name, COUNT_INSERT AS ins, COUNT_UPDATE AS upd,
           COUNT_DELETE AS del, COUNT_WRITE AS count_write
    FROM performance_schema.table_io_waits_summary_by_table
    WHERE OBJECT_SCHEMA = %s
"""

_FRAGMENTATION_QUERY = """
    SELECT TABLE_NAME AS table_name, DATA_LENGTH AS data_length, INDEX_LENGTH AS index_length,
           DATA_FREE AS data_free, TABLE_ROWS AS table_rows
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' AND ENGINE = 'InnoDB'
      AND (DATA_LENGTH + INDEX_LENGTH) >= %s AND DATA_FREE > 0
"""


def build_index_definitions(schema: str, rows: list[dict[str, Any]]) -> list[IndexDefinition]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["table_name"]), str(row["index_name"]))].append(row)
    definitions: list[IndexDefinition] = []
    for (table, index), cols in grouped.items():
        cols.sort(key=lambda r: _int(r["seq"]) or 0)
        keys: list[str] = []
        for col in cols:
            name = col["column_name"]
            if name is None:  # indice funcional (8.0.13+): expressao nao comparavel
                name = f"<expr{col['seq']}>@{index}"
            sub_part = _int(col.get("sub_part"))
            keys.append(f"{name}({sub_part})" if sub_part else str(name))
        definitions.append(
            IndexDefinition(
                database_name=schema,
                schema_name=schema,
                table_name=table,
                index_name=index,
                key_columns=tuple(keys),
                is_unique=_int(cols[0]["non_unique"]) == 0,
                is_primary_key=index == "PRIMARY",
                index_type=str(cols[0].get("index_type") or "") or None,
            )
        )
    return definitions


def drop_index_command(schema: str, table: str, index: str) -> str:
    return (
        f"ALTER TABLE {quote_mysql(schema)}.{quote_mysql(table)} DROP INDEX {quote_mysql(index)};"
    )


async def _index_health_for_schema(
    holder: _ConnHolder, schema: str, ps_on: bool, page_size: int, timeout: float
) -> list[NormalizedIndexHealthItem]:
    settings = get_settings()
    items: list[NormalizedIndexHealthItem] = []
    definitions = build_index_definitions(
        schema, await _fetch(holder, _STATISTICS_QUERY, (schema,), timeout=timeout)
    )
    by_key = {(d.table_name, d.index_name): d for d in definitions}

    sizes: dict[tuple[str, str], int] = {}
    size_rows = await _try_fetch(holder, _INDEX_SIZE_QUERY, (schema,), timeout=timeout)
    for row in size_rows or []:
        pages = _int(row.get("pages"))
        if pages is not None:
            sizes[(str(row["table_name"]), str(row["index_name"]))] = pages * page_size

    for redundant, covered_by, kind in find_redundant_indexes(definitions):
        items.append(
            NormalizedIndexHealthItem(
                category=kind,
                database_name=schema,
                schema_name=schema,
                table_name=redundant.table_name,
                index_name=redundant.index_name,
                index_type=redundant.index_type,
                size_bytes=sizes.get((redundant.table_name, redundant.index_name)),
                duplicate_of=covered_by.index_name,
                key_columns=", ".join(redundant.key_columns),
                is_unique=redundant.is_unique,
                severity="warning" if kind == "duplicate" else "info",
                recommendation=drop_index_command(
                    schema, redundant.table_name, redundant.index_name
                ),
                detail={"covering_key_columns": ", ".join(covered_by.key_columns)},
            )
        )

    if ps_on:
        unused_rows = await _try_fetch(holder, _UNUSED_INDEX_QUERY, (schema,), timeout=timeout)
        write_rows = await _try_fetch(holder, _TABLE_WRITES_QUERY, (schema,), timeout=timeout)
        table_writes = {
            str(r["table_name"]): _int(r.get("count_write")) or 0 for r in write_rows or []
        }
        for row in unused_rows or []:
            key = (str(row["table_name"]), str(row["index_name"]))
            definition = by_key.get(key)
            if definition is None or definition.is_unique:
                continue
            writes = table_writes.get(key[0], _int(row.get("count_write")) or 0)
            items.append(
                NormalizedIndexHealthItem(
                    category="unused",
                    database_name=schema,
                    schema_name=schema,
                    table_name=key[0],
                    index_name=key[1],
                    index_type=definition.index_type,
                    size_bytes=sizes.get(key),
                    reads=0,
                    writes=writes,
                    key_columns=", ".join(definition.key_columns),
                    severity="warning" if writes > 0 else "info",
                    recommendation=drop_index_command(schema, key[0], key[1]),
                )
            )

    min_bytes = settings.maintenance_fragmentation_min_pages * page_size
    for row in await _fetch(holder, _FRAGMENTATION_QUERY, (schema, min_bytes), timeout=timeout):
        used = (_int(row["data_length"]) or 0) + (_int(row["index_length"]) or 0)
        free = _int(row["data_free"]) or 0
        pct = round(free * 100.0 / max(used + free, 1), 2)
        severity = fragmentation_severity(pct)
        if severity is None:
            continue
        table = str(row["table_name"])
        items.append(
            NormalizedIndexHealthItem(
                category="fragmentation",
                database_name=schema,
                schema_name=schema,
                table_name=table,
                # InnoDB: a tabela E o indice clusterizado; DATA_FREE e por tablespace da tabela.
                index_name="*",
                index_type="TABLE",
                size_bytes=used,
                fragmentation_pct=pct,
                page_count=used // page_size,
                severity=severity,
                recommendation=f"OPTIMIZE TABLE {quote_mysql(schema)}.{quote_mysql(table)};",
                detail={
                    "scope": "table",
                    "data_free_bytes": free,
                    "alternative": (
                        f"ALTER TABLE {quote_mysql(schema)}.{quote_mysql(table)} ENGINE=InnoDB;"
                    ),
                },
            )
        )
    return items


async def collect_index_health(
    params: ConnectionParams, database: str | None
) -> NormalizedIndexHealthReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    holder = _ConnHolder(params)
    items: list[NormalizedIndexHealthItem] = []
    scanned: list[str] = []
    notes = ["fragmentation_estimated"]
    try:
        schemas, skipped = await _resolve_schemas(holder, database)
        _now, started = await _server_clock(holder, timeout)
        ps_on = await _performance_schema_on(holder, timeout)
        notes.extend(_usage_since_notes(started) if ps_on else ["index_usage_tracking_unavailable"])
        page_rows = await _try_fetch(holder, "SELECT @@innodb_page_size AS ps", timeout=timeout)
        page_size = (_int(page_rows[0]["ps"]) if page_rows else None) or _INNODB_DEFAULT_PAGE_SIZE

        for schema in schemas:
            try:
                items.extend(
                    await asyncio.wait_for(
                        _index_health_for_schema(holder, schema, ps_on, page_size, timeout),
                        timeout=timeout,
                    )
                )
                scanned.append(schema)
            except Exception:  # noqa: BLE001 - 1 schema nunca derruba a varredura
                logger.warning("Saude de indices MySQL: schema %s pulado", schema, exc_info=True)
                holder.close()
                skipped.append(schema)
    finally:
        holder.close()

    items.sort(key=lambda i: (severity_rank(i.severity), -(i.size_bytes or 0)))
    return NormalizedIndexHealthReport(
        items=items[: settings.maintenance_max_rows],
        meta=NormalizedScanMeta(
            method="information_schema+performance_schema",
            databases_scanned=scanned,
            databases_skipped=skipped,
            usage_stats_since=started,
            notes=notes,
        ),
    )


# ── Estatisticas ─────────────────────────────────────────────────────────────────────────────

_TABLES_STATS_QUERY = """
    SELECT TABLE_NAME AS table_name, ENGINE AS engine, TABLE_ROWS AS table_rows,
           UPDATE_TIME AS update_time
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
"""

_INNODB_TABLE_STATS_QUERY = """
    SELECT table_name, last_update, n_rows
    FROM mysql.innodb_table_stats
    WHERE database_name = %s
"""


def merge_innodb_table_stats(
    rows: list[dict[str, Any]],
) -> dict[str, tuple[datetime | None, int | None]]:
    """Tabela -> (last_update mais antigo, n_rows somado). Tabela particionada nao tem linha
    propria em innodb_table_stats, so `t#p#p0`/`t#P#p0` (5.7) por particao."""
    merged: dict[str, tuple[datetime | None, int | None]] = {}
    for row in rows:
        name = str(row["table_name"])
        lowered = name.lower()
        base = name[: lowered.index("#p#")] if "#p#" in lowered else name
        last, n_rows = row.get("last_update"), _int(row.get("n_rows"))
        if base in merged:
            prev_last, prev_rows = merged[base]
            last = (
                min(d for d in (prev_last, last) if d is not None) if (prev_last or last) else None
            )
            n_rows = (prev_rows or 0) + (n_rows or 0)
        merged[base] = (last, n_rows)
    return merged


async def _statistics_for_schema(
    holder: _ConnHolder,
    schema: str,
    server_now: datetime,
    ps_on: bool,
    timeout: float,
    flags: set[str],
) -> list[NormalizedStatisticsHealthItem]:
    tables = await _fetch(holder, _TABLES_STATS_QUERY, (schema,), timeout=timeout)
    innodb_rows = await _try_fetch(holder, _INNODB_TABLE_STATS_QUERY, (schema,), timeout=timeout)
    if innodb_rows is None:
        flags.add("permission_denied")
    innodb = merge_innodb_table_stats(innodb_rows or [])

    writes: dict[str, int] | None = None
    if ps_on:
        write_rows = await _try_fetch(holder, _TABLE_WRITES_QUERY, (schema,), timeout=timeout)
        if write_rows is not None:
            writes = {
                str(row["table_name"]): sum(_int(row.get(k)) or 0 for k in ("ins", "upd", "del"))
                for row in write_rows
            }
    if writes is None:
        flags.add("modification_counter_unavailable")

    items: list[NormalizedStatisticsHealthItem] = []
    for row in tables:
        table = str(row["table_name"])
        is_innodb = str(row.get("engine") or "").lower() == "innodb"
        last_updated, n_rows = innodb.get(table, (None, None))
        row_count = n_rows if n_rows is not None else _int(row.get("table_rows"))
        modified = writes.get(table, 0) if writes is not None else None
        pct = modified_percentage(modified, row_count)
        days = (
            round((server_now - last_updated).total_seconds() / 86400, 2)
            if last_updated is not None
            else None
        )
        never = is_innodb and innodb_rows is not None and table not in innodb
        assessment = statistics_assessment(
            modified_pct=pct, days_since_update=days, never_updated=never, row_count=row_count
        )
        if assessment is None:
            continue
        is_stale, severity = assessment
        items.append(
            NormalizedStatisticsHealthItem(
                database_name=schema,
                schema_name=schema,
                table_name=table,
                last_updated=last_updated,
                row_count=row_count,
                modified_rows=modified,
                modified_pct=pct,
                days_since_update=days,
                is_stale=is_stale,
                severity=severity,
                recommendation=f"ANALYZE TABLE {quote_mysql(schema)}.{quote_mysql(table)};",
                detail={
                    "engine": row.get("engine"),
                    "last_data_change": (
                        row["update_time"].isoformat() if row.get("update_time") else None
                    ),
                    "modification_window": "since_restart",
                },
            )
        )
    return items


async def collect_statistics_health(
    params: ConnectionParams, database: str | None
) -> NormalizedStatisticsHealthReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    holder = _ConnHolder(params)
    items: list[NormalizedStatisticsHealthItem] = []
    scanned: list[str] = []
    flags: set[str] = set()
    try:
        schemas, skipped = await _resolve_schemas(holder, database)
        server_now, started = await _server_clock(holder, timeout)
        ps_on = await _performance_schema_on(holder, timeout)
        for schema in schemas:
            try:
                items.extend(
                    await asyncio.wait_for(
                        _statistics_for_schema(holder, schema, server_now, ps_on, timeout, flags),
                        timeout=timeout,
                    )
                )
                scanned.append(schema)
            except Exception:  # noqa: BLE001
                logger.warning("Estatisticas MySQL: schema %s pulado", schema, exc_info=True)
                holder.close()
                skipped.append(schema)
    finally:
        holder.close()

    notes = sorted(flags)
    if "modification_counter_unavailable" not in flags:
        notes.append("modifications_since_restart")
    items.sort(key=lambda i: (severity_rank(i.severity), -(i.modified_pct or 0)))
    return NormalizedStatisticsHealthReport(
        items=items[: settings.maintenance_max_rows],
        meta=NormalizedScanMeta(
            method="innodb_table_stats+performance_schema",
            databases_scanned=scanned,
            databases_skipped=skipped,
            usage_stats_since=started,
            notes=notes,
        ),
    )


# ── Memoria ──────────────────────────────────────────────────────────────────────────────────

_ACTIVE_MEMORY_QUERY = """
    SELECT t.PROCESSLIST_ID AS session_id, t.PROCESSLIST_USER AS user_name,
           t.PROCESSLIST_DB AS db, t.PROCESSLIST_TIME AS elapsed_s, t.PROCESSLIST_INFO AS info,
           SUM(m.CURRENT_NUMBER_OF_BYTES_USED) AS current_bytes,
           SUM(m.HIGH_NUMBER_OF_BYTES_USED) AS high_bytes
    FROM performance_schema.memory_summary_by_thread_by_event_name m
    JOIN performance_schema.threads t ON t.THREAD_ID = m.THREAD_ID
    WHERE t.TYPE = 'FOREGROUND' AND t.PROCESSLIST_COMMAND <> 'Sleep'
    GROUP BY t.THREAD_ID, t.PROCESSLIST_ID, t.PROCESSLIST_USER, t.PROCESSLIST_DB,
             t.PROCESSLIST_TIME, t.PROCESSLIST_INFO
    ORDER BY current_bytes DESC
    LIMIT 50
"""

_CURRENT_SPILL_QUERY = """
    SELECT t.PROCESSLIST_ID AS session_id, e.CREATED_TMP_DISK_TABLES AS tmp_disk,
           e.SORT_MERGE_PASSES AS sort_merge
    FROM performance_schema.events_statements_current e
    JOIN performance_schema.threads t ON t.THREAD_ID = e.THREAD_ID
    WHERE t.TYPE = 'FOREGROUND'
"""

_DIGEST_HAS_MAX_MEMORY_QUERY = """
    SELECT COUNT(*) AS n FROM information_schema.COLUMNS
    WHERE TABLE_SCHEMA = 'performance_schema'
      AND TABLE_NAME = 'events_statements_summary_by_digest'
      AND COLUMN_NAME = 'MAX_TOTAL_MEMORY'
"""


def _digest_query(has_max_memory: bool) -> str:
    memory_col = "MAX_TOTAL_MEMORY AS max_memory," if has_max_memory else "NULL AS max_memory,"
    memory_filter = " OR MAX_TOTAL_MEMORY >= 16777216" if has_max_memory else ""
    order = "max_memory DESC, spills DESC" if has_max_memory else "spills DESC"
    return f"""
        SELECT SCHEMA_NAME AS schema_name, DIGEST AS digest, DIGEST_TEXT AS digest_text,
               COUNT_STAR AS executions, {memory_col}
               SUM_CREATED_TMP_DISK_TABLES AS tmp_disk, SUM_CREATED_TMP_TABLES AS tmp_tables,
               SUM_SORT_MERGE_PASSES AS sort_merge, SUM_SORT_ROWS AS sort_rows,
               (SUM_CREATED_TMP_DISK_TABLES + SUM_SORT_MERGE_PASSES) AS spills,
               LAST_SEEN AS last_seen
        FROM performance_schema.events_statements_summary_by_digest
        WHERE DIGEST_TEXT IS NOT NULL
          AND (SUM_CREATED_TMP_DISK_TABLES > 0 OR SUM_SORT_MERGE_PASSES > 0{memory_filter})
        ORDER BY {order}
        LIMIT 50
    """


def memory_top_query_severity(spills: int, executions: int) -> str:
    if spills <= 0:
        return "info"
    return "warning" if spills / max(executions, 1) >= 0.1 else "info"


async def collect_memory_grants(params: ConnectionParams) -> NormalizedMemoryGrantReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    holder = _ConnHolder(params)
    notes: list[str] = []
    active: list[NormalizedMemoryGrantActive] = []
    top: list[NormalizedMemoryGrantTopQuery] = []
    summary_detail: dict[str, Any] = {}
    try:
        if not await _performance_schema_on(holder, timeout):
            return NormalizedMemoryGrantReport(
                meta=NormalizedScanMeta(
                    method="performance_schema", notes=["performance_schema_memory_disabled"]
                )
            )

        spill_rows = await _try_fetch(holder, _CURRENT_SPILL_QUERY, timeout=timeout)
        current_spills = {
            str(r["session_id"]): (_int(r.get("tmp_disk")) or 0, _int(r.get("sort_merge")) or 0)
            for r in spill_rows or []
            if r.get("session_id") is not None
        }
        active_rows = await _try_fetch(holder, _ACTIVE_MEMORY_QUERY, timeout=timeout)
        if active_rows is None:
            notes.append("performance_schema_memory_disabled")
        for row in active_rows or []:
            if row.get("session_id") is None:
                continue
            session_id = str(row["session_id"])
            info = row.get("info")
            tmp_disk, sort_merge = current_spills.get(session_id, (0, 0))
            active.append(
                NormalizedMemoryGrantActive(
                    session_id=session_id,
                    database_name=row.get("db"),
                    login_name=row.get("user_name"),
                    sql_hash=_sql_hash(info) if info else None,
                    sql_text_masked=mask_sql_literals_for_display(info) if info else None,
                    used_kb=(_int(row.get("current_bytes")) or 0) // 1024,
                    max_used_kb=(_int(row.get("high_bytes")) or 0) // 1024,
                    detail={
                        "elapsed_s": _int(row.get("elapsed_s")),
                        "tmp_disk_tables": tmp_disk,
                        "sort_merge_passes": sort_merge,
                    },
                )
            )

        has_rows = await _try_fetch(holder, _DIGEST_HAS_MAX_MEMORY_QUERY, timeout=timeout)
        has_max_memory = bool(has_rows) and (_int(has_rows[0]["n"]) or 0) > 0
        digest_rows = await _try_fetch(holder, _digest_query(has_max_memory), timeout=timeout)
        for row in digest_rows or []:
            text = str(row["digest_text"])
            executions = _int(row.get("executions")) or 0
            spills = _int(row.get("spills")) or 0
            max_memory = _int(row.get("max_memory"))
            top.append(
                NormalizedMemoryGrantTopQuery(
                    sql_hash=_sql_hash(text),
                    sql_text_masked=mask_sql_literals_for_display(text),
                    database_name=row.get("schema_name"),
                    executions=executions,
                    max_used_kb=max_memory / 1024 if max_memory is not None else None,
                    spill_count=spills,
                    last_execution=row.get("last_seen"),
                    severity=memory_top_query_severity(spills, executions),
                    detail={
                        "digest": row.get("digest"),
                        "tmp_disk_tables": _int(row.get("tmp_disk")),
                        "tmp_tables": _int(row.get("tmp_tables")),
                        "sort_merge_passes": _int(row.get("sort_merge")),
                        "sort_rows": _int(row.get("sort_rows")),
                    },
                )
            )

        var_rows = await _try_fetch(
            holder,
            "SELECT @@tmp_table_size AS tmp_table_size, @@max_heap_table_size AS "
            "max_heap_table_size, @@sort_buffer_size AS sort_buffer_size, "
            "@@join_buffer_size AS join_buffer_size",
            timeout=timeout,
        )
        if var_rows:
            summary_detail.update({k: _int(v) for k, v in var_rows[0].items()})
        status_rows = await _try_fetch(
            holder,
            "SHOW GLOBAL STATUS WHERE Variable_name IN "
            "('Created_tmp_disk_tables','Created_tmp_tables','Sort_merge_passes')",
            timeout=timeout,
        )
        for row in status_rows or []:
            summary_detail[str(row["Variable_name"]).lower()] = _int(row.get("Value"))
    finally:
        holder.close()

    return NormalizedMemoryGrantReport(
        summary=NormalizedMemoryGrantSummary(
            granted_requests=len(active),
            total_used_kb=sum(a.used_kb or 0 for a in active),
            detail=summary_detail,
        ),
        active=active,
        top_queries=top,
        meta=NormalizedScanMeta(method="performance_schema", notes=notes),
    )


# ── Compressao (inventario) ──────────────────────────────────────────────────────────────────

_COMPRESSION_QUERY = """
    SELECT TABLE_NAME AS table_name, ENGINE AS engine, ROW_FORMAT AS row_format,
           CREATE_OPTIONS AS create_options, DATA_LENGTH AS data_length,
           INDEX_LENGTH AS index_length
    FROM information_schema.TABLES
    WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE'
    ORDER BY (DATA_LENGTH + INDEX_LENGTH) DESC
    LIMIT %s
"""


def current_mysql_compression(row_format: str | None, create_options: str | None) -> str:
    options = (create_options or "").lower()
    if (row_format or "").lower() == "compressed":
        for part in options.split():
            if part.startswith("key_block_size="):
                return f"COMPRESSED({part.upper()})"
        return "COMPRESSED"
    if "page_compressed=1" in options or "`page_compressed`=1" in options:
        return "PAGE_COMPRESSED"
    if "compression=" in options:
        value = options.split("compression=", 1)[1].split()[0].strip("'\"")
        if value and value != "none":
            return f"PAGE({value.upper()})"
    return "NONE"


async def estimate_compression(
    params: ConnectionParams, database: str | None, max_objects: int
) -> NormalizedCompressionReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    holder = _ConnHolder(params)
    candidates: list[NormalizedCompressionItem] = []
    scanned: list[str] = []
    notes = ["compression_inventory_only"]
    try:
        schemas, skipped = await _resolve_schemas(holder, database)
        cmp_rows = await _try_fetch(
            holder,
            "SELECT page_size, compress_ops, compress_ops_ok, compress_time, "
            "uncompress_ops, uncompress_time FROM information_schema.INNODB_CMP "
            "WHERE compress_ops > 0",
            timeout=timeout,
        )
        cmp_stats = [{k: _int(v) for k, v in r.items()} for r in cmp_rows or []]
        for schema in schemas:
            try:
                rows = await _fetch(
                    holder, _COMPRESSION_QUERY, (schema, max_objects + 1), timeout=timeout
                )
                scanned.append(schema)
            except Exception:  # noqa: BLE001
                logger.warning("Compressao MySQL: schema %s pulado", schema, exc_info=True)
                holder.close()
                skipped.append(schema)
                continue
            for row in rows:
                table = str(row["table_name"])
                current = current_mysql_compression(
                    row.get("row_format"), row.get("create_options")
                )
                size = (_int(row.get("data_length")) or 0) + (_int(row.get("index_length")) or 0)
                is_innodb = str(row.get("engine") or "").lower() == "innodb"
                command = (
                    f"ALTER TABLE {quote_mysql(schema)}.{quote_mysql(table)} "
                    "ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;"
                    if is_innodb and current == "NONE"
                    else None
                )
                detail: dict[str, Any] = {
                    "engine": row.get("engine"),
                    "row_format": row.get("row_format"),
                }
                if current != "NONE" and cmp_stats:
                    detail["innodb_cmp"] = cmp_stats
                candidates.append(
                    NormalizedCompressionItem(
                        database_name=schema,
                        schema_name=schema,
                        table_name=table,
                        current_compression=current,
                        requested_compression="COMPRESSED" if command else None,
                        current_size_kb=size // 1024,
                        command=command,
                        detail=detail,
                    )
                )
    finally:
        holder.close()

    candidates.sort(key=lambda i: -(i.current_size_kb or 0))
    if len(candidates) > max_objects:
        notes.append("max_objects_limited")
    return NormalizedCompressionReport(
        items=candidates[:max_objects],
        meta=NormalizedScanMeta(
            method="information_schema.TABLES",
            databases_scanned=scanned,
            databases_skipped=skipped,
            notes=notes,
        ),
    )


# ── Particoes ────────────────────────────────────────────────────────────────────────────────

_PARTITIONS_QUERY = """
    SELECT TABLE_NAME AS table_name, PARTITION_NAME AS partition_name,
           PARTITION_ORDINAL_POSITION AS position, PARTITION_METHOD AS method,
           PARTITION_EXPRESSION AS expression, PARTITION_DESCRIPTION AS description,
           TABLE_ROWS AS table_rows
    FROM information_schema.PARTITIONS
    WHERE TABLE_SCHEMA = %s AND PARTITION_NAME IS NOT NULL
    ORDER BY TABLE_NAME, PARTITION_ORDINAL_POSITION
"""


def build_mysql_partition_info(
    schema: str, table: str, rows: list[dict[str, Any]]
) -> NormalizedPartitionInfo:
    """`rows` = linhas de information_schema.PARTITIONS de UMA tabela (subparticoes somadas)."""
    partitions: dict[str, dict[str, Any]] = {}
    for row in rows:
        name = str(row["partition_name"])
        entry = partitions.setdefault(
            name,
            {
                "number": _int(row.get("position")),
                "name": name,
                "boundary": row.get("description"),
                "rows": 0,
            },
        )
        entry["rows"] += _int(row.get("table_rows")) or 0
    ordered = sorted(partitions.values(), key=lambda p: p["number"] or 0)
    method = str(rows[0].get("method") or "") or None
    is_range = bool(method) and method.upper().startswith("RANGE")

    last_boundary: str | None = None
    rows_beyond: int | None = None
    severity = "info"
    recommendation: str | None = None
    qualified = f"{quote_mysql(schema)}.{quote_mysql(table)}"
    if is_range:
        bounded = [p for p in ordered if str(p["boundary"]).upper() != "MAXVALUE"]
        last_boundary = str(bounded[-1]["boundary"]) if bounded else None
        tail = ordered[-1]
        if str(tail["boundary"]).upper() == "MAXVALUE":
            rows_beyond = tail["rows"]
            if rows_beyond > 0:
                severity = "warning"
                recommendation = (
                    f"ALTER TABLE {qualified} REORGANIZE PARTITION {quote_mysql(tail['name'])} "
                    f"INTO (PARTITION p_novo VALUES LESS THAN (<novo_limite>), "
                    f"PARTITION {quote_mysql(tail['name'])} VALUES LESS THAN MAXVALUE);"
                )
        else:
            recommendation = (
                f"ALTER TABLE {qualified} ADD PARTITION "
                "(PARTITION p_novo VALUES LESS THAN (<novo_limite>));"
            )
    elif ordered:
        last_boundary = ordered[-1]["boundary"]

    return NormalizedPartitionInfo(
        database_name=schema,
        schema_name=schema,
        table_name=table,
        partition_method=method,
        partition_key=rows[0].get("expression"),
        partition_count=len(ordered),
        last_boundary_value=last_boundary,
        rows_beyond_last_boundary=rows_beyond,
        total_rows=sum(p["rows"] for p in ordered),
        severity=severity,
        recommendation=recommendation,
        detail={"partitions": ordered[-_MAX_PARTITIONS_IN_DETAIL:]},
    )


async def collect_partitions(
    params: ConnectionParams, database: str | None
) -> NormalizedPartitionReport:
    settings = get_settings()
    timeout = settings.maintenance_per_database_timeout_seconds
    holder = _ConnHolder(params)
    items: list[NormalizedPartitionInfo] = []
    scanned: list[str] = []
    try:
        schemas, skipped = await _resolve_schemas(holder, database)
        for schema in schemas:
            try:
                rows = await _fetch(holder, _PARTITIONS_QUERY, (schema,), timeout=timeout)
                scanned.append(schema)
            except Exception:  # noqa: BLE001
                logger.warning("Particoes MySQL: schema %s pulado", schema, exc_info=True)
                holder.close()
                skipped.append(schema)
                continue
            by_table: dict[str, list[dict[str, Any]]] = defaultdict(list)
            for row in rows:
                by_table[str(row["table_name"])].append(row)
            items.extend(
                build_mysql_partition_info(schema, table, table_rows)
                for table, table_rows in by_table.items()
            )
    finally:
        holder.close()

    items.sort(key=lambda i: (severity_rank(i.severity), i.schema_name, i.table_name))
    return NormalizedPartitionReport(
        items=items[: settings.maintenance_max_rows],
        meta=NormalizedScanMeta(
            method="information_schema.PARTITIONS",
            databases_scanned=scanned,
            databases_skipped=skipped,
            notes=["partition_rows_estimated"],
        ),
    )


async def list_databases(params: ConnectionParams) -> list[str]:
    holder = _ConnHolder(params)
    try:
        return await list_schemas(holder, get_settings().collection_query_timeout_seconds)
    finally:
        holder.close()
