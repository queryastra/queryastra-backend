"""Varreduras de manutencao on-demand — IBM Db2 (LUW).

Escopo = o banco da conexao (`database_scope_current_only`). O driver `ibm_db_dbi` e sincrono:
cada query roda no executor com timeout proprio (`Db2Runner`), mesmo padrao do adaptador.
Somente leitura — catalogo SYSCAT, funcoes MON_GET_* e o estimador nativo
`SYSPROC.ADMIN_GET_TAB_COMPRESS_INFO` (nao altera dados; amostra a tabela). Comandos sugeridos
(REORG/RUNSTATS/ALTER) sao so texto.
"""

from __future__ import annotations

import asyncio
import functools
import hashlib
from collections import defaultdict
from datetime import UTC, date, datetime
from typing import Any
from uuid import UUID

from app.collectors.mysql.maintenance_helpers import (
    IndexDefinition,
    find_redundant_indexes,
    fragmentation_severity,
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

_MAX_PARTITIONS_IN_DETAIL = 24
_UNUSED_STALE_DAYS = 30
_DB2_PAGE_KB = 4  # unidade de SORT_SHRHEAP_* / sheapthres_shr / sortheap


def _execute_params_sync(conn: Any, query: str, params: tuple) -> list[dict[str, Any]]:
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        if cursor.description is None:
            return []
        cols = [d[0].lower() for d in cursor.description]
        return [dict(zip(cols, row, strict=False)) for row in cursor.fetchall()]
    finally:
        cursor.close()


class Db2Runner:
    """Executa queries sincronas do `ibm_db_dbi` no executor, uma por vez, com timeout."""

    def __init__(self, conn: Any, timeout: float) -> None:
        self._conn = conn
        self._timeout = timeout

    async def fetch(self, query: str, params: tuple = ()) -> list[dict[str, Any]]:
        loop = asyncio.get_running_loop()
        return await asyncio.wait_for(
            loop.run_in_executor(
                None, functools.partial(_execute_params_sync, self._conn, query, params)
            ),
            timeout=self._timeout,
        )

    async def try_fetch(self, query: str, params: tuple = ()) -> list[dict[str, Any]] | None:
        try:
            return await self.fetch(query, params)
        except TimeoutError:
            raise
        except Exception as exc:  # noqa: BLE001 - funcao/coluna ausente nesta versao/permissao
            logger.debug("Query opcional DB2 falhou: %s", exc)
            return None


def schema_filter(column: str) -> str:
    return (
        f"{column} NOT LIKE 'SYS%' AND {column} NOT IN ('NULLID', 'SQLJ', 'IBM_RTMON', 'SYSTOOLS')"
    )


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


def _qualified(schema: str, name: str) -> str:
    return f"{quote_double(schema.strip())}.{quote_double(name.strip())}"


def _meta(method: str, db_name: str, notes: list[str], **kwargs: Any) -> NormalizedScanMeta:
    return NormalizedScanMeta(
        method=method,
        databases_scanned=[db_name],
        notes=list(dict.fromkeys(["database_scope_current_only", *notes])),
        **kwargs,
    )


async def _activation_time(runner: Db2Runner) -> datetime | None:
    rows = await runner.try_fetch(
        "SELECT DB_ACTIVATION_TIME AS activated FROM TABLE(MON_GET_DATABASE(-2)) AS t "
        "FETCH FIRST 1 ROWS ONLY"
    )
    if not rows or rows[0].get("activated") is None:
        return None
    activated: datetime = rows[0]["activated"]
    # Timestamps do DB2 vem sem fuso (horario local do servidor) — so usado como referencia.
    return activated if activated.tzinfo else activated.replace(tzinfo=UTC)


# ── Saude de indices ─────────────────────────────────────────────────────────────────────────


def build_db2_index_definitions(
    index_rows: list[dict[str, Any]], column_rows: list[dict[str, Any]]
) -> list[IndexDefinition]:
    keys: dict[tuple[str, str], list[tuple[int, str]]] = defaultdict(list)
    includes: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in column_rows:
        key = (str(row["indschema"]).strip(), str(row["indname"]).strip())
        name = str(row["colname"]).strip()
        order = str(row.get("colorder") or "A").strip().upper()
        if order == "I":
            includes[key].append(name)
        else:
            keys[key].append((_int(row["colseq"]) or 0, name + (" DESC" if order == "D" else "")))
    definitions: list[IndexDefinition] = []
    for row in index_rows:
        key = (str(row["indschema"]).strip(), str(row["indname"]).strip())
        rule = str(row.get("uniquerule") or "D").strip().upper()
        definitions.append(
            IndexDefinition(
                schema_name=str(row["tabschema"]).strip(),
                table_name=str(row["tabname"]).strip(),
                index_name=key[1],
                key_columns=tuple(name for _seq, name in sorted(keys.get(key, []))),
                included_columns=tuple(includes.get(key, [])),
                is_unique=rule in ("U", "P"),
                is_primary_key=rule == "P",
                index_type=str(row.get("indextype") or "").strip() or None,
            )
        )
    return definitions


def db2_never_used(last_used: Any) -> bool:
    """SYSCAT.INDEXES.LASTUSED = '0001-01-01' quando o indice nunca foi usado."""
    return isinstance(last_used, date) and last_used.year <= 1


async def collect_index_health(runner: Db2Runner, db_name: str) -> NormalizedIndexHealthReport:
    settings = get_settings()
    notes = ["fragmentation_estimated"]
    index_rows = await runner.fetch(
        f"""
        SELECT INDSCHEMA, INDNAME, TABSCHEMA, TABNAME, UNIQUERULE, INDEXTYPE, NLEAF, NLEVELS,
               DENSITY, SEQUENTIAL_PAGES, LASTUSED, STATS_TIME, CREATE_TIME,
               DAYS(CURRENT DATE) - DAYS(LASTUSED) AS days_since_used
        FROM SYSCAT.INDEXES WHERE {schema_filter("TABSCHEMA")}
        """
    )
    column_rows = await runner.fetch(
        f"""
        SELECT INDSCHEMA, INDNAME, COLNAME, COLSEQ, COLORDER
        FROM SYSCAT.INDEXCOLUSE WHERE {schema_filter("INDSCHEMA")}
        """
    )
    definitions = build_db2_index_definitions(index_rows, column_rows)
    by_index = {(str(r["indschema"]).strip(), str(r["indname"]).strip()): r for r in index_rows}
    index_schema = {
        (str(r["tabschema"]).strip(), str(r["indname"]).strip()): str(r["indschema"]).strip()
        for r in index_rows
    }
    items: list[NormalizedIndexHealthItem] = []

    def drop_cmd(definition: IndexDefinition) -> str:
        schema = index_schema.get(
            (definition.schema_name, definition.index_name), definition.schema_name
        )
        return f"DROP INDEX {_qualified(schema, definition.index_name)};"

    for redundant, covered_by, kind in find_redundant_indexes(definitions):
        items.append(
            NormalizedIndexHealthItem(
                category=kind,
                database_name=db_name,
                schema_name=redundant.schema_name,
                table_name=redundant.table_name,
                index_name=redundant.index_name,
                index_type=redundant.index_type,
                duplicate_of=covered_by.index_name,
                key_columns=", ".join(redundant.key_columns),
                included_columns=", ".join(redundant.included_columns) or None,
                is_unique=redundant.is_unique,
                severity="warning" if kind == "duplicate" else "info",
                recommendation=drop_cmd(redundant),
                detail={"covering_key_columns": ", ".join(covered_by.key_columns)},
            )
        )

    writes: dict[tuple[str, str], int] = {}
    write_rows = await runner.try_fetch(
        f"""
        SELECT TABSCHEMA, TABNAME,
               SUM(ROWS_INSERTED + ROWS_UPDATED + ROWS_DELETED) AS dml
        FROM TABLE(MON_GET_TABLE('', '', -2)) AS t
        WHERE {schema_filter("TABSCHEMA")}
        GROUP BY TABSCHEMA, TABNAME
        """
    )
    for row in write_rows or []:
        writes[(str(row["tabschema"]).strip(), str(row["tabname"]).strip())] = _int(row["dml"]) or 0

    definitions_by_key = {(d.schema_name, d.index_name): d for d in definitions}
    min_pages = settings.maintenance_fragmentation_min_pages
    missing_stats = False
    for (schema, index), row in by_index.items():
        table_schema, table = str(row["tabschema"]).strip(), str(row["tabname"]).strip()
        definition = definitions_by_key.get((table_schema, index))
        rule = str(row.get("uniquerule") or "D").strip().upper()
        last_used = row.get("lastused")
        days_since = _int(row.get("days_since_used"))
        never = db2_never_used(last_used)
        if rule == "D" and (never or (days_since is not None and days_since >= _UNUSED_STALE_DAYS)):
            table_writes = writes.get((table_schema, table)) if write_rows is not None else None
            items.append(
                NormalizedIndexHealthItem(
                    category="unused",
                    database_name=db_name,
                    schema_name=table_schema,
                    table_name=table,
                    index_name=index,
                    index_type=str(row.get("indextype") or "").strip() or None,
                    reads=0 if never else None,
                    writes=table_writes,
                    last_used=(
                        None
                        if never or not isinstance(last_used, date)
                        else datetime(last_used.year, last_used.month, last_used.day, tzinfo=UTC)
                    ),
                    key_columns=", ".join(definition.key_columns) if definition else None,
                    severity="warning" if never and (table_writes or 0) > 0 else "info",
                    recommendation=f"DROP INDEX {_qualified(schema, index)};",
                    detail={
                        "created": row["create_time"].isoformat()
                        if row.get("create_time")
                        else None
                    },
                )
            )

        if row.get("stats_time") is None:
            missing_stats = True
            continue
        nlevels = _int(row.get("nlevels")) or 0
        nleaf = _int(row.get("nleaf")) or 0
        density = _int(row.get("density"))
        pct = float(100 - density) if density is not None and 0 <= density <= 100 else None
        severity = fragmentation_severity(pct) if nleaf >= min_pages else None
        reason = "density"
        if nlevels >= 4:
            severity, reason = "warning", "nlevels"
        if severity is None:
            continue
        items.append(
            NormalizedIndexHealthItem(
                category="fragmentation",
                database_name=db_name,
                schema_name=table_schema,
                table_name=table,
                index_name=index,
                index_type=str(row.get("indextype") or "").strip() or None,
                fragmentation_pct=pct,
                page_count=nleaf,
                severity=severity,
                recommendation=(
                    f"REORG INDEXES ALL FOR TABLE {_qualified(table_schema, table)} "
                    "ALLOW WRITE ACCESS;"
                ),
                detail={
                    "reason": reason,
                    "nlevels": nlevels,
                    "density": density,
                    "sequential_pages": _int(row.get("sequential_pages")),
                },
            )
        )
    if missing_stats:
        notes.append("requires_runstats")

    items.sort(key=lambda i: (severity_rank(i.severity), -(i.page_count or 0)))
    return NormalizedIndexHealthReport(
        items=items[: settings.maintenance_max_rows],
        meta=_meta("syscat.indexes+mon_get_table", db_name, notes),
    )


# ── Estatisticas ─────────────────────────────────────────────────────────────────────────────


async def collect_statistics_health(
    runner: Db2Runner, db_name: str
) -> NormalizedStatisticsHealthReport:
    settings = get_settings()
    notes: list[str] = []
    tables = await runner.fetch(
        f"""
        SELECT TABSCHEMA, TABNAME, CARD, STATS_TIME,
               DAYS(CURRENT DATE) - DAYS(STATS_TIME) AS days_since
        FROM SYSCAT.TABLES WHERE TYPE = 'T' AND {schema_filter("TABSCHEMA")}
        """
    )
    mod_rows = await runner.try_fetch(
        f"""
        SELECT TABSCHEMA, TABNAME, SUM(ROWS_INSERTED) AS ins, SUM(ROWS_UPDATED) AS upd,
               SUM(ROWS_DELETED) AS del
        FROM TABLE(MON_GET_TABLE('', '', -2)) AS t
        WHERE {schema_filter("TABSCHEMA")}
        GROUP BY TABSCHEMA, TABNAME
        """
    )
    if mod_rows is None:
        notes.append("modification_counter_unavailable")
    else:
        notes.append("modifications_since_restart")
    mods = {
        (str(r["tabschema"]).strip(), str(r["tabname"]).strip()): sum(
            _int(r.get(k)) or 0 for k in ("ins", "upd", "del")
        )
        for r in mod_rows or []
    }

    items: list[NormalizedStatisticsHealthItem] = []
    for row in tables:
        schema, table = str(row["tabschema"]).strip(), str(row["tabname"]).strip()
        card = _int(row.get("card"))
        row_count = card if card is not None and card >= 0 else None
        modified = mods.get((schema, table), 0) if mod_rows is not None else None
        pct = modified_percentage(modified, row_count)
        days = _int(row.get("days_since"))
        never = row.get("stats_time") is None
        assessment = statistics_assessment(
            modified_pct=pct,
            days_since_update=float(days) if days is not None else None,
            never_updated=never,
            row_count=row_count,
        )
        if assessment is None:
            continue
        is_stale, severity = assessment
        stats_time = row.get("stats_time")
        items.append(
            NormalizedStatisticsHealthItem(
                database_name=db_name,
                schema_name=schema,
                table_name=table,
                last_updated=(
                    stats_time.replace(tzinfo=UTC)
                    if isinstance(stats_time, datetime) and stats_time.tzinfo is None
                    else stats_time
                ),
                row_count=row_count,
                modified_rows=modified,
                modified_pct=pct,
                days_since_update=float(days) if days is not None else None,
                is_stale=is_stale,
                severity=severity,
                recommendation=(
                    f"RUNSTATS ON TABLE {_qualified(schema, table)} "
                    "WITH DISTRIBUTION AND DETAILED INDEXES ALL;"
                ),
                detail={"modification_window": "since_activation"},
            )
        )
    items.sort(key=lambda i: (severity_rank(i.severity), -(i.modified_pct or 0)))
    return NormalizedStatisticsHealthReport(
        items=items[: settings.maintenance_max_rows],
        meta=_meta(
            "syscat.tables+mon_get_table",
            db_name,
            notes,
            usage_stats_since=await _activation_time(runner),
        ),
    )


# ── Memoria (sort heap) ──────────────────────────────────────────────────────────────────────

_TOP_SORT_QUERY = """
    SELECT STMT_TEXT, NUM_EXECUTIONS, SORT_OVERFLOWS, TOTAL_SORTS, POST_THRESHOLD_SORTS,
           POST_SHRTHRESHOLD_SORTS, {heap_top} AS heap_top_pages, LAST_METRICS_UPDATE
    FROM TABLE(MON_GET_PKG_CACHE_STMT(NULL, NULL, NULL, -2)) AS t
    WHERE SORT_OVERFLOWS > 0 OR POST_THRESHOLD_SORTS > 0 OR POST_SHRTHRESHOLD_SORTS > 0
    ORDER BY SORT_OVERFLOWS DESC
    FETCH FIRST 50 ROWS ONLY
"""

_ACTIVE_SORT_QUERY = """
    SELECT APPLICATION_HANDLE, SESSION_AUTH_ID, ACTIVITY_STATE, STMT_TEXT,
           SORT_SHRHEAP_ALLOCATED, SORT_OVERFLOWS, TOTAL_SORTS
    FROM TABLE(MON_GET_ACTIVITY(NULL, -2)) AS t
    WHERE ACTIVITY_STATE IN ('EXECUTING', 'IDLE', 'QUEUED', 'LOCK_WAIT')
    ORDER BY SORT_SHRHEAP_ALLOCATED DESC
    FETCH FIRST 50 ROWS ONLY
"""


def sort_overflow_severity(overflows: int, total_sorts: int) -> str:
    if overflows <= 0:
        return "info"
    return "warning" if overflows / max(total_sorts, 1) >= 0.1 else "info"


def _hash_and_mask(text: str | None) -> tuple[str | None, str | None]:
    if not text:
        return None, None
    return (
        hashlib.sha256(mask_sql_literals(text).encode()).hexdigest(),
        mask_sql_literals_for_display(text),
    )


async def collect_memory_grants(runner: Db2Runner, db_name: str) -> NormalizedMemoryGrantReport:
    notes: list[str] = []
    active: list[NormalizedMemoryGrantActive] = []
    for row in await runner.try_fetch(_ACTIVE_SORT_QUERY) or []:
        sql_hash, masked = _hash_and_mask(_text(row.get("stmt_text")))
        allocated = _int(row.get("sort_shrheap_allocated"))
        active.append(
            NormalizedMemoryGrantActive(
                session_id=str(row.get("application_handle")),
                database_name=db_name,
                login_name=_text(row.get("session_auth_id")),
                sql_hash=sql_hash,
                sql_text_masked=masked,
                used_kb=allocated * _DB2_PAGE_KB if allocated is not None else None,
                is_waiting=str(row.get("activity_state") or "").strip() == "QUEUED",
                detail={
                    "activity_state": _text(row.get("activity_state")),
                    "sort_overflows": _int(row.get("sort_overflows")),
                    "total_sorts": _int(row.get("total_sorts")),
                },
            )
        )

    top_rows = await runner.try_fetch(_TOP_SORT_QUERY.format(heap_top="SORT_SHRHEAP_TOP"))
    if top_rows is None:  # DB2 < 10.5: sem SORT_SHRHEAP_TOP
        top_rows = await runner.try_fetch(_TOP_SORT_QUERY.format(heap_top="CAST(NULL AS BIGINT)"))
    if top_rows is None:
        notes.append("permission_denied")
    top: list[NormalizedMemoryGrantTopQuery] = []
    for row in top_rows or []:
        text = _text(row.get("stmt_text"))
        sql_hash, masked = _hash_and_mask(text)
        if not masked:
            continue
        overflows = _int(row.get("sort_overflows")) or 0
        total_sorts = _int(row.get("total_sorts")) or 0
        heap_top = _int(row.get("heap_top_pages"))
        last = row.get("last_metrics_update")
        top.append(
            NormalizedMemoryGrantTopQuery(
                sql_hash=sql_hash,
                sql_text_masked=masked,
                database_name=db_name,
                executions=_int(row.get("num_executions")) or 0,
                max_grant_kb=float(heap_top * _DB2_PAGE_KB) if heap_top is not None else None,
                spill_count=overflows,
                last_execution=(
                    last.replace(tzinfo=UTC)
                    if isinstance(last, datetime) and last.tzinfo is None
                    else last
                ),
                severity=sort_overflow_severity(overflows, total_sorts),  # type: ignore[arg-type]
                detail={
                    "total_sorts": total_sorts,
                    "post_threshold_sorts": _int(row.get("post_threshold_sorts")),
                    "post_shrthreshold_sorts": _int(row.get("post_shrthreshold_sorts")),
                },
            )
        )

    db_rows = await runner.try_fetch(
        "SELECT SORT_SHRHEAP_ALLOCATED, SORT_SHRHEAP_TOP, SORT_OVERFLOWS, TOTAL_SORTS, "
        "POST_THRESHOLD_SORTS FROM TABLE(MON_GET_DATABASE(-2)) AS t FETCH FIRST 1 ROWS ONLY"
    )
    db_row = db_rows[0] if db_rows else {}
    cfg = {
        str(r["name"]).strip().lower(): _int(r.get("value"))
        for r in await runner.try_fetch(
            "SELECT NAME, VALUE FROM SYSIBMADM.DBCFG WHERE NAME IN ('sheapthres_shr', 'sortheap')"
        )
        or []
    }
    allocated = _int(db_row.get("sort_shrheap_allocated"))
    threshold = cfg.get("sheapthres_shr")
    summary = NormalizedMemoryGrantSummary(
        granted_requests=sum(1 for a in active if not a.is_waiting),
        waiting_requests=sum(1 for a in active if a.is_waiting),
        total_granted_kb=allocated * _DB2_PAGE_KB if allocated is not None else None,
        target_kb=threshold * _DB2_PAGE_KB if threshold else None,
        available_kb=(
            max(threshold - allocated, 0) * _DB2_PAGE_KB
            if threshold and allocated is not None
            else None
        ),
        detail={
            "sort_shrheap_top_kb": (
                _int(db_row.get("sort_shrheap_top")) * _DB2_PAGE_KB
                if _int(db_row.get("sort_shrheap_top")) is not None
                else None
            ),
            "sort_overflows": _int(db_row.get("sort_overflows")),
            "total_sorts": _int(db_row.get("total_sorts")),
            "post_threshold_sorts": _int(db_row.get("post_threshold_sorts")),
            "sortheap_pages": cfg.get("sortheap"),
        },
    )
    return NormalizedMemoryGrantReport(
        summary=summary,
        active=active,
        top_queries=top,
        meta=_meta("mon_get_pkg_cache_stmt+mon_get_activity", db_name, notes),
    )


# ── Compressao (estimativa nativa) ───────────────────────────────────────────────────────────


def db2_current_compression(compression: str | None, rowcompmode: str | None) -> str:
    comp = (compression or "N").strip().upper()
    mode = (rowcompmode or "").strip().upper()
    if comp in ("R", "B"):
        row_kind = {"A": "ADAPTIVE", "S": "STATIC"}.get(mode, "ROW")
        return f"{row_kind}+VALUE" if comp == "B" else row_kind
    if comp == "V":
        return "VALUE"
    return "NONE"


_ESTIMATE_COLUMNS = {
    # Ordem de preferencia por tipo pedido; nomes variam entre versoes do DB2.
    "PAGE": ("pctpagessaved_adaptive", "pages_saved_percent", "pctpagessaved_static"),
    "ROW": ("pctpagessaved_static", "pages_saved_percent", "pctpagessaved_adaptive"),
}


def extract_savings_pct(rows: list[dict[str, Any]], compression: str) -> float | None:
    """Media do % de paginas economizadas entre as particoes de dados devolvidas."""
    for column in _ESTIMATE_COLUMNS.get(compression, _ESTIMATE_COLUMNS["PAGE"]):
        values = [
            float(r[column]) for r in rows if r.get(column) is not None and float(r[column]) >= 0
        ]
        if values:
            return round(sum(values) / len(values), 2)
    return None


def db2_compression_command(schema: str, table: str, compression: str) -> str:
    mode = "STATIC" if compression == "ROW" else "ADAPTIVE"
    qualified = _qualified(schema, table)
    return f"ALTER TABLE {qualified} COMPRESS YES {mode}; REORG TABLE {qualified} RESETDICTIONARY;"


async def estimate_compression(
    runner: Db2Runner, db_name: str, compression: str, max_objects: int
) -> NormalizedCompressionReport:
    notes = ["compression_estimate_native"]
    tables = await runner.fetch(
        f"""
        SELECT t.TABSCHEMA, t.TABNAME, t.COMPRESSION, t.ROWCOMPMODE, t.FPAGES, t.PCTPAGESSAVED,
               ts.PAGESIZE
        FROM SYSCAT.TABLES t
        LEFT JOIN SYSCAT.TABLESPACES ts ON ts.TBSPACE = t.TBSPACE
        WHERE t.TYPE = 'T' AND {schema_filter("t.TABSCHEMA")}
        ORDER BY t.FPAGES DESC
        FETCH FIRST {int(max_objects) + 1} ROWS ONLY
        """
    )
    if len(tables) > max_objects:
        notes.append("max_objects_limited")
        tables = tables[:max_objects]

    items: list[NormalizedCompressionItem] = []
    estimator_available = True
    for row in tables:
        schema, table = str(row["tabschema"]).strip(), str(row["tabname"]).strip()
        fpages = _int(row.get("fpages"))
        page_size = _int(row.get("pagesize")) or 4096
        current_kb = fpages * page_size // 1024 if fpages is not None and fpages >= 0 else None
        current = db2_current_compression(row.get("compression"), row.get("rowcompmode"))
        savings: float | None = None
        detail: dict[str, Any] = {"pct_pages_saved_current": _int(row.get("pctpagessaved"))}
        if estimator_available:
            try:
                estimate_rows = await runner.fetch(
                    "SELECT * FROM TABLE(SYSPROC.ADMIN_GET_TAB_COMPRESS_INFO(?, ?)) AS t",
                    (schema, table),
                )
            except TimeoutError:
                # A conexao segue ocupada com a amostragem no executor — nao enfileira outra.
                detail["estimate_error"] = "timeout"
                estimator_available = False
                estimate_rows = []
            except Exception as exc:  # noqa: BLE001
                detail["estimate_error"] = type(exc).__name__
                estimate_rows = []
            savings = extract_savings_pct(estimate_rows, compression)
        estimated_kb = (
            int(current_kb * (1 - savings / 100))
            if current_kb is not None and savings is not None
            else None
        )
        target = "STATIC" if compression == "ROW" else "ADAPTIVE"
        command = (
            db2_compression_command(schema, table, compression)
            if current not in (target, f"{target}+VALUE")
            else None
        )
        items.append(
            NormalizedCompressionItem(
                database_name=db_name,
                schema_name=schema,
                table_name=table,
                current_compression=current,
                requested_compression=target,
                current_size_kb=current_kb,
                estimated_size_kb=estimated_kb,
                savings_pct=savings,
                command=command,
                detail=detail,
            )
        )
    items.sort(key=lambda i: (-(i.savings_pct or -1), -(i.current_size_kb or 0)))
    return NormalizedCompressionReport(
        items=items,
        meta=_meta("admin_get_tab_compress_info", db_name, notes),
    )


# ── Particoes ────────────────────────────────────────────────────────────────────────────────


def build_db2_partition_info(
    db_name: str,
    schema: str,
    table: str,
    partition_key: str | None,
    rows: list[dict[str, Any]],
) -> NormalizedPartitionInfo:
    """`rows` = SYSCAT.DATAPARTITIONS de UMA tabela, ordenadas por SEQNO."""
    partitions = [
        {
            "number": _int(r.get("seqno")),
            "name": _text(r.get("datapartitionname")),
            "boundary": _text(r.get("highvalue")),
            "rows": (_int(r.get("card")) if (_int(r.get("card")) or 0) >= 0 else None),
        }
        for r in rows
    ]

    def is_open(boundary: str | None) -> bool:
        tokens = [t.strip().upper() for t in (boundary or "").split(",") if t.strip()]
        return bool(tokens) and all(t == "MAXVALUE" for t in tokens)

    bounded = [p for p in partitions if not is_open(p["boundary"])]
    last_boundary = bounded[-1]["boundary"] if bounded else None
    rows_beyond: int | None = None
    severity = "info"
    qualified = _qualified(schema, table)
    recommendation = (
        f"ALTER TABLE {qualified} ADD PARTITION p_novo STARTING ({last_boundary or '<inicio>'}) "
        "EXCLUSIVE ENDING (<novo_limite>) INCLUSIVE;"
    )
    if partitions and is_open(partitions[-1]["boundary"]):
        rows_beyond = partitions[-1]["rows"]
        if (rows_beyond or 0) > 0:
            severity = "warning"
            tail_name = quote_double(str(partitions[-1]["name"]))
            recommendation = (
                f"ALTER TABLE {qualified} DETACH PARTITION {tail_name} "
                f"INTO <tabela_temp>; ALTER TABLE {qualified} ADD PARTITION p_novo STARTING "
                f"({last_boundary or '<inicio>'}) EXCLUSIVE ENDING (<novo_limite>) INCLUSIVE;"
            )
        else:
            recommendation = None
    known_rows = [p["rows"] for p in partitions if p["rows"] is not None]
    return NormalizedPartitionInfo(
        database_name=db_name,
        schema_name=schema,
        table_name=table,
        partition_method="RANGE",
        partition_key=partition_key,
        partition_count=len(partitions),
        last_boundary_value=last_boundary,
        rows_beyond_last_boundary=rows_beyond,
        total_rows=sum(known_rows) if known_rows else None,
        severity=severity,  # type: ignore[arg-type]
        recommendation=recommendation,
        detail={"partitions": partitions[-_MAX_PARTITIONS_IN_DETAIL:]},
    )


async def collect_partitions(runner: Db2Runner, db_name: str) -> NormalizedPartitionReport:
    settings = get_settings()
    key_rows = await runner.fetch(
        f"""
        SELECT TABSCHEMA, TABNAME, DATAPARTITIONKEYSEQ, DATAPARTITIONEXPRESSION
        FROM SYSCAT.DATAPARTITIONEXPRESSION WHERE {schema_filter("TABSCHEMA")}
        ORDER BY TABSCHEMA, TABNAME, DATAPARTITIONKEYSEQ
        """
    )
    keys: dict[tuple[str, str], list[str]] = defaultdict(list)
    for row in key_rows:
        keys[(str(row["tabschema"]).strip(), str(row["tabname"]).strip())].append(
            str(_text(row["datapartitionexpression"]) or "").strip()
        )
    items: list[NormalizedPartitionInfo] = []
    if keys:
        part_rows = await runner.fetch(
            f"""
            SELECT p.TABSCHEMA, p.TABNAME, p.DATAPARTITIONNAME, p.SEQNO, p.HIGHVALUE, p.CARD
            FROM SYSCAT.DATAPARTITIONS p
            WHERE {schema_filter("p.TABSCHEMA")}
              AND EXISTS (SELECT 1 FROM SYSCAT.DATAPARTITIONEXPRESSION e
                          WHERE e.TABSCHEMA = p.TABSCHEMA AND e.TABNAME = p.TABNAME)
            ORDER BY p.TABSCHEMA, p.TABNAME, p.SEQNO
            """
        )
        by_table: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for row in part_rows:
            by_table[(str(row["tabschema"]).strip(), str(row["tabname"]).strip())].append(row)
        items = [
            build_db2_partition_info(
                db_name, schema, table, ", ".join(keys.get((schema, table), [])) or None, rows
            )
            for (schema, table), rows in by_table.items()
        ]
    items.sort(key=lambda i: (severity_rank(i.severity), i.schema_name, i.table_name))
    return NormalizedPartitionReport(
        items=items[: settings.maintenance_max_rows],
        meta=_meta("syscat.datapartitions", db_name, ["partition_rows_estimated"]),
    )


async def collect_missing_index_suggestions(
    runner: Db2Runner, db_name: str, instance_id: "UUID | None"
) -> "list[NormalizedMissingIndexSuggestion]":
    """Heuristica de indice ausente: tabelas sem nenhum indice com alto numero de linhas.

    Usa syscat.tables LEFT JOIN syscat.indexes. Nao indica coluna especifica."""
    if instance_id is None:
        return []
    from app.core.config import get_settings as _gs
    settings = _gs()
    min_rows = 1000
    rows = await runner.try_fetch(
        f"""
        SELECT t.TABSCHEMA, t.TABNAME, t.CARD
        FROM   SYSCAT.TABLES t
        WHERE  {schema_filter("t.TABSCHEMA")}
          AND  t.TYPE = 'T'
          AND  t.CARD > {min_rows}
          AND  NOT EXISTS (
                 SELECT 1 FROM SYSCAT.INDEXES i
                 WHERE  i.TABSCHEMA = t.TABSCHEMA
                   AND  i.TABNAME   = t.TABNAME
               )
        ORDER  BY t.CARD DESC
        """
    )
    if not rows:
        return []
    suggestions: list[NormalizedMissingIndexSuggestion] = []
    for row in rows[: settings.collection_max_rows]:
        schema = str(row.get("tabschema") or "").strip()
        table = str(row.get("tabname") or "").strip()
        card = int(row.get("card") or 0)
        suggestions.append(
            NormalizedMissingIndexSuggestion(
                instance_id=instance_id,
                database_name=db_name,
                table_name=f"{schema}.{table}",
                suggestion=(
                    f"Tabela sem nenhum indice com ~{card:,} linhas: {schema}.{table}"
                ),
                impact_score=float(card),
                detail={"card": card, "source": "syscat.tables", "schema": schema},
            )
        )
    return suggestions


async def get_engine_start_time(runner: Db2Runner, db_name: str) -> "datetime | None":
    """Hora de startup do Db2 via sysibmadm.env_inst_info.db2start_time."""
    rows = await runner.try_fetch(
        "SELECT db2start_time FROM sysibmadm.env_inst_info FETCH FIRST 1 ROWS ONLY"
    )
    if rows and rows[0].get("db2start_time"):
        val = rows[0]["db2start_time"]
        if isinstance(val, datetime):
            return val
        try:
            return datetime.fromisoformat(str(val))
        except ValueError:
            pass
    return None
