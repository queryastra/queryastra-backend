"""SQL Server — varreduras de manutencao on-demand (saude de indices, estatisticas, concessoes de
memoria, estimativa de compressao, particionamento).

Helpers sincronos (`pymssql`), chamados por `SQLServerCollector` via `run_in_instance_executor`.
Somente leitura: nenhuma tabela e criada no servidor monitorado (os scripts de referencia do DBA
gravavam `ESTIMATIVA_COMPRESS` — aqui o resultado de `sp_estimate_data_compression_savings` e
lido direto do result set). Comandos sugeridos sao so texto.

Iteracao por banco: cada banco roda com `USE [db]` numa conexao cujo timeout de statement e
`maintenance_per_database_timeout_seconds`; falha/timeout num banco descarta a conexao (pode ter
ficado em estado indefinido apos o cancelamento), registra o banco em `databases_skipped` e segue.
"""

from __future__ import annotations

import hashlib
import math
from collections import defaultdict
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any

import pymssql

from app.collectors.connection import ConnectionParams
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

# Note codes (traduzidos no frontend).
NOTE_USAGE_SINCE_RESTART = "usage_stats_since_restart"
NOTE_USAGE_RECENT = "usage_stats_recent"
NOTE_PERMISSION_DENIED = "permission_denied"
NOTE_GRANT_COLUMNS_UNAVAILABLE = "query_stats_grant_columns_unavailable"
NOTE_MEMORY_GRANTS_NATIVE = "memory_grants_native"
NOTE_COMPRESSION_NATIVE = "compression_estimate_native"
NOTE_COMPRESSION_NOT_SUPPORTED = "compression_not_supported_edition"
NOTE_MAX_OBJECTS_LIMITED = "max_objects_limited"
NOTE_PARTITION_ROWS_ESTIMATED = "partition_rows_estimated"
NOTE_DATABASE_NOT_ACCESSIBLE = "database_not_accessible"
NOTE_FRAGMENTATION_PARTIAL = "fragmentation_partial"

_ENTERPRISE_ENGINE_EDITIONS = {3, 5, 8}  # Enterprise/Developer/Evaluation, Azure SQL DB, MI
_FRAGMENTATION_REBUILD_PCT = 30.0
_FRAGMENTATION_REORGANIZE_PCT = 5.0
_UNUSED_WARNING_WRITES = 1000
_STATS_MIN_ROWS = 1000
_STATS_STALE_PCT = 20.0
_STATS_CRITICAL_PCT = 50.0
_STATS_OLD_DAYS = 30.0
_STATS_SAMPLE_ROWS = 10_000_000
_COMPRESSION_RECOMMENDED_PCT = 25.0
_PARTITION_DETAIL_MAX = 24
_PARTITION_NEAR_DAYS = 7
_TOP_QUERIES_LIMIT = 30
_SQL_TEXT_MAX = 4000

_DATE_BASE_TYPES = ("date", "datetime", "datetime2", "smalldatetime", "datetimeoffset")


def quote_ident(name: str) -> str:
    """`QUOTENAME` em Python — `]` vira `]]`. So usado com nomes vindos do proprio catalogo."""
    return "[" + name.replace("]", "]]") + "]"


def three_part(database: str | None, schema: str, obj: str) -> str:
    parts = [quote_ident(schema), quote_ident(obj)]
    if database:
        parts.insert(0, quote_ident(database))
    return ".".join(parts)


def _hash_and_mask(text: str | None) -> tuple[str | None, str | None]:
    if not text:
        return None, None
    text = text.strip()
    if not text:
        return None, None
    sql_hash = hashlib.sha256(mask_sql_literals(text).encode("utf-8")).hexdigest()
    return sql_hash, mask_sql_literals_for_display(text)[:_SQL_TEXT_MAX]


def _is_permission_error(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "permission" in msg or "view server state" in msg or "view database state" in msg


# ── Conexao / iteracao por banco ─────────────────────────────────────────────────────────────


class _Session:
    """Conexao preguicosa e descartavel — reaberta apos qualquer falha (um timeout de statement
    pode deixar o TDS num estado em que o proximo `execute` falha de forma confusa)."""

    def __init__(self, connection: ConnectionParams, statement_timeout_seconds: float) -> None:
        self._params = connection
        self._timeout = max(1, int(statement_timeout_seconds))
        self._conn: Any = None

    def cursor(self) -> Any:
        if self._conn is None:
            self._conn = pymssql.connect(
                server=self._params.host,
                port=str(self._params.port),
                user=self._params.username,
                password=self._params.secret_value,
                timeout=self._timeout,
                login_timeout=int(self._params.connect_timeout_seconds),
            )
        return self._conn.cursor(as_dict=True)

    def reset(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:  # noqa: BLE001
                pass
        self._conn = None

    def close(self) -> None:
        self.reset()


def _fetch_all(session: _Session, sql: str, params: tuple | None = None) -> list[dict]:
    cursor = session.cursor()
    try:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
        return list(cursor.fetchall())
    finally:
        cursor.close()


def _use_database(session: _Session, database: str) -> None:
    cursor = session.cursor()
    try:
        cursor.execute(f"USE {quote_ident(database)}")
    finally:
        cursor.close()


_LIST_DATABASES_AG_QUERY = """
SELECT d.name
FROM sys.databases d
LEFT JOIN sys.dm_hadr_availability_replica_states ars
       ON ars.replica_id = d.replica_id AND ars.is_local = 1
LEFT JOIN sys.availability_replicas ar ON ar.replica_id = d.replica_id
WHERE d.database_id > 4
  AND d.state_desc = 'ONLINE'
  AND HAS_DBACCESS(d.name) = 1
  AND NOT (ISNULL(ars.role_desc, '') = 'SECONDARY'
           AND ISNULL(ar.secondary_role_allow_connections_desc, 'NO') = 'NO')
ORDER BY d.name
"""

_LIST_DATABASES_SIMPLE_QUERY = """
SELECT d.name
FROM sys.databases d
WHERE d.database_id > 4 AND d.state_desc = 'ONLINE' AND HAS_DBACCESS(d.name) = 1
ORDER BY d.name
"""


def _list_user_databases(session: _Session, max_databases: int) -> list[str]:
    try:
        rows = _fetch_all(session, _LIST_DATABASES_AG_QUERY)
    except Exception:  # noqa: BLE001 - versao sem AG (2008) ou sem permissao nas DMVs de HADR
        session.reset()
        rows = _fetch_all(session, _LIST_DATABASES_SIMPLE_QUERY)
    return [str(r["name"]) for r in rows][:max_databases]


def resolve_target_databases(
    available: list[str], database: str | None
) -> tuple[list[str], list[str]]:
    """(bancos a varrer, bancos pedidos mas inacessiveis). `database` so e aceito se estiver na
    lista vinda do catalogo — nunca interpolado sem validacao."""
    if database is None:
        return list(available), []
    if database in available:
        return [database], []
    return [], [database]


@dataclass
class _ServerInfo:
    start_time: datetime | None = None
    engine_edition: int | None = None
    edition: str | None = None
    product_version: str | None = None
    notes: list[str] = field(default_factory=list)

    @property
    def major_version(self) -> int:
        try:
            return int((self.product_version or "0").split(".")[0])
        except ValueError:
            return 0

    @property
    def is_enterprise(self) -> bool:
        return self.engine_edition in _ENTERPRISE_ENGINE_EDITIONS


_SERVER_PROPS_QUERY = """
SELECT CAST(SERVERPROPERTY('EngineEdition') AS int) AS engine_edition,
       CAST(SERVERPROPERTY('Edition') AS nvarchar(128)) AS edition,
       CAST(SERVERPROPERTY('ProductVersion') AS nvarchar(128)) AS product_version
"""

_START_TIME_QUERY = "SELECT sqlserver_start_time FROM sys.dm_os_sys_info"


def _server_info(session: _Session) -> _ServerInfo:
    info = _ServerInfo()
    rows = _fetch_all(session, _SERVER_PROPS_QUERY)
    if rows:
        info.engine_edition = rows[0].get("engine_edition")
        info.edition = rows[0].get("edition")
        info.product_version = rows[0].get("product_version")
    try:
        start_rows = _fetch_all(session, _START_TIME_QUERY)
        if start_rows and start_rows[0].get("sqlserver_start_time"):
            info.start_time = _as_utc(start_rows[0]["sqlserver_start_time"])
    except Exception as exc:  # noqa: BLE001
        session.reset()
        if _is_permission_error(exc):
            info.notes.append(NOTE_PERMISSION_DENIED)
    return info


def _as_utc(value: datetime | None) -> datetime | None:
    """`sqlserver_start_time`/`last_updated` vem no horario LOCAL do servidor, sem fuso. Sem
    saber o fuso do servidor, marca como UTC (diferenca de horas nao muda nenhuma conclusao de
    dias sem atualizar / ha quanto tempo reiniciou)."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def usage_notes(start_time: datetime | None, now: datetime) -> list[str]:
    notes = [NOTE_USAGE_SINCE_RESTART]
    if start_time is not None and now - start_time < timedelta(days=7):
        notes.append(NOTE_USAGE_RECENT)
    return notes


@dataclass
class _ScanContext:
    session: _Session
    info: _ServerInfo
    databases: list[str]
    meta: NormalizedScanMeta


def _open_scan(
    connection: ConnectionParams,
    per_database_timeout: float,
    max_databases: int,
    database: str | None,
    method: str,
) -> _ScanContext:
    session = _Session(connection, per_database_timeout)
    info = _server_info(session)
    available = _list_user_databases(session, max_databases)
    targets, missing = resolve_target_databases(available, database)
    meta = NormalizedScanMeta(method=method, databases_skipped=list(missing))
    for note in info.notes:
        _add_note(meta, note)
    if missing:
        _add_note(meta, NOTE_DATABASE_NOT_ACCESSIBLE)
    return _ScanContext(session=session, info=info, databases=targets, meta=meta)


def _add_note(meta: NormalizedScanMeta, note: str) -> None:
    if note not in meta.notes:
        meta.notes.append(note)


def _for_each_database[T](
    ctx: _ScanContext, label: str, work: Callable[[str], T]
) -> list[tuple[str, T]]:
    results: list[tuple[str, T]] = []
    for db in ctx.databases:
        try:
            _use_database(ctx.session, db)
            results.append((db, work(db)))
            ctx.meta.databases_scanned.append(db)
        except Exception as exc:  # noqa: BLE001 - um banco nunca derruba a varredura
            logger.warning("Varredura %s falhou no banco %s: %s", label, db, type(exc).__name__)
            ctx.session.reset()
            ctx.meta.databases_skipped.append(db)
            if _is_permission_error(exc):
                _add_note(ctx.meta, NOTE_PERMISSION_DENIED)
    return results


# ── list_databases ───────────────────────────────────────────────────────────────────────────


def list_databases(connection: ConnectionParams, timeout: float, max_databases: int) -> list[str]:
    session = _Session(connection, timeout)
    try:
        return _list_user_databases(session, max_databases)
    finally:
        session.close()


# ── Saude de indices ─────────────────────────────────────────────────────────────────────────

_INDEX_META_QUERY = """
SELECT i.object_id, i.index_id, s.name AS schema_name, t.name AS table_name, i.name AS index_name,
       i.type_desc, i.type, i.is_unique, i.is_primary_key, i.is_unique_constraint,
       i.has_filter, i.filter_definition,
       (SELECT SUM(au.used_pages) FROM sys.partitions p
          JOIN sys.allocation_units au ON au.container_id = p.partition_id
         WHERE p.object_id = i.object_id AND p.index_id = i.index_id) * 8192 AS size_bytes
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
WHERE t.is_ms_shipped = 0 AND i.index_id > 0 AND i.type IN (1, 2)
  AND i.is_hypothetical = 0 AND i.is_disabled = 0
"""

_INDEX_COLUMNS_QUERY = """
SELECT ic.object_id, ic.index_id, c.name AS column_name, ic.key_ordinal,
       ic.is_included_column, ic.is_descending_key
FROM sys.index_columns ic
JOIN sys.columns c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
JOIN sys.tables t ON t.object_id = ic.object_id
WHERE t.is_ms_shipped = 0 AND ic.index_id > 0
"""

_INDEX_USAGE_QUERY = """
SELECT i.object_id, i.index_id,
       ISNULL(us.user_seeks, 0) + ISNULL(us.user_scans, 0) + ISNULL(us.user_lookups, 0) AS reads,
       ISNULL(us.user_updates, 0) AS writes,
       (SELECT MAX(v) FROM (VALUES (us.last_user_seek), (us.last_user_scan),
                                   (us.last_user_lookup)) AS x(v)) AS last_used
FROM sys.indexes i
JOIN sys.tables t ON t.object_id = i.object_id
LEFT JOIN sys.dm_db_index_usage_stats us
       ON us.object_id = i.object_id AND us.index_id = i.index_id AND us.database_id = DB_ID()
WHERE t.is_ms_shipped = 0 AND i.index_id > 0
"""

_FRAGMENTATION_QUERY = """
SELECT ps.object_id, ps.index_id,
       SUM(ps.avg_fragmentation_in_percent * ps.page_count) / NULLIF(SUM(ps.page_count), 0)
           AS fragmentation_pct,
       SUM(ps.page_count) AS page_count
FROM sys.dm_db_index_physical_stats(DB_ID(), NULL, NULL, NULL, 'LIMITED') ps
WHERE ps.index_id > 0 AND ps.alloc_unit_type_desc = 'IN_ROW_DATA'
GROUP BY ps.object_id, ps.index_id
HAVING SUM(ps.page_count) >= %s
"""


@dataclass
class IndexDef:
    """Definicao de indice para deteccao de duplicata/sobreposicao (puro, testavel)."""

    object_id: int
    index_id: int
    schema_name: str
    table_name: str
    index_name: str
    is_clustered: bool
    is_unique: bool = False
    is_primary_key: bool = False
    is_unique_constraint: bool = False
    filter_definition: str | None = None
    key_columns: tuple[tuple[str, bool], ...] = ()  # (coluna, descendente)
    included_columns: frozenset[str] = frozenset()
    size_bytes: int | None = None
    index_type: str | None = None

    @property
    def protected(self) -> bool:
        """PK/unique nunca recebem sugestao de DROP (garantem integridade, nao so performance)."""
        return self.is_primary_key or self.is_unique_constraint or self.is_unique

    @property
    def key_text(self) -> str:
        return ", ".join(f"{c} DESC" if d else c for c, d in self.key_columns)

    @property
    def include_text(self) -> str | None:
        return ", ".join(sorted(self.included_columns)) or None


@dataclass(frozen=True)
class RedundancyFinding:
    category: str  # "duplicate" | "overlapping"
    candidate: IndexDef
    keeper: IndexDef


def _keep_preference(idx: IndexDef) -> tuple[int, int, int, int]:
    """Maior tupla = indice a MANTER. PK > unique constraint > unique > clustered > mais antigo."""
    return (
        int(idx.is_primary_key),
        int(idx.is_unique_constraint),
        int(idx.is_unique) + int(idx.is_clustered),
        -idx.index_id,
    )


def find_redundant_indexes(indexes: list[IndexDef]) -> list[RedundancyFinding]:
    """Duplicados exatos (mesmas chaves+ordem+direcao, mesmo INCLUDE, mesmo filtro) e
    sobrepostos (chaves de A sao prefixo a esquerda estrito das chaves de B, mesmo filtro, e o
    INCLUDE de A esta coberto pelas colunas de B). Cada indice aparece no maximo uma vez como
    candidato."""
    by_table: dict[int, list[IndexDef]] = defaultdict(list)
    for idx in indexes:
        if idx.key_columns:
            by_table[idx.object_id].append(idx)

    findings: list[RedundancyFinding] = []
    for table_indexes in by_table.values():
        flagged: set[int] = set()
        ordered = sorted(table_indexes, key=lambda i: i.index_id)

        # Duplicados exatos
        for i, a in enumerate(ordered):
            for b in ordered[i + 1 :]:
                if a.index_id in flagged or b.index_id in flagged:
                    continue
                if a.key_columns != b.key_columns or a.filter_definition != b.filter_definition:
                    continue
                if a.is_clustered or b.is_clustered:
                    same_payload = True  # clustered cobre todas as colunas
                else:
                    same_payload = a.included_columns == b.included_columns
                if not same_payload:
                    continue
                keeper, candidate = (a, b) if _keep_preference(a) >= _keep_preference(b) else (b, a)
                findings.append(RedundancyFinding("duplicate", candidate, keeper))
                flagged.add(candidate.index_id)

        # Sobrepostos (prefixo a esquerda)
        for a in ordered:
            if a.index_id in flagged or a.is_clustered:
                continue
            for b in ordered:
                if b.index_id == a.index_id or b.index_id in flagged:
                    continue
                if len(a.key_columns) >= len(b.key_columns):
                    continue
                if b.key_columns[: len(a.key_columns)] != a.key_columns:
                    continue
                if a.filter_definition != b.filter_definition:
                    continue
                covered = {c for c, _ in b.key_columns} | set(b.included_columns)
                if not b.is_clustered and not a.included_columns <= covered:
                    continue
                findings.append(RedundancyFinding("overlapping", a, b))
                flagged.add(a.index_id)
                break
    return findings


def classify_fragmentation(fragmentation_pct: float) -> tuple[str, str] | None:
    """(severity, acao) ou None quando abaixo do limiar de manutencao."""
    if fragmentation_pct >= _FRAGMENTATION_REBUILD_PCT:
        return "warning", "REBUILD"
    if fragmentation_pct >= _FRAGMENTATION_REORGANIZE_PCT:
        return "info", "REORGANIZE"
    return None


def fragmentation_command(
    database: str | None, schema: str, table: str, index: str, action: str, enterprise: bool
) -> str:
    cmd = f"ALTER INDEX {quote_ident(index)} ON {three_part(database, schema, table)} {action};"
    if action == "REBUILD" and enterprise:
        cmd += " -- Enterprise: WITH (ONLINE = ON)"
    return cmd


def drop_index_command(database: str | None, schema: str, table: str, index: str) -> str:
    return f"DROP INDEX {quote_ident(index)} ON {three_part(database, schema, table)};"


def classify_unused(writes: int) -> str:
    return "warning" if writes >= _UNUSED_WARNING_WRITES else "info"


def _build_index_defs(meta_rows: list[dict], column_rows: list[dict]) -> list[IndexDef]:
    keys: dict[tuple[int, int], list[tuple[int, str, bool]]] = defaultdict(list)
    includes: dict[tuple[int, int], set[str]] = defaultdict(set)
    for r in column_rows:
        k = (int(r["object_id"]), int(r["index_id"]))
        if r["is_included_column"]:
            includes[k].add(str(r["column_name"]))
        elif int(r["key_ordinal"] or 0) > 0:
            keys[k].append(
                (int(r["key_ordinal"]), str(r["column_name"]), bool(r["is_descending_key"]))
            )
    defs: list[IndexDef] = []
    for r in meta_rows:
        k = (int(r["object_id"]), int(r["index_id"]))
        ordered = tuple((c, d) for _, c, d in sorted(keys.get(k, [])))
        defs.append(
            IndexDef(
                object_id=k[0],
                index_id=k[1],
                schema_name=str(r["schema_name"]),
                table_name=str(r["table_name"]),
                index_name=str(r["index_name"]),
                is_clustered=int(r["type"]) == 1,
                is_unique=bool(r["is_unique"]),
                is_primary_key=bool(r["is_primary_key"]),
                is_unique_constraint=bool(r["is_unique_constraint"]),
                filter_definition=r.get("filter_definition") if r.get("has_filter") else None,
                key_columns=ordered,
                included_columns=frozenset(includes.get(k, set())),
                size_bytes=int(r["size_bytes"]) if r.get("size_bytes") is not None else None,
                index_type=str(r["type_desc"]),
            )
        )
    return defs


def _scan_index_health_db(
    ctx: _ScanContext, database: str, min_pages: int
) -> list[NormalizedIndexHealthItem]:
    session = ctx.session
    defs = _build_index_defs(
        _fetch_all(session, _INDEX_META_QUERY), _fetch_all(session, _INDEX_COLUMNS_QUERY)
    )
    by_key = {(d.object_id, d.index_id): d for d in defs}

    usage: dict[tuple[int, int], dict] = {}
    try:
        for r in _fetch_all(session, _INDEX_USAGE_QUERY):
            usage[(int(r["object_id"]), int(r["index_id"]))] = r
    except Exception as exc:  # noqa: BLE001
        session.reset()
        _use_database(session, database)
        if _is_permission_error(exc):
            _add_note(ctx.meta, NOTE_PERMISSION_DENIED)

    items: list[NormalizedIndexHealthItem] = []

    def usage_fields(d: IndexDef) -> dict[str, Any]:
        u = usage.get((d.object_id, d.index_id))
        if u is None:
            return {}
        return {
            "reads": int(u["reads"] or 0),
            "writes": int(u["writes"] or 0),
            "last_used": _as_utc(u.get("last_used")),
        }

    for f in find_redundant_indexes(defs):
        c, k = f.candidate, f.keeper
        items.append(
            NormalizedIndexHealthItem(
                category=f.category,  # type: ignore[arg-type]
                database_name=database,
                schema_name=c.schema_name,
                table_name=c.table_name,
                index_name=c.index_name,
                index_type=c.index_type,
                size_bytes=c.size_bytes,
                duplicate_of=k.index_name,
                key_columns=c.key_text,
                included_columns=c.include_text,
                is_unique=c.is_unique,
                is_primary_key=c.is_primary_key,
                severity="warning" if f.category == "duplicate" else "info",
                recommendation=None
                if c.protected
                else drop_index_command(database, c.schema_name, c.table_name, c.index_name),
                detail={
                    "duplicate_of_key_columns": k.key_text,
                    "duplicate_of_included_columns": k.include_text,
                    "filter_definition": c.filter_definition,
                },
                **usage_fields(c),
            )
        )

    if usage:
        for d in defs:
            u = usage.get((d.object_id, d.index_id))
            if d.is_clustered or d.protected or u is None:
                continue
            reads, writes = int(u["reads"] or 0), int(u["writes"] or 0)
            if reads > 0 or writes <= 0:
                continue
            items.append(
                NormalizedIndexHealthItem(
                    category="unused",
                    database_name=database,
                    schema_name=d.schema_name,
                    table_name=d.table_name,
                    index_name=d.index_name,
                    index_type=d.index_type,
                    size_bytes=d.size_bytes,
                    reads=reads,
                    writes=writes,
                    last_used=_as_utc(u.get("last_used")),
                    key_columns=d.key_text,
                    included_columns=d.include_text,
                    severity=classify_unused(writes),  # type: ignore[arg-type]
                    recommendation=drop_index_command(
                        database, d.schema_name, d.table_name, d.index_name
                    )
                    + " -- valide por um ciclo de negocio completo antes",
                )
            )

    try:
        frag_rows = _fetch_all(session, _FRAGMENTATION_QUERY, (min_pages,))
    except Exception as exc:  # noqa: BLE001
        session.reset()
        _add_note(ctx.meta, NOTE_FRAGMENTATION_PARTIAL)
        if database not in ctx.meta.databases_skipped:
            ctx.meta.databases_skipped.append(database)
        if _is_permission_error(exc):
            _add_note(ctx.meta, NOTE_PERMISSION_DENIED)
        frag_rows = []

    for r in frag_rows:
        d = by_key.get((int(r["object_id"]), int(r["index_id"])))
        pct = float(r["fragmentation_pct"] or 0.0)
        cls = classify_fragmentation(pct)
        if d is None or cls is None:
            continue
        severity, action = cls
        items.append(
            NormalizedIndexHealthItem(
                category="fragmentation",
                database_name=database,
                schema_name=d.schema_name,
                table_name=d.table_name,
                index_name=d.index_name,
                index_type=d.index_type,
                size_bytes=d.size_bytes,
                fragmentation_pct=round(pct, 2),
                page_count=int(r["page_count"] or 0),
                key_columns=d.key_text,
                is_unique=d.is_unique,
                is_primary_key=d.is_primary_key,
                severity=severity,  # type: ignore[arg-type]
                recommendation=fragmentation_command(
                    database,
                    d.schema_name,
                    d.table_name,
                    d.index_name,
                    action,
                    ctx.info.is_enterprise,
                ),
                detail={"action": action},
                **usage_fields(d),
            )
        )
    return items


_SEVERITY_RANK = {"critical": 0, "warning": 1, "info": 2}


def collect_index_health(
    connection: ConnectionParams,
    database: str | None,
    per_database_timeout: float,
    max_databases: int,
    max_rows: int,
    min_pages: int,
) -> NormalizedIndexHealthReport:
    ctx = _open_scan(
        connection,
        per_database_timeout,
        max_databases,
        database,
        "dm_db_index_physical_stats(LIMITED) + dm_db_index_usage_stats",
    )
    try:
        ctx.meta.usage_stats_since = ctx.info.start_time
        for note in usage_notes(ctx.info.start_time, datetime.now(UTC)):
            _add_note(ctx.meta, note)
        per_db = _for_each_database(
            ctx, "index-health", lambda db: _scan_index_health_db(ctx, db, min_pages)
        )
    finally:
        ctx.session.close()
    items = [item for _, db_items in per_db for item in db_items]
    items.sort(key=lambda i: (_SEVERITY_RANK[i.severity], -(i.size_bytes or 0)))
    return NormalizedIndexHealthReport(items=items[:max_rows], meta=ctx.meta)


# ── Estatisticas ─────────────────────────────────────────────────────────────────────────────

_STATS_QUERY = """
SELECT s.name AS schema_name, t.name AS table_name, st.name AS stats_name, st.stats_id,
       st.auto_created, st.user_created, st.no_recompute, st.has_filter,
       sp.last_updated, sp.rows, sp.rows_sampled, sp.modification_counter,
       (SELECT SUM(p.rows) FROM sys.partitions p
         WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)) AS table_rows
FROM sys.stats st
JOIN sys.tables t ON t.object_id = st.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
OUTER APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
WHERE t.is_ms_shipped = 0
"""

# Fallback para versoes sem `sys.dm_db_stats_properties` (< 2008 R2 SP2 / 2012 SP1).
_STATS_LEGACY_QUERY = """
SELECT s.name AS schema_name, t.name AS table_name, st.name AS stats_name, st.stats_id,
       st.auto_created, st.user_created, st.no_recompute, st.has_filter,
       STATS_DATE(st.object_id, st.stats_id) AS last_updated,
       si.rowcnt AS rows, NULL AS rows_sampled, si.rowmodctr AS modification_counter,
       (SELECT SUM(p.rows) FROM sys.partitions p
         WHERE p.object_id = t.object_id AND p.index_id IN (0, 1)) AS table_rows
FROM sys.stats st
JOIN sys.tables t ON t.object_id = st.object_id
JOIN sys.schemas s ON s.schema_id = t.schema_id
LEFT JOIN sys.sysindexes si ON si.id = st.object_id AND si.indid = st.stats_id
WHERE t.is_ms_shipped = 0
"""


@dataclass(frozen=True)
class StatsAssessment:
    is_stale: bool
    severity: str
    modified_pct: float | None
    days_since_update: float | None


def assess_statistics(
    rows: int | None,
    modified: int | None,
    last_updated: datetime | None,
    now: datetime,
) -> StatsAssessment | None:
    """None = nada a reportar. Stale quando: nunca atualizada com linhas; >= 20% modificado; ou
    modificacoes acima do limiar dinamico SQRT(1000 * linhas) (o mesmo do auto-update 2016+)."""
    rows = rows or 0
    if rows < _STATS_MIN_ROWS:
        return None
    days = (now - last_updated).total_seconds() / 86400 if last_updated else None
    if last_updated is None:
        return StatsAssessment(True, "warning", None, None)
    modified = modified or 0
    pct = modified / rows * 100 if rows else None
    dynamic_threshold = math.sqrt(1000 * rows)
    stale = (pct is not None and pct >= _STATS_STALE_PCT) or modified > dynamic_threshold
    if stale:
        severity = "critical" if pct is not None and pct >= _STATS_CRITICAL_PCT else "warning"
        return StatsAssessment(True, severity, pct, days)
    if days is not None and days >= _STATS_OLD_DAYS and modified > 0:
        return StatsAssessment(False, "info", pct, days)
    return None


def update_statistics_command(
    database: str | None, schema: str, table: str, stats_name: str | None, rows: int | None
) -> str:
    target = three_part(database, schema, table)
    if stats_name:
        target += f" ({quote_ident(stats_name)})"
    option = "WITH SAMPLE 10 PERCENT" if (rows or 0) >= _STATS_SAMPLE_ROWS else "WITH FULLSCAN"
    return f"UPDATE STATISTICS {target} {option};"


def _scan_statistics_db(
    ctx: _ScanContext, database: str, now: datetime
) -> list[NormalizedStatisticsHealthItem]:
    try:
        rows = _fetch_all(ctx.session, _STATS_QUERY)
    except Exception as exc:  # noqa: BLE001
        if _is_permission_error(exc):
            raise
        ctx.session.reset()
        _use_database(ctx.session, database)
        rows = _fetch_all(ctx.session, _STATS_LEGACY_QUERY)
        ctx.meta.method = "STATS_DATE + sysindexes.rowmodctr"

    items: list[NormalizedStatisticsHealthItem] = []
    for r in rows:
        row_count = r.get("rows")
        if row_count is None:
            row_count = r.get("table_rows")
        row_count = int(row_count) if row_count is not None else None
        modified = (
            int(r["modification_counter"]) if r.get("modification_counter") is not None else None
        )
        last_updated = _as_utc(r.get("last_updated"))
        assessment = assess_statistics(row_count, modified, last_updated, now)
        if assessment is None:
            continue
        items.append(
            NormalizedStatisticsHealthItem(
                database_name=database,
                schema_name=str(r["schema_name"]),
                table_name=str(r["table_name"]),
                stats_name=str(r["stats_name"]),
                last_updated=last_updated,
                row_count=row_count,
                modified_rows=modified,
                modified_pct=round(assessment.modified_pct, 2)
                if assessment.modified_pct is not None
                else None,
                days_since_update=round(assessment.days_since_update, 1)
                if assessment.days_since_update is not None
                else None,
                is_stale=assessment.is_stale,
                severity=assessment.severity,  # type: ignore[arg-type]
                recommendation=update_statistics_command(
                    database,
                    str(r["schema_name"]),
                    str(r["table_name"]),
                    str(r["stats_name"]),
                    row_count,
                ),
                detail={
                    "auto_created": bool(r.get("auto_created")),
                    "user_created": bool(r.get("user_created")),
                    "no_recompute": bool(r.get("no_recompute")),
                    "has_filter": bool(r.get("has_filter")),
                    "rows_sampled": int(r["rows_sampled"])
                    if r.get("rows_sampled") is not None
                    else None,
                },
            )
        )
    return items


def collect_statistics_health(
    connection: ConnectionParams,
    database: str | None,
    per_database_timeout: float,
    max_databases: int,
    max_rows: int,
) -> NormalizedStatisticsHealthReport:
    ctx = _open_scan(
        connection, per_database_timeout, max_databases, database, "sys.dm_db_stats_properties"
    )
    now = datetime.now(UTC)
    try:
        per_db = _for_each_database(ctx, "statistics", lambda db: _scan_statistics_db(ctx, db, now))
    finally:
        ctx.session.close()
    items = [item for _, db_items in per_db for item in db_items]
    items.sort(
        key=lambda i: (_SEVERITY_RANK[i.severity], -(i.modified_pct or 0), -(i.row_count or 0))
    )
    return NormalizedStatisticsHealthReport(items=items[:max_rows], meta=ctx.meta)


# ── Concessoes de memoria ────────────────────────────────────────────────────────────────────

_ACTIVE_GRANTS_QUERY = """
SELECT mg.session_id, mg.request_id, DB_NAME(r.database_id) AS database_name, s.login_name,
       mg.requested_memory_kb, mg.granted_memory_kb, mg.used_memory_kb, mg.max_used_memory_kb,
       mg.ideal_memory_kb, mg.wait_time_ms, mg.dop, mg.grant_time, mg.request_time,
       mg.query_cost, mg.resource_semaphore_id, mg.wait_order, mg.is_next_candidate,
       CASE WHEN r.statement_start_offset IS NULL THEN t.text
            ELSE SUBSTRING(t.text, (r.statement_start_offset / 2) + 1,
                 ((CASE r.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
                   ELSE r.statement_end_offset END - r.statement_start_offset) / 2) + 1)
       END AS statement_text
FROM sys.dm_exec_query_memory_grants mg
LEFT JOIN sys.dm_exec_requests r ON r.session_id = mg.session_id AND r.request_id = mg.request_id
LEFT JOIN sys.dm_exec_sessions s ON s.session_id = mg.session_id
OUTER APPLY sys.dm_exec_sql_text(mg.sql_handle) t
WHERE mg.session_id <> @@SPID
ORDER BY mg.requested_memory_kb DESC
"""

_SEMAPHORES_QUERY = """
SELECT SUM(target_memory_kb) AS target_kb, SUM(total_memory_kb) AS total_kb,
       SUM(available_memory_kb) AS available_kb, SUM(granted_memory_kb) AS granted_kb,
       SUM(used_memory_kb) AS used_kb, SUM(grantee_count) AS grantees,
       SUM(waiter_count) AS waiters, SUM(timeout_error_count) AS timeout_errors,
       SUM(forced_grant_count) AS forced_grants
FROM sys.dm_exec_query_resource_semaphores
"""

_RESOURCE_SEMAPHORE_WAIT_QUERY = """
SELECT wait_time_ms, waiting_tasks_count, max_wait_time_ms
FROM sys.dm_os_wait_stats WHERE wait_type = 'RESOURCE_SEMAPHORE'
"""

# `{spills}` = colunas de spill (2016 SP2+/2017 CU3+) ou NULLs no fallback.
_TOP_GRANTS_QUERY_TEMPLATE = """
WITH top_qs AS (
    SELECT TOP ({limit}) qs.sql_handle, qs.plan_handle, qs.statement_start_offset,
           qs.statement_end_offset, qs.execution_count, qs.last_execution_time,
           qs.total_grant_kb, qs.last_grant_kb, qs.min_grant_kb, qs.max_grant_kb,
           qs.total_used_grant_kb, qs.max_used_grant_kb, qs.total_ideal_grant_kb,
           qs.max_ideal_grant_kb, {spills}
    FROM sys.dm_exec_query_stats qs
    WHERE {where}
    ORDER BY {order_by} DESC
)
SELECT q.*, DB_NAME(CAST(pa.value AS int)) AS database_name,
       SUBSTRING(t.text, (q.statement_start_offset / 2) + 1,
           ((CASE q.statement_end_offset WHEN -1 THEN DATALENGTH(t.text)
             ELSE q.statement_end_offset END - q.statement_start_offset) / 2) + 1)
           AS statement_text
FROM top_qs q
OUTER APPLY sys.dm_exec_sql_text(q.sql_handle) t
OUTER APPLY (SELECT value FROM sys.dm_exec_plan_attributes(q.plan_handle)
             WHERE attribute = 'dbid') pa
"""

_SPILL_COLUMNS = "qs.total_spills, qs.max_spills, qs.last_spills"
_NO_SPILL_COLUMNS = "NULL AS total_spills, NULL AS max_spills, NULL AS last_spills"


def classify_memory_query(
    max_grant_kb: float | None, unused_grant_pct: float | None, spill_kb: int | None
) -> str:
    max_grant_kb = max_grant_kb or 0
    spill_kb = spill_kb or 0
    if spill_kb >= 1024 * 1024 or max_grant_kb >= 4 * 1024 * 1024:
        return "critical"
    if (
        max_grant_kb >= 512 * 1024
        or spill_kb > 0
        or ((unused_grant_pct or 0) >= 80 and max_grant_kb >= 50 * 1024)
    ):
        return "warning"
    return "info"


def aggregate_top_queries(rows: list[dict]) -> list[NormalizedMemoryGrantTopQuery]:
    """Agrega linhas de `dm_exec_query_stats` (um statement pode ter varios planos) por
    `sql_hash`. Spills sao em PAGINAS de 8KB."""
    groups: dict[str, dict[str, Any]] = {}
    for r in rows:
        sql_hash, masked = _hash_and_mask(r.get("statement_text"))
        if sql_hash is None or masked is None:
            continue
        g = groups.setdefault(
            sql_hash,
            {
                "text": masked,
                "database_name": r.get("database_name"),
                "executions": 0,
                "total_grant": 0,
                "total_used": 0,
                "max_grant": 0,
                "max_used": 0,
                "spill_pages": None,
                "last_execution": None,
                "max_ideal": 0,
                "last_grant": None,
                "plans": 0,
                "seen": set(),
            },
        )
        identity = (r.get("plan_handle"), r.get("statement_start_offset"))
        if identity in g["seen"]:
            continue
        g["seen"].add(identity)
        g["plans"] += 1
        g["executions"] += int(r.get("execution_count") or 0)
        g["total_grant"] += int(r.get("total_grant_kb") or 0)
        g["total_used"] += int(r.get("total_used_grant_kb") or 0)
        g["max_grant"] = max(g["max_grant"], int(r.get("max_grant_kb") or 0))
        g["max_used"] = max(g["max_used"], int(r.get("max_used_grant_kb") or 0))
        g["max_ideal"] = max(g["max_ideal"], int(r.get("max_ideal_grant_kb") or 0))
        if r.get("total_spills") is not None:
            g["spill_pages"] = (g["spill_pages"] or 0) + int(r["total_spills"])
        last = _as_utc(r.get("last_execution_time"))
        if last and (g["last_execution"] is None or last > g["last_execution"]):
            g["last_execution"] = last
            g["last_grant"] = r.get("last_grant_kb")

    result: list[NormalizedMemoryGrantTopQuery] = []
    for sql_hash, g in groups.items():
        execs = g["executions"] or 0
        avg_grant = g["total_grant"] / execs if execs else None
        avg_used = g["total_used"] / execs if execs else None
        unused_pct = (
            round((1 - g["total_used"] / g["total_grant"]) * 100, 1)
            if g["total_grant"] > 0
            else None
        )
        spill_kb = g["spill_pages"] * 8 if g["spill_pages"] is not None else None
        result.append(
            NormalizedMemoryGrantTopQuery(
                sql_hash=sql_hash,
                sql_text_masked=g["text"],
                database_name=g["database_name"],
                executions=execs,
                avg_grant_kb=round(avg_grant, 1) if avg_grant is not None else None,
                max_grant_kb=float(g["max_grant"]),
                avg_used_kb=round(avg_used, 1) if avg_used is not None else None,
                max_used_kb=float(g["max_used"]),
                unused_grant_pct=unused_pct,
                spill_kb=spill_kb,
                last_execution=g["last_execution"],
                severity=classify_memory_query(g["max_grant"], unused_pct, spill_kb),  # type: ignore[arg-type]
                detail={
                    "plans": g["plans"],
                    "max_ideal_grant_kb": g["max_ideal"],
                    "last_grant_kb": g["last_grant"],
                },
            )
        )
    result.sort(
        key=lambda q: (_SEVERITY_RANK[q.severity], -(q.spill_kb or 0), -(q.max_grant_kb or 0))
    )
    return result[:_TOP_QUERIES_LIMIT]


def _fetch_top_grant_rows(session: _Session, meta: NormalizedScanMeta) -> list[dict]:
    def build(spills: str, where: str, order_by: str, limit: int) -> str:
        return _TOP_GRANTS_QUERY_TEMPLATE.format(
            spills=spills, where=where, order_by=order_by, limit=limit
        )

    try:
        by_grant = _fetch_all(
            session, build(_SPILL_COLUMNS, "qs.max_grant_kb > 0", "qs.max_grant_kb", 50)
        )
        by_spill = _fetch_all(
            session, build(_SPILL_COLUMNS, "qs.total_spills > 0", "qs.total_spills", 20)
        )
        return by_grant + by_spill
    except Exception:  # noqa: BLE001 - sem colunas de spill (< 2016 SP2)
        session.reset()
    try:
        rows = _fetch_all(
            session, build(_NO_SPILL_COLUMNS, "qs.max_grant_kb > 0", "qs.max_grant_kb", 50)
        )
    except Exception as exc:  # noqa: BLE001 - sem colunas de grant (< 2012 SP3/2014 SP2)
        session.reset()
        _add_note(meta, NOTE_GRANT_COLUMNS_UNAVAILABLE)
        if _is_permission_error(exc):
            _add_note(meta, NOTE_PERMISSION_DENIED)
        return []
    _add_note(meta, NOTE_GRANT_COLUMNS_UNAVAILABLE)
    return rows


def build_active_grant(r: dict) -> NormalizedMemoryGrantActive:
    sql_hash, masked = _hash_and_mask(r.get("statement_text"))
    return NormalizedMemoryGrantActive(
        session_id=str(r["session_id"]),
        database_name=r.get("database_name"),
        login_name=r.get("login_name"),
        sql_hash=sql_hash,
        sql_text_masked=masked,
        requested_kb=_int_or_none(r.get("requested_memory_kb")),
        granted_kb=_int_or_none(r.get("granted_memory_kb")),
        used_kb=_int_or_none(r.get("used_memory_kb")),
        max_used_kb=_int_or_none(r.get("max_used_memory_kb")),
        ideal_kb=_int_or_none(r.get("ideal_memory_kb")),
        wait_time_ms=_int_or_none(r.get("wait_time_ms")),
        is_waiting=r.get("grant_time") is None,
        dop=_int_or_none(r.get("dop")),
        detail={
            "request_id": r.get("request_id"),
            "query_cost": float(r["query_cost"]) if r.get("query_cost") is not None else None,
            "resource_semaphore_id": r.get("resource_semaphore_id"),
            "wait_order": r.get("wait_order"),
            "is_next_candidate": r.get("is_next_candidate"),
        },
    )


def _int_or_none(value: Any) -> int | None:
    return int(value) if value is not None else None


def collect_memory_grants(
    connection: ConnectionParams, timeout: float
) -> NormalizedMemoryGrantReport:
    session = _Session(connection, timeout)
    meta = NormalizedScanMeta(
        method="dm_exec_query_memory_grants", notes=[NOTE_MEMORY_GRANTS_NATIVE]
    )
    summary = NormalizedMemoryGrantSummary()
    active: list[NormalizedMemoryGrantActive] = []
    try:
        try:
            active = [build_active_grant(r) for r in _fetch_all(session, _ACTIVE_GRANTS_QUERY)]
        except Exception as exc:  # noqa: BLE001
            session.reset()
            if not _is_permission_error(exc):
                raise
            _add_note(meta, NOTE_PERMISSION_DENIED)
            meta.supported = False
            return NormalizedMemoryGrantReport(meta=meta)

        sem = (_fetch_all(session, _SEMAPHORES_QUERY) or [{}])[0]
        wait = (_fetch_all(session, _RESOURCE_SEMAPHORE_WAIT_QUERY) or [{}])[0]
        summary = NormalizedMemoryGrantSummary(
            granted_requests=sum(1 for a in active if not a.is_waiting),
            waiting_requests=sum(1 for a in active if a.is_waiting),
            total_granted_kb=_int_or_none(sem.get("granted_kb")),
            total_used_kb=_int_or_none(sem.get("used_kb")),
            available_kb=_int_or_none(sem.get("available_kb")),
            target_kb=_int_or_none(sem.get("target_kb")),
            memory_wait_ms_total=_int_or_none(wait.get("wait_time_ms")),
            detail={
                "total_kb": _int_or_none(sem.get("total_kb")),
                "semaphore_waiters": _int_or_none(sem.get("waiters")),
                "semaphore_grantees": _int_or_none(sem.get("grantees")),
                "timeout_errors": _int_or_none(sem.get("timeout_errors")),
                "forced_grants": _int_or_none(sem.get("forced_grants")),
                "resource_semaphore_waiting_tasks": _int_or_none(wait.get("waiting_tasks_count")),
                "resource_semaphore_max_wait_ms": _int_or_none(wait.get("max_wait_time_ms")),
            },
        )
        top = aggregate_top_queries(_fetch_top_grant_rows(session, meta))
        info = _server_info(session)
        meta.usage_stats_since = info.start_time
    finally:
        session.close()
    return NormalizedMemoryGrantReport(summary=summary, active=active, top_queries=top, meta=meta)


# ── Compressao ───────────────────────────────────────────────────────────────────────────────

# Script de referencia (CalculoEstimativaCompress.sql) exclui tabelas `DT_%` — o `_` ali era
# curinga sem querer; aqui e literal.
_COMPRESSION_CANDIDATES_QUERY = """
SELECT s.name AS schema_name, t.name AS table_name, i.index_id, i.name AS index_name,
       i.type_desc, SUM(au.used_pages) * 8 AS used_kb,
       MIN(p.data_compression_desc) AS min_compression,
       MAX(p.data_compression_desc) AS max_compression,
       COUNT(DISTINCT p.partition_number) AS partition_count
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.indexes i ON i.object_id = t.object_id
JOIN sys.partitions p ON p.object_id = i.object_id AND p.index_id = i.index_id
JOIN sys.allocation_units au ON au.container_id = p.partition_id
WHERE t.is_ms_shipped = 0 AND i.type IN (0, 1, 2) AND i.is_hypothetical = 0
  AND i.is_disabled = 0 AND t.name NOT LIKE 'DT[_]%'
GROUP BY s.name, t.name, i.index_id, i.name, i.type_desc
"""

_ESTIMATE_SQL = (
    "EXEC sys.sp_estimate_data_compression_savings "
    "@schema_name = %s, @object_name = %s, @index_id = NULL, @partition_number = NULL, "
    "@data_compression = %s"
)

_EST_CURRENT = "size_with_current_compression_setting(KB)"
_EST_REQUESTED = "size_with_requested_compression_setting(KB)"
_EST_SAMPLE_CURRENT = "sample_size_with_current_compression_setting(KB)"
_EST_SAMPLE_REQUESTED = "sample_size_with_requested_compression_setting(KB)"


def compression_supported(info: _ServerInfo) -> bool:
    """Enterprise/Developer (qualquer versao) ou qualquer edicao a partir do 2016 SP1."""
    if info.is_enterprise:
        return True
    parts = (info.product_version or "0").split(".")
    try:
        major, build = int(parts[0]), int(parts[2]) if len(parts) > 2 else 0
    except ValueError:
        return False
    return major > 13 or (major == 13 and build >= 4001)


def compression_savings_pct(current_kb: int | None, requested_kb: int | None) -> float | None:
    """Ganho = 1 - requested/current (o script original dividia sample pelo requested)."""
    if not current_kb or requested_kb is None:
        return None
    return round((1 - requested_kb / current_kb) * 100, 2)


def compression_command(
    database: str | None,
    schema: str,
    table: str,
    index_id: int,
    index_name: str | None,
    compression: str,
) -> str:
    target = three_part(database, schema, table)
    if index_id in (0, 1) or not index_name:
        return (
            f"ALTER TABLE {target} REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = {compression});"
        )
    return (
        f"ALTER INDEX {quote_ident(index_name)} ON {target} "
        f"REBUILD PARTITION = ALL WITH (DATA_COMPRESSION = {compression});"
    )


@dataclass
class _CompressionCandidate:
    database: str
    schema_name: str
    table_name: str
    used_kb: int
    indexes: dict[int, dict] = field(default_factory=dict)


def select_compression_candidates(
    rows_by_db: list[tuple[str, list[dict]]], compression: str, max_objects: int
) -> tuple[list[_CompressionCandidate], bool]:
    """Tabelas (soma dos indices ainda nao comprimidos no nivel pedido), maiores primeiro.
    Devolve (selecionadas, truncou?)."""
    tables: dict[tuple[str, str, str], _CompressionCandidate] = {}
    for db, rows in rows_by_db:
        for r in rows:
            key = (db, str(r["schema_name"]), str(r["table_name"]))
            cand = tables.setdefault(key, _CompressionCandidate(db, key[1], key[2], 0))
            cand.indexes[int(r["index_id"])] = r
            already = (
                r.get("min_compression") == compression and r.get("max_compression") == compression
            )
            if not already:
                cand.used_kb += int(r.get("used_kb") or 0)
    pending = [c for c in tables.values() if c.used_kb > 0]
    pending.sort(key=lambda c: -c.used_kb)
    return pending[:max_objects], len(pending) > max_objects


def _current_compression(row: dict | None) -> str | None:
    if not row:
        return None
    lo, hi = row.get("min_compression"), row.get("max_compression")
    return lo if lo == hi else "MIXED"


def build_compression_items(
    cand: _CompressionCandidate, estimate_rows: list[dict], compression: str
) -> list[NormalizedCompressionItem]:
    per_index: dict[int, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for r in estimate_rows:
        idx = per_index[int(r["index_id"])]
        for col in (_EST_CURRENT, _EST_REQUESTED, _EST_SAMPLE_CURRENT, _EST_SAMPLE_REQUESTED):
            idx[col] += int(r.get(col) or 0)
        idx["partitions"] += 1
    items: list[NormalizedCompressionItem] = []
    for index_id, sizes in sorted(per_index.items()):
        meta_row = cand.indexes.get(index_id)
        index_name = meta_row.get("index_name") if meta_row else None
        current = _current_compression(meta_row)
        if current == compression:
            continue
        savings = compression_savings_pct(sizes[_EST_CURRENT], sizes[_EST_REQUESTED])
        items.append(
            NormalizedCompressionItem(
                database_name=cand.database,
                schema_name=cand.schema_name,
                table_name=cand.table_name,
                index_name=index_name,
                index_id=index_id,
                current_compression=current,
                requested_compression=compression,
                current_size_kb=sizes[_EST_CURRENT],
                estimated_size_kb=sizes[_EST_REQUESTED],
                savings_pct=savings,
                command=compression_command(
                    cand.database,
                    cand.schema_name,
                    cand.table_name,
                    index_id,
                    index_name,
                    compression,
                )
                if savings is not None and savings > 0
                else None,
                detail={
                    "index_type": meta_row.get("type_desc") if meta_row else None,
                    "partitions": sizes["partitions"],
                    "sample_current_kb": sizes[_EST_SAMPLE_CURRENT],
                    "sample_requested_kb": sizes[_EST_SAMPLE_REQUESTED],
                    "recommended": savings is not None and savings >= _COMPRESSION_RECOMMENDED_PCT,
                },
            )
        )
    return items


def estimate_compression(
    connection: ConnectionParams,
    database: str | None,
    compression: str,
    max_objects: int,
    per_database_timeout: float,
    max_databases: int,
) -> NormalizedCompressionReport:
    ctx = _open_scan(
        connection,
        per_database_timeout,
        max_databases,
        database,
        "sp_estimate_data_compression_savings",
    )
    try:
        if not compression_supported(ctx.info):
            ctx.meta.supported = False
            _add_note(ctx.meta, NOTE_COMPRESSION_NOT_SUPPORTED)
            return NormalizedCompressionReport(meta=ctx.meta)
        _add_note(ctx.meta, NOTE_COMPRESSION_NATIVE)
        candidates_by_db = _for_each_database(
            ctx, "compression", lambda _db: _fetch_all(ctx.session, _COMPRESSION_CANDIDATES_QUERY)
        )
        selected, truncated = select_compression_candidates(
            candidates_by_db, compression, max_objects
        )
        if truncated:
            _add_note(ctx.meta, NOTE_MAX_OBJECTS_LIMITED)

        items: list[NormalizedCompressionItem] = []
        current_db: str | None = None
        for cand in selected:
            try:
                if cand.database != current_db:
                    _use_database(ctx.session, cand.database)
                    current_db = cand.database
                rows = _fetch_all(
                    ctx.session, _ESTIMATE_SQL, (cand.schema_name, cand.table_name, compression)
                )
                items.extend(build_compression_items(cand, rows, compression))
            except Exception as exc:  # noqa: BLE001 - 1 objeto nunca derruba a estimativa
                logger.warning(
                    "sp_estimate_data_compression_savings falhou em %s.%s.%s: %s",
                    cand.database,
                    cand.schema_name,
                    cand.table_name,
                    type(exc).__name__,
                )
                ctx.session.reset()
                current_db = None
                if _is_permission_error(exc):
                    _add_note(ctx.meta, NOTE_PERMISSION_DENIED)
                items.append(
                    NormalizedCompressionItem(
                        database_name=cand.database,
                        schema_name=cand.schema_name,
                        table_name=cand.table_name,
                        requested_compression=compression,
                        current_size_kb=cand.used_kb,
                        detail={"estimate_failed": True, "error": type(exc).__name__},
                    )
                )
    finally:
        ctx.session.close()
    items.sort(key=lambda i: -(i.savings_pct if i.savings_pct is not None else -1e9))
    return NormalizedCompressionReport(items=items, meta=ctx.meta)


# ── Particionamento ──────────────────────────────────────────────────────────────────────────

_PARTITIONS_QUERY = """
SELECT s.name AS schema_name, t.name AS table_name, i.index_id, i.name AS index_name,
       i.type_desc AS index_type, ps.name AS partition_scheme, pf.name AS partition_function,
       pf.boundary_value_on_right, pf.fanout, p.partition_number, p.rows,
       p.data_compression_desc, fg.name AS filegroup_name, col.name AS partition_key,
       CAST(SQL_VARIANT_PROPERTY(rv.value, 'BaseType') AS nvarchar(64)) AS boundary_type,
       CASE WHEN rv.value IS NULL THEN NULL
            WHEN CAST(SQL_VARIANT_PROPERTY(rv.value, 'BaseType') AS nvarchar(64))
                 IN ('date', 'datetime', 'datetime2', 'smalldatetime', 'datetimeoffset')
                 THEN CONVERT(nvarchar(64), CAST(rv.value AS datetime2), 121)
            ELSE CONVERT(nvarchar(4000), rv.value) END AS boundary_value
FROM sys.tables t
JOIN sys.schemas s ON s.schema_id = t.schema_id
JOIN sys.indexes i ON i.object_id = t.object_id
JOIN sys.partition_schemes ps ON ps.data_space_id = i.data_space_id
JOIN sys.partition_functions pf ON pf.function_id = ps.function_id
JOIN sys.partitions p ON p.object_id = i.object_id AND p.index_id = i.index_id
LEFT JOIN sys.partition_range_values rv
       ON rv.function_id = pf.function_id AND rv.boundary_id = p.partition_number
LEFT JOIN sys.destination_data_spaces dds
       ON dds.partition_scheme_id = ps.data_space_id AND dds.destination_id = p.partition_number
LEFT JOIN sys.filegroups fg ON fg.data_space_id = dds.data_space_id
LEFT JOIN sys.index_columns ic
       ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND ic.partition_ordinal = 1
LEFT JOIN sys.columns col ON col.object_id = ic.object_id AND col.column_id = ic.column_id
WHERE t.is_ms_shipped = 0
ORDER BY s.name, t.name, i.index_id, p.partition_number
"""


def _parse_boundary_date(value: str | None, base_type: str | None) -> datetime | None:
    if not value or (base_type or "").lower() not in _DATE_BASE_TYPES:
        return None
    try:
        return datetime.fromisoformat(value[:26]).replace(tzinfo=UTC)
    except ValueError:
        return None


def summarize_partitions(
    database: str | None, rows: list[dict], now: datetime
) -> list[NormalizedPartitionInfo]:
    """Uma linha por tabela/indice particionado. A ultima particao (sem limite superior) e a que
    recebe dados alem do ultimo range: linhas nela = ranges acabaram."""
    groups: dict[tuple[str, str, int], list[dict]] = defaultdict(list)
    for r in rows:
        groups[(str(r["schema_name"]), str(r["table_name"]), int(r["index_id"]))].append(r)

    infos: list[NormalizedPartitionInfo] = []
    for (schema, table, _index_id), parts in groups.items():
        parts.sort(key=lambda r: int(r["partition_number"]))
        first, last = parts[0], parts[-1]
        bounded = [p for p in parts if p.get("boundary_value") is not None]
        last_bounded = bounded[-1] if bounded else None
        last_boundary = last_bounded.get("boundary_value") if last_bounded else None
        boundary_type = last_bounded.get("boundary_type") if last_bounded else None
        beyond_rows = int(last["rows"] or 0) if last.get("boundary_value") is None else 0
        total_rows = sum(int(p["rows"] or 0) for p in parts)
        compressions = {p.get("data_compression_desc") for p in parts}
        range_type = "RIGHT" if first.get("boundary_value_on_right") else "LEFT"

        severity = "info"
        reason = None
        boundary_date = _parse_boundary_date(last_boundary, boundary_type)
        if beyond_rows > 0:
            severity, reason = "warning", "rows_beyond_last_boundary"
        elif boundary_date is not None and boundary_date < now + timedelta(
            days=_PARTITION_NEAR_DAYS
        ):
            severity, reason = "warning", "last_boundary_near"

        recommendation = None
        if (
            severity == "warning"
            and first.get("partition_scheme")
            and first.get("partition_function")
        ):
            filegroup = last.get("filegroup_name") or "<filegroup>"
            recommendation = (
                "-- Crie o proximo range antes que os dados caiam na ultima particao:\n"
                f"ALTER PARTITION SCHEME {quote_ident(str(first['partition_scheme']))} "
                f"NEXT USED {quote_ident(str(filegroup))};\n"
                f"ALTER PARTITION FUNCTION {quote_ident(str(first['partition_function']))}() "
                "SPLIT RANGE (<novo_limite>);"
            )
            if database:
                recommendation = f"USE {quote_ident(database)};\n{recommendation}"

        infos.append(
            NormalizedPartitionInfo(
                database_name=database,
                schema_name=schema,
                table_name=table,
                index_name=first.get("index_name"),
                index_type=first.get("index_type"),
                partition_method="RANGE",
                partition_scheme=first.get("partition_scheme"),
                partition_function=first.get("partition_function"),
                partition_key=first.get("partition_key"),
                range_type=range_type,
                partition_count=len(parts),
                last_boundary_value=last_boundary,
                rows_beyond_last_boundary=beyond_rows,
                total_rows=total_rows,
                data_compression=compressions.pop() if len(compressions) == 1 else "MIXED",
                severity=severity,  # type: ignore[arg-type]
                recommendation=recommendation,
                detail={
                    "reason": reason,
                    "boundary_type": boundary_type,
                    "partitions": [
                        {
                            "number": int(p["partition_number"]),
                            "boundary": p.get("boundary_value"),
                            "rows": int(p["rows"] or 0),
                            "compression": p.get("data_compression_desc"),
                            "filegroup": p.get("filegroup_name"),
                        }
                        for p in parts[-_PARTITION_DETAIL_MAX:]
                    ],
                },
            )
        )
    return infos


def collect_partitions(
    connection: ConnectionParams,
    database: str | None,
    per_database_timeout: float,
    max_databases: int,
    max_rows: int,
) -> NormalizedPartitionReport:
    ctx = _open_scan(connection, per_database_timeout, max_databases, database, "sys.partition_*")
    _add_note(ctx.meta, NOTE_PARTITION_ROWS_ESTIMATED)
    now = datetime.now(UTC)
    try:
        per_db = _for_each_database(
            ctx,
            "partitions",
            lambda db: summarize_partitions(db, _fetch_all(ctx.session, _PARTITIONS_QUERY), now),
        )
    finally:
        ctx.session.close()
    items = [i for _, db_items in per_db for i in db_items]
    items.sort(key=lambda i: (_SEVERITY_RANK[i.severity], -(i.rows_beyond_last_boundary or 0)))
    return NormalizedPartitionReport(items=items[:max_rows], meta=ctx.meta)
