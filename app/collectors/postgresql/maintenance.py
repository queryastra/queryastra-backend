"""Varreduras de manutencao on-demand para PostgreSQL (e Aurora PostgreSQL).

Saude de indices, estatisticas desatualizadas, "concessoes de memoria" (proxy via spill de
work_mem), inventario de compressao TOAST e particionamento declarativo. Tudo somente leitura;
comandos sugeridos sao so texto.

As views `pg_stat_user_*`/`pg_index`/`pg_partitioned_table` sao escopadas ao banco CONECTADO,
entao toda varredura itera banco a banco (`scan_databases`) com timeout proprio por banco — um
banco lento/sem permissao vai para `meta.databases_skipped` sem derrubar o resto.

Funcoes puras (sem I/O) ficam no topo para teste unitario; as funcoes async recebem um
`ConnectFn` (o `_connect` do adaptador) para nao depender da classe do coletor.
"""

from __future__ import annotations

import asyncio
import hashlib
import re
from collections.abc import Awaitable, Callable, Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, TypeVar

import asyncpg

from app.core.config import get_settings
from app.core.logging import get_logger
from app.masking.sql_masking import mask_sql_literals, mask_sql_literals_for_display
from app.normalizers.models import (
    NormalizedCompressionItem,
    NormalizedIndexHealthItem,
    NormalizedMemoryGrantActive,
    NormalizedMemoryGrantReport,
    NormalizedMemoryGrantSummary,
    NormalizedMemoryGrantTopQuery,
    NormalizedPartitionInfo,
    NormalizedScanMeta,
    NormalizedStatisticsHealthItem,
)

logger = get_logger(__name__)

ConnectFn = Callable[[str], Awaitable[asyncpg.Connection]]
T = TypeVar("T")

PG11 = 110000
PG13 = 130000
PG14 = 140000
PG16 = 160000

# Contadores com menos que isso de historia nao cobrem um ciclo de negocio completo (fechamento
# semanal etc.) — "indice sem uso" fica pouco confiavel.
USAGE_STATS_RECENT_DAYS = 7
# pgstatindex le o indice inteiro: limita aos maiores para nao gerar I/O pesado.
PGSTATINDEX_MAX_INDEXES = 20
TOP_QUERIES_LIMIT = 25
ACTIVE_LIMIT = 50
PARTITION_DETAIL_LIMIT = 24
# Indice "sem uso" menor que isso e ruido (custo de escrita desprezivel).
UNUSED_MIN_WARNING_BYTES = 10 * 1024 * 1024


# ── Funcoes puras ──────────────────────────────────────────────────────────────────────────


def quote_ident(name: str) -> str:
    """Sempre entre aspas: nome vindo do catalogo pode ter maiusculas/espacos/aspas."""
    return '"' + name.replace('"', '""') + '"'


def qualified(schema: str, name: str) -> str:
    return f"{quote_ident(schema)}.{quote_ident(name)}"


def sql_hash(query_text: str) -> str:
    """Mesma identidade usada por `query_samples.sql_hash` (mascaramento blanket)."""
    return hashlib.sha256(mask_sql_literals(query_text).encode("utf-8")).hexdigest()


def fragmentation_severity(pct: float) -> str:
    if pct >= 60:
        return "critical"
    if pct >= 30:
        return "warning"
    return "info"


def density_to_fragmentation(avg_leaf_density: float | None, fillfactor: int = 90) -> float:
    """Converte densidade media das folhas em "% de espaco desperdicado" relativo ao fillfactor.

    Um btree recem-criado tem densidade ~= fillfactor (90 por padrao) — so o que esta abaixo
    disso e desperdicio. Densidade NaN/None (indice vazio) = 0."""
    if avg_leaf_density is None or avg_leaf_density != avg_leaf_density:  # NaN
        return 0.0
    ff = max(10, min(100, fillfactor))
    return round(max(0.0, (ff - float(avg_leaf_density)) / ff * 100.0), 1)


def parse_reloptions(reloptions: Sequence[str] | None) -> dict[str, str]:
    result: dict[str, str] = {}
    for opt in reloptions or []:
        key, _, value = opt.partition("=")
        result[key.strip()] = value.strip()
    return result


def analyze_threshold(
    reltuples: float, reloptions: Sequence[str] | None, scale_factor: float, base_threshold: int
) -> float:
    """Mesmo calculo do autovacuum: threshold + scale_factor * reltuples (reloptions por tabela
    sobrepoem os GUCs globais)."""
    opts = parse_reloptions(reloptions)
    try:
        sf = float(opts.get("autovacuum_analyze_scale_factor", scale_factor))
    except ValueError:
        sf = scale_factor
    try:
        th = int(opts.get("autovacuum_analyze_threshold", base_threshold))
    except ValueError:
        th = base_threshold
    return th + sf * max(0.0, reltuples)


def classify_statistics(
    *,
    n_live_tup: int,
    reltuples: float,
    n_mod_since_analyze: int,
    last_updated: datetime | None,
    reloptions: Sequence[str] | None,
    scale_factor: float,
    base_threshold: int,
    now: datetime,
) -> dict[str, Any]:
    """Classifica a estatistica de UMA tabela: % modificado, dias desde o ANALYZE, stale e
    severidade. Retorna dict com os campos de `NormalizedStatisticsHealthItem` (sem nomes)."""
    rows = max(n_live_tup, int(reltuples) if reltuples > 0 else 0)
    modified_pct = round(n_mod_since_analyze / rows * 100.0, 2) if rows > 0 else None
    days = round((now - last_updated).total_seconds() / 86400.0, 1) if last_updated else None
    threshold = analyze_threshold(reltuples, reloptions, scale_factor, base_threshold)

    never_analyzed = last_updated is None and n_live_tup > 0
    over_threshold = n_mod_since_analyze > threshold and n_mod_since_analyze > 0
    is_stale = never_analyzed or over_threshold

    if never_analyzed:
        severity = "warning" if n_live_tup >= 1000 else "info"
    elif over_threshold and (modified_pct or 0) >= 50:
        severity = "critical"
    elif over_threshold:
        severity = "warning"
    else:
        severity = "info"
    return {
        "row_count": rows,
        "modified_rows": n_mod_since_analyze,
        "modified_pct": modified_pct,
        "days_since_update": days,
        "is_stale": is_stale,
        "severity": severity,
        "detail": {"analyze_threshold_rows": int(threshold), "never_analyzed": never_analyzed},
    }


@dataclass(frozen=True)
class IndexDef:
    """Forma normalizada de um indice para deteccao de duplicado/sobreposto (PG e CRDB)."""

    table_key: str  # chave unica da tabela (ex.: "schema.table")
    name: str
    method: str  # btree/hash/gin... ("btree"/"prefix" para CRDB)
    keys: tuple[str, ...]  # colunas-chave em ordem (com opclass/direcao quando relevante)
    includes: tuple[str, ...] = ()
    expr: str = ""
    predicate: str = ""
    is_unique: bool = False
    is_primary: bool = False
    backs_constraint: bool = False
    size_bytes: int | None = None
    reads: int | None = None
    definition: str | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def protected(self) -> bool:
        """Nao pode ser removido sem remover uma constraint (PK/UNIQUE/EXCLUDE/FK alvo)."""
        return self.is_primary or self.backs_constraint


def _keep_rank(idx: IndexDef) -> tuple[int, int, int]:
    # Prefere manter: PK > constraint > unique > maior uso.
    return (int(idx.is_primary), int(idx.backs_constraint), int(idx.is_unique))


def find_redundant_indexes(indexes: Iterable[IndexDef]) -> list[tuple[str, IndexDef, IndexDef]]:
    """Devolve (categoria, candidato_a_remover, indice_que_cobre).

    duplicate: mesma tabela/metodo/chaves/INCLUDE/expressao/predicado.
    overlapping: chaves do candidato sao prefixo ESTRITO a esquerda das chaves de outro btree
    da mesma tabela; candidato sem expressao/predicado/INCLUDE, nao-unique e sem constraint
    (um indice unique "sobreposto" garante unicidade — nao e redundante)."""
    by_table: dict[str, list[IndexDef]] = {}
    for idx in indexes:
        by_table.setdefault(idx.table_key, []).append(idx)

    results: list[tuple[str, IndexDef, IndexDef]] = []
    for table_indexes in by_table.values():
        flagged: set[str] = set()
        ordered = sorted(table_indexes, key=lambda i: i.name)
        for i, a in enumerate(ordered):
            for b in ordered[i + 1 :]:
                same = (
                    a.method == b.method
                    and a.keys == b.keys
                    and a.includes == b.includes
                    and a.expr == b.expr
                    and a.predicate == b.predicate
                )
                if not same:
                    continue
                if a.protected and b.protected:
                    continue
                # Mantem o de maior "rank" (PK > constraint > unique); empate: mantem o mais
                # usado (e, por fim, o de menor nome, para ser deterministico).
                drop, keep = sorted(
                    (a, b), key=lambda x: (_keep_rank(x), x.reads or 0, x.name == min(a.name, b.name))
                )
                if drop.protected or drop.name in flagged:
                    continue
                flagged.add(drop.name)
                results.append(("duplicate", drop, keep))

        for a in ordered:
            if a.name in flagged or a.method not in ("btree", "prefix"):
                continue
            if a.is_unique or a.protected or a.expr or a.predicate or a.includes or not a.keys:
                continue
            for b in ordered:
                if b is a or b.method != a.method or b.predicate:
                    continue
                if len(b.keys) > len(a.keys) and b.keys[: len(a.keys)] == a.keys:
                    flagged.add(a.name)
                    results.append(("overlapping", a, b))
                    break
    return results


_TO_RE = re.compile(r"TO\s*\((.*)\)\s*$", re.IGNORECASE | re.DOTALL)
_FROM_RE = re.compile(r"FROM\s*\((.*?)\)\s*TO", re.IGNORECASE | re.DOTALL)


def _first_value(tuple_text: str) -> str:
    """Primeiro elemento de uma lista de valores de bound: "('2024-01-01', 3)" -> 2024-01-01."""
    text = tuple_text.strip()
    if text.startswith("'"):
        end = 1
        while end < len(text):
            if text[end] == "'" and (end + 1 >= len(text) or text[end + 1] != "'"):
                break
            end += 2 if text[end] == "'" else 1
        return text[1:end].replace("''", "'")
    return text.split(",", 1)[0].strip()


def parse_range_bound(bound: str | None) -> tuple[str | None, str | None]:
    """'FOR VALUES FROM (x) TO (y)' -> (x, y) — primeiro elemento de cada lado, sem aspas."""
    if not bound:
        return None, None
    from_match = _FROM_RE.search(bound)
    to_match = _TO_RE.search(bound)
    lower = _first_value(from_match.group(1)) if from_match else None
    upper = _first_value(to_match.group(1)) if to_match else None
    return lower, upper


def coerce_bound(value: str | None) -> tuple[int, Any] | None:
    """Valor comparavel: (0, numero) | (1, datetime) | None para MINVALUE/MAXVALUE/texto livre."""
    if value is None:
        return None
    v = value.strip()
    if v.upper() in ("MINVALUE", "MAXVALUE"):
        return None
    try:
        return (0, float(v))
    except ValueError:
        pass
    parsed = _parse_datetime(v)
    if parsed is not None:
        return (1, parsed)
    return None


def _parse_datetime(value: str) -> datetime | None:
    v = value.strip()
    candidates = [v, v.replace(" ", "T", 1)]
    # "2024-01-01 00:00:00-03" -> isoformat precisa de "-03:00"
    m = re.match(r"^(.*[T ]\d{2}:\d{2}(?::\d{2}(?:\.\d+)?)?)([+-]\d{2})$", v)
    if m:
        candidates.append(f"{m.group(1)}{m.group(2)}:00")
    for c in candidates:
        try:
            dt = datetime.fromisoformat(c)
        except ValueError:
            continue
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
    return None


def next_range(lower: str | None, upper: str | None) -> tuple[str, str] | None:
    """Proximo intervalo com a mesma largura do ultimo (datas ou inteiros)."""
    lo, up = coerce_bound(lower), coerce_bound(upper)
    if lo is None or up is None or lo[0] != up[0]:
        return None
    if lo[0] == 0:
        width = up[1] - lo[1]
        if width <= 0 or int(width) != width or int(up[1]) != up[1]:
            return None
        return str(int(up[1])), str(int(up[1] + width))
    start: datetime = up[1]
    prev: datetime = lo[1]
    width_days = (start - prev).days
    if 28 <= width_days <= 31 and prev.day == 1 and start.day == 1:
        month = start.month + 1
        nxt = start.replace(year=start.year + (month - 1) // 12, month=(month - 1) % 12 + 1)
    elif 365 <= width_days <= 366 and prev.month == 1 and prev.day == 1:
        nxt = start.replace(year=start.year + 1)
    else:
        nxt = start + (start - prev)
    if (start - prev) <= timedelta(0):
        return None
    if start.time() == datetime.min.time() and nxt.time() == datetime.min.time():
        return start.date().isoformat(), nxt.date().isoformat()
    return start.isoformat(), nxt.isoformat()


def summarize_partitions(
    children: Sequence[dict[str, Any]], strategy: str, now: datetime
) -> dict[str, Any]:
    """Resumo de uma tabela particionada a partir das particoes filhas.

    `children`: [{name, bound, rows (int|None), has_rows (bool|None)}].
    Devolve last_boundary_value, rows_beyond_last_boundary, total_rows, severity, partitions
    (ultimas 24 ordenadas pelo limite), next_range (para gerar CREATE) e days_ahead."""
    default = [c for c in children if (c.get("bound") or "").strip().upper() == "DEFAULT"]
    regular = [c for c in children if c not in default]

    total_rows = 0
    any_rows_known = False
    for c in children:
        if c.get("rows") is not None:
            total_rows += max(0, int(c["rows"]))
            any_rows_known = True

    out: dict[str, Any] = {
        "last_boundary_value": None,
        "rows_beyond_last_boundary": None,
        "total_rows": total_rows if any_rows_known else None,
        "severity": "info",
        "partitions": [],
        "next_range": None,
        "days_ahead": None,
        "catch_all": None,
    }

    ranked: list[tuple[Any, dict[str, Any], str | None, str | None]] = []
    for c in regular:
        lower, upper = parse_range_bound(c.get("bound")) if strategy == "RANGE" else (None, None)
        key = coerce_bound(upper)
        ranked.append((key, c, lower, upper))

    if strategy == "RANGE":
        finite = [r for r in ranked if r[0] is not None]
        unbounded = [r for r in ranked if r[0] is None and (r[3] or "").upper() == "MAXVALUE"]
        finite.sort(key=lambda r: r[0])
        ordered = finite + unbounded
        if finite:
            last_key, last_child, last_lower, last_upper = finite[-1]
            out["last_boundary_value"] = last_upper
            out["next_range"] = next_range(last_lower, last_upper)
            if last_key[0] == 1:
                out["days_ahead"] = round((last_key[1] - now).total_seconds() / 86400.0, 1)
        catch_all = default or [r[1] for r in unbounded]
    else:
        ordered = sorted(ranked, key=lambda r: str(r[1].get("name")))
        catch_all = default

    if catch_all:
        beyond = 0
        for c in catch_all:
            if c.get("has_rows"):
                beyond += max(1, int(c.get("rows") or 0))
            elif c.get("rows"):
                beyond += max(0, int(c["rows"]))
        out["rows_beyond_last_boundary"] = beyond
        out["catch_all"] = [c.get("name") for c in catch_all]
        if beyond > 0:
            out["severity"] = "warning"

    days_ahead = out["days_ahead"]
    if days_ahead is not None:
        if days_ahead <= 0:
            out["severity"] = "critical"
        elif days_ahead < 7 and out["severity"] == "info":
            out["severity"] = "warning"

    detail_rows = [
        {"number": i + 1, "name": r[1].get("name"), "boundary": r[1].get("bound"),
         "rows": r[1].get("rows")}
        for i, r in enumerate(ordered)
    ]
    detail_rows += [
        {"number": len(detail_rows) + i + 1, "name": c.get("name"), "boundary": "DEFAULT",
         "rows": c.get("rows")}
        for i, c in enumerate(default)
    ]
    out["partitions"] = detail_rows[-PARTITION_DETAIL_LIMIT:]
    return out


def usage_notes(since: datetime | None, from_restart: bool, now: datetime) -> list[str]:
    notes: list[str] = []
    if from_restart:
        notes.append("usage_stats_since_restart")
    if since is not None and now - since < timedelta(days=USAGE_STATS_RECENT_DAYS):
        notes.append("usage_stats_recent")
    return notes


# ── Iteracao por banco ─────────────────────────────────────────────────────────────────────


LIST_DATABASES_SQL = """
    SELECT datname FROM pg_database
    WHERE datistemplate = false AND datallowconn = true
      AND has_database_privilege(datname, 'CONNECT')
    ORDER BY datname
"""


@dataclass
class DbScan:
    items: list[Any] = field(default_factory=list)
    notes: set[str] = field(default_factory=set)
    usage_since: datetime | None = None
    usage_from_restart: bool = False


@dataclass
class ScanOutcome:
    items: list[Any]
    meta: NormalizedScanMeta


async def list_databases(connect: ConnectFn, bootstrap_db: str) -> list[str]:
    settings = get_settings()
    conn = await connect(bootstrap_db)
    try:
        rows = await asyncio.wait_for(
            conn.fetch(LIST_DATABASES_SQL), timeout=settings.collection_query_timeout_seconds
        )
    finally:
        await conn.close()
    return [str(r["datname"]) for r in rows]


async def scan_databases(
    connect: ConnectFn,
    bootstrap_db: str,
    database: str | None,
    per_db: Callable[[asyncpg.Connection, str], Awaitable[DbScan]],
    *,
    method: str | None = None,
    list_fn: Callable[[], Awaitable[list[str]]] | None = None,
) -> ScanOutcome:
    """Roda `per_db` em cada banco (ou so em `database`), com timeout por banco."""
    settings = get_settings()
    names = await (list_fn() if list_fn else list_databases(connect, bootstrap_db))
    meta = NormalizedScanMeta(method=method)
    if database is not None:
        if database not in names:
            # Inexistente ou sem CONNECT: vai para skipped, a tela mostra o motivo generico.
            meta.databases_skipped.append(database)
            return ScanOutcome([], meta)
        names = [database]
    names = names[: settings.maintenance_max_databases]

    items: list[Any] = []
    notes: set[str] = set()
    since_values: list[datetime] = []
    from_restart = False
    for name in names:
        try:
            conn = await asyncio.wait_for(
                connect(name), timeout=settings.maintenance_per_database_timeout_seconds
            )
        except Exception:  # noqa: BLE001 - banco inacessivel vai para skipped
            logger.warning("Varredura de manutencao: falha ao conectar no banco %s", name)
            meta.databases_skipped.append(name)
            continue
        try:
            result = await asyncio.wait_for(
                per_db(conn, name), timeout=settings.maintenance_per_database_timeout_seconds
            )
        except asyncpg.InsufficientPrivilegeError:
            meta.databases_skipped.append(name)
            notes.add("permission_denied")
            continue
        except Exception:  # noqa: BLE001 - 1 banco nunca derruba a varredura
            logger.warning("Varredura de manutencao falhou no banco %s", name, exc_info=True)
            meta.databases_skipped.append(name)
            continue
        finally:
            try:
                await conn.close()
            except Exception:  # noqa: BLE001
                pass
        meta.databases_scanned.append(name)
        items.extend(result.items)
        notes |= result.notes
        if result.usage_since is not None:
            since_values.append(result.usage_since)
        from_restart = from_restart or result.usage_from_restart

    if since_values:
        # O banco com reset mais recente e o elo fraco: e ele que limita a confianca.
        meta.usage_stats_since = max(since_values)
    meta.notes = sorted(notes)
    return ScanOutcome(items, meta)


async def _fetch(conn: asyncpg.Connection, sql: str, *args: Any) -> list[asyncpg.Record]:
    return await asyncio.wait_for(
        conn.fetch(sql, *args), timeout=get_settings().maintenance_per_database_timeout_seconds
    )


async def _fetchval(conn: asyncpg.Connection, sql: str, *args: Any) -> Any:
    return await asyncio.wait_for(
        conn.fetchval(sql, *args),
        timeout=get_settings().maintenance_per_database_timeout_seconds,
    )


_USAGE_SINCE_SQL = """
    SELECT d.stats_reset, pg_postmaster_start_time() AS started
    FROM pg_stat_database d WHERE d.datname = current_database()
"""


async def _usage_since(conn: asyncpg.Connection) -> tuple[datetime | None, bool]:
    try:
        rows = await _fetch(conn, _USAGE_SINCE_SQL)
    except asyncpg.PostgresError:
        return None, False
    if not rows:
        return None, False
    reset, started = rows[0]["stats_reset"], rows[0]["started"]
    if reset is not None and (started is None or reset >= started):
        return reset, False
    return started, True


# ── Saude de indices ───────────────────────────────────────────────────────────────────────

_SYSTEM_SCHEMA_FILTER = (
    "n.nspname NOT IN ('pg_catalog', 'information_schema') "
    "AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%'"
)
# Indices/colunas de chunks TimescaleDB sao gerenciados pela hypertable (DROP/ALTER no chunk e
# a acao errada) — ficam fora de indices/compressao/particoes. Estatisticas continuam: ANALYZE
# de chunk e valido e afeta o plano.
_SCHEMA_FILTER = _SYSTEM_SCHEMA_FILTER + " AND n.nspname NOT LIKE '\\_timescaledb\\_%'"

_EXTENSION_SCHEMA_SQL = """
    SELECT n.nspname FROM pg_extension e JOIN pg_namespace n ON n.oid = e.extnamespace
    WHERE e.extname = $1
"""


def _pgstatindex_sql(ext_schema: str) -> str:
    return f"""
    SELECT cand.schema_name, cand.table_name, cand.index_name, cand.size_bytes,
           cand.relpages, cand.fillfactor,
           s.leaf_fragmentation::float AS leaf_fragmentation,
           s.avg_leaf_density::float AS avg_leaf_density,
           s.leaf_pages, s.deleted_pages
    FROM (
        SELECT n.nspname AS schema_name, t.relname AS table_name, c.relname AS index_name,
               c.oid AS index_oid, pg_relation_size(c.oid) AS size_bytes, c.relpages,
               COALESCE(substring(array_to_string(c.reloptions, ' ')
                        FROM 'fillfactor=([0-9]+)')::int, 90) AS fillfactor
        FROM pg_index i
        JOIN pg_class c ON c.oid = i.indexrelid
        JOIN pg_class t ON t.oid = i.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_am am ON am.oid = c.relam
        WHERE am.amname = 'btree' AND i.indisvalid AND c.relpersistence <> 't'
          AND c.relpages >= $1 AND {_SCHEMA_FILTER}
        ORDER BY c.relpages DESC
        LIMIT $2
    ) cand
    CROSS JOIN LATERAL {quote_ident(ext_schema)}.pgstatindex(cand.index_oid::regclass) s
    """


# Estimativa de bloat de btree sem pgstattuple: paginas esperadas a partir de reltuples e da
# largura media das colunas (pg_stats) vs paginas reais. Adaptado do btree_bloat.sql
# (ioguix/pgsql-bloat-estimation, licenca BSD).
_BTREE_BLOAT_ESTIMATE_SQL = f"""
WITH idx AS (
    SELECT n.nspname AS schema_name, t.relname AS table_name, c.relname AS index_name,
           c.oid AS index_oid, i.indrelid, c.reltuples, c.relpages,
           pg_relation_size(c.oid) AS size_bytes,
           COALESCE(substring(array_to_string(c.reloptions, ' ')
                    FROM 'fillfactor=([0-9]+)')::int, 90) AS fillfactor,
           i.indkey::int2[] AS indkey
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_class t ON t.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_am am ON am.oid = c.relam
    WHERE am.amname = 'btree' AND i.indisvalid AND c.relpages >= $1 AND c.reltuples > 0
      AND {_SCHEMA_FILTER}
), cols AS (
    SELECT idx.index_oid,
           SUM((1 - COALESCE(s.null_frac, 0)) * COALESCE(s.avg_width, 1024)) AS data_width,
           MAX(COALESCE(s.null_frac, 0)) AS max_null_frac,
           bool_and(s.attname IS NOT NULL) AS has_stats
    FROM idx
    CROSS JOIN LATERAL unnest(idx.indkey) WITH ORDINALITY AS k(attnum, pos)
    LEFT JOIN pg_attribute a ON a.attrelid = CASE WHEN k.attnum = 0 THEN idx.index_oid
                                                  ELSE idx.indrelid END
                            AND a.attnum = CASE WHEN k.attnum = 0 THEN k.pos::int2
                                                ELSE k.attnum END
    LEFT JOIN pg_stats s ON s.schemaname = idx.schema_name
                        AND s.tablename = CASE WHEN k.attnum = 0 THEN idx.index_name
                                               ELSE idx.table_name END
                        AND s.attname = a.attname
    GROUP BY idx.index_oid
), est AS (
    SELECT idx.*, cols.has_stats,
           current_setting('block_size')::numeric AS bs,
           -- tupla de indice: cabecalho (8, +bitmap de nulos) + dados alinhados a 8 + item id 4
           (CASE WHEN cols.max_null_frac = 0 THEN 8 ELSE 12 END
            + ceil(COALESCE(cols.data_width, 0) / 8.0) * 8 + 4) AS tuple_width
    FROM idx JOIN cols ON cols.index_oid = idx.index_oid
)
SELECT schema_name, table_name, index_name, size_bytes, relpages, fillfactor, has_stats,
       GREATEST(1, 1 + ceil(reltuples / floor((bs - 24 - 16) * fillfactor / 100.0
                                               / tuple_width))) AS est_pages
FROM est
ORDER BY relpages DESC
LIMIT $2
"""


def _unused_sql(version: int) -> str:
    last_used = "s.last_idx_scan" if version >= PG16 else "NULL::timestamptz"
    return f"""
    SELECT s.schemaname AS schema_name, s.relname AS table_name, s.indexrelname AS index_name,
           am.amname AS index_type, pg_relation_size(s.indexrelid) AS size_bytes,
           COALESCE(s.idx_scan, 0) AS reads, {last_used} AS last_used,
           COALESCE(t.n_tup_ins, 0) + COALESCE(t.n_tup_upd, 0) + COALESCE(t.n_tup_del, 0)
               AS writes,
           pg_get_indexdef(s.indexrelid) AS index_def
    FROM pg_stat_user_indexes s
    JOIN pg_index i ON i.indexrelid = s.indexrelid
    JOIN pg_class c ON c.oid = s.indexrelid
    JOIN pg_am am ON am.oid = c.relam
    LEFT JOIN pg_stat_user_tables t ON t.relid = s.relid
    WHERE COALESCE(s.idx_scan, 0) = 0
      AND NOT i.indisunique AND NOT i.indisprimary AND NOT i.indisexclusion AND i.indisvalid
      -- indice de particao pertence ao indice do pai: DROP direto nele falha
      AND NOT c.relispartition
      AND s.schemaname NOT LIKE '\\_timescaledb\\_%'
      AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = s.indexrelid)
    ORDER BY pg_relation_size(s.indexrelid) DESC
    LIMIT $1
    """


def _index_defs_sql(version: int) -> str:
    nkey = "i.indnkeyatts" if version >= PG11 else "i.indnatts"
    return f"""
    SELECT n.nspname AS schema_name, t.relname AS table_name, c.relname AS index_name,
           am.amname AS method, i.indisunique, i.indisprimary, i.indisexclusion,
           {nkey} AS nkeyatts, i.indkey::int2[] AS indkey,
           i.indclass::oid[]::text AS indclass, i.indcollation::oid[]::text AS indcollation,
           i.indoption::int2[]::text AS indoption,
           COALESCE(pg_get_expr(i.indexprs, i.indrelid), '') AS expr,
           COALESCE(pg_get_expr(i.indpred, i.indrelid), '') AS predicate,
           ARRAY(SELECT COALESCE(a.attname::text, '(expr)')
                 FROM unnest(i.indkey::int2[]) WITH ORDINALITY u(attnum, ord)
                 LEFT JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = u.attnum
                 ORDER BY u.ord) AS column_names,
           EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = i.indexrelid)
               AS backs_constraint,
           pg_relation_size(i.indexrelid) AS size_bytes,
           COALESCE(s.idx_scan, 0) AS reads,
           pg_get_indexdef(i.indexrelid) AS index_def
    FROM pg_index i
    JOIN pg_class c ON c.oid = i.indexrelid
    JOIN pg_class t ON t.oid = i.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    JOIN pg_am am ON am.oid = c.relam
    LEFT JOIN pg_stat_user_indexes s ON s.indexrelid = i.indexrelid
    WHERE i.indisvalid AND t.relkind IN ('r', 'm', 'p') AND NOT c.relispartition
      AND {_SCHEMA_FILTER}
    """


def _parse_text_array(value: str) -> list[str]:
    return [p for p in value.strip("{}").split(",") if p] if value else []


def build_index_defs(rows: Sequence[Any]) -> list[IndexDef]:
    defs: list[IndexDef] = []
    for r in rows:
        indkey = list(r["indkey"] or [])
        nkey = int(r["nkeyatts"])
        names = list(r["column_names"] or [])
        classes = _parse_text_array(r["indclass"])
        collations = _parse_text_array(r["indcollation"])
        options = _parse_text_array(r["indoption"])

        def col(
            pos: int,
            names: list[Any] = names,
            indkey: list[Any] = indkey,
            nkey: int = nkey,
            classes: list[str] = classes,
            collations: list[str] = collations,
            options: list[str] = options,
        ) -> str:
            name = names[pos] if pos < len(names) else str(indkey[pos])
            parts = [name if indkey[pos] != 0 else f"expr{pos}"]
            if pos < nkey:
                parts += [
                    classes[pos] if pos < len(classes) else "",
                    collations[pos] if pos < len(collations) else "",
                    options[pos] if pos < len(options) else "",
                ]
            return ":".join(parts)

        defs.append(
            IndexDef(
                table_key=f"{r['schema_name']}.{r['table_name']}",
                name=str(r["index_name"]),
                method=str(r["method"]),
                keys=tuple(col(p) for p in range(min(nkey, len(indkey)))),
                includes=tuple(col(p) for p in range(nkey, len(indkey))),
                expr=str(r["expr"] or ""),
                predicate=str(r["predicate"] or ""),
                is_unique=bool(r["indisunique"]),
                is_primary=bool(r["indisprimary"]),
                backs_constraint=bool(r["backs_constraint"]) or bool(r["indisexclusion"]),
                size_bytes=int(r["size_bytes"] or 0),
                reads=int(r["reads"] or 0),
                definition=str(r["index_def"]),
                extra={
                    "schema_name": str(r["schema_name"]),
                    "table_name": str(r["table_name"]),
                    "key_columns": ", ".join(names[:nkey]),
                    "included_columns": ", ".join(names[nkey:]) or None,
                },
            )
        )
    return defs


def redundant_items(
    defs: Sequence[IndexDef], database: str | None, drop_template: str
) -> list[NormalizedIndexHealthItem]:
    items: list[NormalizedIndexHealthItem] = []
    for category, drop, keep in find_redundant_indexes(defs):
        schema = drop.extra.get("schema_name", "")
        items.append(
            NormalizedIndexHealthItem(
                category=category,  # type: ignore[arg-type]
                database_name=database,
                schema_name=schema,
                table_name=drop.extra.get("table_name", drop.table_key),
                index_name=drop.name,
                index_type=drop.method,
                size_bytes=drop.size_bytes,
                reads=drop.reads,
                duplicate_of=keep.name,
                key_columns=drop.extra.get("key_columns"),
                included_columns=drop.extra.get("included_columns"),
                is_unique=drop.is_unique,
                is_primary_key=drop.is_primary,
                severity="warning" if category == "duplicate" else "info",
                recommendation=drop_template.format(
                    schema=quote_ident(schema), index=quote_ident(drop.name),
                    table=quote_ident(drop.extra.get("table_name", "")),
                ),
                detail={
                    "index_def": drop.definition,
                    "covering_index_def": keep.definition,
                    "covering_key_columns": keep.extra.get("key_columns"),
                },
            )
        )
    return items


async def pg_index_health_for_db(
    conn: asyncpg.Connection, database: str, version: int
) -> DbScan:
    settings = get_settings()
    scan = DbScan()
    scan.usage_since, scan.usage_from_restart = await _usage_since(conn)
    min_pages = settings.maintenance_fragmentation_min_pages
    reindex = "REINDEX INDEX CONCURRENTLY" if version >= 120000 else "REINDEX INDEX"

    # Fragmentacao: pgstatindex quando disponivel, senao estimativa por paginas esperadas.
    ext_schema = await _fetchval(conn, _EXTENSION_SCHEMA_SQL, "pgstattuple")
    measured = False
    rows: list[asyncpg.Record] = []
    if ext_schema:
        try:
            rows = await _fetch(conn, _pgstatindex_sql(ext_schema), min_pages,
                                PGSTATINDEX_MAX_INDEXES)
            measured = True
        except asyncpg.InsufficientPrivilegeError:
            scan.notes.add("permission_denied")
        for r in rows:
            density_frag = density_to_fragmentation(r["avg_leaf_density"], r["fillfactor"])
            leaf_frag = float(r["leaf_fragmentation"] or 0.0)
            if leaf_frag != leaf_frag:  # NaN
                leaf_frag = 0.0
            pct = round(max(leaf_frag, density_frag), 1)
            if pct < 10:
                continue
            scan.items.append(
                NormalizedIndexHealthItem(
                    category="fragmentation",
                    database_name=database,
                    schema_name=r["schema_name"],
                    table_name=r["table_name"],
                    index_name=r["index_name"],
                    index_type="btree",
                    size_bytes=int(r["size_bytes"] or 0),
                    fragmentation_pct=pct,
                    page_count=int(r["relpages"] or 0),
                    severity=fragmentation_severity(pct),  # type: ignore[arg-type]
                    recommendation=(
                        f"{reindex} {qualified(r['schema_name'], r['index_name'])};"
                    ),
                    detail={
                        "leaf_fragmentation_pct": round(leaf_frag, 1),
                        "avg_leaf_density_pct": r["avg_leaf_density"],
                        "fillfactor": r["fillfactor"],
                        "deleted_pages": r["deleted_pages"],
                        "source": "pgstatindex",
                    },
                )
            )
    else:
        scan.notes.add("pgstattuple_missing")

    if not measured:
        scan.notes.add("fragmentation_estimated")
        rows = await _fetch(conn, _BTREE_BLOAT_ESTIMATE_SQL, min_pages, settings.maintenance_max_rows)
        for r in rows:
            relpages = int(r["relpages"] or 0)
            est = int(r["est_pages"] or 0)
            if relpages <= 0 or not r["has_stats"]:
                continue
            pct = round(max(0.0, (relpages - est) / relpages * 100.0), 1)
            if pct < 20:  # estimativa tem erro de ~10-20%: so reporta o que e claramente ruim
                continue
            scan.items.append(
                NormalizedIndexHealthItem(
                    category="fragmentation",
                    database_name=database,
                    schema_name=r["schema_name"],
                    table_name=r["table_name"],
                    index_name=r["index_name"],
                    index_type="btree",
                    size_bytes=int(r["size_bytes"] or 0),
                    fragmentation_pct=pct,
                    page_count=relpages,
                    severity=fragmentation_severity(pct),  # type: ignore[arg-type]
                    recommendation=(
                        f"{reindex} {qualified(r['schema_name'], r['index_name'])};"
                    ),
                    detail={
                        "estimated_pages": est,
                        "fillfactor": r["fillfactor"],
                        "source": "estimate",
                    },
                )
            )

    # Sem uso.
    for r in await _fetch(conn, _unused_sql(version), settings.maintenance_max_rows):
        size = int(r["size_bytes"] or 0)
        writes = int(r["writes"] or 0)
        scan.items.append(
            NormalizedIndexHealthItem(
                category="unused",
                database_name=database,
                schema_name=r["schema_name"],
                table_name=r["table_name"],
                index_name=r["index_name"],
                index_type=r["index_type"],
                size_bytes=size,
                reads=0,
                writes=writes,
                last_used=r["last_used"],
                severity="warning" if size >= UNUSED_MIN_WARNING_BYTES and writes > 0 else "info",
                recommendation=(
                    f"DROP INDEX CONCURRENTLY {qualified(r['schema_name'], r['index_name'])};"
                ),
                detail={"index_def": r["index_def"]},
            )
        )

    # Duplicados e sobrepostos.
    defs = build_index_defs(await _fetch(conn, _index_defs_sql(version)))
    scan.items.extend(
        redundant_items(defs, database, "DROP INDEX CONCURRENTLY {schema}.{index};")
    )
    scan.notes |= set(usage_notes(scan.usage_since, scan.usage_from_restart, datetime.now(UTC)))
    return scan


# ── Estatisticas ───────────────────────────────────────────────────────────────────────────

_STATISTICS_SQL = f"""
    SELECT s.schemaname AS schema_name, s.relname AS table_name,
           c.reltuples::float AS reltuples, COALESCE(s.n_live_tup, 0) AS n_live_tup,
           COALESCE(s.n_mod_since_analyze, 0) AS n_mod_since_analyze,
           s.last_analyze, s.last_autoanalyze,
           GREATEST(s.last_analyze, s.last_autoanalyze) AS last_updated,
           c.reloptions, pg_table_size(s.relid) AS table_size_bytes,
           COALESCE(s.analyze_count, 0) + COALESCE(s.autoanalyze_count, 0) AS analyze_count
    FROM pg_stat_user_tables s
    JOIN pg_class c ON c.oid = s.relid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE {_SYSTEM_SCHEMA_FILTER}
      AND (COALESCE(s.n_live_tup, 0) > 0 OR COALESCE(s.n_mod_since_analyze, 0) > 0)
    ORDER BY COALESCE(s.n_mod_since_analyze, 0) DESC
    LIMIT $1
"""

_ANALYZE_SETTINGS_SQL = """
    SELECT current_setting('autovacuum_analyze_scale_factor')::float AS sf,
           current_setting('autovacuum_analyze_threshold')::int AS th
"""


async def pg_statistics_for_db(conn: asyncpg.Connection, database: str) -> DbScan:
    settings = get_settings()
    scan = DbScan()
    cfg = (await _fetch(conn, _ANALYZE_SETTINGS_SQL))[0]
    now = datetime.now(UTC)
    for r in await _fetch(conn, _STATISTICS_SQL, settings.maintenance_max_rows):
        info = classify_statistics(
            n_live_tup=int(r["n_live_tup"]),
            reltuples=float(r["reltuples"] or 0),
            n_mod_since_analyze=int(r["n_mod_since_analyze"]),
            last_updated=r["last_updated"],
            reloptions=r["reloptions"],
            scale_factor=float(cfg["sf"]),
            base_threshold=int(cfg["th"]),
            now=now,
        )
        info["detail"].update(
            {
                "last_manual_analyze": r["last_analyze"].isoformat() if r["last_analyze"] else None,
                "last_autoanalyze": (
                    r["last_autoanalyze"].isoformat() if r["last_autoanalyze"] else None
                ),
                "table_size_bytes": int(r["table_size_bytes"] or 0),
                "analyze_count": int(r["analyze_count"] or 0),
            }
        )
        scan.items.append(
            NormalizedStatisticsHealthItem(
                database_name=database,
                schema_name=r["schema_name"],
                table_name=r["table_name"],
                last_updated=r["last_updated"],
                recommendation=f"ANALYZE {qualified(r['schema_name'], r['table_name'])};",
                **info,
            )
        )
    return scan


_SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}


def sort_statistics(items: list[NormalizedStatisticsHealthItem]) -> list[NormalizedStatisticsHealthItem]:
    return sorted(
        items,
        key=lambda i: (_SEVERITY_ORDER.get(i.severity, 3), not i.is_stale,
                       -(i.modified_pct or 0.0)),
    )


def sort_index_items(items: list[NormalizedIndexHealthItem]) -> list[NormalizedIndexHealthItem]:
    return sorted(
        items, key=lambda i: (_SEVERITY_ORDER.get(i.severity, 3), -(i.size_bytes or 0))
    )


# ── Memoria (proxy: spill de work_mem) ─────────────────────────────────────────────────────

_MEMORY_SETTINGS_SQL = """
    SELECT current_setting('work_mem') AS work_mem,
           current_setting('hash_mem_multiplier', true) AS hash_mem_multiplier,
           current_setting('maintenance_work_mem') AS maintenance_work_mem,
           current_setting('shared_buffers') AS shared_buffers,
           current_setting('temp_file_limit') AS temp_file_limit,
           current_setting('block_size')::int AS block_size,
           pg_postmaster_start_time() AS started
"""

_TEMP_BY_DB_SQL = """
    SELECT datname AS database_name, temp_files, temp_bytes, stats_reset
    FROM pg_stat_database WHERE datname IS NOT NULL AND temp_bytes > 0
    ORDER BY temp_bytes DESC
"""

_ACTIVE_SQL = """
    SELECT pid, datname, usename, query, wait_event_type, wait_event, state,
           EXTRACT(EPOCH FROM (clock_timestamp() - query_start)) * 1000 AS duration_ms
    FROM pg_stat_activity
    WHERE state = 'active' AND pid <> pg_backend_pid() AND query <> ''
      AND backend_type = 'client backend'
    ORDER BY query_start NULLS LAST
    LIMIT $1
"""


def _pgss_sql(version: int) -> str:
    total = "total_exec_time" if version >= PG13 else "total_time"
    mean = "mean_exec_time" if version >= PG13 else "mean_time"
    # Uma linha por (userid, dbid, queryid, toplevel) no pg_stat_statements: agrega por
    # (queryid, banco). queryid NULL = linhas de outros usuarios sem pg_read_all_stats.
    return f"""
    SELECT ss.queryid, min(ss.query) AS query, d.datname AS database_name,
           sum(ss.calls) AS calls, sum(ss.{total}) AS total_ms,
           sum(ss.{total}) / NULLIF(sum(ss.calls), 0) AS mean_ms,
           max(ss.{mean}) AS max_mean_ms, sum(ss.rows) AS rows,
           sum(ss.temp_blks_written) AS temp_blks_written,
           sum(ss.temp_blks_read) AS temp_blks_read
    FROM pg_stat_statements ss
    LEFT JOIN pg_database d ON d.oid = ss.dbid
    WHERE ss.temp_blks_written > 0
    GROUP BY ss.queryid, d.datname
    ORDER BY sum(ss.temp_blks_written) DESC
    LIMIT $1
    """


def spill_severity(spill_kb_total: int) -> str:
    if spill_kb_total >= 10 * 1024 * 1024:
        return "critical"
    if spill_kb_total >= 1024 * 1024:
        return "warning"
    return "info"


async def pg_memory_grants(
    connect: ConnectFn, bootstrap_db: str, version: int
) -> NormalizedMemoryGrantReport:
    settings = get_settings()
    meta = NormalizedScanMeta(method="pg_stat_statements.temp_blks_written",
                              notes=["workmem_spill_proxy"])
    report = NormalizedMemoryGrantReport(meta=meta)

    conn = await connect(bootstrap_db)
    try:
        cfg = (await _fetch(conn, _MEMORY_SETTINGS_SQL))[0]
        temp_rows = await _fetch(conn, _TEMP_BY_DB_SQL)
        active_rows = await _fetch(conn, _ACTIVE_SQL, ACTIVE_LIMIT)
    finally:
        await conn.close()
    block_kb = int(cfg["block_size"]) // 1024

    for r in active_rows:
        text = r["query"] or ""
        spilling = r["wait_event"] in ("BufFileRead", "BufFileWrite")
        report.active.append(
            NormalizedMemoryGrantActive(
                session_id=str(r["pid"]),
                database_name=r["datname"],
                login_name=r["usename"],
                sql_hash=sql_hash(text) if text else None,
                sql_text_masked=mask_sql_literals_for_display(text) if text else None,
                detail={
                    "duration_ms": int(r["duration_ms"] or 0),
                    "wait_event_type": r["wait_event_type"],
                    "wait_event": r["wait_event"],
                    "spilling_now": spilling,
                },
            )
        )

    temp_total = sum(int(r["temp_bytes"] or 0) for r in temp_rows)
    report.summary = NormalizedMemoryGrantSummary(
        granted_requests=len(report.active),
        waiting_requests=0,
        detail={
            "work_mem": cfg["work_mem"],
            "hash_mem_multiplier": cfg["hash_mem_multiplier"],
            "maintenance_work_mem": cfg["maintenance_work_mem"],
            "shared_buffers": cfg["shared_buffers"],
            "temp_file_limit": cfg["temp_file_limit"],
            "temp_bytes_total": temp_total,
            "temp_files_total": sum(int(r["temp_files"] or 0) for r in temp_rows),
            "spilling_now": sum(1 for a in report.active if a.detail.get("spilling_now")),
            "temp_by_database": [
                {"database_name": r["database_name"], "temp_files": int(r["temp_files"] or 0),
                 "temp_bytes": int(r["temp_bytes"] or 0)}
                for r in temp_rows[:20]
            ],
        },
    )
    meta.usage_stats_since = cfg["started"]

    # pg_stat_statements precisa estar criada no banco consultado — procura o primeiro que tem.
    candidates = [bootstrap_db] + [
        d for d in await list_databases(connect, bootstrap_db) if d != bootstrap_db
    ]
    for db in candidates[: settings.maintenance_max_databases]:
        try:
            db_conn = await asyncio.wait_for(
                connect(db), timeout=settings.maintenance_per_database_timeout_seconds
            )
        except Exception:  # noqa: BLE001
            continue
        try:
            if not await _fetchval(db_conn, _EXTENSION_SCHEMA_SQL, "pg_stat_statements"):
                continue
            rows = await _fetch(db_conn, _pgss_sql(version), TOP_QUERIES_LIMIT)
            if version >= PG14:
                try:
                    reset = await _fetchval(db_conn, "SELECT stats_reset FROM pg_stat_statements_info")
                    if reset is not None:
                        meta.usage_stats_since = reset
                except asyncpg.PostgresError:
                    pass
            meta.databases_scanned.append(db)
        except asyncpg.InsufficientPrivilegeError:
            meta.notes.append("permission_denied")
            meta.databases_skipped.append(db)
            continue
        except Exception:  # noqa: BLE001
            logger.warning("Falha ao ler pg_stat_statements no banco %s", db, exc_info=True)
            meta.databases_skipped.append(db)
            continue
        finally:
            await db_conn.close()

        for r in rows:
            text = r["query"] or ""
            hidden = text.startswith("<insufficient privilege>") or r["queryid"] is None
            if hidden:
                meta.notes.append("permission_denied")
            calls = int(r["calls"] or 0)
            spill_kb = int(r["temp_blks_written"] or 0) * block_kb
            report.top_queries.append(
                NormalizedMemoryGrantTopQuery(
                    sql_hash=None if hidden else sql_hash(text),
                    sql_text_masked=(
                        "<insufficient privilege>" if hidden else mask_sql_literals_for_display(text)
                    ),
                    database_name=r["database_name"],
                    executions=calls,
                    spill_kb=spill_kb,
                    severity=spill_severity(spill_kb),  # type: ignore[arg-type]
                    detail={
                        "queryid": str(r["queryid"]) if r["queryid"] is not None else None,
                        "mean_ms": round(float(r["mean_ms"] or 0), 3),
                        "max_mean_ms": round(float(r["max_mean_ms"] or 0), 3),
                        "total_ms": round(float(r["total_ms"] or 0), 3),
                        "spill_kb_per_call": round(spill_kb / calls, 1) if calls else None,
                        "temp_read_kb": int(r["temp_blks_read"] or 0) * block_kb,
                        "rows": int(r["rows"] or 0),
                    },
                )
            )
        break
    else:
        meta.notes.append("pg_stat_statements_missing")

    meta.notes = sorted(set(meta.notes))
    return report


# ── Compressao (inventario TOAST) ──────────────────────────────────────────────────────────

_LARGEST_TABLES_SQL = f"""
    SELECT c.oid, n.nspname AS schema_name, c.relname AS table_name,
           pg_table_size(c.oid) AS table_bytes, pg_relation_size(c.oid) AS heap_bytes,
           COALESCE(pg_total_relation_size(NULLIF(c.reltoastrelid, 0)), 0) AS toast_bytes
    FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind IN ('r', 'm') AND {_SCHEMA_FILTER}
    ORDER BY pg_table_size(c.oid) DESC
    LIMIT $1
"""


def _toast_columns_sql(version: int) -> str:
    compression = "a.attcompression::text" if version >= PG14 else "''::text"
    return f"""
    SELECT a.attrelid AS oid, a.attname, format_type(a.atttypid, a.atttypmod) AS data_type,
           a.attstorage::text AS storage, {compression} AS compression
    FROM pg_attribute a
    WHERE a.attrelid = ANY($1::oid[]) AND a.attnum > 0 AND NOT a.attisdropped
      AND a.attstorage IN ('x', 'm', 'e')
    ORDER BY a.attrelid, a.attnum
    """


_COMPRESSION_NAMES = {"p": "pglz", "l": "lz4", "": None}
# TOAST so comprime valores > ~2KB: abaixo disso mudar o metodo nao muda nada.
LZ4_SUGGESTION_MIN_TOAST_BYTES = 10 * 1024 * 1024


def summarize_toast_columns(
    columns: Sequence[dict[str, Any]], default_method: str
) -> tuple[str, list[dict[str, Any]]]:
    """(metodo resumido, colunas) — "none" quando nenhuma coluna comprimivel."""
    methods: set[str] = set()
    out: list[dict[str, Any]] = []
    for col in columns:
        storage = col["storage"]
        explicit = _COMPRESSION_NAMES.get(col.get("compression") or "", col.get("compression"))
        effective = None if storage == "e" else (explicit or default_method)
        if effective:
            methods.add(effective)
        out.append(
            {"name": col["attname"], "type": col["data_type"], "storage": storage,
             "compression": effective}
        )
    if not methods:
        return "none", out
    return (methods.pop() if len(methods) == 1 else "mixed"), out


async def pg_compression_for_db(
    conn: asyncpg.Connection, database: str, version: int, max_objects: int
) -> DbScan:
    scan = DbScan(notes={"compression_inventory_only"})
    default_method = "pglz"
    if version >= PG14:
        default_method = (
            await _fetchval(conn, "SELECT current_setting('default_toast_compression', true)")
            or "pglz"
        )
    tables = await _fetch(conn, _LARGEST_TABLES_SQL, max_objects)
    if not tables:
        return scan
    oids = [r["oid"] for r in tables]
    col_rows = await _fetch(conn, _toast_columns_sql(version), oids)
    by_table: dict[int, list[dict[str, Any]]] = {}
    for c in col_rows:
        by_table.setdefault(c["oid"], []).append(dict(c))

    for t in tables:
        method, cols = summarize_toast_columns(by_table.get(t["oid"], []), default_method)
        toast_bytes = int(t["toast_bytes"] or 0)
        command = None
        requested = None
        if version >= PG14 and toast_bytes >= LZ4_SUGGESTION_MIN_TOAST_BYTES:
            targets = [c for c in cols if c["compression"] == "pglz"]
            if targets:
                requested = "lz4"
                table = qualified(t["schema_name"], t["table_name"])
                command = "\n".join(
                    f"ALTER TABLE {table} ALTER COLUMN {quote_ident(c['name'])} "
                    "SET COMPRESSION lz4;"
                    for c in targets
                )
        scan.items.append(
            NormalizedCompressionItem(
                database_name=database,
                schema_name=t["schema_name"],
                table_name=t["table_name"],
                current_compression=method,
                requested_compression=requested,
                current_size_kb=int(t["table_bytes"] or 0) // 1024,
                command=command,
                detail={
                    "heap_kb": int(t["heap_bytes"] or 0) // 1024,
                    "toast_kb": toast_bytes // 1024,
                    "default_toast_compression": default_method,
                    "columns": cols,
                    # SET COMPRESSION so afeta valores gravados depois; os existentes so mudam
                    # com reescrita (VACUUM FULL / pg_repack). Requer build com --with-lz4.
                    "applies_to_new_values_only": command is not None,
                },
            )
        )
    return scan


# ── Particionamento declarativo ────────────────────────────────────────────────────────────

_PARTITIONED_SQL = f"""
    SELECT c.oid, n.nspname AS schema_name, c.relname AS table_name,
           pt.partstrat::text AS strategy, pg_get_partkeydef(c.oid) AS partition_key
    FROM pg_partitioned_table pt
    JOIN pg_class c ON c.oid = pt.partrelid
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE c.relkind = 'p' AND NOT c.relispartition AND {_SCHEMA_FILTER}
    ORDER BY n.nspname, c.relname
    LIMIT $1
"""

_PARTITION_CHILDREN_SQL = """
    SELECT i.inhparent AS parent_oid, ch.oid, cn.nspname AS schema_name, ch.relname AS name,
           pg_get_expr(ch.relpartbound, ch.oid) AS bound, ch.reltuples::bigint AS reltuples,
           ch.relkind::text AS relkind
    FROM pg_inherits i
    JOIN pg_class ch ON ch.oid = i.inhrelid
    JOIN pg_namespace cn ON cn.oid = ch.relnamespace
    WHERE i.inhparent = ANY($1::oid[])
"""

_STRATEGY_NAMES = {"r": "RANGE", "l": "LIST", "h": "HASH"}


async def pg_partitions_for_db(conn: asyncpg.Connection, database: str) -> DbScan:
    settings = get_settings()
    scan = DbScan()
    parents = await _fetch(conn, _PARTITIONED_SQL, settings.maintenance_max_rows)
    if not parents:
        return scan
    children = await _fetch(conn, _PARTITION_CHILDREN_SQL, [p["oid"] for p in parents])
    by_parent: dict[int, list[dict[str, Any]]] = {}
    estimated = False
    for c in children:
        rows = int(c["reltuples"]) if c["reltuples"] is not None and c["reltuples"] >= 0 else None
        if c["relkind"] == "p":
            rows = None  # subparticionada: reltuples do pai e sempre 0/-1
        estimated = estimated or rows is not None
        entry = {"name": c["name"], "schema_name": c["schema_name"], "bound": c["bound"],
                 "rows": rows, "has_rows": None, "relkind": c["relkind"]}
        if (c["bound"] or "").strip().upper() == "DEFAULT" and c["relkind"] == "r":
            # Particao DEFAULT: o que importa e se tem QUALQUER linha (EXISTS e barato).
            entry["has_rows"] = bool(
                await _fetchval(
                    conn, f"SELECT EXISTS (SELECT 1 FROM {qualified(c['schema_name'], c['name'])})"
                )
            )
        by_parent.setdefault(c["parent_oid"], []).append(entry)
    if estimated:
        scan.notes.add("partition_rows_estimated")

    now = datetime.now(UTC)
    for p in parents:
        strategy = _STRATEGY_NAMES.get(p["strategy"], p["strategy"])
        kids = by_parent.get(p["oid"], [])
        summary = summarize_partitions(kids, strategy, now)
        table = qualified(p["schema_name"], p["table_name"])
        recommendation = None
        if summary["next_range"] is not None and summary["severity"] != "info":
            lo, up = summary["next_range"]
            suffix = re.sub(r"[^0-9A-Za-z]", "", lo)[:16]
            recommendation = (
                f"CREATE TABLE {qualified(p['schema_name'], p['table_name'] + '_p' + suffix)} "
                f"PARTITION OF {table} FOR VALUES FROM ('{lo}') TO ('{up}');"
            )
        elif summary["rows_beyond_last_boundary"]:
            recommendation = (
                f"-- Linhas na particao DEFAULT de {table}: crie as particoes que faltam e mova "
                "as linhas (DETACH da DEFAULT, CREATE das novas, INSERT ... SELECT, ATTACH)."
            )
        scan.items.append(
            NormalizedPartitionInfo(
                database_name=database,
                schema_name=p["schema_name"],
                table_name=p["table_name"],
                partition_method=strategy,
                partition_key=p["partition_key"],
                partition_count=len(kids),
                last_boundary_value=summary["last_boundary_value"],
                rows_beyond_last_boundary=summary["rows_beyond_last_boundary"],
                total_rows=summary["total_rows"],
                severity=summary["severity"],  # type: ignore[arg-type]
                recommendation=recommendation,
                detail={
                    "partitions": summary["partitions"],
                    "days_ahead": summary["days_ahead"],
                    "catch_all_partitions": summary["catch_all"],
                },
            )
        )
    return scan
