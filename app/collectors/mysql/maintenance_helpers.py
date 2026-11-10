"""Helpers puros (sem I/O) das varreduras de manutencao — independentes de motor.

Usados pelos adaptadores MySQL/MariaDB, Oracle e DB2: deteccao de indices duplicados/
sobrepostos, classificacao de severidade e quoting de identificadores nos comandos sugeridos
(comandos sao so texto exibido na tela, nunca executados).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Literal

Severity = Literal["critical", "warning", "info"]
RedundancyKind = Literal["duplicate", "overlapping"]

# Abaixo disso o periodo de contadores cumulativos e curto demais para concluir "sem uso".
USAGE_STATS_MIN_RELIABLE_PERIOD = timedelta(days=7)


@dataclass(frozen=True, slots=True)
class IndexDefinition:
    schema_name: str
    table_name: str
    index_name: str
    key_columns: tuple[str, ...]
    included_columns: tuple[str, ...] = ()
    is_unique: bool = False
    is_primary_key: bool = False
    index_type: str | None = None
    predicate: str | None = None
    database_name: str | None = None


def _norm(columns: Iterable[str]) -> tuple[str, ...]:
    return tuple(c.strip().lower() for c in columns)


def _keep_priority(index: IndexDefinition) -> tuple[int, str]:
    # Mantem PK > unique > o de menor nome; os demais do grupo sao os redundantes.
    rank = 0 if index.is_primary_key else (1 if index.is_unique else 2)
    return (rank, index.index_name.lower())


def find_redundant_indexes(
    indexes: Iterable[IndexDefinition],
) -> list[tuple[IndexDefinition, IndexDefinition, RedundancyKind]]:
    """Devolve `(redundante, coberto_por, tipo)`.

    duplicate: mesmas colunas-chave (ordem incluida), mesmo conjunto INCLUDE, mesmo predicado e
    tipo — fica o PK/unique/menor nome. overlapping: chaves de A sao prefixo estrito das de B (mesmo
    tipo/predicado), A nao e unique/PK (garante restricao, nao pode sair) e INCLUDE de A esta
    coberto pelas colunas de B."""
    by_table: dict[tuple[str | None, str, str], list[IndexDefinition]] = defaultdict(list)
    for idx in indexes:
        if not idx.key_columns:
            continue
        by_table[(idx.database_name, idx.schema_name.lower(), idx.table_name.lower())].append(idx)

    results: list[tuple[IndexDefinition, IndexDefinition, RedundancyKind]] = []
    for table_indexes in by_table.values():
        buckets: dict[tuple, list[IndexDefinition]] = defaultdict(list)
        for idx in table_indexes:
            signature = (
                _norm(idx.key_columns),
                tuple(sorted(_norm(idx.included_columns))),
                (idx.predicate or "").strip().lower(),
                (idx.index_type or "").upper(),
            )
            buckets[signature].append(idx)

        redundant_names: set[str] = set()
        for group in buckets.values():
            if len(group) < 2:
                continue
            ordered = sorted(group, key=_keep_priority)
            keep = ordered[0]
            for other in ordered[1:]:
                if other.is_primary_key:
                    continue
                results.append((other, keep, "duplicate"))
                redundant_names.add(other.index_name)

        candidates = sorted(table_indexes, key=lambda i: i.index_name.lower())
        for a in candidates:
            if a.is_unique or a.is_primary_key or a.index_name in redundant_names:
                continue
            a_keys = _norm(a.key_columns)
            for b in candidates:
                if b is a or b.index_name in redundant_names:
                    continue
                b_keys = _norm(b.key_columns)
                if len(b_keys) <= len(a_keys) or b_keys[: len(a_keys)] != a_keys:
                    continue
                if (a.index_type or "").upper() != (b.index_type or "").upper():
                    continue
                if (a.predicate or "").strip().lower() != (b.predicate or "").strip().lower():
                    continue
                covered = set(b_keys) | set(_norm(b.included_columns))
                if not set(_norm(a.included_columns)) <= covered:
                    continue
                results.append((a, b, "overlapping"))
                break
    return results


def fragmentation_severity(fragmentation_pct: float | None) -> Severity | None:
    """>=30% reconstruir (warning), 10-30% reorganizar (info), abaixo disso nao reporta."""
    if fragmentation_pct is None:
        return None
    if fragmentation_pct >= 30:
        return "warning"
    if fragmentation_pct >= 10:
        return "info"
    return None


def statistics_assessment(
    *,
    modified_pct: float | None,
    days_since_update: float | None,
    never_updated: bool,
    row_count: int | None,
    engine_stale_flag: bool = False,
) -> tuple[bool, Severity] | None:
    """`(is_stale, severity)` ou `None` quando a estatistica esta saudavel (nao reportar)."""
    if never_updated:
        if row_count is None or row_count > 0:
            return (True, "warning")
        return None
    if modified_pct is not None and modified_pct >= 50:
        return (True, "critical")
    if engine_stale_flag or (modified_pct is not None and modified_pct >= 20):
        return (True, "warning")
    if modified_pct is not None and modified_pct >= 10:
        return (True, "info")
    if days_since_update is not None and days_since_update >= 30 and (row_count or 0) > 0:
        return (False, "info")
    return None


def modified_percentage(modified_rows: int | None, row_count: int | None) -> float | None:
    if modified_rows is None:
        return None
    return round(modified_rows * 100.0 / max(row_count or 0, 1), 2)


def usage_notes(started_at: datetime | None, now: datetime) -> list[str]:
    if started_at is None:
        return []
    if now - started_at < USAGE_STATS_MIN_RELIABLE_PERIOD:
        return ["usage_stats_recent"]
    return ["usage_stats_since_restart"]


def quote_mysql(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def quote_double(name: str) -> str:
    """Identificador entre aspas duplas (Oracle/DB2)."""
    return '"' + name.replace('"', '""') + '"'


_SEVERITY_ORDER = {"critical": 0, "warning": 1, "info": 2}


def severity_rank(severity: str) -> int:
    return _SEVERITY_ORDER.get(severity, 3)
