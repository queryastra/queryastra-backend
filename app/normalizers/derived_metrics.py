"""Calculo puro (sem I/O) das metricas derivadas em % emitidas pelos coletores.

Ver `app.normalizers.metric_names.DERIVED_PERCENT_METRICS`. Cada funcao devolve `None` quando a
entrada nao permite um valor confiavel (ausente, denominador zero, negativo) — o coletor
simplesmente nao emite a amostra nesse ciclo, mesmo padrao de degradacao graciosa do resto.
"""

from __future__ import annotations


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None


def _clamp_percent(value: float) -> float:
    return max(0.0, min(100.0, value))


def buffer_hit_ratio_percent(hits: object, disk_reads: object) -> float | None:
    """`hits / (hits + disk_reads) * 100` — PostgreSQL (`blks_hit`/`blks_read`)."""
    hit = _to_float(hits)
    read = _to_float(disk_reads)
    if hit is None or read is None or hit < 0 or read < 0:
        return None
    total = hit + read
    if total <= 0:
        return None
    return _clamp_percent(hit / total * 100.0)


def ratio_counter_percent(value: object, base: object) -> float | None:
    """Contador de razao do SQL Server (`Buffer cache hit ratio` / `... base`)."""
    numerator = _to_float(value)
    denominator = _to_float(base)
    if numerator is None or denominator is None or denominator <= 0 or numerator < 0:
        return None
    return _clamp_percent(numerator / denominator * 100.0)


def logical_read_hit_ratio_percent(disk_reads: object, read_requests: object) -> float | None:
    """MySQL InnoDB: `(1 - Innodb_buffer_pool_reads / Innodb_buffer_pool_read_requests) * 100`."""
    reads = _to_float(disk_reads)
    requests = _to_float(read_requests)
    if reads is None or requests is None or requests <= 0 or reads < 0:
        return None
    return _clamp_percent((1.0 - reads / requests) * 100.0)


def connections_used_percent(used: object, maximum: object) -> float | None:
    """`used / maximum * 100` — conexoes em uso vs limite configurado."""
    current = _to_float(used)
    limit = _to_float(maximum)
    if current is None or limit is None or limit <= 0 or current < 0:
        return None
    return _clamp_percent(current / limit * 100.0)
