"""Funcoes puras das varreduras de manutencao PostgreSQL/CockroachDB/Redshift."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from app.collectors.cockroachdb.maintenance import (
    aggregate_crdb_statements,
    build_crdb_index_defs,
    classify_crdb_statistics,
    group_crdb_partitions,
)
from app.collectors.postgresql.maintenance import (
    IndexDef,
    analyze_threshold,
    classify_statistics,
    density_to_fragmentation,
    find_redundant_indexes,
    fragmentation_severity,
    next_range,
    parse_range_bound,
    quote_ident,
    redundant_items,
    summarize_partitions,
    summarize_toast_columns,
    usage_notes,
)
from app.collectors.redshift.maintenance import aggregate_disk_based, classify_stats_off

NOW = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def _idx(name: str, keys: tuple[str, ...], **kw) -> IndexDef:
    return IndexDef(table_key="public.t", name=name, method=kw.pop("method", "btree"), keys=keys,
                    extra={"schema_name": "public", "table_name": "t"}, **kw)


# ── quoting / severities ─────────────────────────────────────────────────────


def test_quote_ident_escapes_double_quotes() -> None:
    assert quote_ident('we"ird') == '"we""ird"'


def test_fragmentation_severity_bands() -> None:
    assert fragmentation_severity(10) == "info"
    assert fragmentation_severity(30) == "warning"
    assert fragmentation_severity(75) == "critical"


def test_density_to_fragmentation_relative_to_fillfactor() -> None:
    assert density_to_fragmentation(90.0, 90) == 0.0
    assert density_to_fragmentation(45.0, 90) == 50.0
    assert density_to_fragmentation(float("nan")) == 0.0
    assert density_to_fragmentation(None) == 0.0


# ── statistics ───────────────────────────────────────────────────────────────


def test_analyze_threshold_honors_reloptions() -> None:
    assert analyze_threshold(1000, None, 0.1, 50) == 150
    assert analyze_threshold(1000, ["autovacuum_analyze_scale_factor=0.01"], 0.1, 50) == 60


def test_classify_statistics_stale_over_threshold() -> None:
    info = classify_statistics(
        n_live_tup=1000, reltuples=1000, n_mod_since_analyze=600,
        last_updated=NOW - timedelta(days=3), reloptions=None,
        scale_factor=0.1, base_threshold=50, now=NOW,
    )
    assert info["is_stale"] is True
    assert info["severity"] == "critical"
    assert info["modified_pct"] == 60.0
    assert info["days_since_update"] == 3.0


def test_classify_statistics_never_analyzed() -> None:
    info = classify_statistics(
        n_live_tup=5000, reltuples=-1, n_mod_since_analyze=5000, last_updated=None,
        reloptions=None, scale_factor=0.1, base_threshold=50, now=NOW,
    )
    assert info["is_stale"] and info["severity"] == "warning"
    assert info["detail"]["never_analyzed"] is True


def test_classify_statistics_fresh() -> None:
    info = classify_statistics(
        n_live_tup=1000, reltuples=1000, n_mod_since_analyze=10, last_updated=NOW,
        reloptions=None, scale_factor=0.1, base_threshold=50, now=NOW,
    )
    assert not info["is_stale"] and info["severity"] == "info"


def test_usage_notes() -> None:
    assert usage_notes(NOW - timedelta(days=1), True, NOW) == [
        "usage_stats_since_restart", "usage_stats_recent",
    ]
    assert usage_notes(NOW - timedelta(days=30), False, NOW) == []


# ── duplicate / overlapping ──────────────────────────────────────────────────


def test_exact_duplicate_keeps_unique_constraint() -> None:
    plain = _idx("ix_a", ("a:1:0:0",), reads=5)
    uniq = _idx("uq_a", ("a:1:0:0",), is_unique=True, backs_constraint=True)
    [(cat, drop, keep)] = find_redundant_indexes([plain, uniq])
    assert cat == "duplicate" and drop.name == "ix_a" and keep.name == "uq_a"


def test_duplicate_tie_drops_least_used() -> None:
    a = _idx("ix_a1", ("a",), reads=100)
    b = _idx("ix_a2", ("a",), reads=0)
    [(_, drop, keep)] = find_redundant_indexes([a, b])
    assert drop.name == "ix_a2" and keep.name == "ix_a1"


def test_both_protected_not_reported() -> None:
    a = _idx("pk", ("a",), is_primary=True)
    b = _idx("uq", ("a",), is_unique=True, backs_constraint=True)
    assert find_redundant_indexes([a, b]) == []


def test_different_predicate_or_include_is_not_duplicate() -> None:
    a = _idx("ix1", ("a",))
    b = _idx("ix2", ("a",), predicate="(a > 0)")
    c = _idx("ix3", ("a",), includes=("b",))
    cats = [r[0] for r in find_redundant_indexes([a, b, c])]
    # ix1 nao e duplicado de nenhum; ix1 e prefixo? nao (mesmo numero de chaves)
    assert "duplicate" not in cats


def test_left_prefix_overlap() -> None:
    a = _idx("ix_a", ("a",))
    ab = _idx("ix_ab", ("a", "b"))
    ba = _idx("ix_ba", ("b", "a"))
    result = find_redundant_indexes([a, ab, ba])
    assert [(c, d.name, k.name) for c, d, k in result] == [("overlapping", "ix_a", "ix_ab")]


def test_unique_prefix_is_not_redundant() -> None:
    a = _idx("uq_a", ("a",), is_unique=True)
    ab = _idx("ix_ab", ("a", "b"))
    assert find_redundant_indexes([a, ab]) == []


def test_non_btree_not_overlapping() -> None:
    a = _idx("gin_a", ("a",), method="gin")
    ab = _idx("gin_ab", ("a", "b"), method="gin")
    assert find_redundant_indexes([a, ab]) == []


def test_redundant_items_build_drop_command() -> None:
    a = _idx("ix_a", ("a",))
    ab = _idx("ix_ab", ("a", "b"))
    [item] = redundant_items([a, ab], "db1", "DROP INDEX CONCURRENTLY {schema}.{index};")
    assert item.category == "overlapping"
    assert item.recommendation == 'DROP INDEX CONCURRENTLY "public"."ix_a";'
    assert item.duplicate_of == "ix_ab" and item.database_name == "db1"


# ── partitions ───────────────────────────────────────────────────────────────


def test_parse_range_bound() -> None:
    assert parse_range_bound("FOR VALUES FROM ('2026-01-01') TO ('2026-02-01')") == (
        "2026-01-01", "2026-02-01",
    )
    assert parse_range_bound("FOR VALUES FROM (1, 'x') TO (100, 'y')") == ("1", "100")
    assert parse_range_bound("FOR VALUES FROM (MINVALUE) TO (MAXVALUE)") == (
        "MINVALUE", "MAXVALUE",
    )
    assert parse_range_bound("FOR VALUES FROM ('it''s') TO ('z')") == ("it's", "z")
    assert parse_range_bound(None) == (None, None)


def test_next_range_month_and_int() -> None:
    assert next_range("2026-10-01", "2026-11-01") == ("2026-11-01", "2026-12-01")
    assert next_range("2026-12-01", "2027-01-01") == ("2027-01-01", "2027-02-01")
    assert next_range("2026-11-09", "2026-11-10") == ("2026-11-10", "2026-11-11")
    assert next_range("0", "1000") == ("1000", "2000")
    assert next_range("a", "b") is None


def _child(name: str, bound: str, rows: int | None = 0, has_rows: bool | None = None) -> dict:
    return {"name": name, "bound": bound, "rows": rows, "has_rows": has_rows}


def test_summarize_partitions_running_out_is_critical() -> None:
    kids = [
        _child("p_2026_09", "FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')", 10),
        _child("p_2026_10", "FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')", 20),
    ]
    s = summarize_partitions(kids, "RANGE", NOW)
    assert s["last_boundary_value"] == "2026-11-01"
    assert s["severity"] == "critical" and s["days_ahead"] < 0
    assert s["next_range"] == ("2026-11-01", "2026-12-01")
    assert s["total_rows"] == 30


def test_summarize_partitions_default_with_rows_is_warning() -> None:
    kids = [
        _child("p1", "FOR VALUES FROM (0) TO (100)", 5),
        _child("p_def", "DEFAULT", 0, has_rows=True),
    ]
    s = summarize_partitions(kids, "RANGE", NOW)
    assert s["rows_beyond_last_boundary"] == 1
    assert s["severity"] == "warning"
    assert s["partitions"][-1]["boundary"] == "DEFAULT"


def test_summarize_partitions_healthy_future_range() -> None:
    kids = [_child("p", "FOR VALUES FROM ('2026-11-01') TO ('2027-01-01')", 1)]
    s = summarize_partitions(kids, "RANGE", NOW)
    assert s["severity"] == "info" and s["days_ahead"] > 7


def test_summarize_partitions_list_and_hash() -> None:
    kids = [_child("b", "FOR VALUES IN (2)", 1), _child("a", "FOR VALUES IN (1)", 1)]
    s = summarize_partitions(kids, "LIST", NOW)
    assert [p["name"] for p in s["partitions"]] == ["a", "b"]
    assert s["last_boundary_value"] is None


# ── compression ──────────────────────────────────────────────────────────────


def test_summarize_toast_columns() -> None:
    cols = [
        {"attname": "payload", "data_type": "jsonb", "storage": "x", "compression": ""},
        {"attname": "blob", "data_type": "bytea", "storage": "e", "compression": ""},
    ]
    method, out = summarize_toast_columns(cols, "pglz")
    assert method == "pglz"
    assert out[1]["compression"] is None
    cols.append({"attname": "doc", "data_type": "text", "storage": "x", "compression": "l"})
    assert summarize_toast_columns(cols, "pglz")[0] == "mixed"
    assert summarize_toast_columns([], "pglz")[0] == "none"


# ── CockroachDB ──────────────────────────────────────────────────────────────


def test_crdb_index_defs_and_redundancy() -> None:
    idx_rows = [
        {"table_id": 1, "schema_name": "public", "table_name": "t", "index_id": 1,
         "index_name": "t_pkey", "index_type": "primary", "is_unique": True,
         "is_inverted": False, "total_reads": 10, "indexdef": "CREATE UNIQUE INDEX t_pkey"},
        {"table_id": 1, "schema_name": "public", "table_name": "t", "index_id": 2,
         "index_name": "t_a_idx", "index_type": "secondary", "is_unique": False,
         "is_inverted": False, "total_reads": 0, "indexdef": "CREATE INDEX t_a_idx"},
        {"table_id": 1, "schema_name": "public", "table_name": "t", "index_id": 3,
         "index_name": "t_a_b_idx", "index_type": "secondary", "is_unique": False,
         "is_inverted": False, "total_reads": 3, "indexdef": "CREATE INDEX t_a_b_idx"},
    ]
    col_rows = [
        {"table_id": 1, "index_id": 1, "column_type": "key", "column_name": "id",
         "column_direction": "ASC", "implicit": False},
        {"table_id": 1, "index_id": 2, "column_type": "key", "column_name": "a",
         "column_direction": "ASC", "implicit": False},
        {"table_id": 1, "index_id": 2, "column_type": "extra", "column_name": "id",
         "column_direction": None, "implicit": False},
        {"table_id": 1, "index_id": 3, "column_type": "key", "column_name": "a",
         "column_direction": "ASC", "implicit": False},
        {"table_id": 1, "index_id": 3, "column_type": "key", "column_name": "b",
         "column_direction": "ASC", "implicit": False},
    ]
    defs = build_crdb_index_defs(idx_rows, col_rows)
    assert {d.name: d.keys for d in defs}["t_a_idx"] == ("a:ASC",)
    result = find_redundant_indexes(defs)
    assert [(c, d.name, k.name) for c, d, k in result] == [("overlapping", "t_a_idx", "t_a_b_idx")]


def test_crdb_statistics_classification() -> None:
    assert classify_crdb_statistics(None, 10, NOW)["is_stale"] is True
    assert classify_crdb_statistics(NOW - timedelta(days=30), 10, NOW)["severity"] == "warning"
    assert classify_crdb_statistics(NOW - timedelta(days=1), 10, NOW)["is_stale"] is False


def test_crdb_statement_aggregation() -> None:
    rows = [
        {"fingerprint": "f1", "query": "SELECT 1", "database_name": "d", "executions": 10,
         "sampled": 2, "mem_mean": 1000.0, "disk_mean": 0.0, "aggregated_ts": NOW},
        {"fingerprint": "f1", "query": "SELECT 1", "database_name": "d", "executions": 5,
         "sampled": 2, "mem_mean": 3000.0, "disk_mean": 2048.0,
         "aggregated_ts": NOW - timedelta(hours=1)},
        {"fingerprint": "f2", "query": "SELECT 2", "database_name": "d", "executions": 1,
         "sampled": 0, "mem_mean": None, "disk_mean": None, "aggregated_ts": NOW},
    ]
    [agg] = aggregate_crdb_statements(rows)
    assert agg["executions"] == 15 and agg["mem_max"] == 3000.0 and agg["mem_avg"] == 2000.0
    assert agg["disk_max"] == 2048.0 and agg["last"] == NOW


def test_crdb_partitions_grouping() -> None:
    rows = [
        {"schema_name": "public", "table_name": "t", "index_name": "t_pkey",
         "index_type": "primary", "partition_name": "p1", "column_names": "k",
         "list_value": None, "range_value": "(1) TO (10)"},
        {"schema_name": "public", "table_name": "t", "index_name": "t_pkey",
         "index_type": "primary", "partition_name": "p2", "column_names": "k",
         "list_value": None, "range_value": "(10) TO (MAXVALUE)"},
    ]
    [item] = group_crdb_partitions(rows, "db")
    assert item.partition_method == "RANGE" and item.partition_count == 2
    assert item.detail["catch_all_partitions"] == ["p2"]


# ── Redshift ─────────────────────────────────────────────────────────────────


def test_redshift_stats_off_classification() -> None:
    assert classify_stats_off(0.0)["is_stale"] is False
    assert classify_stats_off(20.0)["severity"] == "warning"
    assert classify_stats_off(80.0)["severity"] == "critical"
    assert classify_stats_off(None)["is_stale"] is True


def test_redshift_disk_based_aggregation() -> None:
    rows = [
        {"querytxt": "select * from t where id = 1", "database_name": "dev",
         "max_workmem": 2048, "diskbased_steps": 2, "starttime": NOW},
        {"querytxt": "select * from t where id = 2", "database_name": "dev",
         "max_workmem": 4096, "diskbased_steps": 1, "starttime": NOW - timedelta(hours=1)},
    ]
    [agg] = aggregate_disk_based(rows)  # mesmo sql_hash (literais mascarados)
    assert agg["executions"] == 2 and agg["steps"] == 3 and agg["workmem_max"] == 4096
