"""Testes dos helpers puros de manutencao do DB2 — `app/collectors/db2/maintenance.py`."""

from __future__ import annotations

from datetime import date

import pytest

from app.collectors.db2.maintenance import (
    build_db2_index_definitions,
    build_db2_partition_info,
    db2_compression_command,
    db2_current_compression,
    db2_never_used,
    extract_savings_pct,
    sort_overflow_severity,
)


def test_build_index_definitions_orders_keys_and_separates_includes() -> None:
    index_rows = [
        {"indschema": "APP ", "indname": "IX1", "tabschema": "APP", "tabname": "T",
         "uniquerule": "U", "indextype": "REG"},
        {"indschema": "APP", "indname": "PK", "tabschema": "APP", "tabname": "T",
         "uniquerule": "P", "indextype": None},
    ]
    column_rows = [
        {"indschema": "APP", "indname": "IX1", "colname": "B", "colseq": 2, "colorder": "D"},
        {"indschema": "APP", "indname": "IX1", "colname": "A", "colseq": 1, "colorder": "A"},
        {"indschema": "APP", "indname": "IX1", "colname": "C", "colseq": 3, "colorder": "I"},
        {"indschema": "APP", "indname": "PK", "colname": "ID", "colseq": 1, "colorder": None},
    ]
    ix1, pk = build_db2_index_definitions(index_rows, column_rows)
    assert ix1.key_columns == ("A", "B DESC")
    assert ix1.included_columns == ("C",)
    assert ix1.is_unique and not ix1.is_primary_key
    assert ix1.index_type == "REG"
    assert pk.key_columns == ("ID",)
    assert pk.is_primary_key and pk.is_unique
    assert pk.index_type is None


def test_never_used_sentinel_date() -> None:
    assert db2_never_used(date(1, 1, 1))
    assert not db2_never_used(date(2026, 1, 1))
    assert not db2_never_used(None)


@pytest.mark.parametrize(
    ("overflows", "total", "expected"),
    [(0, 100, "info"), (5, 100, "info"), (10, 100, "warning"), (3, 0, "warning")],
)
def test_sort_overflow_severity(overflows: int, total: int, expected: str) -> None:
    assert sort_overflow_severity(overflows, total) == expected


@pytest.mark.parametrize(
    ("compression", "mode", "expected"),
    [
        ("N", None, "NONE"),
        (None, None, "NONE"),
        ("V", "", "VALUE"),
        ("R", "A", "ADAPTIVE"),
        ("R", "S", "STATIC"),
        ("R", "", "ROW"),
        ("B", "A", "ADAPTIVE+VALUE"),
    ],
)
def test_current_compression(compression: str | None, mode: str | None, expected: str) -> None:
    assert db2_current_compression(compression, mode) == expected


def test_extract_savings_pct_prefers_column_by_type_and_ignores_negatives() -> None:
    rows = [
        {"pctpagessaved_adaptive": 40, "pctpagessaved_static": 30},
        {"pctpagessaved_adaptive": 60, "pctpagessaved_static": -1},
    ]
    assert extract_savings_pct(rows, "PAGE") == 50.0
    assert extract_savings_pct(rows, "ROW") == 30.0
    assert extract_savings_pct([{"other": 1}], "PAGE") is None


def test_compression_command_mode() -> None:
    assert "COMPRESS YES STATIC" in db2_compression_command("APP", "T", "ROW")
    assert "COMPRESS YES ADAPTIVE" in db2_compression_command("APP", "T", "PAGE")
    assert "RESETDICTIONARY" in db2_compression_command("APP", "T", "PAGE")


def _part(seq: int, high: str, card: int) -> dict[str, object]:
    return {"seqno": seq, "datapartitionname": f"P{seq}", "highvalue": high, "card": card}


def test_partition_rows_in_maxvalue_tail_is_warning() -> None:
    info = build_db2_partition_info(
        "DB", "APP", "T", "DT",
        [_part(0, "'2026-01-01'", 10), _part(1, "'2026-02-01'", 20), _part(2, "MAXVALUE", 5)],
    )
    assert info.partition_count == 3
    assert info.last_boundary_value == "'2026-02-01'"
    assert info.rows_beyond_last_boundary == 5
    assert info.total_rows == 35
    assert info.severity == "warning"
    assert info.recommendation and "DETACH PARTITION" in info.recommendation


def test_partition_empty_maxvalue_tail_needs_no_action() -> None:
    info = build_db2_partition_info(
        "DB", "APP", "T", None, [_part(0, "'2026-01-01'", 10), _part(1, "MAXVALUE, MAXVALUE", 0)]
    )
    assert info.severity == "info"
    assert info.rows_beyond_last_boundary == 0
    assert info.recommendation is None


def test_partition_without_tail_suggests_add_partition() -> None:
    info = build_db2_partition_info("DB", "APP", "T", None, [_part(0, "'2026-01-01'", -1)])
    assert info.rows_beyond_last_boundary is None
    assert info.total_rows is None  # card -1 = estatistica ausente
    assert info.recommendation and "ADD PARTITION" in info.recommendation
