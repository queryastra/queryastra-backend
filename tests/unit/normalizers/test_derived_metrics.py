"""Testes das metricas derivadas em % (`app.normalizers.derived_metrics`) e da emissao delas pelos
coletores PostgreSQL e SQL Server."""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import uuid4

from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.adapter import PostgreSQLCollector
from app.collectors.sqlserver.adapter import _build_derived_metric_samples
from app.normalizers.derived_metrics import (
    buffer_hit_ratio_percent,
    connections_used_percent,
    logical_read_hit_ratio_percent,
    ratio_counter_percent,
)
from app.normalizers.metric_names import (
    DERIVED_PERCENT_METRICS,
    PG_BUFFER_HIT_RATIO_PERCENT,
    PG_CONNECTIONS_USED_PERCENT,
    PG_MAX_CONNECTIONS,
    SQLSERVER_BUFFER_CACHE_HIT_RATIO_PERCENT,
    SQLSERVER_CONNECTIONS_USED_PERCENT,
)


def test_buffer_hit_ratio() -> None:
    assert buffer_hit_ratio_percent(990, 10) == 99.0
    assert buffer_hit_ratio_percent(0, 0) is None
    assert buffer_hit_ratio_percent(None, 10) is None
    assert buffer_hit_ratio_percent(-1, 10) is None


def test_ratio_counter_and_logical_reads() -> None:
    assert ratio_counter_percent(95, 100) == 95.0
    assert ratio_counter_percent(1, 0) is None
    assert logical_read_hit_ratio_percent("50", "1000") == 95.0
    assert logical_read_hit_ratio_percent("1", "0") is None


def test_connections_used_percent_clamped() -> None:
    assert connections_used_percent(25, 100) == 25.0
    assert connections_used_percent(150, 100) == 100.0
    assert connections_used_percent(5, 0) is None
    assert connections_used_percent("x", 10) is None


def test_derived_metrics_are_registered() -> None:
    assert PG_BUFFER_HIT_RATIO_PERCENT in DERIVED_PERCENT_METRICS
    assert PG_CONNECTIONS_USED_PERCENT in DERIVED_PERCENT_METRICS
    assert all(name.endswith("_percent") for name in DERIVED_PERCENT_METRICS)


def test_postgresql_collector_emits_derived_gauges() -> None:
    collector = PostgreSQLCollector(
        ConnectionParams(host="localhost", port=5432, auth_method="encrypted_fallback"),
        instance_id=uuid4(),
    )
    stats_row = {
        "numbackends": 30,
        "xact_commit": 1,
        "xact_rollback": 0,
        "blks_read": 10,
        "blks_hit": 990,
        "tup_returned": 0,
        "tup_fetched": 0,
        "tup_inserted": 0,
        "tup_updated": 0,
        "tup_deleted": 0,
        "deadlocks": 0,
        "temp_files": 0,
        "temp_bytes": 0,
    }
    samples = collector._build_metric_samples(
        [], stats_row, None, 160000, datetime.now(UTC), max_connections=120
    )
    by_name = {s.metric_name: s.value for s in samples}
    assert by_name[PG_BUFFER_HIT_RATIO_PERCENT] == 99.0
    assert by_name[PG_MAX_CONNECTIONS] == 120.0
    assert by_name[PG_CONNECTIONS_USED_PERCENT] == 25.0


def test_postgresql_collector_omits_connection_gauge_without_max_connections() -> None:
    collector = PostgreSQLCollector(
        ConnectionParams(host="localhost", port=5432, auth_method="encrypted_fallback"),
        instance_id=uuid4(),
    )
    stats_row = {
        k: 0
        for k in (
            "numbackends",
            "xact_commit",
            "xact_rollback",
            "blks_read",
            "blks_hit",
            "tup_returned",
            "tup_fetched",
            "tup_inserted",
            "tup_updated",
            "tup_deleted",
            "deadlocks",
            "temp_files",
            "temp_bytes",
        )
    }
    samples = collector._build_metric_samples([], stats_row, None, 160000, datetime.now(UTC))
    names = {s.metric_name for s in samples}
    assert PG_CONNECTIONS_USED_PERCENT not in names
    # hit ratio sem nenhuma leitura ainda (0/0) tambem nao e emitido
    assert PG_BUFFER_HIT_RATIO_PERCENT not in names


def test_sqlserver_derived_samples() -> None:
    rows = [
        {"counter_name": "Buffer cache hit ratio", "cntr_value": 980},
        {"counter_name": "Buffer cache hit ratio base", "cntr_value": 1000},
        {"counter_name": "User Connections", "cntr_value": 50},
        {"counter_name": "Max Connections", "cntr_value": 200},
    ]
    samples = _build_derived_metric_samples(rows, uuid4(), datetime.now(UTC))
    by_name = {s.metric_name: s.value for s in samples}
    assert by_name[SQLSERVER_BUFFER_CACHE_HIT_RATIO_PERCENT] == 98.0
    assert by_name[SQLSERVER_CONNECTIONS_USED_PERCENT] == 25.0
    assert _build_derived_metric_samples([], uuid4(), datetime.now(UTC)) == []
