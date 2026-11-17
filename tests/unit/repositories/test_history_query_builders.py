"""Testes dos construtores de SQL (sem banco): queries lentas/regressoes, heatmap de deadlocks,
anotacoes e visao geral."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from app.normalizers.metric_names import PG_DEADLOCKS
from app.repositories import deadlock_history_repository as dh
from app.repositories import slow_queries_repository as sq
from app.repositories.annotations_repository import MAX_ANNOTATIONS, build_list_statement
from app.repositories.instance_overview_repository import COUNTER_DELTAS_SQL, LATEST_VALUES_SQL


def _binds(sql: str) -> set[str]:
    return set(sa.text(sql).compile().binds.keys())


# ── slow queries ──────────────────────────────────────────────────────────────


@pytest.mark.parametrize("order_by", ["avg", "max", "p95", "total"])
def test_slow_queries_sql_orders_by_requested_column(order_by: sq.SlowQueryOrder) -> None:
    sql = sq.build_slow_queries_sql(order_by)
    col = sq._ORDER_COLUMNS[order_by]
    assert f"ORDER BY agg.{col} DESC" in sql
    assert "percentile_cont(0.95)" in sql
    assert _binds(sql) == {
        "instance_id",
        "since",
        "until",
        "database_name",
        "min_executions",
        "limit",
    }


def test_slow_queries_reconstructs_executions_not_samples() -> None:
    sql = sq.build_slow_queries_sql()
    # uma execucao = mesma sessao + hash + instante de inicio estimado
    assert "GROUP BY sql_hash, coalesce(session_id, '')" in sql
    assert "extract(epoch FROM time) - duration_ms / 1000.0" in sql
    assert "max(duration_ms)" in sql
    # texto mascarado, nunca o cifrado
    assert "sql_text_masked" in sql
    assert "sql_text_encrypted" not in sql


def test_daily_series_restricts_to_hashes() -> None:
    sql = sq.build_daily_series_sql()
    assert "sql_hash = ANY(:sql_hashes)" in sql
    assert "sql_hashes" in _binds(sql)
    assert "AT TIME ZONE 'UTC'" in sql


def test_regressions_sql_binds_and_ordering() -> None:
    sql = sq.build_regressions_sql()
    assert _binds(sql) == {
        "instance_id",
        "since",
        "until",
        "database_name",
        "split",
        "min_executions",
        "min_change_pct",
        "limit",
    }
    assert "before_avg_ms > 0" in sql
    assert "ORDER BY ranked.change_pct DESC" in sql


@pytest.mark.parametrize(
    ("before", "after", "expected"),
    [(100.0, 150.0, 50.0), (200.0, 100.0, -50.0), (10.0, 10.0, 0.0), (0.0, 10.0, None)],
)
def test_compute_change_pct(before: float, after: float, expected: float | None) -> None:
    assert sq.compute_change_pct(before, after) == expected


# ── deadlock history ──────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    ("db_type", "source"),
    [
        ("sqlserver", "events"),
        ("postgresql", "counter"),
        ("oracle", "counter"),
        ("mysql", "none"),
    ],
)
def test_resolve_source(db_type: str, source: str) -> None:
    assert dh.resolve_source(db_type) == source


def test_heatmap_sql_events_reads_deadlock_events() -> None:
    sql = dh.build_heatmap_sql("events")
    assert "FROM deadlock_events" in sql
    assert "isodow" in sql
    assert "AT TIME ZONE :tz" in sql
    assert _binds(sql) == {"instance_id", "since", "until", "tz"}


def test_counter_sql_ignores_resets_and_seeds_lag() -> None:
    sql = dh.build_daily_sql("counter")
    assert "LAG(value)" in sql
    assert "n > 0" in sql  # delta negativo (reset) descartado
    assert _binds(sql) == {"instance_id", "since", "until", "tz", "metric_name", "seed_since"}


def test_no_source_sql_raises() -> None:
    with pytest.raises(ValueError):
        dh.build_heatmap_sql("none")


def test_build_params_counter_vs_events() -> None:
    since = datetime(2026, 11, 1, tzinfo=UTC)
    until = since + timedelta(days=7)
    iid = uuid4()
    pg = dh.build_params(instance_id=iid, since=since, until=until, tz="UTC", db_type="postgresql")
    assert pg["metric_name"] == PG_DEADLOCKS
    assert pg["seed_since"] == since - timedelta(hours=1)
    ss = dh.build_params(instance_id=iid, since=since, until=until, tz="UTC", db_type="sqlserver")
    assert "metric_name" not in ss


# ── annotations ───────────────────────────────────────────────────────────────


def _compile(stmt: sa.Select) -> str:  # type: ignore[type-arg]
    return str(stmt.compile(dialect=postgresql.dialect()))


def test_annotations_instance_scope_includes_globals() -> None:
    sql = _compile(
        build_list_statement(instance_id=uuid4(), since=None, until=None, include_global=True)
    )
    assert "annotations.instance_id = " in sql
    assert "annotations.instance_id IS NULL" in sql
    assert "ORDER BY annotations.occurred_at DESC" in sql


def test_annotations_instance_scope_without_globals() -> None:
    sql = _compile(
        build_list_statement(instance_id=uuid4(), since=None, until=None, include_global=False)
    )
    assert "IS NULL" not in sql


def test_annotations_time_range_and_category() -> None:
    now = datetime.now(UTC)
    sql = _compile(
        build_list_statement(
            instance_id=None,
            since=now - timedelta(days=1),
            until=now,
            category="deploy",
        )
    )
    assert "annotations.occurred_at >= " in sql
    assert "annotations.occurred_at <= " in sql
    assert "annotations.category = " in sql
    assert "instance_id" not in sql.split("WHERE", 1)[1]  # admin: sem filtro de instancia


def test_annotations_non_admin_without_instance_filters_by_accessible() -> None:
    sql = _compile(
        build_list_statement(
            instance_id=None, since=None, until=None, accessible_instance_ids=[uuid4()]
        )
    )
    assert "annotations.instance_id IS NULL" in sql
    assert "annotations.instance_id IN" in sql
    only_globals = _compile(
        build_list_statement(instance_id=None, since=None, until=None, accessible_instance_ids=[])
    )
    assert "IN (" not in only_globals


def test_annotations_limit_is_capped() -> None:
    stmt = build_list_statement(instance_id=None, since=None, until=None, limit=10**6)
    assert stmt._limit_clause is not None
    assert stmt._limit_clause.value == MAX_ANNOTATIONS  # type: ignore[attr-defined]


# ── overview ──────────────────────────────────────────────────────────────────


def test_overview_sql_is_batched() -> None:
    assert "target_id = ANY(:instance_ids)" in LATEST_VALUES_SQL
    assert "DISTINCT ON (target_id, metric_name)" in LATEST_VALUES_SQL
    assert "PARTITION BY target_id, metric_name" in COUNTER_DELTAS_SQL
    assert "delta > 0" in COUNTER_DELTAS_SQL
