"""Testes contra o banco real (`ab_xql_pm_dev`) das agregacoes de historico: queries lentas,
regressoes, heatmap de deadlocks e visao geral. Mesmo padrao dos demais testes de repositorio
(fixture `test_instance` limpa `query_samples`/`metric_samples` da instancia ao final)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.deadlock_event import DeadlockEvent
from app.models.instance import Instance
from app.normalizers.metric_names import PG_DEADLOCKS, PG_NUMBACKENDS
from app.repositories._tables import metric_samples_table, query_samples_table
from app.repositories.deadlock_history_repository import DeadlockHistoryRepository
from app.repositories.instance_overview_repository import InstanceOverviewRepository
from app.repositories.slow_queries_repository import SlowQueriesRepository


def _sample(instance_id, time: datetime, sql_hash: str, session_id: str, duration_ms: int):  # noqa: ANN001, ANN202
    return {
        "time": time,
        "instance_id": instance_id,
        "sql_hash": sql_hash,
        "sql_name": None,
        "sql_text_masked": f"SELECT /* {sql_hash} */ ?",
        "duration_ms": duration_ms,
        "session_id": session_id,
        "database_name": "appdb",
    }


def _execution(instance_id, start: datetime, sql_hash: str, session_id: str, total_ms: int):  # noqa: ANN001, ANN202
    """Uma execucao amostrada a cada 10s ate `total_ms` — varias amostras, 1 execucao."""
    rows = []
    elapsed = 0
    while True:
        elapsed = min(elapsed + 10_000, total_ms)
        at = start + timedelta(milliseconds=elapsed)
        rows.append(_sample(instance_id, at, sql_hash, session_id, elapsed))
        if elapsed >= total_ms:
            break
    return rows


async def test_slow_queries_counts_executions_not_samples(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    base = now - timedelta(hours=2)
    rows = []
    # hash "slow": 2 execucoes de 60s e 40s (6 + 4 amostras)
    rows += _execution(test_instance.id, base, "slow", "s1", 60_000)
    rows += _execution(test_instance.id, base + timedelta(minutes=5), "slow", "s1", 40_000)
    # hash "fast": 1 execucao de 10s
    rows += _execution(test_instance.id, base, "fast", "s2", 10_000)
    await db_session.execute(sa.insert(query_samples_table), rows)
    await db_session.commit()

    repo = SlowQueriesRepository(db_session)
    result = await repo.top_slow(
        test_instance.id, now - timedelta(days=1), now, limit=10, min_executions=1
    )
    by_hash = {r.sql_hash: r for r in result}
    assert [r.sql_hash for r in result] == ["slow", "fast"]
    assert by_hash["slow"].executions == 2
    assert by_hash["slow"].avg_duration_ms == 50_000
    assert by_hash["slow"].max_duration_ms == 60_000
    assert by_hash["slow"].total_duration_ms == 100_000
    assert 40_000 <= by_hash["slow"].p95_duration_ms <= 60_000
    assert by_hash["slow"].sql_text_masked == "SELECT /* slow */ ?"
    assert by_hash["fast"].executions == 1

    series = await repo.daily_series(
        test_instance.id, now - timedelta(days=1), now, ["slow", "fast"]
    )
    assert sum(p.executions for p in series["slow"]) == 2

    filtered = await repo.top_slow(
        test_instance.id, now - timedelta(days=1), now, limit=10, min_executions=2
    )
    assert [r.sql_hash for r in filtered] == ["slow"]


async def test_regressions_compare_before_and_after_split(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    now = datetime.now(UTC).replace(microsecond=0)
    split = now - timedelta(hours=10)
    rows = []
    for i in range(3):
        rows += _execution(
            test_instance.id, split - timedelta(hours=5, minutes=i * 5), "q", f"b{i}", 10_000
        )
        rows += _execution(
            test_instance.id, split + timedelta(hours=1, minutes=i * 5), "q", f"a{i}", 30_000
        )
        rows += _execution(
            test_instance.id, split - timedelta(hours=5, minutes=i * 5), "stable", f"c{i}", 20_000
        )
        rows += _execution(
            test_instance.id, split + timedelta(hours=1, minutes=i * 5), "stable", f"d{i}", 20_000
        )
    await db_session.execute(sa.insert(query_samples_table), rows)
    await db_session.commit()

    repo = SlowQueriesRepository(db_session)
    result = await repo.regressions(
        test_instance.id,
        before_start=split - timedelta(hours=24),
        split=split,
        after_end=now,
        limit=10,
        min_executions=3,
        min_change_pct=1.0,
    )
    assert [r.sql_hash for r in result] == ["q"]
    assert result[0].before_executions == 3
    assert result[0].after_executions == 3
    assert result[0].change_pct == 200.0

    too_strict = await repo.regressions(
        test_instance.id,
        before_start=split - timedelta(hours=24),
        split=split,
        after_end=now,
        limit=10,
        min_executions=4,
        min_change_pct=0.0,
    )
    assert too_strict == []


async def test_deadlock_history_counter_ignores_reset(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    t0 = now - timedelta(hours=5)
    values = [10, 12, 12, 15, 2, 3]  # +2, 0, +3, reset (ignorado), +1  => 6
    rows = [
        {
            "time": t0 + timedelta(minutes=10 * i),
            "target_type": "instance",
            "target_id": test_instance.id,
            "metric_name": PG_DEADLOCKS,
            "labels": {},
            "value": float(v),
        }
        for i, v in enumerate(values)
    ]
    await db_session.execute(sa.insert(metric_samples_table), rows)
    await db_session.commit()

    repo = DeadlockHistoryRepository(db_session)
    since = t0 - timedelta(minutes=1)
    daily = await repo.daily(
        instance_id=test_instance.id, db_type="postgresql", since=since, until=now, tz="UTC"
    )
    assert sum(c for _, c in daily) == 6
    heatmap = await repo.heatmap(
        instance_id=test_instance.id, db_type="postgresql", since=since, until=now, tz="UTC"
    )
    assert sum(c for _, _, c in heatmap) == 6
    assert all(1 <= w <= 7 and 0 <= h <= 23 for w, h, _ in heatmap)

    overview = InstanceOverviewRepository(db_session)
    deltas = await overview.counter_deltas([test_instance.id], [PG_DEADLOCKS], since)
    assert deltas[test_instance.id] == 6


async def test_deadlock_history_events_and_timezone(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    # Segunda 23:30 UTC = terca 00:30 em Europe/Berlin (UTC+1 em novembro).
    monday_2330 = datetime(2026, 11, 16, 23, 30, tzinfo=UTC)
    event = DeadlockEvent(instance_id=test_instance.id, time=monday_2330, pending=True)
    db_session.add(event)
    await db_session.commit()
    try:
        repo = DeadlockHistoryRepository(db_session)
        kwargs = {
            "instance_id": test_instance.id,
            "db_type": "sqlserver",
            "since": monday_2330 - timedelta(days=1),
            "until": monday_2330 + timedelta(days=1),
        }
        utc = await repo.heatmap(**kwargs, tz="UTC")
        berlin = await repo.heatmap(**kwargs, tz="Europe/Berlin")
        assert utc == [(1, 23, 1)]
        assert berlin == [(2, 0, 1)]
        counts = await InstanceOverviewRepository(db_session).deadlock_event_counts(
            [test_instance.id], monday_2330 - timedelta(minutes=1)
        )
        assert counts[test_instance.id] == 1
    finally:
        await db_session.execute(
            sa.delete(DeadlockEvent).where(DeadlockEvent.instance_id == test_instance.id)
        )
        await db_session.commit()


async def test_overview_latest_values(db_session: AsyncSession, test_instance: Instance) -> None:
    now = datetime.now(UTC)
    rows = [
        {
            "time": now - timedelta(minutes=m),
            "target_type": "instance",
            "target_id": test_instance.id,
            "metric_name": PG_NUMBACKENDS,
            "labels": {},
            "value": float(v),
        }
        for m, v in ((5, 10), (1, 20))
    ]
    await db_session.execute(sa.insert(metric_samples_table), rows)
    await db_session.commit()
    latest = await InstanceOverviewRepository(db_session).latest_metric_values(
        [test_instance.id], [PG_NUMBACKENDS], now - timedelta(minutes=30)
    )
    assert latest[test_instance.id][PG_NUMBACKENDS][0] == 20.0
