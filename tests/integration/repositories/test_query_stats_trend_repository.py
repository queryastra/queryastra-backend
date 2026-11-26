"""Integracao (banco real) de `QueryStatsTrendRepository.top_query_trends`.

Regressao: a consulta final fazia `MAX(h.query_text)` sem GROUP BY e o PostgreSQL recusava
(GroupingError) — o painel de tendencia de queries da tela DB Health dava erro sempre.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import sqlalchemy as sa

from app.repositories._tables import query_stats_snapshot_table
from app.repositories.query_stats_trend_repository import QueryStatsTrendRepository


async def test_top_query_trends_returns_hourly_deltas(db_session, test_instance) -> None:  # noqa: ANN001
    repo = QueryStatsTrendRepository(db_session)
    base = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(hours=3)
    # Acumulativos (semantica do pg_stat_statements): 3 snapshots -> 2 deltas, em horas distintas.
    for offset, calls, total_ms in [(0, 10, 100.0), (60, 30, 500.0), (120, 40, 900.0)]:
        await repo.insert_snapshot(
            test_instance.id,
            base + timedelta(minutes=offset),
            [
                {
                    "queryid": 123,
                    "query_text": "SELECT 1",
                    "calls": calls,
                    "total_exec_time": total_ms,
                    "total_rows": calls,
                }
            ],
        )
    try:
        rows = await repo.top_query_trends(
            test_instance.id, base - timedelta(minutes=1), base + timedelta(hours=3), limit=10
        )
        assert [(str(r.queryid), r.query_text, r.calls) for r in rows] == [
            ("123", "SELECT 1", 20),
            ("123", "SELECT 1", 10),
        ]
        assert [round(r.mean_exec_ms, 1) for r in rows] == [20.0, 40.0]
    finally:
        await db_session.execute(
            sa.delete(query_stats_snapshot_table).where(
                query_stats_snapshot_table.c.instance_id == test_instance.id
            )
        )
        await db_session.commit()
