"""Repositorio para query_stats_snapshot — Fase 6, T60.

Persiste snapshots periodicos do pg_stat_statements e consulta tendencias de performance
por instancia. Os valores armazenados sao acumulativos (mesma semantica do pg_stat_statements);
o metodo `top_query_trends` calcula deltas via LAG() e agrega por hora.
"""

from __future__ import annotations

from datetime import datetime
from uuid import UUID

import sqlalchemy as sa

from app.repositories._tables import query_stats_snapshot_table
from app.repositories.base import BaseRepository


class QueryStatsTrendRepository(BaseRepository):

    async def insert_snapshot(
        self,
        instance_id: UUID,
        time: datetime,
        snapshots: list[dict],
    ) -> None:
        """Persiste uma lista de linhas do pg_stat_statements para uma instancia/momento."""
        if not snapshots:
            return
        rows = [
            {
                "time": time,
                "instance_id": instance_id,
                "queryid": s["queryid"],
                "query_text": s.get("query_text"),
                "calls": s["calls"],
                "total_exec_time": s["total_exec_time"],
                "total_rows": s["total_rows"],
            }
            for s in snapshots
        ]
        await self._session.execute(sa.insert(query_stats_snapshot_table), rows)
        await self._session.commit()

    async def top_query_trends(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        limit: int = 10,
    ) -> list[sa.Row]:
        """Retorna tendencia horaria para as top `limit` queries por impacto total no periodo.

        Calcula deltas entre snapshots consecutivos via LAG() para obter metricas por
        intervalo de coleta. Ignora deltas negativos (reset de pg_stat_statements).
        Retorna linhas (queryid, query_text, bucket, calls, mean_exec_ms) ordenadas por
        (queryid, bucket).
        """
        stmt = sa.text("""
            WITH deltas AS (
                SELECT
                    time,
                    queryid,
                    MAX(query_text) OVER (PARTITION BY queryid) AS query_text,
                    calls - LAG(calls)
                        OVER (PARTITION BY queryid ORDER BY time)          AS calls_delta,
                    total_exec_time - LAG(total_exec_time)
                        OVER (PARTITION BY queryid ORDER BY time)          AS exec_time_delta
                FROM query_stats_snapshot
                WHERE instance_id = :instance_id
                  AND time >= :since
                  AND time <= :until
            ),
            hourly AS (
                SELECT
                    time_bucket('1 hour', time)          AS bucket,
                    queryid,
                    MAX(query_text)                      AS query_text,
                    SUM(calls_delta)                     AS period_calls,
                    SUM(exec_time_delta)                 AS period_exec_time
                FROM deltas
                WHERE calls_delta > 0 AND exec_time_delta >= 0
                GROUP BY bucket, queryid
            ),
            top_queries AS (
                SELECT queryid
                FROM hourly
                GROUP BY queryid
                ORDER BY SUM(period_exec_time) DESC
                LIMIT :limit
            )
            SELECT
                h.queryid,
                MAX(h.query_text)  AS query_text,
                h.bucket,
                h.period_calls     AS calls,
                CASE WHEN h.period_calls > 0
                     THEN h.period_exec_time / h.period_calls
                     ELSE 0
                END                AS mean_exec_ms
            FROM hourly h
            JOIN top_queries tq ON h.queryid = tq.queryid
            WHERE h.period_calls > 0
            ORDER BY h.queryid, h.bucket ASC
        """)
        result = await self._session.execute(
            stmt,
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
            },
        )
        return list(result.fetchall())
