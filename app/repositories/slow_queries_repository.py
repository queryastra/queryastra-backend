"""Historico de queries lentas e deteccao de regressao — le `query_samples` direto.

Por que nao `query_stats_hourly`: o continuous aggregate so tem `execution_count`/
`total_wait_time_ms`/`avg_wait_time_ms` (nenhum campo de DURACAO) — inutil para "queries mais
lentas". Tudo aqui agrega a hypertable bruta (indice `(instance_id, sql_hash, time DESC)`).

## Reconstrucao de EXECUCOES a partir de amostras

`query_samples` e amostragem periodica de requests ATIVAS: uma mesma execucao longa aparece em
varias amostras consecutivas, cada uma com `duration_ms` = tempo decorrido ATE aquela amostra.
Somar/promediar amostras distorceria tudo (uma query de 60s amostrada a cada 10s contaria como 6
"execucoes" com media ~35s). Por isso agrupamos as amostras em "runs":

    run = (sql_hash, session_id, round(epoch(time) - duration_ms/1000))

ou seja, mesma sessao + mesmo hash + mesmo INSTANTE DE INICIO estimado (arredondado ao segundo).
A duracao da execucao e `max(duration_ms)` do run (ultimo instante em que foi vista ativa — um
LIMITE INFERIOR da duracao real, ja que ela pode ter terminado entre duas amostras). Execucoes
mais curtas que o intervalo de poll podem nunca ser vistas — limitacao estrutural de amostragem
ja documentada em `QuerySamplesRepository`.

So entram runs que COMECARAM dentro da janela: requests de vida longa iniciadas antes dela (ex.:
agentes de replicacao do SQL Server, cujo `total_elapsed_time` cresce por semanas e e herdado por
cada statement do batch) dominariam o ranking com duracoes que nao pertencem a janela.

`p95_duration_ms` e o `percentile_cont(0.95)` EXATO sobre as duracoes dos runs da janela (barato:
o numero de runs e ordens de grandeza menor que o de amostras).

Os construtores de SQL (`build_*`) sao funcoes puras para teste unitario sem banco.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Literal
from uuid import UUID

import sqlalchemy as sa

from app.repositories.base import BaseRepository

SlowQueryOrder = Literal["avg", "max", "p95", "total"]

_ORDER_COLUMNS: dict[str, str] = {
    "avg": "avg_duration_ms",
    "max": "max_duration_ms",
    "p95": "p95_duration_ms",
    "total": "total_duration_ms",
}

_RUNS_CTE = """
    runs AS (
        SELECT
            sql_hash,
            max(duration_ms)::double precision AS run_ms,
            min(time) AS first_seen
        FROM query_samples
        WHERE instance_id = :instance_id
          AND time >= :since AND time <= :until
          AND duration_ms IS NOT NULL AND duration_ms >= 0
          AND (CAST(:database_name AS text) IS NULL OR database_name = :database_name)
          {extra_filter}
        GROUP BY sql_hash, coalesce(session_id, ''),
                 round(extract(epoch FROM time) - duration_ms / 1000.0)
        HAVING min(time - duration_ms * interval '1 millisecond') >= :since
    )
"""

_LATEST_TEXT_LATERAL = """
    LEFT JOIN LATERAL (
        SELECT q.sql_name, q.sql_text_masked, q.database_name
        FROM query_samples q
        WHERE q.instance_id = :instance_id AND q.sql_hash = {alias}.sql_hash
          AND q.time >= :since AND q.time <= :until
        ORDER BY q.time DESC
        LIMIT 1
    ) latest ON true
"""


def _runs_cte(extra_filter: str = "") -> str:
    return _RUNS_CTE.format(extra_filter=extra_filter)


def build_slow_queries_sql(order_by: SlowQueryOrder = "avg") -> str:
    """Top N por duracao (`order_by` e um Literal fechado — nunca texto do cliente)."""
    order_col = _ORDER_COLUMNS[order_by]
    return f"""
        WITH {_runs_cte()},
        agg AS (
            SELECT
                sql_hash,
                count(*) AS executions,
                avg(run_ms) AS avg_duration_ms,
                max(run_ms) AS max_duration_ms,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY run_ms) AS p95_duration_ms,
                sum(run_ms) AS total_duration_ms,
                max(first_seen) AS last_seen
            FROM runs
            GROUP BY sql_hash
            HAVING count(*) >= :min_executions
            ORDER BY {order_col} DESC
            LIMIT :limit
        )
        SELECT agg.*, latest.sql_name, latest.sql_text_masked, latest.database_name
        FROM agg
        {_LATEST_TEXT_LATERAL.format(alias="agg")}
        ORDER BY agg.{order_col} DESC
    """


def build_daily_series_sql() -> str:
    """Media diaria (UTC) da duracao por execucao, so para os `sql_hashes` pedidos."""
    return f"""
        WITH {_runs_cte("AND sql_hash = ANY(:sql_hashes)")}
        SELECT
            sql_hash,
            (date_trunc('day', first_seen AT TIME ZONE 'UTC'))::date AS day,
            avg(run_ms) AS avg_duration_ms,
            count(*) AS executions
        FROM runs
        GROUP BY sql_hash, day
        ORDER BY sql_hash, day
    """


def build_regressions_sql() -> str:
    """Compara a media por execucao antes/depois de `:split`. `:since`/`:until` delimitam as duas
    janelas juntas (`since` = inicio da janela "antes", `until` = fim da janela "depois")."""
    return f"""
        WITH {_runs_cte()},
        win AS (
            SELECT
                sql_hash,
                count(*) FILTER (WHERE first_seen < :split) AS before_executions,
                avg(run_ms) FILTER (WHERE first_seen < :split) AS before_avg_ms,
                count(*) FILTER (WHERE first_seen >= :split) AS after_executions,
                avg(run_ms) FILTER (WHERE first_seen >= :split) AS after_avg_ms,
                max(run_ms) FILTER (WHERE first_seen >= :split) AS after_max_ms
            FROM runs
            GROUP BY sql_hash
        ),
        cmp AS (
            SELECT
                *,
                (after_avg_ms - before_avg_ms) / before_avg_ms * 100.0 AS change_pct
            FROM win
            WHERE before_executions >= :min_executions
              AND after_executions >= :min_executions
              AND before_avg_ms > 0
        ),
        ranked AS (
            SELECT * FROM cmp
            WHERE change_pct >= :min_change_pct
            ORDER BY change_pct DESC
            LIMIT :limit
        )
        SELECT ranked.*, latest.sql_name, latest.sql_text_masked, latest.database_name
        FROM ranked
        {_LATEST_TEXT_LATERAL.format(alias="ranked")}
        ORDER BY ranked.change_pct DESC
    """


def compute_change_pct(before_avg_ms: float, after_avg_ms: float) -> float | None:
    """Mesma formula da SQL — exposta para testes e para o frontend documentar o calculo."""
    if before_avg_ms <= 0:
        return None
    return (after_avg_ms - before_avg_ms) / before_avg_ms * 100.0


@dataclass(frozen=True, slots=True)
class DailyPoint:
    day: date
    avg_duration_ms: float
    executions: int


class SlowQueriesRepository(BaseRepository):
    async def top_slow(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        *,
        limit: int,
        min_executions: int = 1,
        database_name: str | None = None,
        order_by: SlowQueryOrder = "avg",
    ) -> list[sa.Row[Any]]:
        result = await self._session.execute(
            sa.text(build_slow_queries_sql(order_by)),
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "limit": limit,
                "min_executions": min_executions,
                "database_name": database_name,
            },
        )
        return list(result.fetchall())

    async def daily_series(
        self,
        instance_id: UUID,
        since: datetime,
        until: datetime,
        sql_hashes: list[str],
        *,
        database_name: str | None = None,
    ) -> dict[str, list[DailyPoint]]:
        if not sql_hashes:
            return {}
        result = await self._session.execute(
            sa.text(build_daily_series_sql()),
            {
                "instance_id": instance_id,
                "since": since,
                "until": until,
                "sql_hashes": sql_hashes,
                "database_name": database_name,
            },
        )
        series: dict[str, list[DailyPoint]] = {}
        for row in result.fetchall():
            series.setdefault(row.sql_hash, []).append(
                DailyPoint(
                    day=row.day,
                    avg_duration_ms=float(row.avg_duration_ms or 0.0),
                    executions=int(row.executions),
                )
            )
        return series

    async def regressions(
        self,
        instance_id: UUID,
        *,
        before_start: datetime,
        split: datetime,
        after_end: datetime,
        limit: int,
        min_executions: int,
        min_change_pct: float,
        database_name: str | None = None,
    ) -> list[sa.Row[Any]]:
        result = await self._session.execute(
            sa.text(build_regressions_sql()),
            {
                "instance_id": instance_id,
                "since": before_start,
                "until": after_end,
                "split": split,
                "limit": limit,
                "min_executions": min_executions,
                "min_change_pct": min_change_pct,
                "database_name": database_name,
            },
        )
        return list(result.fetchall())
