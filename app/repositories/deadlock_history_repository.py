"""Historico agregado de deadlocks (heatmap dia-da-semana x hora + serie diaria).

Duas fontes, conforme o engine:

- **SQL Server** — `deadlock_events` (um registro por deadlock, inclusive os `pending` da deteccao
  instantanea, que tambem sao deadlocks reais).
- **PostgreSQL** (e Oracle/DB2) — so existe o contador CUMULATIVO (`postgresql_deadlocks_total`
  etc.) em `metric_samples`. Contamos o DELTA positivo entre amostras consecutivas
  (`value - LAG(value)`), atribuido ao instante da amostra. Delta negativo = reset do contador
  (restart do servico / `pg_stat_reset`) e e IGNORADO (nao vira "deadlocks negativos" nem conta
  o valor absoluto pos-reset). A janela interna comeca `_LAG_SEED` antes de `since` para que a
  primeira amostra da janela tenha um anterior com que comparar.

Fuso horario: o frontend envia o fuso do navegador (`tz`, IANA) e o agrupamento e feito no banco
com `AT TIME ZONE :tz` — o mesmo efeito visual do `WaitHeatmap` (que recebe buckets UTC e formata
no fuso local do navegador), mas aqui o agrupamento por dia/hora precisa acontecer ANTES da soma.
`weekday` segue ISO-8601: 1 = segunda ... 7 = domingo.
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Literal
from uuid import UUID

import sqlalchemy as sa

from app.normalizers.metric_names import (
    DB2_DEADLOCKS_TOTAL,
    ORACLE_DEADLOCKS_TOTAL,
    PG_DEADLOCKS,
)
from app.repositories.base import BaseRepository

DeadlockSource = Literal["events", "counter", "none"]

_LAG_SEED = timedelta(hours=1)

COUNTER_METRIC_BY_DB_TYPE: dict[str, str] = {
    "postgresql": PG_DEADLOCKS,
    "redshift": PG_DEADLOCKS,
    "oracle": ORACLE_DEADLOCKS_TOTAL,
    "db2": DB2_DEADLOCKS_TOTAL,
}

_EVENTS_SOURCE = """
    SELECT time AS t, 1::bigint AS n
    FROM deadlock_events
    WHERE instance_id = :instance_id AND time >= :since AND time <= :until
"""

_COUNTER_SOURCE = """
    SELECT t, n FROM (
        SELECT
            time AS t,
            (value - LAG(value) OVER (ORDER BY time))::bigint AS n
        FROM metric_samples
        WHERE target_type = 'instance'
          AND target_id = :instance_id
          AND metric_name = :metric_name
          AND time >= :seed_since AND time <= :until
    ) d
    WHERE t >= :since AND n > 0
"""


def resolve_source(db_type: str) -> DeadlockSource:
    if db_type == "sqlserver":
        return "events"
    if db_type in COUNTER_METRIC_BY_DB_TYPE:
        return "counter"
    return "none"


def _source_sql(source: DeadlockSource) -> str:
    if source == "events":
        return _EVENTS_SOURCE
    if source == "counter":
        return _COUNTER_SOURCE
    raise ValueError(f"no deadlock source for {source!r}")


def build_heatmap_sql(source: DeadlockSource) -> str:
    return f"""
        WITH ev AS ({_source_sql(source)})
        SELECT
            extract(isodow FROM t AT TIME ZONE :tz)::int AS weekday,
            extract(hour FROM t AT TIME ZONE :tz)::int AS hour,
            sum(n)::bigint AS total
        FROM ev
        GROUP BY 1, 2
        ORDER BY 1, 2
    """


def build_daily_sql(source: DeadlockSource) -> str:
    return f"""
        WITH ev AS ({_source_sql(source)})
        SELECT (t AT TIME ZONE :tz)::date AS day, sum(n)::bigint AS total
        FROM ev
        GROUP BY 1
        ORDER BY 1
    """


def build_params(
    *,
    instance_id: UUID,
    since: datetime,
    until: datetime,
    tz: str,
    db_type: str,
) -> dict[str, object]:
    params: dict[str, object] = {
        "instance_id": instance_id,
        "since": since,
        "until": until,
        "tz": tz,
    }
    metric = COUNTER_METRIC_BY_DB_TYPE.get(db_type)
    if db_type != "sqlserver" and metric is not None:
        params["metric_name"] = metric
        params["seed_since"] = since - _LAG_SEED
    return params


class DeadlockHistoryRepository(BaseRepository):
    async def heatmap(
        self, *, instance_id: UUID, db_type: str, since: datetime, until: datetime, tz: str
    ) -> list[tuple[int, int, int]]:
        source = resolve_source(db_type)
        if source == "none":
            return []
        params = build_params(
            instance_id=instance_id, since=since, until=until, tz=tz, db_type=db_type
        )
        result = await self._session.execute(sa.text(build_heatmap_sql(source)), params)
        return [(int(r.weekday), int(r.hour), int(r.total)) for r in result.fetchall()]

    async def daily(
        self, *, instance_id: UUID, db_type: str, since: datetime, until: datetime, tz: str
    ) -> list[tuple[date, int]]:
        source = resolve_source(db_type)
        if source == "none":
            return []
        params = build_params(
            instance_id=instance_id, since=since, until=until, tz=tz, db_type=db_type
        )
        result = await self._session.execute(sa.text(build_daily_sql(source)), params)
        return [(r.day, int(r.total)) for r in result.fetchall()]
