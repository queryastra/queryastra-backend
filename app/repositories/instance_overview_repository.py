"""Consultas em LOTE do dashboard de visao geral (`GET /overview/instances`).

Cada metodo recebe a lista inteira de instancias e responde com UMA query (sem N+1 por instancia
ou por metrica):

- `latest_metric_values` — ultimo valor de cada (instancia, metrica) numa janela recente
  (`DISTINCT ON`), cobre telemetria de ciclo, conexoes, sessoes ativas e CPU.
- `deadlock_event_counts` — `deadlock_events` (SQL Server) desde `since`.
- `counter_deltas` — soma dos deltas POSITIVOS de contadores cumulativos (PostgreSQL etc.);
  delta negativo = reset, ignorado.
- `open_alerts_by_severity` — alertas nao fechados por (instancia, severidade).
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timedelta
from uuid import UUID

import sqlalchemy as sa

from app.repositories.base import BaseRepository

LATEST_VALUES_SQL = """
    SELECT DISTINCT ON (target_id, metric_name)
        target_id, metric_name, value, time
    FROM metric_samples
    WHERE target_type = 'instance'
      AND target_id = ANY(:instance_ids)
      AND metric_name = ANY(:metric_names)
      AND time >= :since
    ORDER BY target_id, metric_name, time DESC
"""

DEADLOCK_EVENT_COUNTS_SQL = """
    SELECT instance_id, count(*) AS deadlocks
    FROM deadlock_events
    WHERE instance_id = ANY(:instance_ids) AND time >= :since
    GROUP BY instance_id
"""

COUNTER_DELTAS_SQL = """
    SELECT target_id, sum(delta)::bigint AS total
    FROM (
        SELECT
            target_id,
            time,
            value - LAG(value) OVER (PARTITION BY target_id, metric_name ORDER BY time) AS delta
        FROM metric_samples
        WHERE target_type = 'instance'
          AND target_id = ANY(:instance_ids)
          AND metric_name = ANY(:metric_names)
          AND time >= :seed_since
    ) d
    WHERE time >= :since AND delta > 0
    GROUP BY target_id
"""

OPEN_ALERTS_SQL = """
    SELECT target_id, severity, count(*) AS open_count
    FROM alerts
    WHERE target_type = 'instance'
      AND target_id = ANY(:instance_ids)
      AND state <> 'closed'
    GROUP BY target_id, severity
"""

COUNTER_LAG_SEED = timedelta(hours=1)


class InstanceOverviewRepository(BaseRepository):
    async def latest_metric_values(
        self, instance_ids: Sequence[UUID], metric_names: Sequence[str], since: datetime
    ) -> dict[UUID, dict[str, tuple[float, datetime]]]:
        if not instance_ids or not metric_names:
            return {}
        result = await self._session.execute(
            sa.text(LATEST_VALUES_SQL),
            {
                "instance_ids": list(instance_ids),
                "metric_names": list(metric_names),
                "since": since,
            },
        )
        out: dict[UUID, dict[str, tuple[float, datetime]]] = {}
        for row in result.fetchall():
            out.setdefault(row.target_id, {})[row.metric_name] = (float(row.value), row.time)
        return out

    async def deadlock_event_counts(
        self, instance_ids: Sequence[UUID], since: datetime
    ) -> dict[UUID, int]:
        if not instance_ids:
            return {}
        result = await self._session.execute(
            sa.text(DEADLOCK_EVENT_COUNTS_SQL),
            {"instance_ids": list(instance_ids), "since": since},
        )
        return {row.instance_id: int(row.deadlocks) for row in result.fetchall()}

    async def counter_deltas(
        self, instance_ids: Sequence[UUID], metric_names: Sequence[str], since: datetime
    ) -> dict[UUID, int]:
        if not instance_ids or not metric_names:
            return {}
        result = await self._session.execute(
            sa.text(COUNTER_DELTAS_SQL),
            {
                "instance_ids": list(instance_ids),
                "metric_names": list(metric_names),
                "since": since,
                "seed_since": since - COUNTER_LAG_SEED,
            },
        )
        return {row.target_id: int(row.total or 0) for row in result.fetchall()}

    async def open_alerts_by_severity(
        self, instance_ids: Sequence[UUID]
    ) -> dict[UUID, dict[str, int]]:
        if not instance_ids:
            return {}
        result = await self._session.execute(
            sa.text(OPEN_ALERTS_SQL), {"instance_ids": list(instance_ids)}
        )
        out: dict[UUID, dict[str, int]] = {}
        for row in result.fetchall():
            out.setdefault(row.target_id, {})[str(row.severity)] = int(row.open_count)
        return out
