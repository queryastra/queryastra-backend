"""Repositorio da hypertable `metric_samples`.

`metric_samples` (`Docs/database-schema.md` §2) nao tem model ORM dedicado — hypertables e
continuous aggregates exigem TimescaleDB de verdade (`create_hypertable`,
`timescaledb.continuous`), criadas via `migrations/versions/66ae4664300c_hypertables.py`. Este
repositorio escreve via SQLAlchemy Core (`app/repositories/_tables.py`), apontando para a tabela
real ja migrada.
"""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID

import sqlalchemy as sa

from app.normalizers.models import NormalizedMetricSample
from app.repositories._tables import metric_samples_table
from app.repositories.base import BaseRepository


class MetricSamplesRepository(BaseRepository):
    """Persistencia e leitura de `NormalizedMetricSample` na hypertable `metric_samples`.

    Leitura direta da hypertable bruta — os continuous aggregates
    (`metric_samples_hourly`/`daily`) foram criados `WITH NO DATA` e sem policy de refresh, estao
    vazios e nunca atualizados. Nenhum metodo de leitura aqui consulta os aggregates.
    """

    async def insert_batch(self, samples: list[NormalizedMetricSample]) -> None:
        if not samples:
            return
        rows = [
            {
                "time": s.time,
                "target_type": s.target_type,
                "target_id": s.target_id,
                "metric_name": s.metric_name,
                "labels": s.labels,
                "value": s.value,
            }
            for s in samples
        ]
        await self._session.execute(sa.insert(metric_samples_table), rows)
        await self._session.commit()

    async def list_series(
        self,
        target_type: str,
        target_id: UUID,
        metric_names: Sequence[str] | None,
        since: datetime,
        until: datetime,
        limit: int,
    ) -> list[sa.Row]:
        """Linhas brutas ordenadas por (metric_name, time) para agrupar em serie por
        metric_name na camada de API. `metric_names` None/vazio devolve todas as metricas do
        alvo no periodo."""
        stmt = (
            sa.select(metric_samples_table)
            .where(
                metric_samples_table.c.target_type == target_type,
                metric_samples_table.c.target_id == target_id,
                metric_samples_table.c.time >= since,
                metric_samples_table.c.time <= until,
            )
            .order_by(metric_samples_table.c.metric_name, metric_samples_table.c.time)
            .limit(limit)
        )
        if metric_names:
            stmt = stmt.where(metric_samples_table.c.metric_name.in_(metric_names))
        return list((await self._session.execute(stmt)).fetchall())

    async def get_latest_value(
        self, target_type: str, target_id: UUID, metric_name: str
    ) -> sa.Row | None:
        """Ultima amostra de uma metrica pontual (telemetria de ciclo do collector)."""
        stmt = (
            sa.select(metric_samples_table.c.time, metric_samples_table.c.value)
            .where(
                metric_samples_table.c.target_type == target_type,
                metric_samples_table.c.target_id == target_id,
                metric_samples_table.c.metric_name == metric_name,
            )
            .order_by(metric_samples_table.c.time.desc())
            .limit(1)
        )
        return (await self._session.execute(stmt)).first()

    async def count_distinct_metrics_since(
        self,
        target_type: str,
        target_id: UUID,
        since: datetime,
        exclude_metric_names: Sequence[str],
    ) -> int:
        """Metricas distintas (fora da telemetria de ciclo) desde `since` — criterio simples de
        collection_coverage: 0 = 'unavailable', >0 = 'full'."""
        stmt = sa.select(sa.func.count(sa.distinct(metric_samples_table.c.metric_name))).where(
            metric_samples_table.c.target_type == target_type,
            metric_samples_table.c.target_id == target_id,
            metric_samples_table.c.time >= since,
            metric_samples_table.c.metric_name.notin_(exclude_metric_names),
        )
        return (await self._session.execute(stmt)).scalar_one()
