"""Repositorio da hypertable `metric_samples`.

`metric_samples` (`Docs/database-schema.md` §2) nao tem model ORM dedicado — hypertables e
continuous aggregates exigem TimescaleDB de verdade (`create_hypertable`,
`timescaledb.continuous`), criadas via `migrations/versions/66ae4664300c_hypertables.py`. Este
repositorio escreve via SQLAlchemy Core (`app/repositories/_tables.py`), apontando para a tabela
real ja migrada.
"""

from __future__ import annotations

import sqlalchemy as sa

from app.normalizers.models import NormalizedMetricSample
from app.repositories._tables import metric_samples_table
from app.repositories.base import BaseRepository


class MetricSamplesRepository(BaseRepository):
    """Persistencia de `NormalizedMetricSample` na hypertable `metric_samples`."""

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
