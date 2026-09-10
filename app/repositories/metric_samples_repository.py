"""Repositorio da hypertable `metric_samples` — stub.

`metric_samples` (`Docs/database-schema.md` §2) nao tem model ORM dedicado neste scaffold:
hypertables/continuous aggregates exigem TimescaleDB de verdade (`create_hypertable`,
`timescaledb.continuous`) e ficam fora da migration baseline (ver `migrations/versions/`) — este
repositorio deve escrever via SQL Core/text() quando implementado, apontando para a tabela criada
manualmente (ou por migration futura dedicada) com as extensoes do TimescaleDB habilitadas.
"""

from __future__ import annotations

from app.normalizers.models import NormalizedMetricSample
from app.repositories.base import BaseRepository


class MetricSamplesRepository(BaseRepository):
    """Persistencia de `NormalizedMetricSample` na hypertable `metric_samples`."""

    async def insert_batch(self, samples: list[NormalizedMetricSample]) -> None:
        raise NotImplementedError("MetricSamplesRepository.insert_batch ainda nao implementado.")
