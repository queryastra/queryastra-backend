"""Repositorio da hypertable `query_samples` — stub.

Mesma observacao de `metric_samples_repository.py`: `query_samples` (`Docs/database-schema.md`
§3) e hypertable, fora da migration baseline deste scaffold. Antes de inserir, todo texto de SQL
deve ja ter passado por `app.masking.sql_masking.mask_sql_literals` e pela checagem de
`sql_exclusions` (`Docs/rules/03-sql-privacy.md`: "Aplicar exclusoes por hash, programa ou
usuario antes de gravar `query_samples`.").
"""

from __future__ import annotations

from app.normalizers.models import NormalizedQuerySample
from app.repositories.base import BaseRepository


class QuerySamplesRepository(BaseRepository):
    """Persistencia de `NormalizedQuerySample` na hypertable `query_samples`."""

    async def insert_batch(self, samples: list[NormalizedQuerySample]) -> None:
        raise NotImplementedError("QuerySamplesRepository.insert_batch ainda nao implementado.")
