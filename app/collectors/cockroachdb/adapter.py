"""Adaptador CockroachDB — delega ao PostgreSQLCollector.

CockroachDB e compativel com o protocolo de wire do PostgreSQL, entao usa asyncpg
internamente via `PostgreSQLCollector`. Este wrapper adiciona apenas a deteccao
da versao do CockroachDB e a capability `COCKROACHDB_ENGINE`.

Detecta versao via `SELECT version()` — retorna string como:
    "CockroachDB CCL v22.2.0 (..."
"""

from __future__ import annotations

import dataclasses

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import COCKROACHDB_ENGINE
from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.pg_backed import PostgreSQLBackedCollector
from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class CockroachDBCollector(PostgreSQLBackedCollector):
    """Implementa `app.collectors.base.Collector` para instancias CockroachDB.

    Delega tudo ao `PostgreSQLCollector` exceto a deteccao da versao
    e a adicao da capability `COCKROACHDB_ENGINE`.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._crdb_version: str | None = None

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Delega ao PG collector e adiciona capability COCKROACHDB_ENGINE."""
        import asyncio

        import asyncpg

        settings = get_settings()
        results = await self._pg.check_capabilities()

        try:
            conn = await asyncpg.connect(
                host=self._connection.host,
                port=self._connection.port,
                user=self._connection.username or "root",
                password=self._connection.secret_value or "",
                timeout=self._connection.connect_timeout_seconds,
            )
            try:
                row = await asyncio.wait_for(
                    conn.fetchrow("SELECT version()"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                if row:
                    self._crdb_version = str(row[0])
                results.append(
                    CapabilityResult(
                        capability=COCKROACHDB_ENGINE,
                        status=CapabilityStatus.FULL,
                    )
                )
            finally:
                await conn.close()
        except Exception as exc:
            logger.debug("Nao foi possivel detectar versao CockroachDB: %s", exc)
            results.append(
                CapabilityResult(
                    capability=COCKROACHDB_ENGINE,
                    status=CapabilityStatus.DEGRADED,
                    reason=str(exc),
                )
            )

        return results

    async def collect(
        self,
        *,
        sleeping_min_duration_ms: int | None = None,
        sleeping_database_overrides_ms: dict[str, int] | None = None,
    ) -> CollectionResult:
        """Delega ao PG collector e sobrescreve engine_version se detectada."""
        result = await self._pg.collect(
            sleeping_min_duration_ms=sleeping_min_duration_ms,
            sleeping_database_overrides_ms=sleeping_database_overrides_ms,
        )
        if self._crdb_version is not None:
            result = dataclasses.replace(result, engine_version=self._crdb_version)
        return result
