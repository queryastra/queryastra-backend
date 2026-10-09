"""Adaptador Aurora PostgreSQL — estende o PostgreSQLCollector.

Aurora PostgreSQL e compativel com PostgreSQL padrao, entao delega tudo ao
`PostgreSQLCollector`. A diferenca e a deteccao da capability `AURORA_POSTGRESQL_ENGINE`
via `SELECT aurora_version()`.

Se `aurora_version()` falhar (instancia PostgreSQL vanilla), a capability e DEGRADED
— nao reprova o cadastro.
"""

from __future__ import annotations

from uuid import UUID

from app.collectors.base import CapabilityResult, CapabilityStatus
from app.collectors.capabilities import AURORA_POSTGRESQL_ENGINE
from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.pg_backed import PostgreSQLBackedCollector
from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class AuroraPostgreSQLCollector(PostgreSQLBackedCollector):
    """Implementa `app.collectors.base.Collector` para instancias Aurora PostgreSQL.

    Delega tudo ao `PostgreSQLCollector` e adiciona deteccao de `aurora_version()`.
    """

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Delega ao PG collector e adiciona capability AURORA_POSTGRESQL_ENGINE."""
        import asyncio

        import asyncpg

        settings = get_settings()
        results = await self._pg.check_capabilities()

        try:
            conn = await asyncpg.connect(
                host=self._connection.host,
                port=self._connection.port,
                user=self._connection.username or "postgres",
                password=self._connection.secret_value or "",
                timeout=self._connection.connect_timeout_seconds,
            )
            try:
                row = await asyncio.wait_for(
                    conn.fetchrow("SELECT aurora_version()"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                aurora_ver = str(row[0]) if row else None
                results.append(
                    CapabilityResult(
                        capability=AURORA_POSTGRESQL_ENGINE,
                        status=CapabilityStatus.FULL,
                        reason=aurora_ver,
                    )
                )
            finally:
                await conn.close()
        except Exception as exc:
            logger.debug("aurora_version() nao disponivel (talvez PG vanilla): %s", exc)
            results.append(
                CapabilityResult(
                    capability=AURORA_POSTGRESQL_ENGINE,
                    status=CapabilityStatus.DEGRADED,
                    reason=str(exc),
                )
            )

        return results
