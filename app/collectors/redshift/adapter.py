"""Adaptador Amazon Redshift.

Redshift é compatível com protocolo PostgreSQL — delega tudo ao PostgreSQLCollector,
exatamente como AuroraPostgreSQLCollector faz. A diferença: testa acesso à SVV_SESSIONS
(view Redshift-específica) para confirmar que a instância é Redshift e não PostgreSQL vanilla.

Porta padrão Redshift: 5439. Database padrão: "dev".
"""

from __future__ import annotations

from uuid import UUID

from app.collectors.base import CapabilityResult, CapabilityStatus
from app.collectors.capabilities import REDSHIFT_SESSION_ACTIVITY
from app.collectors.connection import ConnectionParams
from app.collectors.postgresql.pg_backed import PostgreSQLBackedCollector
from app.core.config import get_settings
from app.core.logging import get_logger

logger = get_logger(__name__)


class RedshiftCollector(PostgreSQLBackedCollector):
    """Implementa `app.collectors.base.Collector` para instâncias Amazon Redshift.

    Delega tudo ao PostgreSQLCollector (Redshift é wire-compatible com PostgreSQL)
    e adiciona detecção de REDSHIFT_SESSION_ACTIVITY via SVV_SESSIONS.
    """

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Delega ao PG collector e adiciona capability REDSHIFT_SESSION_ACTIVITY."""
        import asyncio

        import asyncpg

        settings = get_settings()
        results = await self._pg.check_capabilities()

        try:
            conn = await asyncpg.connect(
                host=self._connection.host,
                port=self._connection.port,
                user=self._connection.username or "awsuser",
                password=self._connection.secret_value or "",
                database="dev",
                timeout=self._connection.connect_timeout_seconds,
            )
            try:
                await asyncio.wait_for(
                    conn.fetch("SELECT 1 FROM SVV_SESSIONS LIMIT 1"),
                    timeout=settings.collection_query_timeout_seconds,
                )
                results.append(
                    CapabilityResult(
                        capability=REDSHIFT_SESSION_ACTIVITY,
                        status=CapabilityStatus.FULL,
                    )
                )
            finally:
                await conn.close()
        except Exception as exc:
            logger.debug(
                "SVV_SESSIONS não disponível (possivelmente PostgreSQL vanilla): %s", exc
            )
            results.append(
                CapabilityResult(
                    capability=REDSHIFT_SESSION_ACTIVITY,
                    status=CapabilityStatus.DEGRADED,
                    reason=str(exc),
                )
            )

        return results
