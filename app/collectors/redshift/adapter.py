"""Adaptador Amazon Redshift.

Redshift é compatível com protocolo PostgreSQL — delega tudo ao PostgreSQLCollector,
exatamente como AuroraPostgreSQLCollector faz. A diferença: testa acesso à SVV_SESSIONS
(view Redshift-específica) para confirmar que a instância é Redshift e não PostgreSQL vanilla.

Porta padrão Redshift: 5439. Database padrão: "dev".
"""

from __future__ import annotations

from app.collectors.base import CapabilityResult, CapabilityStatus
from app.collectors.capabilities import REDSHIFT_SESSION_ACTIVITY
from app.collectors.postgresql.pg_backed import PostgreSQLBackedCollector
from app.collectors.redshift import maintenance as rs_maintenance
from app.core.config import get_settings
from app.core.logging import get_logger
from app.normalizers.models import (
    NormalizedCompressionReport,
    NormalizedIndexHealthReport,
    NormalizedMemoryGrantReport,
    NormalizedPartitionReport,
    NormalizedPlanSnapshot,
    NormalizedScanMeta,
    NormalizedStatisticsHealthReport,
)

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

    async def explain_queries(
        self, queries: list[tuple[str, str]]
    ) -> list[NormalizedPlanSnapshot]:
        # Redshift não suporta EXPLAIN FORMAT JSON — retorna lista vazia sem tentar conectar.
        return []

    # ── Varreduras de manutencao (`redshift/maintenance.py`) ────────────────────────────────

    async def list_databases(self) -> list[str]:
        return await rs_maintenance.list_fn(self._pg._connect_db)()

    async def collect_index_health(self, database: str | None = None) -> NormalizedIndexHealthReport:
        # Sem indices no Redshift (sort keys / dist keys).
        return NormalizedIndexHealthReport(
            meta=NormalizedScanMeta(supported=False, notes=["no_indexes_concept"])
        )

    async def collect_statistics_health(
        self, database: str | None = None
    ) -> NormalizedStatisticsHealthReport:
        return await rs_maintenance.redshift_statistics(self._pg._connect_db, database)

    async def collect_memory_grants(self) -> NormalizedMemoryGrantReport:
        return await rs_maintenance.redshift_memory_grants(self._pg._connect_db)

    async def estimate_compression(
        self, database: str | None, compression: str, max_objects: int
    ) -> NormalizedCompressionReport:
        return await rs_maintenance.redshift_compression(
            self._pg._connect_db, database, max_objects
        )

    async def collect_partitions(self, database: str | None = None) -> NormalizedPartitionReport:
        return NormalizedPartitionReport(meta=NormalizedScanMeta(supported=False))
