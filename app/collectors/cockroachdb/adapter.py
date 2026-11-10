"""Adaptador CockroachDB — delega ao PostgreSQLCollector para coleta de dados.

CockroachDB e compativel com o protocolo de wire do PostgreSQL, entao usa asyncpg
internamente. Este wrapper substitui apenas a deteccao de capacidades por queries
nativas do CRDB, pois CockroachDB nao tem os roles pg_read_all_stats /
pg_read_all_settings / pg_signal_backend do PostgreSQL — esses roles causam
UndefinedObjectError quando consultados via pg_has_role.

Detecta versao via `SELECT version()` — retorna string como:
    "CockroachDB CCL v22.2.0 (..."
"""

from __future__ import annotations

import asyncio
import dataclasses

import asyncpg

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    COCKROACHDB_ENGINE,
    CONNECTIVITY,
    POSTGRESQL_CONFIGURATION_VISIBILITY,
    POSTGRESQL_QUALSTATS,
    POSTGRESQL_SESSION_ACTIVITY,
    POSTGRESQL_SESSION_TERMINATION,
    POSTGRESQL_TOP_SQL,
)
from app.collectors.cockroachdb import maintenance as crdb_maintenance
from app.collectors.postgresql.pg_backed import PostgreSQLBackedCollector
from app.core.config import get_settings
from app.core.logging import get_logger
from app.normalizers.models import (
    NormalizedCompressionReport,
    NormalizedIndexHealthReport,
    NormalizedMemoryGrantReport,
    NormalizedPartitionReport,
    NormalizedScanMeta,
    NormalizedStatisticsHealthReport,
)

logger = get_logger(__name__)


class CockroachDBCollector(PostgreSQLBackedCollector):
    """Implementa `app.collectors.base.Collector` para instancias CockroachDB.

    Delega coleta de dados ao `PostgreSQLCollector`. Substitui `check_capabilities`
    por queries nativas do CRDB (crdb_internal.*) em vez das pg_has_role calls do
    adaptador PostgreSQL, que nao funcionam em CockroachDB.
    """

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self._crdb_version: str | None = None
        # CockroachDB não suporta pg_blocking_pids(), pg_stat_bgwriter/checkpointer nem pg_locks
        self._pg.skip_blocking_queries = True
        self._pg.skip_pg_catalog_metrics = True

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Capability checks nativos para CockroachDB.

        Abre uma conexao direta e usa introspeccao nativa do CRDB:
        - session_activity: SELECT em crdb_internal.node_sessions (VIEWACTIVITY)
        - configuration_visibility: SHOW CLUSTER SETTING (VIEWCLUSTERSETTING)
        - session_termination: pg_cancel_backend (CANCELQUERY privilege no CRDB)
        - top_sql: crdb_internal.node_statement_statistics
        - qualstats: nao disponivel em CockroachDB (DEGRADED)
        - cockroachdb_engine: deteccao de versao via SELECT version()
        """
        settings = get_settings()
        results: list[CapabilityResult] = []

        connect_kwargs: dict = {
            "host": self._connection.host,
            "port": self._connection.port,
            "user": self._connection.username or "root",
            "database": "defaultdb",
            "timeout": self._connection.connect_timeout_seconds,
        }
        if self._connection.auth_method != "integrated":
            connect_kwargs["password"] = self._connection.secret_value or ""

        try:
            conn = await asyncpg.connect(**connect_kwargs)
        except Exception as exc:
            results.append(
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=str(exc),
                )
            )
            return results

        timeout = settings.collection_query_timeout_seconds

        try:
            # session_activity: VIEWACTIVITY privilege
            has_session_activity, session_reason = await _try_query(
                conn,
                "SELECT count(*) FROM crdb_internal.node_sessions",
                timeout,
            )
            results.append(
                CapabilityResult(
                    capability=POSTGRESQL_SESSION_ACTIVITY,
                    status=CapabilityStatus.FULL if has_session_activity else CapabilityStatus.UNAVAILABLE,
                    reason=session_reason,
                )
            )

            # configuration_visibility: VIEWCLUSTERSETTING privilege
            has_config, config_reason = await _try_query(
                conn,
                "SHOW CLUSTER SETTING version",
                timeout,
            )
            results.append(
                CapabilityResult(
                    capability=POSTGRESQL_CONFIGURATION_VISIBILITY,
                    status=CapabilityStatus.FULL if has_config else CapabilityStatus.UNAVAILABLE,
                    reason=config_reason,
                )
            )

            # session_termination: CANCELQUERY privilege
            # CRDB nao tem pg_cancel_backend — usa CANCEL QUERIES com subquery vazia (no-op).
            # Se o usuario nao tiver CANCELQUERY, recebe permission denied; root em insecure tem tudo.
            has_termination, term_reason = await _try_execute(
                conn,
                "CANCEL QUERIES (SELECT query_id FROM crdb_internal.cluster_queries WHERE false)",
                timeout,
            )
            results.append(
                CapabilityResult(
                    capability=POSTGRESQL_SESSION_TERMINATION,
                    status=CapabilityStatus.FULL if has_termination else CapabilityStatus.UNAVAILABLE,
                    reason=term_reason,
                )
            )

            # top_sql: crdb_internal.node_statement_statistics (CRDB equivalent of pg_stat_statements)
            has_top_sql, top_reason = await _try_query(
                conn,
                "SELECT count(*) FROM crdb_internal.node_statement_statistics",
                timeout,
            )
            results.append(
                CapabilityResult(
                    capability=POSTGRESQL_TOP_SQL,
                    status=CapabilityStatus.FULL if has_top_sql else CapabilityStatus.DEGRADED,
                    reason=top_reason,
                )
            )

            # qualstats: pg_qualstats extension nao existe em CockroachDB
            results.append(
                CapabilityResult(
                    capability=POSTGRESQL_QUALSTATS,
                    status=CapabilityStatus.DEGRADED,
                    reason="pg_qualstats nao disponivel em CockroachDB",
                )
            )

            # cockroachdb_engine: deteccao de versao
            try:
                row = await asyncio.wait_for(
                    conn.fetchrow("SELECT version()"),
                    timeout=timeout,
                )
                if row:
                    self._crdb_version = str(row[0])
                results.append(
                    CapabilityResult(
                        capability=COCKROACHDB_ENGINE,
                        status=CapabilityStatus.FULL,
                    )
                )
            except Exception as exc:
                logger.debug("Nao foi possivel detectar versao CockroachDB: %s", exc)
                results.append(
                    CapabilityResult(
                        capability=COCKROACHDB_ENGINE,
                        status=CapabilityStatus.DEGRADED,
                        reason=str(exc),
                    )
                )

        finally:
            await conn.close()

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

    # ── Varreduras de manutencao (catalogos crdb_internal — `cockroachdb/maintenance.py`) ──

    async def list_databases(self) -> list[str]:
        return await crdb_maintenance.list_databases(self._pg._connect_db)

    async def collect_index_health(self, database: str | None = None) -> NormalizedIndexHealthReport:
        return await crdb_maintenance.crdb_index_health(self._pg._connect_db, database)

    async def collect_statistics_health(
        self, database: str | None = None
    ) -> NormalizedStatisticsHealthReport:
        return await crdb_maintenance.crdb_statistics(self._pg._connect_db, database)

    async def collect_memory_grants(self) -> NormalizedMemoryGrantReport:
        return await crdb_maintenance.crdb_memory_grants(self._pg._connect_db)

    async def estimate_compression(
        self, database: str | None, compression: str, max_objects: int
    ) -> NormalizedCompressionReport:
        # Pebble comprime todos os blocos SST (snappy/zstd) por padrao — nao ha escolha por tabela.
        return NormalizedCompressionReport(
            meta=NormalizedScanMeta(supported=False, notes=["compression_inventory_only"])
        )

    async def collect_partitions(self, database: str | None = None) -> NormalizedPartitionReport:
        return await crdb_maintenance.crdb_partitions(self._pg._connect_db, database)


async def _try_query(
    conn: asyncpg.Connection,
    sql: str,
    timeout: float,
) -> tuple[bool, str | None]:
    """Executa uma query (fetchval) e retorna (sucesso, motivo_falha)."""
    try:
        await asyncio.wait_for(conn.fetchval(sql), timeout=timeout)
        return True, None
    except Exception as exc:
        return False, str(exc)


async def _try_execute(
    conn: asyncpg.Connection,
    sql: str,
    timeout: float,
) -> tuple[bool, str | None]:
    """Executa um statement (sem resultado) e retorna (sucesso, motivo_falha)."""
    try:
        await asyncio.wait_for(conn.execute(sql), timeout=timeout)
        return True, None
    except Exception as exc:
        return False, str(exc)
