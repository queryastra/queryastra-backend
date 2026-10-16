"""Supervisor de historico de plano de execucao.

Roda periodicamente (`plan_history_check_interval_seconds`, default 1 dia) para todas as
instancias ativas. Dois modos de coleta:

  - SQL Server: `collector.scan_plan_history()` — lê o cache de planos das DMVs do próprio banco
    (sys.dm_exec_query_stats + sys.dm_exec_query_plan). Não precisa de contexto externo.

  - PostgreSQL, MySQL/MariaDB, Oracle, Aurora: `collector.explain_queries(queries)` — recebe a
    lista das top-50 queries (por duração média, últimas 24h) buscada da tabela `query_samples`
    do QueryAstra e gera planos via EXPLAIN/V$SQL_PLAN na instância alvo. O sql_hash já vem
    correto (o mesmo hash usado ao persistir as amostras), garantindo a ligação entre o plano e
    os registros de query_samples.

  - DB2, CockroachDB, Aurora PostgreSQL, Redshift: CockroachDB/Aurora PG/Redshift herdam o
    adapter PostgreSQL (plan_format='postgresql_json'); DB2 devolve lista vazia por enquanto.

Cada snapshot só vira linha nova em `query_plan_snapshots` quando `plan_hash` mudou em relação
ao último snapshot conhecido para o mesmo par (instance_id, sql_hash) —
`QueryPlanSnapshotsRepository.insert_if_changed` faz essa deduplicação. Erro em uma instância
nunca derruba outra instância nem o supervisor inteiro.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from uuid import UUID

import sqlalchemy as sa

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.collectors.base import Collector
from app.collectors.connection import ConnectionParams
from app.collectors.factory import create_collector
from app.core.logging import get_logger
from app.repositories.instances_repository import InstancesRepository
from app.repositories.query_plan_snapshots_repository import QueryPlanSnapshotsRepository
from app.security.credential_reference import (
    CredentialReference,
    CredentialResolutionError,
    CredentialResolver,
)

logger = get_logger(__name__)

# DB types que usam explain_queries() em vez de scan_plan_history()
_EXPLAIN_BASED_TYPES = frozenset({
    "postgresql", "mysql", "oracle", "cockroachdb",
    "aurora_postgresql", "aurora_mysql", "redshift",
})

_CollectorFactory = Callable[..., Collector]
_InstancesRepositoryFactory = Callable[[AsyncSession], InstancesRepository]
_QueryPlanSnapshotsRepositoryFactory = Callable[[AsyncSession], QueryPlanSnapshotsRepository]


async def _fetch_top_queries(
    session: AsyncSession, instance_id: UUID, limit: int = 50
) -> list[tuple[str, str]]:
    """Retorna as top queries (sql_hash, sql_text_masked) das últimas 24h para a instância,
    ordenadas por duração média decrescente. Usado para alimentar explain_queries()."""
    since = datetime.now(UTC) - timedelta(hours=24)
    # ROW_NUMBER escolhe a linha mais recente por hash (sql_text_masked atual);
    # AVG é calculado sobre toda a janela do hash antes do LIMIT — resultado já
    # vem ordenado por avg_duration DESC, sem precisar de sort Python posterior.
    stmt = sa.text("""
        SELECT sql_hash, sql_text_masked
        FROM (
            SELECT sql_hash,
                   sql_text_masked,
                   AVG(duration_ms) OVER (PARTITION BY sql_hash) AS avg_duration,
                   ROW_NUMBER() OVER (PARTITION BY sql_hash ORDER BY time DESC) AS rn
            FROM query_samples
            WHERE instance_id = :instance_id
              AND time >= :since
              AND sql_text_masked IS NOT NULL
              AND sql_text_masked != ''
        ) sub
        WHERE rn = 1
        ORDER BY avg_duration DESC
        LIMIT :limit
    """)
    result = await session.execute(
        stmt,
        {"instance_id": instance_id, "since": since, "limit": limit},
    )
    rows = result.fetchall()
    return [(r.sql_hash, r.sql_text_masked) for r in rows]


class PlanHistorySupervisor:
    """Roda `collector.scan_plan_history()` para cada instancia SQL Server ativa, no intervalo
    configurado — worker de baixa frequencia, separado do `PollingSupervisor`."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        check_interval_seconds: int,
        *,
        credential_resolver: CredentialResolver | None = None,
        collector_factory: _CollectorFactory = create_collector,
        instances_repository_factory: _InstancesRepositoryFactory = InstancesRepository,
        query_plan_snapshots_repository_factory: _QueryPlanSnapshotsRepositoryFactory = (
            QueryPlanSnapshotsRepository
        ),
    ) -> None:
        self._session_factory = session_factory
        self._check_interval_seconds = check_interval_seconds
        self._credential_resolver = credential_resolver or CredentialResolver()
        self._create_collector = collector_factory
        self._instances_repository_factory = instances_repository_factory
        self._query_plan_snapshots_repository_factory = query_plan_snapshots_repository_factory
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        logger.info(
            "PlanHistorySupervisor iniciado check_interval=%ss", self._check_interval_seconds
        )
        while not self._stop_event.is_set():
            try:
                await self._run_all_instances()
            except Exception:  # noqa: BLE001 - um ciclo nunca derruba o supervisor
                logger.exception("Erro inesperado em PlanHistorySupervisor._run_all_instances")

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._check_interval_seconds
                )
            except TimeoutError:
                pass
        logger.info("PlanHistorySupervisor finalizado")

    def stop(self) -> None:
        self._stop_event.set()

    async def _run_all_instances(self) -> None:
        async with self._session_factory() as session:
            instances = await self._instances_repository_factory(session).list_active()

        for instance in instances:
            await self._run_one_instance(instance.id)

    async def _run_one_instance(self, instance_id: UUID) -> None:
        try:
            async with self._session_factory() as session:
                instances_repository = self._instances_repository_factory(session)
                instance = await instances_repository.get_by_id(instance_id)

                if instance is None or not instance.active:
                    logger.info(
                        "Instancia instance_id=%s inativa/removida - varredura de historico de "
                        "plano pulada",
                        instance_id,
                    )
                    return

                if instance.db_type not in ("sqlserver",) | _EXPLAIN_BASED_TYPES:
                    # db2 e outros sem suporte ainda
                    return

                username: str | None = None
                secret_value: str | None = None
                if instance.auth_method == "encrypted_fallback":
                    if instance.encrypted_credential_ref is None:
                        logger.error(
                            "encrypted_credential_ref ausente para instance_id=%s "
                            "(auth_method=encrypted_fallback) - varredura pulada",
                            instance_id,
                        )
                        return
                    try:
                        resolved = await self._credential_resolver.resolve(
                            CredentialReference(instance.encrypted_credential_ref)
                        )
                    except CredentialResolutionError:
                        logger.exception(
                            "Falha ao resolver credencial de instance_id=%s - varredura pulada",
                            instance_id,
                        )
                        return
                    username, secret_value = resolved.username, resolved.secret_value

                connection = ConnectionParams(
                    host=instance.host,
                    port=instance.port,
                    auth_method=instance.auth_method,
                    username=username,
                    secret_value=secret_value,
                )
                collector = self._create_collector(
                    instance.db_type, connection=connection, instance_id=instance.id
                )
                try:
                    if instance.db_type == "sqlserver":
                        snapshots = await collector.scan_plan_history()
                    else:
                        # Para engines sem cache de plano exposto, buscamos as top queries
                        # recentes de query_samples e geramos planos via EXPLAIN/V$SQL_PLAN.
                        top_queries = await _fetch_top_queries(session, instance.id)
                        snapshots = await collector.explain_queries(top_queries)

                    new_rows = []
                    if snapshots:
                        repository = self._query_plan_snapshots_repository_factory(session)
                        new_rows = await repository.insert_if_changed(snapshots)
                    logger.info(
                        "Varredura de historico de plano concluida instance_id=%s db_type=%s "
                        "scanned=%s changed=%s",
                        instance_id,
                        instance.db_type,
                        len(snapshots),
                        len(new_rows),
                    )
                finally:
                    await collector.close()
        except Exception:  # noqa: BLE001 - varredura de 1 instancia nunca derruba o supervisor
            logger.exception(
                "Erro inesperado na varredura de historico de plano de instance_id=%s",
                instance_id,
            )
