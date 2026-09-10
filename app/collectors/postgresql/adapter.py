"""Adaptador PostgreSQL — stub. Fonte de verdade: `Docs/collector-queries.md`.

Usa `asyncpg` (Docs/database-schema.md, ARCHITECTURE_SPEC.md §4) como driver assincrono. Nenhuma
query real e executada neste scaffold — cada metodo levanta `NotImplementedError` documentando
exatamente qual view/DMV do PostgreSQL deve ser usada quando a logica for implementada.

Views usadas como referencia (nao implementadas ainda):
    - `pg_stat_activity` — sessao/query ativa (collector-queries.md §1).
    - `pg_blocking_pids(pid)` — deteccao de bloqueio (collector-queries.md §2).
    - `pg_stat_database`, `pg_stat_bgwriter` (< PG17) / `pg_stat_checkpointer` (>= PG17),
      `pg_stat_replication`, `pg_stat_archiver` — metricas de recurso/instancia
      (collector-queries.md §5).
    - `pg_stat_statements` (extensao opcional, requer restart) — Top SQL agregado
      (collector-queries.md §5, "Resumo").
    - Detalhe de deadlock: **nao disponivel via SQL** neste motor — so
      `pg_stat_database.deadlocks` (contador agregado); detalhe fica em log do servidor
      (collector-queries.md §4) — decisao de escopo pendente antes da Fase 2.

Permissao minima esperada: role `pg_read_all_stats`/`pg_read_all_settings` (ou superuser) —
collector-queries.md §7.
"""

from __future__ import annotations

from datetime import UTC, datetime
from uuid import UUID

from app.collectors.base import CapabilityResult, CollectionResult
from app.core.logging import get_logger

logger = get_logger(__name__)


class PostgreSQLCollector:
    """Implementa `app.collectors.base.Collector` para instancias PostgreSQL.

    Stub: mantem so a assinatura e a intencao de cada metodo. `dsn`/pool de conexao real ainda
    nao sao abertos aqui — a resolucao de credencial deve passar por
    `app.security.credential_reference.CredentialResolver`, nunca por senha em texto puro.
    """

    def __init__(self, instance_id: UUID) -> None:
        self._instance_id = instance_id
        self._pool: object | None = None  # asyncpg.Pool quando implementado

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Deveria detectar `server_version_num` e checar extensoes/roles disponiveis.

        Referencia: `SELECT current_setting('server_version_num')` para o branching de versao
        (`pg_stat_bgwriter` vs. `pg_stat_checkpointer`, PG17+ — collector-queries.md §5/§9) e
        checagem de `pg_stat_statements` via `pg_extension` para degradar "Top SQL" quando
        ausente (ARCHITECTURE_SPEC.md, decisao pendente #4).
        """
        logger.info(
            "check_capabilities ainda nao implementado para instancia postgresql (instance_id=%s)",
            self._instance_id,
        )
        raise NotImplementedError(
            "PostgreSQLCollector.check_capabilities: implementar deteccao de versao via "
            "current_setting('server_version_num') e checagem de pg_stat_statements via "
            "pg_extension, conforme Docs/collector-queries.md §5, §7, §9."
        )

    async def collect(self) -> CollectionResult:
        """Deveria consultar `pg_stat_activity`/`pg_blocking_pids`/`pg_stat_*` e normalizar.

        Ver `Docs/collector-queries.md` §1 (sessao ativa), §2 (bloqueio), §5 (metricas de
        recurso) para as queries exatas por versao.
        """
        logger.info(
            "collect ainda nao implementado para instancia postgresql (instance_id=%s)",
            self._instance_id,
        )
        raise NotImplementedError(
            "PostgreSQLCollector.collect: implementar consulta a pg_stat_activity, "
            "pg_blocking_pids(pid) e pg_stat_database/pg_stat_bgwriter|pg_stat_checkpointer, "
            "conforme Docs/collector-queries.md §1, §2, §5."
        )

    async def close(self) -> None:
        """Deveria fechar o pool `asyncpg`, se aberto."""
        logger.info("close chamado para instancia postgresql (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
