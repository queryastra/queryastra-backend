"""Adaptador PostgreSQL. Fonte de verdade: `Docs/collector-queries.md`.

Usa `asyncpg` (Docs/database-schema.md, ARCHITECTURE_SPEC.md §4) como driver assincrono.

`check_capabilities()` abre uma conexao curta (timeout configuravel) e checa, via
`pg_has_role`, se o usuario de coleta tem as roles minimas confirmadas em
`Docs/collector-queries.md` §7 (`pg_read_all_stats`, `pg_read_all_settings`,
`pg_signal_backend`) — mandatorias — e a disponibilidade opcional de `pg_stat_statements`
("Top SQL"), que so degrada, nunca reprova o cadastro sozinha.

Views usadas como referencia para `collect()` (ainda fora de escopo desta feature):
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

Permissao minima esperada: role `pg_read_all_stats`/`pg_read_all_settings`/`pg_signal_backend`
(ou superuser) — collector-queries.md §7.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import asyncpg

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    CONNECTIVITY,
    POSTGRESQL_CONFIGURATION_VISIBILITY,
    POSTGRESQL_SESSION_ACTIVITY,
    POSTGRESQL_SESSION_TERMINATION,
    POSTGRESQL_TOP_SQL,
)
from app.collectors.connection import ConnectionParams
from app.core.logging import get_logger

logger = get_logger(__name__)

# `track_activity_query_size` minimo para nao truncar query capturada demais cedo
# (default do Postgres e 1024 — Docs/collector-queries.md §7).
_MIN_TRACK_ACTIVITY_QUERY_SIZE = 4096


class PostgreSQLCollector:
    """Implementa `app.collectors.base.Collector` para instancias PostgreSQL."""

    def __init__(self, connection: ConnectionParams, instance_id: UUID | None = None) -> None:
        self._connection_params = connection
        self._instance_id = instance_id

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Conecta e checa roles (`pg_has_role`) e extensao `pg_stat_statements`.

        Conexao com timeout (`Docs/rules/02-safe-collection.md`); qualquer erro de
        conectividade vira uma unica `CapabilityResult(CONNECTIVITY, UNAVAILABLE, ...)` — nunca
        a mensagem bruta da excecao, so o nome da classe (pode conter host/senha no texto
        default do driver).
        """
        try:
            conn = await asyncio.wait_for(
                self._connect(), timeout=self._connection_params.connect_timeout_seconds
            )
        except (OSError, asyncpg.PostgresError, TimeoutError) as exc:
            return [
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=f"Falha ao conectar: {type(exc).__name__}",
                )
            ]

        try:
            return await self._check_roles_and_extensions(conn)
        finally:
            await conn.close()

    async def _connect(self) -> asyncpg.Connection:
        params = self._connection_params
        connect_kwargs: dict[str, object] = {
            "host": params.host,
            "port": params.port,
            "user": params.username,
            "database": "postgres",
            "timeout": params.connect_timeout_seconds,
        }
        # `auth_method == "integrated"`: nao passamos `password` — a conexao depende de
        # confianca do ambiente (ex.: `pg_hba.conf` com `trust`/peer, ou certificado de cliente
        # ja provisionado fora deste adaptador). Caminho ainda nao totalmente resolvido no MVP
        # (nao ha integracao Kerberos/GSSAPI aqui) mas nao deve quebrar: o `asyncpg.connect`
        # segue sem senha, e o servidor decide se aceita.
        if params.auth_method != "integrated":
            connect_kwargs["password"] = params.secret_value
        return await asyncpg.connect(**connect_kwargs)

    async def _check_roles_and_extensions(self, conn: asyncpg.Connection) -> list[CapabilityResult]:
        results: list[CapabilityResult] = []

        has_session_activity = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_read_all_stats', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_SESSION_ACTIVITY,
                status=CapabilityStatus.FULL
                if has_session_activity
                else CapabilityStatus.UNAVAILABLE,
                reason=None if has_session_activity else "role pg_read_all_stats ausente",
            )
        )

        has_configuration_visibility = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_read_all_settings', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_CONFIGURATION_VISIBILITY,
                status=(
                    CapabilityStatus.FULL
                    if has_configuration_visibility
                    else CapabilityStatus.UNAVAILABLE
                ),
                reason=None
                if has_configuration_visibility
                else "role pg_read_all_settings ausente",
            )
        )

        has_session_termination = await conn.fetchval(
            "SELECT pg_has_role(current_user, 'pg_signal_backend', 'member')"
        )
        results.append(
            CapabilityResult(
                capability=POSTGRESQL_SESSION_TERMINATION,
                status=(
                    CapabilityStatus.FULL
                    if has_session_termination
                    else CapabilityStatus.UNAVAILABLE
                ),
                reason=None if has_session_termination else "role pg_signal_backend ausente",
            )
        )

        results.append(await self._check_top_sql(conn))
        return results

    async def _check_top_sql(self, conn: asyncpg.Connection) -> CapabilityResult:
        has_extension = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM pg_extension WHERE extname='pg_stat_statements')"
        )
        # `current_setting()` devolve o valor "bonito" com unidade (ex.: "4kB") para GUCs com
        # unidade de exibicao — `pg_settings.setting` sempre devolve o numero cru (bytes),
        # seguro para castar direto em inteiro.
        query_size_ok = await conn.fetchval(
            "SELECT setting::int >= $1 FROM pg_settings WHERE name = 'track_activity_query_size'",
            _MIN_TRACK_ACTIVITY_QUERY_SIZE,
        )

        if has_extension and query_size_ok:
            return CapabilityResult(capability=POSTGRESQL_TOP_SQL, status=CapabilityStatus.FULL)

        missing = []
        if not has_extension:
            missing.append("extensao pg_stat_statements nao instalada")
        if not query_size_ok:
            missing.append(f"track_activity_query_size menor que {_MIN_TRACK_ACTIVITY_QUERY_SIZE}")
        return CapabilityResult(
            capability=POSTGRESQL_TOP_SQL,
            status=CapabilityStatus.DEGRADED,
            reason="; ".join(missing),
        )

    async def collect(self) -> CollectionResult:
        """Deveria consultar `pg_stat_activity`/`pg_blocking_pids`/`pg_stat_*` e normalizar.

        Ver `Docs/collector-queries.md` §1 (sessao ativa), §2 (bloqueio), §5 (metricas de
        recurso) para as queries exatas por versao. Fora de escopo desta feature (cadastro).
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
        """Sem estado persistente entre chamadas (cada `check_capabilities` fecha sua conexao)."""
        logger.info("close chamado para instancia postgresql (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
