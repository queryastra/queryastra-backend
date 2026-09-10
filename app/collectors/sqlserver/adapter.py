"""Adaptador SQL Server. Fonte de verdade: `Docs/collector-queries.md`.

Usa `pymssql` (sincrono) — cada chamada roda em thread separada (`asyncio.to_thread`) sob
`asyncio.wait_for` para respeitar o timeout de conexao configurado
(Docs/rules/02-safe-collection.md).

`check_capabilities()` conecta e checa, via `fn_my_permissions(NULL, 'SERVER')` +
`IS_SRVROLEMEMBER`/`IS_ROLEMEMBER`, a permissao mandatoria (`VIEW SERVER STATE` ou, no SQL
Server 2022+, `VIEW SERVER PERFORMANCE STATE` — Docs/collector-queries.md §7, §9) e as
permissoes opcionais que so degradam funcionalidades especificas quando ausentes.

DMVs/fontes usadas como referencia para `collect()` (ainda fora de escopo desta feature):
    - `sys.dm_exec_requests` + `sys.dm_exec_sessions` + `sys.dm_exec_sql_text()` — sessao/query
      ativa (collector-queries.md §1). Cuidado com queries paralelas (dop>1): so a coordinator
      thread aparece, `reads`/`writes`/`logical_reads` nao atualizam nessa linha.
    - `sys.dm_exec_requests.blocking_session_id` (+ CTE recursiva oficial da Microsoft p/ arvore
      completa) — deteccao de bloqueio (collector-queries.md §2). Sentinel negativo (-2 a -5) =
      bloqueador nao determinavel, nao "nao bloqueado".
    - `sys.dm_os_wait_stats` (cumulativo da instancia, 2005+) / `sys.dm_exec_session_wait_stats`
      (cumulativo por sessao, 2016+) — wait statistics (collector-queries.md §3).
    - `sys.dm_xe_session_targets` / `sys.dm_xe_sessions` (`system_health`, ring buffer,
      `xml_deadlock_report`) — deadlock detalhado, 2008+ (collector-queries.md §4).
    - `sys.dm_os_performance_counters` (atencao ao `cntr_type`: raw vs. delta vs. razao
      pareada com contador base) — metricas de recurso (collector-queries.md §5).
    - `sys.dm_io_virtual_file_stats`, `sys.dm_db_file_space_usage` — latencia de IO por arquivo,
      espaco livre do TempDB (collector-queries.md §5).
    - `msdb.dbo.backupset` + `msdb.dbo.backupmediafamily` — backup (collector-queries.md §6).

Permissao minima esperada: `VIEW SERVER STATE` (< 2022) / `VIEW SERVER PERFORMANCE STATE`
(>= 2022) — collector-queries.md §7, §9.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from uuid import UUID

import pymssql

from app.collectors.base import CapabilityResult, CapabilityStatus, CollectionResult
from app.collectors.capabilities import (
    CONNECTIVITY,
    SQLSERVER_DATABASE_VISIBILITY,
    SQLSERVER_DEFINITION_VISIBILITY,
    SQLSERVER_SERVER_STATE,
    SQLSERVER_SESSION_TERMINATION,
    SQLSERVER_TABLE_READ_ACCESS,
)
from app.collectors.connection import ConnectionParams
from app.core.logging import get_logger

logger = get_logger(__name__)

_SERVER_STATE_PERMISSIONS = {"VIEW SERVER STATE", "VIEW SERVER PERFORMANCE STATE"}


def _connect_and_check(connection: ConnectionParams) -> list[CapabilityResult]:
    """Helper sincrono: conecta via `pymssql` e roda as checagens de permissao.

    Roda em thread separada (`asyncio.to_thread`) — `pymssql` nao e assincrono.
    """
    timeout_seconds = int(connection.connect_timeout_seconds)
    try:
        conn = pymssql.connect(
            server=connection.host,
            port=str(connection.port),
            user=connection.username,
            password=connection.secret_value,
            timeout=timeout_seconds,
            login_timeout=timeout_seconds,
        )
    except (pymssql.OperationalError, OSError, TimeoutError) as exc:
        return [
            CapabilityResult(
                capability=CONNECTIVITY,
                status=CapabilityStatus.UNAVAILABLE,
                reason=f"Falha ao conectar: {type(exc).__name__}",
            )
        ]

    try:
        cursor = conn.cursor()
        cursor.execute("SELECT permission_name FROM fn_my_permissions(NULL, 'SERVER')")
        granted_permissions = {str(row[0]) for row in cursor.fetchall()}

        cursor.execute(
            "SELECT IS_SRVROLEMEMBER('processadmin') AS is_processadmin, "
            "IS_ROLEMEMBER('db_datareader') AS is_db_datareader"
        )
        role_membership_row = cursor.fetchone()
        if role_membership_row is None:  # pragma: no cover - SELECT literal sempre devolve 1 linha
            raise pymssql.OperationalError("fn_my_permissions/IS_ROLEMEMBER nao retornou linha")
        is_processadmin_row, is_db_datareader_row = role_membership_row
        cursor.close()

        return _build_capability_results(
            granted_permissions=granted_permissions,
            is_processadmin=bool(is_processadmin_row),
            is_db_datareader=bool(is_db_datareader_row),
        )
    finally:
        conn.close()


def _build_capability_results(
    *, granted_permissions: set[str], is_processadmin: bool, is_db_datareader: bool
) -> list[CapabilityResult]:
    has_server_state = bool(granted_permissions & _SERVER_STATE_PERMISSIONS)
    results = [
        CapabilityResult(
            capability=SQLSERVER_SERVER_STATE,
            status=CapabilityStatus.FULL if has_server_state else CapabilityStatus.UNAVAILABLE,
            reason=(
                None
                if has_server_state
                else "VIEW SERVER STATE / VIEW SERVER PERFORMANCE STATE ausente"
            ),
        )
    ]

    has_database_visibility = "VIEW ANY DATABASE" in granted_permissions
    results.append(
        CapabilityResult(
            capability=SQLSERVER_DATABASE_VISIBILITY,
            status=CapabilityStatus.FULL if has_database_visibility else CapabilityStatus.DEGRADED,
            reason=(
                None
                if has_database_visibility
                else "sem VIEW ANY DATABASE, visibilidade de bancos fica limitada"
            ),
        )
    )

    has_definition_visibility = "VIEW ANY DEFINITION" in granted_permissions
    results.append(
        CapabilityResult(
            capability=SQLSERVER_DEFINITION_VISIBILITY,
            status=(
                CapabilityStatus.FULL if has_definition_visibility else CapabilityStatus.DEGRADED
            ),
            reason=(
                None
                if has_definition_visibility
                else "sem VIEW ANY DEFINITION, definicao de objetos (planos/procs) fica limitada"
            ),
        )
    )

    results.append(
        CapabilityResult(
            capability=SQLSERVER_SESSION_TERMINATION,
            status=CapabilityStatus.FULL if is_processadmin else CapabilityStatus.DEGRADED,
            reason=None if is_processadmin else "sem processadmin, Kill Session nao vai funcionar",
        )
    )

    results.append(
        CapabilityResult(
            capability=SQLSERVER_TABLE_READ_ACCESS,
            status=CapabilityStatus.FULL if is_db_datareader else CapabilityStatus.DEGRADED,
            reason=(
                None
                if is_db_datareader
                else "sem db_datareader, leitura de dados de tabela fica indisponivel"
            ),
        )
    )

    return results


class SQLServerCollector:
    """Implementa `app.collectors.base.Collector` para instancias SQL Server."""

    def __init__(self, connection: ConnectionParams, instance_id: UUID | None = None) -> None:
        self._connection_params = connection
        self._instance_id = instance_id

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Conecta (via thread, `pymssql` e sincrono) e checa permissoes de servidor/banco.

        `asyncio.wait_for` cobre o caso em que a propria conexao trava alem do timeout (a thread
        sincrona nao e cancelavel, mas o resultado deixa de ser aguardado) — tratado igual a
        qualquer outro erro de conectividade.
        """
        try:
            return await asyncio.wait_for(
                asyncio.to_thread(_connect_and_check, self._connection_params),
                timeout=self._connection_params.connect_timeout_seconds,
            )
        except TimeoutError as exc:
            return [
                CapabilityResult(
                    capability=CONNECTIVITY,
                    status=CapabilityStatus.UNAVAILABLE,
                    reason=f"Falha ao conectar: {type(exc).__name__}",
                )
            ]

    async def collect(self) -> CollectionResult:
        """Deveria consultar `sys.dm_exec_requests`, `sys.dm_os_wait_stats` e afins.

        Ver `Docs/collector-queries.md` §1 (sessao ativa), §2 (bloqueio), §3 (wait stats via
        delta/`LAG()`), §5 (metricas de recurso, atencao ao `cntr_type`) para as queries exatas.
        Fora de escopo desta feature (cadastro).
        """
        logger.info(
            "collect ainda nao implementado para instancia sqlserver (instance_id=%s)",
            self._instance_id,
        )
        raise NotImplementedError(
            "SQLServerCollector.collect: implementar consulta a sys.dm_exec_requests, "
            "sys.dm_exec_sessions, sys.dm_exec_sql_text(), sys.dm_os_wait_stats e "
            "sys.dm_os_performance_counters, conforme Docs/collector-queries.md §1, §2, §3, §5."
        )

    async def close(self) -> None:
        """Sem estado persistente entre chamadas (cada `check_capabilities` fecha sua conexao)."""
        logger.info("close chamado para instancia sqlserver (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
