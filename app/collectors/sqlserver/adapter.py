"""Adaptador SQL Server — stub. Fonte de verdade: `Docs/collector-queries.md`.

Usa `pyodbc`/`pymssql` (ARCHITECTURE_SPEC.md §4) como driver. Nenhuma query real e executada
neste scaffold — cada metodo levanta `NotImplementedError` documentando exatamente qual
DMV/view/Extended Events session do SQL Server deve ser usada quando a logica for implementada.

DMVs/fontes usadas como referencia (nao implementadas ainda):
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

from datetime import UTC, datetime
from uuid import UUID

from app.collectors.base import CapabilityResult, CollectionResult
from app.core.logging import get_logger

logger = get_logger(__name__)


class SQLServerCollector:
    """Implementa `app.collectors.base.Collector` para instancias SQL Server.

    Stub: mantem so a assinatura e a intencao de cada metodo. Conexao real ainda nao e aberta
    aqui — a resolucao de credencial deve passar por
    `app.security.credential_reference.CredentialResolver`, nunca por senha em texto puro.
    """

    def __init__(self, instance_id: UUID) -> None:
        self._instance_id = instance_id
        # pyodbc.Connection / pymssql.Connection quando implementado.
        self._connection: object | None = None

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Deveria detectar `SERVERPROPERTY('ProductVersion')` e checar permissoes/DMVs.

        Referencia: branching por versao para `VIEW SERVER STATE` vs.
        `VIEW SERVER PERFORMANCE STATE` (>= 2022), disponibilidade de
        `sys.dm_exec_session_wait_stats` (>= 2016) e de Extended Events/`system_health`
        (>= 2008) — collector-queries.md §3, §4, §7, §9.
        """
        logger.info(
            "check_capabilities ainda nao implementado para instancia sqlserver (instance_id=%s)",
            self._instance_id,
        )
        raise NotImplementedError(
            "SQLServerCollector.check_capabilities: implementar deteccao de versao via "
            "SERVERPROPERTY('ProductVersion') e checagem de VIEW SERVER STATE / "
            "VIEW SERVER PERFORMANCE STATE, conforme Docs/collector-queries.md §7, §9."
        )

    async def collect(self) -> CollectionResult:
        """Deveria consultar `sys.dm_exec_requests`, `sys.dm_os_wait_stats` e afins.

        Ver `Docs/collector-queries.md` §1 (sessao ativa), §2 (bloqueio), §3 (wait stats via
        delta/`LAG()`), §5 (metricas de recurso, atencao ao `cntr_type`) para as queries exatas.
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
        """Deveria fechar a conexao `pyodbc`/`pymssql`, se aberta."""
        logger.info("close chamado para instancia sqlserver (instance_id=%s)", self._instance_id)

    def _now(self) -> datetime:
        return datetime.now(UTC)
