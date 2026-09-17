"""Interface comum de coletor.

Fonte: `Docs/specs/01-collection-contract.md`, `Docs/rules/04-engine-capabilities.md`.

Ciclo do coletor (01-collection-contract.md): resolver instancia e capacidades -> abrir conexao
com timeout -> executar queries somente leitura com limite adequado -> normalizar resultado por
engine -> mascarar SQL -> persistir amostras e eventos -> registrar duracao, erro e cobertura.

"O health-check deve reportar capacidade completa, limitada ou indisponivel, sem bloquear o
cadastro quando a degradacao for aceitavel." (04-engine-capabilities.md) — `CapabilityStatus` e
`CapabilityResult` formalizam esse contrato; um adaptador (`postgresql/adapter.py`,
`sqlserver/adapter.py`) implementa o `Protocol` `Collector` abaixo.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from enum import StrEnum
from typing import Protocol, runtime_checkable
from uuid import UUID

from app.normalizers.models import (
    NormalizedBackupStatus,
    NormalizedBlockingEvent,
    NormalizedDeadlockEvent,
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
    NormalizedPlanCompileTimeoutFinding,
    NormalizedQuerySample,
    NormalizedTransactionEvent,
)


class CapabilityStatus(StrEnum):
    """Nivel de suporte de uma capacidade de coleta numa instancia especifica.

    Um erro de permissao ou de versao (Docs/rules/02-safe-collection.md) degrada so a capacidade
    afetada — nunca derruba a coleta inteira. Ex.: falta de `pg_stat_statements` degrada "Top SQL"
    para `UNAVAILABLE`, sem impedir que sessao/wait continue `FULL`.
    """

    FULL = "full"  # capacidade disponivel sem restricao conhecida
    DEGRADED = "degraded"  # disponivel com limitacao (versao antiga, permissao parcial)
    UNAVAILABLE = "unavailable"  # nao disponivel nesta instancia (versao, permissao, extensao)


@dataclass(frozen=True, slots=True)
class CapabilityResult:
    """Resultado da checagem de uma capacidade individual.

    Ex.: capability="top_sql" | "blocking" | "wait_stats".
    """

    capability: str
    status: CapabilityStatus
    # Motivo legivel para humano quando status != FULL (ex.: "pg_stat_statements nao instalada",
    # "VIEW SERVER PERFORMANCE STATE ausente"). Nunca deve conter SQL bruto, senha ou connection
    # string (Docs/rules/03-sql-privacy.md, 01-credential-security.md).
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class CollectionResult:
    """Envelope de uma rodada de coleta: amostras normalizadas + cobertura de capacidade.

    "registrar duracao, erro e cobertura da coleta" (01-collection-contract.md) — `duration_ms` e
    `capabilities` cobrem esse requisito; erros de capacidade especifica ficam dentro de
    `capabilities`, nao interrompem as amostras que deram certo.
    """

    instance_id: UUID
    collected_at: datetime
    duration_ms: int
    metric_samples: list[NormalizedMetricSample] = field(default_factory=list)
    query_samples: list[NormalizedQuerySample] = field(default_factory=list)
    blocking_events: list[NormalizedBlockingEvent] = field(default_factory=list)
    # Etapa 10 (rastreamento de transacoes), redesenhado pos-uso real: so as transacoes ABERTAS
    # neste ciclo que sao "dignas de nota" (bloqueio/erro/orfa — ver
    # `app.collectors.transaction_flagging.compute_flag_reason`), ja filtradas pelo coletor.
    # `TransactionEventsRepository.upsert_batch` grava/atualiza essas e FECHA (`closed_at`, nunca
    # deleta) qualquer uma ja rastreada que sumiu do conjunto aberto — log de incidente retido
    # para analise posterior, nao mais um espelho reconciliado de toda transacao aberta.
    transaction_events: list[NormalizedTransactionEvent] = field(default_factory=list)
    # TODOS os identificadores de transacao ABERTA neste ciclo, flagged ou nao — usado so por
    # `TransactionEventsRepository.upsert_batch` para saber quais linhas JA RASTREADAS (de um
    # ciclo anterior, quando foram flagged) devem ser fechadas (`closed_at`) por terem deixado de
    # estar abertas. Uma transacao aberta comum, nunca flagged, nunca aparece em
    # `transaction_events` mas aparece aqui enquanto durar.
    open_transaction_identifiers: list[str] = field(default_factory=list)
    capabilities: list[CapabilityResult] = field(default_factory=list)
    # Versao do motor detectada NESTE ciclo (o coletor ja detecta pra decidir qual variante de
    # query usar — ex.: `pg_stat_checkpointer` vs `pg_stat_bgwriter` — mas so passou a expor isso
    # aqui pra `PollingWorker` persistir de volta em `instances.engine_version*` a partir desta
    # feature; `None` nao apaga o valor ja persistido, so significa "nao detectado neste ciclo").
    engine_version: str | None = None
    engine_version_major: int | None = None


@runtime_checkable
class Collector(Protocol):
    """Interface que os adaptadores PostgreSQL e SQL Server implementam.

    Toda query executada por um `Collector` e somente leitura, com timeout e limite de linhas
    configuraveis (Docs/rules/02-safe-collection.md) — nunca DDL/DML/DBCC/kill/mudanca de
    configuracao no banco monitorado.
    """

    async def check_capabilities(self) -> list[CapabilityResult]:
        """Detecta versao/permissoes da instancia e retorna a cobertura de capacidade atual.

        Chamado no cadastro (health-check) e periodicamente para refletir mudanca de versao ou
        permissao sem exigir recadastro.
        """
        ...

    async def collect(self) -> CollectionResult:
        """Executa um ciclo completo de coleta e retorna as amostras normalizadas.

        Implementacoes devem: abrir conexao com timeout: normalizar por engine antes de
        retornar; nunca persistir texto de SQL sem passar por
        `app.masking.sql_masking.mask_sql_literals` antes.
        """
        ...

    async def close(self) -> None:
        """Libera a conexao/pool mantido pelo adaptador."""
        ...

    async def collect_missing_index_suggestions(self) -> list[NormalizedMissingIndexSuggestion]:
        """Executa a checagem de indices ausentes (`Docs/collector-queries.md` §10, Q21).

        Separado de `collect()` de proposito: roda num worker de baixa frequencia
        (`app/workers/missing_index_supervisor.py`, 1x/dia por padrao), nao a cada poucos
        segundos como a coleta de sessao/wait/metrica. PostgreSQL: heuristica via
        `pg_stat_user_tables`, iterando por banco (essa view e escopada ao banco conectado, ao
        contrario de `pg_stat_activity`/`pg_stat_database`). SQL Server: sugestao real do
        otimizador via `sys.dm_db_missing_index_*`, instancia inteira numa unica chamada.
        """
        ...

    async def collect_backup_status(self) -> list[NormalizedBackupStatus]:
        """Executa a checagem de backup/archiving (`Docs/collector-queries.md` §6, Q16).

        Separado de `collect()` pelo mesmo motivo de `collect_missing_index_suggestions()` —
        roda num worker de baixa frequencia (`app/workers/backup_status_supervisor.py`, 1x/dia
        por padrao). SQL Server: `msdb.dbo.backupset` (`type='D'`, so backup completo), uma
        linha por banco. PostgreSQL: `pg_stat_archiver` (cluster-wide), uma unica linha por
        instancia — proxy de saude de WAL archiving, NAO um backup de fato.
        """
        ...

    async def collect_deadlock_events(self) -> list[NormalizedDeadlockEvent]:
        """Executa a extracao de deadlocks recentes (Etapa 10, item 7).

        Separado de `collect()` pelo mesmo motivo dos demais — roda num worker de baixa
        frequencia (`app/workers/deadlock_supervisor.py`). So SQL Server (ring buffer nativo
        `system_health`, sempre ativo por padrao, evento `xml_deadlock_report`) — PostgreSQL nao
        expoe detalhe de deadlock via SQL, devolve lista vazia sempre (so o contador agregado
        `pg_stat_database.deadlocks`, ja coletado como metrica em `collect()`).
        """
        ...

    async def collect_freespace_metrics(self) -> list[NormalizedMetricSample]:
        """Fase 2, item 2 (expansao do catalogo de alertas) — roda num worker de baixa
        frequencia proprio. So SQL Server (`sys.dm_os_volume_stats`) — PostgreSQL nao expoe
        espaco livre de disco via SQL puro sem acesso ao SO, devolve lista vazia sempre (mesma
        classe de limitacao ja documentada pra CPU/deadlock neste projeto).
        """
        ...

    async def collect_replication_lag_metrics(self) -> list[NormalizedMetricSample]:
        """Fase 2, item 2 (expansao do catalogo de alertas) — roda num worker de baixa
        frequencia proprio, nao a cada ciclo de poll rapido. SQL Server:
        `sys.dm_hadr_database_replica_states` (Always On AG, fila de log/redo em KB).
        PostgreSQL: `pg_stat_replication` (`replay_lag`, em segundos). Sem replica configurada,
        devolve lista vazia em ambos os motores — nao e erro.
        """
        ...

    async def run_scalar_query(self, sql: str, timeout_seconds: float) -> float | None:
        """Executa `sql` (ja validada por `app.services.custom_sql_guard` — este metodo nunca
        valida, so executa) e devolve a primeira coluna da primeira linha como numero, ou `None`
        se nao houver linha ou o valor nao for numerico. Usado por `rule_type="custom_sql"`
        (Fase 2, item 2).
        """
        ...

    async def scan_plan_compile_timeouts(self) -> list[NormalizedPlanCompileTimeoutFinding]:
        """Varre o cache de planos procurando `StatementOptmEarlyAbortReason` (Fase 2, item 5).

        Roda num worker de baixa frequencia proprio
        (`app/workers/plan_compile_timeout_supervisor.py`). So SQL Server (cache de plano via
        `sys.dm_exec_query_stats`/`sys.dm_exec_query_plan`) — PostgreSQL nao tem cache de plano
        exposto do mesmo jeito, devolve lista vazia sempre.
        """
        ...
