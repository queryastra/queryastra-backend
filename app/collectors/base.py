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
    NormalizedMetricSample,
    NormalizedMissingIndexSuggestion,
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
    # Etapa 10 (rastreamento de transacoes) — transacoes ABERTAS neste ciclo (nao serie
    # temporal bruta: `TransactionEventsRepository.upsert_batch` reconcilia o conjunto inteiro,
    # removendo qualquer transacao vista antes mas ausente agora — ja fechou).
    transaction_events: list[NormalizedTransactionEvent] = field(default_factory=list)
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
