"""Contrato cross-engine de normalizacao — `Docs/specs/01-collection-contract.md`.

"Cada amostra deve carregar instancia, timestamp, nome canonico, valor, labels e origem nativa
quando aplicavel. Campos especificos de engine nao devem ser descartados: ficam no payload
normalizado ou em tabelas de eventos."

Estes modelos sao o formato que um `Collector` (`app/collectors/base.py`) produz, antes da
persistencia via `app/repositories`. Nao sao os modelos ORM de tabela (`app/models`) — servem de
DTO entre a camada de coleta e a camada de persistencia/normalizacao.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

from app.normalizers.wait_taxonomy import WaitCategory

EngineType = Literal["postgresql", "sqlserver"]
TargetType = Literal["server", "instance"]


class NormalizedMetricSample(BaseModel):
    """Amostra numerica normalizada — destino: hypertable `metric_samples`.

    Formato Prometheus-like (`Docs/database-schema.md` §2): nome canonico + valor + labels.
    Cobre tanto metricas de SO (node_exporter/windows_exporter) quanto metricas numericas
    simples de banco (ex.: `buffer_cache_hit_ratio`, `active_sessions`).
    """

    model_config = ConfigDict(frozen=True)

    time: datetime
    target_type: TargetType
    target_id: UUID
    metric_name: str
    value: float
    labels: dict[str, Any] = Field(default_factory=dict)


class NormalizedQuerySample(BaseModel):
    """Amostra de query ativa normalizada — destino: hypertable `query_samples`.

    `sql_text_masked` e o texto ja mascarado (`app/masking/sql_masking.py`, obrigatorio antes de
    persistir — `Docs/rules/03-sql-privacy.md`) do STATEMENT em execucao — no SQL Server, isso e
    um `SUBSTRING` de `sys.dm_exec_sql_text` via `statement_start_offset`/`statement_end_offset`
    (`Docs/collector-queries.md` §1), nao o batch/procedure inteiro. `sql_text_full_masked` guarda
    o contexto completo mascarado (o batch/procedure de onde o statement veio), usado so na tela
    de detalhe pra destacar o statement dentro do texto inteiro — no PostgreSQL, onde
    `pg_stat_activity.query` ja e so o statement, os dois campos recebem o mesmo texto. `wait_type`
    e a categoria canonica (`app/normalizers/wait_taxonomy.py`); `wait_event_native` preserva o
    valor bruto do motor para nao perder detalhe no drill-down.
    """

    model_config = ConfigDict(frozen=True)

    time: datetime
    instance_id: UUID
    engine: EngineType

    sql_hash: str
    sql_name: str | None = None
    sql_text_masked: str
    sql_text_full_masked: str | None = None
    # Texto original cifrado so e preenchido quando a politica de privacidade da instancia
    # permitir manter o nativo (Docs/rules/03-sql-privacy.md) — nunca em texto puro aqui.
    sql_text_encrypted: bytes | None = None

    wait_type: WaitCategory | None = None
    wait_event_native: str | None = None
    wait_time_ms: int | None = None
    # CPU consumida pela request atual, em ms, desde que ela comecou (`sys.dm_exec_requests.
    # cpu_time` no SQL Server — cumulativo por request, zera numa request nova). So SQL Server
    # por enquanto (`Docs/collector-queries.md` §1) — PostgreSQL nao expoe um equivalente direto
    # em `pg_stat_activity`, fica sempre None la. Usado pra fechar o gap real confirmado
    # comparando com o SolarWinds DPA (categoria "Memory/CPU"): sem isso, tempo gasto rodando na
    # CPU sem nenhuma espera ativa (`wait_type IS NULL`) nao contava em lugar nenhum.
    cpu_time_ms: int | None = None
    duration_ms: int | None = None

    session_id: str | None = None
    db_user: str | None = None
    program: str | None = None
    database_name: str | None = None
    machine: str | None = None


class NormalizedBlockingEvent(BaseModel):
    """Evento de bloqueio normalizado — destino: hypertable `blocking_events`.

    Nivel direto apenas (`blocking_session_id`/`pg_blocking_pids`), sem arvore recursiva
    completa (corte de escopo). `wait_time_ms` sempre preenchido (nunca None) mesmo quando o
    motor nao expoe duracao de espera diretamente.
    """

    model_config = ConfigDict(frozen=True)

    time: datetime
    instance_id: UUID
    blocker_session_id: str
    blocked_session_id: str
    blocker_sql_hash: str | None = None
    blocked_sql_hash: str | None = None
    wait_time_ms: int


class NormalizedMissingIndexSuggestion(BaseModel):
    """Sugestao de indice ausente normalizada — destino: tabela relacional
    `missing_index_suggestions` (upsert por instance_id/database_name/table_name/suggestion, nao
    serie temporal). Ver `Docs/collector-queries.md` §10, ARCHITECTURE_SPEC.md Q21.

    `impact_score` NAO e comparavel entre engines — SQL Server usa a formula real do otimizador
    (`avg_total_user_cost * avg_user_impact * (user_seeks + user_scans)`), PostgreSQL usa um proxy
    heuristico (`seq_scan - idx_scan`) que so serve para ordenar candidatos dentro do proprio
    cluster. `detail` guarda a estrutura bruta especifica do engine, para nao perder informacao
    que a `suggestion` textual resume."""

    model_config = ConfigDict(frozen=True)

    instance_id: UUID
    database_name: str | None = None
    table_name: str
    suggestion: str
    impact_score: float | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class NormalizedBackupStatus(BaseModel):
    """Estado de backup/archiving normalizado — destino: tabela relacional `backup_status`
    (upsert por instance_id/database_name, nao serie temporal). Ver `Docs/collector-queries.md`
    §6, ARCHITECTURE_SPEC.md Q16.

    SQL Server: uma instancia desta classe por banco (`database_name` preenchido),
    `last_backup_at` = `MAX(backup_finish_date)` de `msdb.dbo.backupset` (`type='D'`, so backup
    completo/full). PostgreSQL: uma UNICA instancia por instancia monitorada (`database_name`
    None — `pg_stat_archiver` e cluster-wide, nao por banco), `last_backup_at` =
    `last_archived_time` como PROXY de saude de WAL archiving, NAO um backup de fato — Postgres
    nao tem catalogo nativo de backup completo como o `msdb` do SQL Server. `detail` guarda a
    estrutura bruta especifica do engine."""

    model_config = ConfigDict(frozen=True)

    instance_id: UUID
    database_name: str | None = None
    last_backup_at: datetime | None = None
    detail: dict[str, Any] = Field(default_factory=dict)
