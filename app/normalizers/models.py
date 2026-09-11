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
