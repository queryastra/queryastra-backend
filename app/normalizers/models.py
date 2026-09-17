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

    Uma linha por aresta DIRETA (bloqueador imediato -> bloqueado) — a arvore completa de
    bloqueio (Etapa 9) e reconstruida no app (`app.api.v1.waits_blocking._build_blocking_tree`)
    encadeando as arestas de um mesmo snapshot, nao via CTE recursiva ao vivo contra o servidor
    monitorado. Pro PostgreSQL, `pg_blocking_pids()` devolve o fecho TRANSITIVO de bloqueadores
    (nao so o direto) — o adaptador reduz isso pra arestas diretas antes de gerar os eventos (ver
    `app.collectors.postgresql.adapter._reduce_to_direct_edges`), entao mesmo aqui cada linha
    continua representando so um par direto. `wait_time_ms` sempre preenchido (nunca None) mesmo
    quando o motor nao expoe duracao de espera diretamente (aproximacao documentada no PostgreSQL).

    Campos de enriquecimento (`blocker_*`/`blocked_*` abaixo) sao capturados no MOMENTO da coleta,
    nao reconstruidos depois via join contra `query_samples` — e o unico jeito de identificar um
    bloqueador OCIOSO (sessao que segura lock sem ter request ativa, portanto sem nenhuma linha em
    `query_samples` naquele ciclo). Todos opcionais/melhor-esforco:
    - `blocker_db_user`/`blocker_program`/`blocker_machine`: identidade do bloqueador, vinda de
      TODAS as sessoes conectadas (nao so as com request ativa) — unico jeito de identificar um
      bloqueador ocioso.
    - `blocker_wait_event_native`/`blocked_wait_event_native`: o wait nativo de cada lado, quando
      aquela sessao tambem estiver esperando por algo (None pra um bloqueador puro/ocioso).
    - `blocked_wait_resource`: recurso bruto que o lado bloqueado esta esperando (SQL Server:
      `sys.dm_exec_requests.wait_resource`; PostgreSQL: `locktype`/`mode` de `pg_locks`).
    - `blocked_object`: nome resolvido do objeto bloqueado quando possivel (SQL Server via
      `sys.dm_tran_locks JOIN sys.partitions` + `OBJECT_NAME()`; PostgreSQL via
      `pg_locks.relation::regclass`) — None quando nao resolvivel (ex. lock de metadado) ou a
      linha ja foi liberada entre a consulta de bloqueio e a de recurso no mesmo ciclo.
    - `blocker_database_name`/`blocked_database_name` (a pedido explicito do usuario, mesma
      rodada do item 3 de Etapa 10): banco de dados de cada lado. Lado bloqueado sempre vem de
      `DB_NAME(r.database_id)`/`datname` (tem request ativa por definicao — esta esperando).
      Lado bloqueador, quando ocioso (sem request ativa), cai pro banco resolvido via
      `blocker_transaction_id` -> `sys.dm_tran_database_transactions` no SQL Server (mesmo
      lookup ja usado por `NormalizedTransactionEvent`) — `None` so no caso raro de bloqueador
      ocioso sem transacao rastreavel. PostgreSQL nao precisa desse fallback: `datname` esta
      disponivel pra QUALQUER sessao conectada, ativa ou ociosa.
    """

    model_config = ConfigDict(frozen=True)

    time: datetime
    instance_id: UUID
    blocker_session_id: str
    blocked_session_id: str
    blocker_sql_hash: str | None = None
    blocked_sql_hash: str | None = None
    wait_time_ms: int
    blocker_db_user: str | None = None
    blocker_program: str | None = None
    blocker_machine: str | None = None
    blocker_wait_event_native: str | None = None
    blocked_db_user: str | None = None
    blocked_program: str | None = None
    blocked_machine: str | None = None
    blocked_wait_event_native: str | None = None
    blocked_wait_resource: str | None = None
    blocked_object: str | None = None
    # Etapa 10 (rastreamento de transacoes) — liga cada lado da aresta a sua transacao (coluna
    # "Transacao" na tela de Bloqueios). SQL Server:
    # `sys.dm_tran_session_transactions.transaction_id`; PostgreSQL: o proprio `pid` do backend.
    blocker_transaction_id: str | None = None
    blocked_transaction_id: str | None = None
    blocker_database_name: str | None = None
    blocked_database_name: str | None = None
    # Redesenho "log de incidente" (a pedido explicito do usuario): inicio da transacao de cada
    # lado, denormalizado dos mesmos dados de transacao ja lidos no ciclo (SQL Server:
    # _TRANSACTION_QUERY; PostgreSQL: xact_start, ja em session_rows) — sem query extra. O ciclo
    # de vida do INCIDENTE em si (started_at/ended_at) NAO vive nesta tabela (hypertable,
    # insert-only) — e computado por `BlockingIncidentsRepository.upsert_batch` na tabela irma
    # `blocking_incidents` (tabela relacional comum, ver `app/models/blocking_incident.py`), a
    # partir do MESMO objeto `NormalizedBlockingEvent` (representa so a aresta OBSERVADA neste
    # ciclo, sem saber nada de historico).
    blocker_transaction_begin_time: datetime | None = None
    blocked_transaction_begin_time: datetime | None = None


class NormalizedTransactionEvent(BaseModel):
    """Transacao "digna de nota" normalizada — destino: tabela relacional `transaction_events`,
    um LOG DE INCIDENTE retido para analise posterior (upsert + fechamento via `closed_at`, nunca
    deletado por reconciliacao — ver `TransactionEventsRepository.upsert_batch` e a secao "Etapa
    10, redesenho" do plano). Etapa 10, itens 2/3.

    So transacoes com `flag_reason` preenchido chegam a ser construidas pelo adaptador
    (`app.collectors.transaction_flagging.compute_flag_reason`) — uma transacao aberta comum,
    sem bloqueio/erro/duracao suspeita/deadlock, nunca vira uma linha aqui.

    `transaction_identifier`: SQL Server usa `sys.dm_tran_session_transactions.transaction_id`
    (texto); PostgreSQL usa o proprio `pid` do backend (sem conceito de transacao nomeada
    separada da sessao). `transaction_type`/`transaction_uow` so fazem sentido no SQL Server —
    `transaction_uow` (GUID) so preenchido quando a transacao e uma distribuida promovida pelo
    MSDTC (unico caso em que correlacionar 2 transacoes entre instancias e confiavel, ver
    `ARCHITECTURE_SPEC.md`). `last_error_code` e melhor esforco
    (`sys.dm_exec_requests.prev_error`), ausente quando a sessao ja ficou idle apos o erro."""

    model_config = ConfigDict(frozen=True)

    instance_id: UUID
    transaction_identifier: str
    session_id: str | None = None
    database_name: str | None = None
    transaction_type: str | None = None
    transaction_uow: str | None = None
    begin_time: datetime | None = None
    state: str | None = None
    last_error_code: int | None = None
    flag_reason: str | None = None


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


class NormalizedDeadlockEvent(BaseModel):
    """Deadlock normalizado — destino: tabela relacional `deadlock_events` (INSERT-ONLY, um
    deadlock e um evento instantaneo, sem "abrir/fechar"). Etapa 10, item 7.

    So SQL Server (`sys.dm_xe_session_targets`, ring buffer `system_health`, evento
    `xml_deadlock_report`) — PostgreSQL nao expoe detalhe de deadlock via SQL, so o contador
    agregado `pg_stat_database.deadlocks` (ja coletado como metrica). `victim_sql_text_masked`/
    `survivor_sql_text_masked` e `raw_xml_masked` ja vem mascarados
    (`mask_sql_literals`/`mask_showplan_xml_literals`-style, melhor esforco) — nunca persistir
    texto de query em claro.

    `detail` (a pedido explicito do usuario — gravar TODAS as informacoes do deadlock, nao so
    vitima+1 sobrevivente): grafo completo `{"processes": [...], "resources": [...]}`, ver
    docstring de `app.models.deadlock_event.DeadlockEvent`. `database_name` e melhor esforco
    (banco do primeiro processo com `currentdbname` disponivel)."""

    model_config = ConfigDict(frozen=True)

    instance_id: UUID
    time: datetime
    database_name: str | None = None
    victim_session_id: str | None = None
    victim_sql_text_masked: str | None = None
    survivor_session_id: str | None = None
    survivor_sql_text_masked: str | None = None
    resource_description: str | None = None
    raw_xml_masked: str | None = None
    detail: dict[str, Any] = Field(default_factory=dict)


class NormalizedPlanCompileTimeoutFinding(BaseModel):
    """Plano em cache cujo XML sinaliza que o OTIMIZADOR abortou cedo por timeout ou limite de
    memoria (Fase 2, item 5) — `StatementOptmEarlyAbortReason` em `{"TimeOutReached",
    "MemoryLimitExceeded"}` num elemento `StmtSimple` do `ShowPlanXML`. So SQL Server (Postgres
    nao tem cache de plano exposto do mesmo jeito, devolve lista vazia sempre).

    Nao e uma metrica nem um evento com timestamp de ocorrencia real — e um ACHADO da varredura
    periodica do cache (`app.workers.plan_compile_timeout_supervisor`), que cria/dedupe um
    `Alert` por `sql_hash` novo encontrado."""

    model_config = ConfigDict(frozen=True)

    sql_hash: str
    reason: str
    statement_text_masked: str
