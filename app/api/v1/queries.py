"""Endpoints de queries ativas / top SQL / drill-down hierarquico por banco de dados.

Destino de dados: hypertable bruta `query_samples` (`Docs/database-schema.md` §3), consultada
diretamente — o continuous aggregate `query_stats_hourly` foi criado `WITH NO DATA` e sem
policy de refresh, esta vazio e nunca atualizado, portanto nunca e usado aqui (a agregacao de
"Top SQL" e feita na propria query contra `query_samples`). Texto de SQL retornado a UI e sempre
`sql_text_masked` (`Docs/rules/03-sql-privacy.md`) — o texto original cifrado so e acessivel pelo
fluxo de "revelar" auditado da Fase 2 (RBAC), fora de escopo deste MVP.

`GET /database-names/{instance_id}` — só a lista de nomes distintos, sem agregação nenhuma
(bem mais barato que `/databases/{instance_id}`) — usado por seletores simples de banco fora do
drill-down (ex.: override ad-hoc de tempo mínimo de bloqueio).

Drill-down hierarquico (Instancia -> Banco -> Query): `GET /databases/{instance_id}` (nivel 1,
agrega por `database_name`), `GET /top-sql/{instance_id}?database_name=X` (nivel 2, mesmo
agregado por `sql_hash` de sempre, agora filtravel por banco) e `GET /detail/{instance_id}/
{sql_hash}` (nivel 3/folha, amostras brutas daquele hash).

Visao historica "Tudo" (um dia por barra, equivalente ao grafico multi-dia de ferramentas de
referencia): `GET /databases-by-day/{instance_id}` (nivel 1) e `GET /top-sql-by-day/{instance_id}`
(nivel 2) — mesma hierarquia, granularidade de dia em vez de agregado unico do periodo.

Detalhamento por hora dentro de um dia (passo intermediario entre "Tudo" e o grafico de um dia
inteiro, equivalente ao "Interval: 1 hour" de ferramentas de referencia): `GET
/databases-by-hour/{instance_id}` (nivel 1) e `GET /top-sql-by-hour/{instance_id}` (nivel 2).

Dimensao "Waits" (irma de "Bancos de dados"/"SQL", mesmo padrao Tudo/dia/hora, mas por tipo de
espera nativo — folha da propria dimensao, sem decomposicao adicional): `GET
/wait-types/{instance_id}`, `GET /wait-types-by-day/{instance_id}`, `GET
/wait-types-by-hour/{instance_id}`.

Dimensao "Usuarios" (irma das demais, mesmo padrao Tudo/dia/hora, agregada por `db_user` e
decomposta por wait nativo — mesmo padrao de "Bancos de dados", nao de "Waits"): `GET
/users/{instance_id}`, `GET /users-by-day/{instance_id}`, `GET /users-by-hour/{instance_id}`.

"Fatia de Tempo" (drill-down temporal fino DENTRO de uma janela ja estreita, equivalente ao
"Timeslice" de ferramentas de monitoramento): `GET /timeslice/{instance_id}?dimension=X&bucket=Y` — um unico
endpoint parametrizado (`dimension`: database/sql/wait_type/db_user; `bucket`: ten_minutes/
minute/ten_seconds/second), generalizando os `wait_time_by_bucket_and_*` ja existentes em vez de
multiplicar rotas. O nivel folha (lista de queries num segundo especifico) reusa `GET /top-sql/
{instance_id}?from=X&to=X+1s` — mesmo endpoint do nivel 2 do drill-down normal, so com uma janela
de 1 segundo.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import check_instance_access, get_db_session, resolve_time_window
from app.core.config import get_settings
from app.repositories.instances_repository import InstancesRepository
from app.repositories.query_plan_snapshots_repository import QueryPlanSnapshotsRepository
from app.repositories.query_samples_repository import QuerySamplesRepository
from app.repositories.system_settings_repository import SystemSettingsRepository
from app.security.credential_reference import CredentialResolver
from app.repositories.ai_api_key_repository import AiApiKeyRepository
from app.services.ai_advisor import AiAdvisor, MultiKeyAiAdvisor
from app.services.execution_plan_service import fetch_execution_plan
from app.services.plan_advisor import analyze_plan_by_format
from app.services.sql_advisor import analyze_sql
from app.services.sql_insight_client import SqlInsightClient

router = APIRouter()


class QuerySampleResponse(BaseModel):
    """Amostra de query ativa, com texto ja mascarado."""

    time: datetime
    instance_id: UUID
    sql_hash: str
    sql_name: str | None
    sql_text_masked: str
    wait_type: str | None
    wait_event_native: str | None
    wait_time_ms: int | None
    duration_ms: int | None
    session_id: str | None
    db_user: str | None
    program: str | None
    database_name: str | None
    machine: str | None


class WaitBreakdownEntryResponse(BaseModel):
    """Fatia de wait time por tipo de espera NATIVO do engine (nao o bucket canonico cross-engine
    de 9 categorias — granularidade maior, a pedido explicito). Reutilizado tanto para decompor o
    wait time de um banco (nivel 1) quanto o de uma query (nivel 2)."""

    wait_event_native: str
    total_wait_time_ms: int


class TopSqlEntryResponse(BaseModel):
    """Linha agregada de "Top SQL", calculada em tempo real sobre `query_samples`."""

    instance_id: UUID
    sql_hash: str
    sql_name: str | None
    sql_text_masked: str
    database_name: str | None
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float
    predominant_wait_type: str | None
    wait_breakdown: list[WaitBreakdownEntryResponse]


class DatabaseSummaryResponse(BaseModel):
    """Linha agregada por banco de dados — nivel 1 do drill-down hierarquico (Instancia ->
    Banco -> Query)."""

    database_name: str
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float
    wait_breakdown: list[WaitBreakdownEntryResponse]


class DbUserSummaryResponse(BaseModel):
    """Linha agregada por usuario de banco (`db_user`) — dimensao "Usuarios" do drill-down
    (irma de "Bancos de dados"/"SQL"/"Waits"), decomposta por wait nativo (`wait_breakdown`) do
    mesmo jeito que "Bancos de dados"."""

    db_user: str
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float
    wait_breakdown: list[WaitBreakdownEntryResponse]


class ProgramSummaryResponse(BaseModel):
    program: str
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float
    wait_breakdown: list[WaitBreakdownEntryResponse]


class MachineSummaryResponse(BaseModel):
    machine: str
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float
    wait_breakdown: list[WaitBreakdownEntryResponse]


class BucketEntryResponse(BaseModel):
    """Uma fatia (bucket de tempo, entidade) dos graficos historicos por periodo — usado tanto
    pela visao "Tudo" (`bucket_start` e meia-noite UTC de um dia calendario, um dia por barra)
    quanto pelo detalhamento por hora dentro de um dia (`bucket_start` e o inicio de uma hora
    UTC). `label` e `database_name` nas rotas `*databases*`, `sql_hash` nas rotas `*top-sql*`,
    `wait_event_native` nas rotas `*wait-types*`, ou `db_user` nas rotas `*users*`. Retornado
    plano (nao agrupado) — o cliente agrupa por `bucket_start` para montar cada barra empilhada."""

    bucket_start: datetime
    label: str
    total_wait_time_ms: int


class WaitTypeSummaryResponse(BaseModel):
    """Linha agregada por tipo de espera NATIVO — dimensao "Waits" do drill-down (irma de
    "Bancos de dados"/"SQL"), folha da propria dimensao: um tipo de espera nao se decompoe em
    mais nada (ao contrario de banco/query, que tem `wait_breakdown` por serem decompostos por
    wait nativo)."""

    wait_event_native: str
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float


class QuerySampleDetailResponse(BaseModel):
    """Uma amostra de execucao, dentro do detalhe de uma query (nivel 3/folha).

    `wait_time_ms` continua sendo o valor BRUTO da tabela (cumulativo desde o inicio da mesma
    espera continua - ver docstring de `QuerySamplesRepository`), mantido por transparencia/
    depuracao. `effective_wait_ms`/`effective_cpu_ms` sao o delta corrigido por sessao (mesma
    unidade usada por todo grafico/agregacao desta ferramenta) - a UI deve somar os dois pra
    mostrar a duracao real desta amostra especifica, nao `wait_time_ms` sozinho (bug real
    corrigido: a soma de `wait_time_ms` das amostras nao batia com o total do grafico de
    espera)."""

    time: datetime
    duration_ms: int | None
    wait_type: str | None
    wait_event_native: str | None
    wait_time_ms: int | None
    effective_wait_ms: int | None
    effective_cpu_ms: int | None
    session_id: str | None
    db_user: str | None
    program: str | None
    machine: str | None


class QueryDetailResponse(BaseModel):
    """Detalhe de um `sql_hash` especifico — nivel 3 (folha) do drill-down hierarquico.

    `sql_text_masked` e so o STATEMENT em execucao (usado no hash); `sql_text_full_masked` e o
    contexto completo (o batch/procedure inteiro de onde o statement veio, quando o motor faz
    essa distincao — SQL Server sim, PostgreSQL nao) — a UI destaca `sql_text_masked` dentro de
    `sql_text_full_masked`. `None` quando a amostra e anterior a essa distincao existir (coluna
    adicionada depois) ou quando os dois coincidem."""

    instance_id: UUID
    sql_hash: str
    sql_name: str | None
    sql_text_masked: str
    sql_text_full_masked: str | None
    database_name: str | None
    samples: list[QuerySampleDetailResponse]
    # Etapa 11 — decomposicao de wait time so desta query, reaproveitada pelo painel Advisor
    # (calculado 100% no frontend a partir disso, sem endpoint proprio).
    wait_breakdown: list[WaitBreakdownEntryResponse]


class QueryStatsBucketResponse(BaseModel):
    """Uma fatia (bucket de tempo) do grafico "Execucoes x tempo de espera por execucao" — Etapa
    11, painel de estatisticas do detalhe de query (nivel 3). `avg_wait_time_ms` e derivado
    (`total_wait_time_ms / execution_count`), calculado aqui para nao obrigar o cliente a
    reimplementar a divisao (e tratar `execution_count == 0`)."""

    bucket_start: datetime
    execution_count: int
    total_wait_time_ms: int
    avg_wait_time_ms: float


class SqlInsightRuleResponse(BaseModel):
    """Uma violacao/observacao devolvida pelo motor de regras do AB SQL Insight."""

    rule_id: str
    description: str
    details: str | None


class SqlInsightAnalysisResponse(BaseModel):
    """`available=False` quando o AB SQL Insight nao esta configurado (`SQL_INSIGHT_BASE_URL`
    vazio) ou a chamada falhou (timeout, servico fora) — `rules` sempre vazio nesse caso, nunca
    um erro visivel na tela de detalhe de query."""

    available: bool
    rules: list[SqlInsightRuleResponse]


class ExecutionPlanResponse(BaseModel):
    """Resultado da busca sob demanda de plano de execucao (Etapa 11) — `reason` explica por que
    `available=False` (PostgreSQL sem suporte, plano fora do cache, credencial com problema),
    nunca inventa um plano quando nao encontrado. `source` ("actual"|"cached", so quando
    `available=True`) diz se e o plano REAL (contagens de execucao reais, so quando a sessao
    original ainda esta rodando) ou o CACHEADO/estimado (o caso comum) — a tela sempre informa
    qual dos dois esta mostrando."""

    available: bool
    plan_xml: str | None
    reason: str | None
    source: str | None = None


class PlanHistoryEntryResponse(BaseModel):
    """Um plano DISTINTO visto para esta query numa varredura periodica do cache
    (`PlanHistorySupervisor`, so SQL Server, desligado por padrao) — mais recente primeiro. Lista
    vazia nao significa "sem histórico de verdade": tambem aparece vazia quando o worker esta
    desligado, ou quando ele nunca viu essa query com plano no cache. Sempre `source="cached"`
    (mesmo racional de `NormalizedPlanSnapshot` — um scan periodico nunca produz `"actual"`)."""

    captured_at: datetime
    plan_hash: str
    plan_xml: str
    last_execution_time: datetime | None
    source: Literal["cached"] = "cached"


# --- Dependency providers -------------------------------------------------------------------


def get_query_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> QuerySamplesRepository:
    return QuerySamplesRepository(session)


def get_instances_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> InstancesRepository:
    return InstancesRepository(session)


def get_credential_resolver() -> CredentialResolver:
    return CredentialResolver()


def get_sql_insight_client() -> SqlInsightClient:
    settings = get_settings()
    return SqlInsightClient(settings.sql_insight_base_url, settings.sql_insight_timeout_seconds)


async def get_ai_advisor(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> AiAdvisor | MultiKeyAiAdvisor:
    """Chaves do banco têm precedência sobre env vars.

    Se existirem chaves em `ai_api_keys`, usa `MultiKeyAiAdvisor` com failover automático.
    Caso contrário, cai no legado (system_settings ou env vars).
    """
    settings = get_settings()
    key_repo = AiApiKeyRepository(session)
    enabled_keys = await key_repo.get_enabled_for_failover()

    if enabled_keys:
        key_advisors = [
            (
                row,
                key_repo,
                AiAdvisor(
                    provider=row.provider,
                    api_key=key_repo.get_plain_api_key(row),
                    base_url=row.base_url,
                    model=row.model,
                    timeout_seconds=settings.ai_timeout_seconds,
                ),
            )
            for row in enabled_keys
        ]
        return MultiKeyAiAdvisor(key_advisors)

    # Fallback: system_settings singleton (chave única configurada via UI antiga ou env vars)
    ss_repo = SystemSettingsRepository(session)
    ss_row = await ss_repo.get()
    db_key = ss_repo.get_ai_api_key_plain(ss_row)
    return AiAdvisor(
        provider=ss_row.ai_provider or settings.ai_provider,
        api_key=db_key or settings.ai_api_key,
        base_url=ss_row.ai_base_url or settings.ai_base_url,
        model=ss_row.ai_model or settings.ai_model,
        timeout_seconds=settings.ai_timeout_seconds,
    )


def get_query_plan_snapshots_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> QueryPlanSnapshotsRepository:
    return QueryPlanSnapshotsRepository(session)


# --- Endpoints -------------------------------------------------------------------------------


@router.get("/active/{instance_id}", response_model=list[QuerySampleResponse])
async def list_active_queries(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[QuerySampleResponse]:
    """Lista amostras de query de uma instancia no periodo (sem `from`/`to`, usa a janela
    padrao `API_READ_DEFAULT_LOOKBACK_SECONDS`)."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.list_active(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    # `query_samples_table` ja inclui `instance_id` no select — NAO passar `instance_id=` aqui
    # tambem, colidiria com **row._mapping (TypeError de keyword duplicado).
    return [QuerySampleResponse(**row._mapping) for row in rows]


@router.get("/top-sql/{instance_id}", response_model=list[TopSqlEntryResponse])
async def top_sql(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    database_name: Annotated[str | None, Query()] = None,
    db_user: Annotated[str | None, Query()] = None,
    program: Annotated[str | None, Query()] = None,
    machine: Annotated[str | None, Query()] = None,
) -> list[TopSqlEntryResponse]:
    """Top SQL agregado de uma instancia, calculado em tempo real sobre `query_samples`
    (GROUP BY sql_hash). PostgreSQL: nao depende de `pg_stat_statements` (agrega o que o
    collector ja normalizou de `pg_stat_activity`). SQL Server: idem, via `sql_hash` normalizado
    pelo collector.

    `database_name` (nivel 2 do drill-down hierarquico: Instancia -> Banco -> Query) restringe a
    agregacao a um unico banco; `db_user` (nivel 2 da dimensao "Usuarios") restringe a um unico
    usuario; sem nenhum dos dois, mantem o comportamento historico de agregar tudo junto sob o
    mesmo `sql_hash`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_sql(
        instance_id,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
        database_name=database_name,
        db_user=db_user,
        program=program,
        machine=machine,
    )
    breakdown_rows = await repository.wait_breakdown_by_sql_hash(
        instance_id,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
        database_name=database_name,
        db_user=db_user,
        program=program,
        machine=machine,
    )
    breakdown_by_hash: dict[str, list[WaitBreakdownEntryResponse]] = {}
    for row in breakdown_rows:
        breakdown_by_hash.setdefault(row.sql_hash, []).append(
            WaitBreakdownEntryResponse(
                wait_event_native=row.wait_event_native,
                total_wait_time_ms=row.total_wait_time_ms,
            )
        )
    # Aqui a query de `top_sql` nao seleciona `instance_id` — sem colisao ao passar explicito.
    return [
        TopSqlEntryResponse(
            instance_id=instance_id,
            **row._mapping,
            wait_breakdown=breakdown_by_hash.get(row.sql_hash, []),
        )
        for row in rows
    ]


@router.get("/top-sql-by-wait-type/{instance_id}", response_model=list[TopSqlEntryResponse])
async def top_sql_by_wait_type(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    wait_event_native: Annotated[str, Query()],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[TopSqlEntryResponse]:
    """Drill-down a partir da folha da dimensao "Waits": quais queries mais consumiram um
    `wait_event_native` especifico (clicar num tipo de espera no `WaitTypeBarChart` leva aqui).
    `wait_breakdown` continua sendo o breakdown COMPLETO de cada query (todos os tipos de
    espera, nao so o filtrado) — mesmo padrao de `top_sql()`, da mais contexto sem custo extra."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_sql_by_wait_type(
        instance_id,
        wait_event_native,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
    )
    breakdown_rows = await repository.wait_breakdown_by_sql_hash(
        instance_id,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
    )
    breakdown_by_hash: dict[str, list[WaitBreakdownEntryResponse]] = {}
    for row in breakdown_rows:
        breakdown_by_hash.setdefault(row.sql_hash, []).append(
            WaitBreakdownEntryResponse(
                wait_event_native=row.wait_event_native,
                total_wait_time_ms=row.total_wait_time_ms,
            )
        )
    return [
        TopSqlEntryResponse(
            instance_id=instance_id,
            **row._mapping,
            wait_breakdown=breakdown_by_hash.get(row.sql_hash, []),
        )
        for row in rows
    ]


@router.get("/database-names/{instance_id}", response_model=list[str])
async def list_database_names(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
) -> list[str]:
    """Só os NOMES de banco distintos que essa instância teve recentemente — bem mais barato que
    `GET /databases/{instance_id}` (que agrega wait time/decomposição por wait nativo, incluindo
    a CTE de `effective_wait_ms`, cara sobre muito volume). Usado por seletores simples de banco
    (ex.: override ad-hoc de tempo mínimo de bloqueio, "Configurações → Geral") que só precisam
    da lista de nomes, não de estatística nenhuma.

    Padrão: últimas 24h — bug de performance real relatado pelo usuário ao vivo: mesmo sem a
    agregação cara, um `SELECT DISTINCT` sobre 30 dias de `query_samples` numa instância de muito
    volume (~940 mil linhas em 30 dias numa instância real observada) ainda levava ~9 SEGUNDOS —
    não há índice cobrindo `database_name`, então o scan lê a coluna de toda linha na janela.
    Medido ao vivo: 24h ~170ms, 7 dias ~850ms, 30 dias ~9s — a curva não é linear, piora rápido.
    24h também é mais coerente com o propósito da tela (override AD-HOC pra investigar algo
    acontecendo AGORA, não um banco sem atividade há semanas)."""
    resolved_since = since or (datetime.now(UTC) - timedelta(hours=24))
    return await repository.list_distinct_database_names(instance_id, resolved_since)


@router.get("/databases/{instance_id}", response_model=list[DatabaseSummaryResponse])
async def list_databases(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[DatabaseSummaryResponse]:
    """Nivel 1 do drill-down hierarquico (Instancia -> Banco -> Query): agrega `query_samples`
    por `database_name` (GROUP BY database_name), calculado em tempo real. Amostras sem
    `database_name` sao agrupadas sob a label 'desconhecido', nunca descartadas.

    Cada banco vem com `wait_breakdown`: a mesma soma de wait time decomposta por tipo de espera
    NATIVO do engine (`wait_event_native`) — para o gráfico de barra empilhada por banco mostrar
    exatamente quais waits (ex.: `PAGEIOLATCH_SH`, `WAITFOR`) compoem o total daquele banco."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_sql_by_database(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    breakdown_rows = await repository.wait_breakdown_by_database(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    breakdown_by_database: dict[str, list[WaitBreakdownEntryResponse]] = {}
    for row in breakdown_rows:
        breakdown_by_database.setdefault(row.database_name, []).append(
            WaitBreakdownEntryResponse(
                wait_event_native=row.wait_event_native,
                total_wait_time_ms=row.total_wait_time_ms,
            )
        )
    return [
        DatabaseSummaryResponse(
            **row._mapping, wait_breakdown=breakdown_by_database.get(row.database_name, [])
        )
        for row in rows
    ]


@router.get("/databases-by-day/{instance_id}", response_model=list[BucketEntryResponse])
async def list_databases_by_day(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Visao historica "Tudo" do nivel 1 (Instancia -> Banco -> Query): um dia por barra,
    empilhada por banco de dados — equivalente ao grafico multi-dia de ferramentas de
    referencia. `label` e `database_name`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_database(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="day"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.database_name,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/top-sql-by-day/{instance_id}", response_model=list[BucketEntryResponse])
async def list_top_sql_by_day(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    database_name: Annotated[str | None, Query()] = None,
    db_user: Annotated[str | None, Query()] = None,
    program: Annotated[str | None, Query()] = None,
    machine: Annotated[str | None, Query()] = None,
) -> list[BucketEntryResponse]:
    """Visao historica "Tudo" do nivel 2 (Instancia -> Banco -> Query, ou da dimensao
    "Usuarios"): um dia por barra, empilhada por query — mesmos `database_name`/`db_user`
    opcionais de `top_sql()`. `label` e `sql_hash` (o cliente resolve o rotulo curto/nome, esta
    rota nao traz `sql_name`/`sql_text_masked` — corte de escopo deliberado, o overview historico
    nao precisa do texto completo da query)."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_sql_hash(
        instance_id,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
        database_name=database_name,
        db_user=db_user,
        program=program,
        machine=machine,
        bucket="day",
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.sql_hash,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/databases-by-hour/{instance_id}", response_model=list[BucketEntryResponse])
async def list_databases_by_hour(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Detalhamento por hora do nivel 1, dentro de um dia especifico (`from`/`to` = o dia inteiro
    escolhido pelo filtro de um unico dia) — passo intermediario entre a visao "Tudo" (um dia por
    barra) e o grafico por banco de um dia inteiro. `label` e `database_name`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_database(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="hour"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.database_name,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/top-sql-by-hour/{instance_id}", response_model=list[BucketEntryResponse])
async def list_top_sql_by_hour(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    database_name: Annotated[str | None, Query()] = None,
    db_user: Annotated[str | None, Query()] = None,
    program: Annotated[str | None, Query()] = None,
    machine: Annotated[str | None, Query()] = None,
) -> list[BucketEntryResponse]:
    """Detalhamento por hora do nivel 2, dentro de um dia especifico — mesma ideia de
    `list_databases_by_hour`, empilhado por query em vez de banco (ou por usuario, quando
    `db_user` e informado). `label` e `sql_hash`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_sql_hash(
        instance_id,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
        database_name=database_name,
        db_user=db_user,
        program=program,
        machine=machine,
        bucket="hour",
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.sql_hash,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/wait-types/{instance_id}", response_model=list[WaitTypeSummaryResponse])
async def list_wait_types(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[WaitTypeSummaryResponse]:
    """Dimensao "Waits" do drill-down (irma de "Bancos de dados"/"SQL"): agrega `query_samples`
    por `wait_event_native` (GROUP BY), folha da propria dimensao — sem `wait_breakdown` (um tipo
    de espera nao se decompoe em mais nada)."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_wait_types(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    return [WaitTypeSummaryResponse(**row._mapping) for row in rows]


@router.get("/wait-types-by-day/{instance_id}", response_model=list[BucketEntryResponse])
async def list_wait_types_by_day(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Visao historica "Tudo" da dimensao "Waits": um dia por barra, empilhada por tipo de
    espera. `label` e `wait_event_native`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_wait_type(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="day"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.wait_event_native,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/wait-types-by-hour/{instance_id}", response_model=list[BucketEntryResponse])
async def list_wait_types_by_hour(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Detalhamento por hora da dimensao "Waits", dentro de um dia especifico — mesma ideia de
    `list_wait_types_by_day`, granularidade de hora."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_wait_type(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="hour"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.wait_event_native,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/users/{instance_id}", response_model=list[DbUserSummaryResponse])
async def list_db_users(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[DbUserSummaryResponse]:
    """Dimensao "Usuarios" do drill-down (irma de "Bancos de dados"/"SQL"/"Waits"): agrega
    `query_samples` por `db_user`, calculado em tempo real. Amostras sem `db_user` sao agrupadas
    sob a label 'desconhecido', nunca descartadas.

    Cada usuario vem com `wait_breakdown`: a mesma soma de wait time decomposta por tipo de
    espera NATIVO do engine — mesmo padrao de `list_databases`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_users(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    breakdown_rows = await repository.wait_breakdown_by_db_user(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    breakdown_by_user: dict[str, list[WaitBreakdownEntryResponse]] = {}
    for row in breakdown_rows:
        breakdown_by_user.setdefault(row.db_user, []).append(
            WaitBreakdownEntryResponse(
                wait_event_native=row.wait_event_native,
                total_wait_time_ms=row.total_wait_time_ms,
            )
        )
    return [
        DbUserSummaryResponse(**row._mapping, wait_breakdown=breakdown_by_user.get(row.db_user, []))
        for row in rows
    ]


@router.get("/users-by-day/{instance_id}", response_model=list[BucketEntryResponse])
async def list_db_users_by_day(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Visao historica "Tudo" da dimensao "Usuarios": um dia por barra, empilhada por usuario.
    `label` e `db_user`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_db_user(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="day"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.db_user,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/users-by-hour/{instance_id}", response_model=list[BucketEntryResponse])
async def list_db_users_by_hour(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Detalhamento por hora da dimensao "Usuarios", dentro de um dia especifico — mesma ideia
    de `list_db_users_by_day`, granularidade de hora."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_db_user(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="hour"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.db_user,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/programs/{instance_id}", response_model=list[ProgramSummaryResponse])
async def list_programs(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[ProgramSummaryResponse]:
    """Dimensao "Programs" do drill-down: agrega `query_samples` por `program`, com decomposicao
    por wait nativo — mesmo padrao de `list_databases`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_programs(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    breakdown_rows = await repository.wait_breakdown_by_program(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    breakdown_by_program: dict[str, list[WaitBreakdownEntryResponse]] = {}
    for row in breakdown_rows:
        breakdown_by_program.setdefault(row.program, []).append(
            WaitBreakdownEntryResponse(
                wait_event_native=row.wait_event_native,
                total_wait_time_ms=row.total_wait_time_ms,
            )
        )
    return [
        ProgramSummaryResponse(
            **row._mapping, wait_breakdown=breakdown_by_program.get(row.program, [])
        )
        for row in rows
    ]


@router.get("/programs-by-day/{instance_id}", response_model=list[BucketEntryResponse])
async def list_programs_by_day(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Visao historica "Tudo" da dimensao "Programs": um dia por barra, empilhada por programa.
    `label` e `program`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_program(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="day"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.program,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/programs-by-hour/{instance_id}", response_model=list[BucketEntryResponse])
async def list_programs_by_hour(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Detalhamento por hora da dimensao "Programs", dentro de um dia especifico."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_program(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="hour"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.program,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/machines/{instance_id}", response_model=list[MachineSummaryResponse])
async def list_machines(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[MachineSummaryResponse]:
    """Dimensao "Machines" do drill-down: agrega `query_samples` por `machine`, com decomposicao
    por wait nativo — mesmo padrao de `list_databases`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.top_machines(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    breakdown_rows = await repository.wait_breakdown_by_machine(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows
    )
    breakdown_by_machine: dict[str, list[WaitBreakdownEntryResponse]] = {}
    for row in breakdown_rows:
        breakdown_by_machine.setdefault(row.machine, []).append(
            WaitBreakdownEntryResponse(
                wait_event_native=row.wait_event_native,
                total_wait_time_ms=row.total_wait_time_ms,
            )
        )
    return [
        MachineSummaryResponse(
            **row._mapping, wait_breakdown=breakdown_by_machine.get(row.machine, [])
        )
        for row in rows
    ]


@router.get("/machines-by-day/{instance_id}", response_model=list[BucketEntryResponse])
async def list_machines_by_day(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Visao historica "Tudo" da dimensao "Machines": um dia por barra, empilhada por maquina.
    `label` e `machine`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_machine(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="day"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.machine,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/machines-by-hour/{instance_id}", response_model=list[BucketEntryResponse])
async def list_machines_by_hour(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Detalhamento por hora da dimensao "Machines", dentro de um dia especifico."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_time_by_bucket_and_machine(
        instance_id, resolved_since, resolved_until, settings.api_read_max_rows, bucket="hour"
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=row.machine,
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


_TIMESLICE_DIMENSIONS = {
    "database": ("wait_time_by_bucket_and_database", "database_name"),
    "sql": ("wait_time_by_bucket_and_sql_hash", "sql_hash"),
    "wait_type": ("wait_time_by_bucket_and_wait_type", "wait_event_native"),
    "db_user": ("wait_time_by_bucket_and_db_user", "db_user"),
    "program": ("wait_time_by_bucket_and_program", "program"),
    "machine": ("wait_time_by_bucket_and_machine", "machine"),
}


@router.get("/timeslice/{instance_id}", response_model=list[BucketEntryResponse])
async def list_timeslice(
    instance_id: UUID,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime, Query(alias="from")],
    until: Annotated[datetime, Query(alias="to")],
    dimension: Annotated[
        Literal["database", "sql", "wait_type", "db_user", "program", "machine"], Query()
    ] = "database",
    bucket: Annotated[
        Literal["ten_minutes", "minute", "ten_seconds", "second"], Query()
    ] = "ten_minutes",
    database_name: Annotated[str | None, Query()] = None,
    db_user: Annotated[str | None, Query()] = None,
    wait_event_native: Annotated[str | None, Query()] = None,
    program: Annotated[str | None, Query()] = None,
    machine: Annotated[str | None, Query()] = None,
) -> list[BucketEntryResponse]:
    """ "Fatia de Tempo" — drill-down temporal fino DENTRO de uma janela já estreita (uma hora, um
    intervalo de 10min, etc.). Generaliza os 4 `wait_time_by_bucket_and_*` já existentes (que hoje só
    aceitavam `bucket="day"/"hour"`) pras 4 granularidades sub-hora — um único endpoint com
    `dimension`/`bucket` como parâmetro, em vez de 16 rotas (`4 dimensões × 4 granularidades`).

    `database_name`/`db_user`/`wait_event_native` são aditivos, só usados quando
    `dimension="sql"` — permitem a "Fatia de Tempo" também nos níveis 2 do drill-down (dentro de
    um banco, de um usuário, ou a partir da folha da dimensão "Waits"), não só na dimensão "SQL"
    da instância inteira. Ignorados silenciosamente pras demais dimensões (não faria sentido
    filtrar "por banco" agrupado por banco, por exemplo).

    `from`/`to` são OBRIGATÓRIOS aqui (diferente das demais rotas deste arquivo, que caem pro
    lookback padrão) — a "Fatia de Tempo" sempre ancora num período já escolhido pelo usuário
    (a hora/bucket clicado no nível anterior do drill-down), nunca "os últimos N segundos"."""
    repository_method_name, label_column = _TIMESLICE_DIMENSIONS[dimension]
    repository_method = getattr(repository, repository_method_name)
    settings = get_settings()
    extra_kwargs: dict = {}
    if dimension == "sql":
        extra_kwargs = {
            "database_name": database_name,
            "db_user": db_user,
            "wait_event_native": wait_event_native,
            "program": program,
            "machine": machine,
        }
    elif dimension == "program":
        extra_kwargs = {}
    elif dimension == "machine":
        extra_kwargs = {}
    rows = await repository_method(
        instance_id, since, until, settings.api_read_max_rows, bucket=bucket, **extra_kwargs
    )
    return [
        BucketEntryResponse(
            bucket_start=row.bucket_start,
            label=getattr(row, label_column),
            total_wait_time_ms=row.total_wait_time_ms,
        )
        for row in rows
    ]


@router.get("/detail/{instance_id}/{sql_hash}", response_model=QueryDetailResponse)
async def get_query_detail(
    instance_id: UUID,
    sql_hash: str,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> QueryDetailResponse:
    """Nivel 3 (folha) do drill-down hierarquico: amostras brutas de um `sql_hash` especifico
    dentro de uma instancia, no periodo. 404 quando nao ha nenhuma amostra desse hash no
    periodo."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.query_detail(
        instance_id, sql_hash, resolved_since, resolved_until, settings.api_read_max_rows
    )
    if not rows:
        raise HTTPException(
            status_code=404, detail="Nenhuma amostra encontrada para essa query no periodo."
        )
    first = rows[0]
    wait_breakdown_rows = await repository.wait_breakdown_by_sql_hash(
        instance_id,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
        sql_hash=sql_hash,
    )
    return QueryDetailResponse(
        instance_id=instance_id,
        sql_hash=sql_hash,
        sql_name=first.sql_name,
        sql_text_masked=first.sql_text_masked,
        sql_text_full_masked=first.sql_text_full_masked,
        database_name=first.database_name,
        samples=[
            QuerySampleDetailResponse(
                **{
                    k: v
                    for k, v in r._mapping.items()
                    if k in QuerySampleDetailResponse.model_fields
                }
            )
            for r in rows
        ],
        wait_breakdown=[
            WaitBreakdownEntryResponse(
                wait_event_native=row.wait_event_native,
                total_wait_time_ms=row.total_wait_time_ms,
            )
            for row in wait_breakdown_rows
        ],
    )


@router.get("/detail/{instance_id}/{sql_hash}/stats", response_model=list[QueryStatsBucketResponse])
async def get_query_stats_by_bucket(
    instance_id: UUID,
    sql_hash: str,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    bucket: Annotated[str, Query()] = "day",
) -> list[QueryStatsBucketResponse]:
    """Execucoes + tempo de espera por bucket de tempo, para o grafico "Execucoes x tempo de
    espera por execucao" do painel de estatisticas (Etapa 11). Respeita o mesmo periodo
    (`from`/`to`) selecionado no drill-down de onde a tela de detalhe foi aberta — cai para
    "Tudo" (`ALL_TIME_FROM_ISO` ate agora) so quando ausente (link direto/favorito).
    `bucket="ten_minutes"` (a pedido explicito do usuario): quando o periodo em analise ja e uma
    unica hora, bucket "hour" produziria 1 barra so — 10 em 10 minutos aproveita a granularidade
    real da coleta (~1-10s) sem virar ruido."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    bucket_literal: Literal["day", "hour", "ten_minutes"]
    if bucket == "hour":
        bucket_literal = "hour"
    elif bucket == "ten_minutes":
        bucket_literal = "ten_minutes"
    else:
        bucket_literal = "day"
    rows = await repository.query_stats_by_bucket(
        instance_id,
        sql_hash,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
        bucket=bucket_literal,
    )
    return [
        QueryStatsBucketResponse(
            bucket_start=row.bucket_start,
            execution_count=row.execution_count,
            total_wait_time_ms=row.total_wait_time_ms,
            avg_wait_time_ms=(
                row.total_wait_time_ms / row.execution_count if row.execution_count else 0.0
            ),
        )
        for row in rows
    ]


@router.get(
    "/detail/{instance_id}/{sql_hash}/sql-insight", response_model=SqlInsightAnalysisResponse
)
async def get_query_sql_insight_analysis(
    instance_id: UUID,
    sql_hash: str,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    sql_insight_client: Annotated[SqlInsightClient, Depends(get_sql_insight_client)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> SqlInsightAnalysisResponse:
    """Analise via AB SQL Insight (sistema externo, Etapa 10 item 6) — painel isolado na tela de
    detalhe de query, com o proprio carregamento independente do resto da pagina. Endpoint
    separado do detalhe principal deliberadamente, pra nao atrasar/acoplar o carregamento do
    resto da tela a disponibilidade de um sistema externo. Mesmo `from`/`to` (tipicamente "Tudo")
    ja resolvido pela pagina de detalhe — sem isso, o lookback padrao (1h) poderia nao encontrar
    nenhuma amostra recente."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.query_detail(instance_id, sql_hash, resolved_since, resolved_until, 1)
    if not rows:
        raise HTTPException(
            status_code=404, detail="Nenhuma amostra encontrada para essa query no periodo."
        )
    analysis = await sql_insight_client.check(rows[0].sql_text_masked)
    return SqlInsightAnalysisResponse(
        available=analysis.available,
        rules=[
            SqlInsightRuleResponse(
                rule_id=rule.rule_id, description=rule.description, details=rule.details
            )
            for rule in analysis.rules
        ],
    )


@router.get("/detail/{instance_id}/{sql_hash}/execution-plan", response_model=ExecutionPlanResponse)
async def get_query_execution_plan(
    instance_id: UUID,
    sql_hash: str,
    _access: Annotated[None, Depends(check_instance_access)],
    instances_repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    credential_resolver: Annotated[CredentialResolver, Depends(get_credential_resolver)],
) -> ExecutionPlanResponse:
    """Busca sob demanda (nao parte do ciclo de coleta) do plano de execucao do SQL Server que
    corresponde a este `sql_hash`. Fluxo: plano REAL (sessao ativa) → cache de plano → Query
    Store (Item 10, bancos onde o hash foi visto nos ultimos 30 dias). PostgreSQL: `available=False`
    direto, sem tentar conectar."""
    instance = await instances_repository.get_by_id(instance_id)
    if instance is None:
        raise HTTPException(status_code=404, detail="Instancia nao encontrada.")
    settings = get_settings()
    since = datetime.now(UTC) - timedelta(seconds=settings.api_read_default_lookback_seconds)
    database_names = await repository.list_databases_for_hash(instance_id, sql_hash, since)
    result = await fetch_execution_plan(instance, sql_hash, credential_resolver, database_names)
    return ExecutionPlanResponse(
        available=result.available,
        plan_xml=result.plan_xml,
        reason=result.reason,
        source=result.source,
    )


@router.get(
    "/detail/{instance_id}/{sql_hash}/plan-history", response_model=list[PlanHistoryEntryResponse]
)
async def get_query_plan_history(
    instance_id: UUID,
    sql_hash: str,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[
        QueryPlanSnapshotsRepository, Depends(get_query_plan_snapshots_repository)
    ],
) -> list[PlanHistoryEntryResponse]:
    """Historico de planos DISTINTOS vistos em varreduras periodicas do cache
    (`PlanHistorySupervisor`, so SQL Server, desligado por padrao) para esta query — mais
    recente primeiro. Lista vazia quando o worker esta desligado ou nunca viu esta query com
    plano no cache — nunca um erro, e a tela trata como "sem historico disponivel ainda"."""
    settings = get_settings()
    rows = await repository.list_for_query(instance_id, sql_hash, settings.api_read_max_rows)
    return [
        PlanHistoryEntryResponse(
            captured_at=row.captured_at,
            plan_hash=row.plan_hash,
            plan_xml=row.plan_xml_masked,
            last_execution_time=row.last_execution_time,
        )
        for row in rows
    ]


# ── Recomendações (sql_advisor + plan_advisor + ai_advisor) ──────────────────────────────────


class SqlFindingResponse(BaseModel):
    rule_id: str
    severity: str
    description: str
    suggestion: str


class PlanFindingResponse(BaseModel):
    finding_id: str
    severity: str
    description: str
    suggestion: str
    operator: str | None = None
    object_name: str | None = None


class AiAnalysisResponse(BaseModel):
    configured: bool
    narrative: str | None = None


class RecommendationsResponse(BaseModel):
    sql_findings: list[SqlFindingResponse]
    plan_available: bool
    plan_findings: list[PlanFindingResponse]
    ai: AiAnalysisResponse


@router.get(
    "/detail/{instance_id}/{sql_hash}/recommendations",
    response_model=RecommendationsResponse,
)
async def get_query_recommendations(
    instance_id: UUID,
    sql_hash: str,
    _access: Annotated[None, Depends(check_instance_access)],
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    plan_repository: Annotated[
        QueryPlanSnapshotsRepository, Depends(get_query_plan_snapshots_repository)
    ],
    instances_repository: Annotated[InstancesRepository, Depends(get_instances_repository)],
    ai_advisor: Annotated[AiAdvisor | MultiKeyAiAdvisor, Depends(get_ai_advisor)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> RecommendationsResponse:
    """Recomendações contextuais: regras estáticas (sql_advisor), análise de plano (plan_advisor)
    e diagnóstico opcional por IA (ai_advisor). Endpoint isolado — carregamento independente do
    resto da tela de detalhe, não atrasa o carregamento principal.

    Retorna sempre (nunca 404): sql_findings e plan_findings via análise local; ai.configured
    indica se a chave de IA está configurada independente de haver ou não achados."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )

    # Busca amostra para obter sql_text_masked e db_type.
    # Fallback: se não houver amostras na janela solicitada (ex.: query de ontem vista com filtro
    # "última hora"), busca nos últimos 30 dias para conseguir o texto SQL e ainda gerar diagnóstico.
    rows = await repository.query_detail(instance_id, sql_hash, resolved_since, resolved_until, 1)
    if not rows:
        fallback_since = datetime.now(UTC) - timedelta(days=30)
        rows = await repository.query_detail(instance_id, sql_hash, fallback_since, datetime.now(UTC), 1)
    if not rows:
        return RecommendationsResponse(
            sql_findings=[],
            plan_available=False,
            plan_findings=[],
            ai=AiAnalysisResponse(configured=ai_advisor.is_configured),
        )

    sample = rows[0]
    sql_text = sample.sql_text_masked or ""
    object_text = sample.sql_text_full_masked or None

    # Determina db_type a partir da instância
    instance = await instances_repository.get_by_id(instance_id)
    db_type = instance.db_type if instance else "sqlserver"

    # ── 1. Análise estática de SQL ────────────────────────────────────────────────────
    raw_sql_findings = analyze_sql(sql_text, db_type=db_type)
    sql_findings = [
        SqlFindingResponse(
            rule_id=f.rule_id,
            severity=f.severity,
            description=f.description,
            suggestion=f.suggestion,
        )
        for f in raw_sql_findings
    ]

    # ── 2. Análise do plano de execução ───────────────────────────────────────────────
    plan_available = False
    plan_findings: list[PlanFindingResponse] = []

    plan_rows = await plan_repository.list_for_query(instance_id, sql_hash, limit=1)
    if plan_rows:
        plan_available = True
        raw_plan_findings = analyze_plan_by_format(
            plan_rows[0].plan_xml_masked or "",
            getattr(plan_rows[0], "plan_format", "sqlserver_xml"),
        )
        plan_findings = [
            PlanFindingResponse(
                finding_id=f.finding_id,
                severity=f.severity,
                description=f.description,
                suggestion=f.suggestion,
                operator=f.operator,
                object_name=f.object_name,
            )
            for f in raw_plan_findings
        ]

    # ── 3. Diagnóstico por IA (opcional, BYOK) ────────────────────────────────────────
    avg_duration = getattr(sample, "duration_ms", None)
    ai_result = await ai_advisor.analyze(
        sql_text_masked=sql_text,
        db_type=db_type,
        avg_duration_ms=float(avg_duration) if avg_duration is not None else None,
        sql_findings=[f.model_dump() for f in sql_findings],
        plan_findings=[f.model_dump() for f in plan_findings],
        object_text_masked=object_text,
    )

    return RecommendationsResponse(
        sql_findings=sql_findings,
        plan_available=plan_available,
        plan_findings=plan_findings,
        ai=AiAnalysisResponse(
            configured=ai_result.configured,
            narrative=ai_result.narrative,
        ),
    )
