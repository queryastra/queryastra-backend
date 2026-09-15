"""Endpoints de queries ativas / top SQL / drill-down hierarquico por banco de dados.

Destino de dados: hypertable bruta `query_samples` (`Docs/database-schema.md` §3), consultada
diretamente — o continuous aggregate `query_stats_hourly` foi criado `WITH NO DATA` e sem
policy de refresh, esta vazio e nunca atualizado, portanto nunca e usado aqui (a agregacao de
"Top SQL" e feita na propria query contra `query_samples`). Texto de SQL retornado a UI e sempre
`sql_text_masked` (`Docs/rules/03-sql-privacy.md`) — o texto original cifrado so e acessivel pelo
fluxo de "revelar" auditado da Fase 2 (RBAC), fora de escopo deste MVP.

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
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session, resolve_time_window
from app.core.config import get_settings
from app.repositories.query_samples_repository import QuerySamplesRepository

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
    """Uma amostra bruta de execucao, dentro do detalhe de uma query (nivel 3/folha)."""

    time: datetime
    duration_ms: int | None
    wait_type: str | None
    wait_event_native: str | None
    wait_time_ms: int | None
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


# --- Dependency providers -------------------------------------------------------------------


def get_query_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> QuerySamplesRepository:
    return QuerySamplesRepository(session)


# --- Endpoints -------------------------------------------------------------------------------


@router.get("/active/{instance_id}", response_model=list[QuerySampleResponse])
async def list_active_queries(
    instance_id: UUID,
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
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    database_name: Annotated[str | None, Query()] = None,
) -> list[TopSqlEntryResponse]:
    """Top SQL agregado de uma instancia, calculado em tempo real sobre `query_samples`
    (GROUP BY sql_hash). PostgreSQL: nao depende de `pg_stat_statements` (agrega o que o
    collector ja normalizou de `pg_stat_activity`). SQL Server: idem, via `sql_hash` normalizado
    pelo collector.

    `database_name` (nivel 2 do drill-down hierarquico: Instancia -> Banco -> Query) restringe a
    agregacao a um unico banco; sem o parametro, mantem o comportamento historico de agregar
    todos os bancos juntos sob o mesmo `sql_hash`."""
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
    )
    breakdown_rows = await repository.wait_breakdown_by_sql_hash(
        instance_id,
        resolved_since,
        resolved_until,
        settings.api_read_max_rows,
        database_name=database_name,
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


@router.get("/databases/{instance_id}", response_model=list[DatabaseSummaryResponse])
async def list_databases(
    instance_id: UUID,
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
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Visao historica "Tudo" do nivel 1 (Instancia -> Banco -> Query): um dia por barra,
    empilhada por banco de dados — equivalente ao grafico multi-dia de ferramentas de
    referencia (ex.: SolarWinds DPA "Top SQL" por periodo). `label` e `database_name`."""
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
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    database_name: Annotated[str | None, Query()] = None,
) -> list[BucketEntryResponse]:
    """Visao historica "Tudo" do nivel 2 (Instancia -> Banco -> Query): um dia por barra,
    empilhada por query — mesmo `database_name` opcional de `top_sql()`. `label` e `sql_hash`
    (o cliente resolve o rotulo curto/nome, esta rota nao traz `sql_name`/`sql_text_masked` —
    corte de escopo deliberado, o overview historico nao precisa do texto completo da query)."""
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
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BucketEntryResponse]:
    """Detalhamento por hora do nivel 1, dentro de um dia especifico (`from`/`to` = o dia inteiro
    escolhido pelo filtro de um unico dia) — passo intermediario entre a visao "Tudo" (um dia por
    barra) e o grafico por banco de um dia inteiro, equivalente ao "Interval: 1 hour" de
    ferramentas de referencia (ex.: SolarWinds DPA "Top SQL"). `label` e `database_name`."""
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
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
    database_name: Annotated[str | None, Query()] = None,
) -> list[BucketEntryResponse]:
    """Detalhamento por hora do nivel 2, dentro de um dia especifico — mesma ideia de
    `list_databases_by_hour`, empilhado por query em vez de banco. `label` e `sql_hash`."""
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


@router.get("/detail/{instance_id}/{sql_hash}", response_model=QueryDetailResponse)
async def get_query_detail(
    instance_id: UUID,
    sql_hash: str,
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
    )
