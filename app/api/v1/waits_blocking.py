"""Endpoints de waits e bloqueios.

Bloqueio (redesenhado — Etapa 10): le de `blocking_incidents` (tabela relacional, log de
INCIDENTE retido — `app/models/blocking_incident.py`), nao mais da hypertable bruta
`blocking_events` (que continua existindo, insert-only, so como log cru por ciclo — ver docstring
do modelo). Alimentado por `pg_blocking_pids()` (PostgreSQL, reduzido pro bloqueador direto —
`pg_blocking_pids` devolve o fecho transitivo, ver
`app.collectors.postgresql.adapter._reduce_to_direct_edges`) ou
`sys.dm_exec_requests.blocking_session_id` (SQL Server) — `Docs/collector-queries.md` §2. Wait:
hypertable bruta `query_samples`, agregada por categoria canonica
(`app.normalizers.wait_taxonomy.WaitCategory`) em tempo real — nenhum endpoint aqui consulta os
continuous aggregates (`query_stats_hourly`), vazios e nunca atualizados.

`GET /blocking/{instance_id}` devolve uma arvore recursiva (`BlockingTreeNodeResponse`) so dos
incidentes AINDA EM ANDAMENTO (`ended_at IS NULL`) — cada sessao bloqueadora raiz com seus
bloqueados aninhados em `children`, encadeando as arestas diretas do conjunto aberto agora (Etapa
9: arvore completa, nao so 1 nivel). Identidade/wait nativo/recurso bloqueado/inicio de transacao
vem denormalizado no proprio `blocking_incidents` (capturado no momento da coleta) — so o texto
mascarado do comando continua vindo de um join de leitura contra `query_samples`
(`QuerySamplesRepository.latest_by_session`), que nao guarda o dado no evento (so o hash).

`GET /blocking/{instance_id}/incidents` e uma lista PLANA (nao arvore) para investigacao/
historico — inclui incidentes ja encerrados (retidos ate `settings.problem_events_retention_days`),
com filtro opcional `open_only`/`database_name`.

`GET /blocking/{instance_id}/session/{session_id}` e a folha do drill-down (tela de detalhe) —
resolve incidentes envolvendo essa sessao em andamento OU ja encerrados (dentro da retencao), nao
mais exigindo que a sessao apareca no snapshot atual.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import UTC, datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import Row
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session, resolve_time_window
from app.core.config import get_settings
from app.models.blocking_incident import BlockingIncident
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository
from app.repositories.query_samples_repository import QuerySamplesRepository

router = APIRouter()


class BlockingTreeNodeResponse(BaseModel):
    """Um no da arvore de bloqueio: uma sessao (bloqueadora e/ou bloqueada) e seus bloqueados
    diretos aninhados em `children`.

    `waited_ms` e o tempo que ESSA sessao esperou (None quando ela nunca aparece como bloqueada —
    um bloqueador puro, incluindo o caso "ocioso" do print do SolarWinds DPA que inspirou esta
    tela). `caused_ms` e a soma recursiva de `waited_ms` de toda a subarvore de descendentes —
    quanto tempo, no total, essa sessao e responsavel por travar por baixo dela (mesma semantica
    de "Blocking Time Caused/Waited" do DPA).
    """

    session_id: str
    sql_hash: str | None
    sql_text_masked: str | None
    db_user: str | None
    program: str | None
    machine: str | None
    wait_event_native: str | None
    wait_resource: str | None
    blocked_object: str | None
    # Etapa 10 (rastreamento de transacoes) — SQL Server: `sys.dm_tran_session_transactions.
    # transaction_id`; PostgreSQL: o proprio session_id (sem conceito de transacao nomeada
    # separada da sessao). Liga pra `GET /transactions/{instance_id}/{transaction_identifier}`.
    transaction_id: str | None
    # A pedido explicito do usuario: banco de dados dessa sessao — ver docstring de
    # `NormalizedBlockingEvent` para a logica de fallback do lado bloqueador ocioso (SQL Server).
    database_name: str | None
    # A pedido explicito do usuario: inicio da PROPRIA transacao dessa sessao, e desde quando o
    # INCIDENTE de bloqueio em si esta em andamento (arvore so mostra incidentes com
    # `ended_at IS NULL` — todo `started_at` aqui e "ainda aberto").
    transaction_begin_time: datetime | None
    started_at: datetime | None
    waited_ms: int | None
    caused_ms: int
    children: list[BlockingTreeNodeResponse] = Field(default_factory=list)


BlockingTreeNodeResponse.model_rebuild()  # necessario para tipo recursivo com forward ref


class BlockingEdgeResponse(BaseModel):
    """Uma aresta de bloqueio envolvendo a sessao consultada em `SessionBlockingDetailResponse`
    — ou uma sessao que ela bloqueia (`blocking`), ou uma que a bloqueia (`blocked_by`).

    `started_at`/`ended_at` (a pedido explicito do usuario): ciclo de vida do INCIDENTE — `None`
    em `ended_at` significa "ainda em andamento". `transaction_begin_time`: inicio da transacao
    da OUTRA sessao (a que aparece em `session_id` abaixo, nao a consultada)."""

    session_id: str
    db_user: str | None
    program: str | None
    machine: str | None
    wait_time_ms: int
    transaction_begin_time: datetime | None
    started_at: datetime
    ended_at: datetime | None


class SessionBlockingDetailResponse(BaseModel):
    """Folha do drill-down de bloqueio: identidade, comando (quando houver amostra recente em
    `query_samples` — sessao ociosa sem nenhuma amostra fica com `sql_text_masked=None`, nao
    inventa dado) e as arestas de bloqueio (em andamento OU ja encerradas, dentro da janela de
    retencao) que envolvem essa sessao."""

    session_id: str
    db_user: str | None
    program: str | None
    machine: str | None
    wait_event_native: str | None
    wait_resource: str | None
    blocked_object: str | None
    transaction_id: str | None
    database_name: str | None
    sql_hash: str | None
    sql_text_masked: str | None
    sql_text_full_masked: str | None
    blocking: list[BlockingEdgeResponse]
    blocked_by: list[BlockingEdgeResponse]


class BlockingIncidentResponse(BaseModel):
    """Uma linha da lista PLANA de incidentes (`GET /blocking/{instance_id}/incidents`) — para
    investigacao/historico, sem nesting de arvore (misturar incidentes de janelas de tempo bem
    diferentes numa arvore so seria enganoso, ver `session_id` como identidade nao-permanente)."""

    blocker_session_id: str
    blocked_session_id: str
    blocker_database_name: str | None
    blocked_database_name: str | None
    blocker_db_user: str | None
    blocked_db_user: str | None
    blocker_program: str | None
    blocked_program: str | None
    blocker_transaction_id: str | None
    blocked_transaction_id: str | None
    blocker_transaction_begin_time: datetime | None
    blocked_transaction_begin_time: datetime | None
    blocked_wait_event_native: str | None
    wait_time_ms: int
    started_at: datetime
    last_seen_at: datetime
    ended_at: datetime | None

    model_config = {"from_attributes": True}


class WaitSummaryEntryResponse(BaseModel):
    """Resumo de wait por categoria canonica para uma instancia num periodo."""

    wait_type: str  # WaitCategory
    total_wait_time_ms: int


# --- Dependency providers -------------------------------------------------------------------


def get_blocking_incidents_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> BlockingIncidentsRepository:
    return BlockingIncidentsRepository(session)


def get_query_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> QuerySamplesRepository:
    return QuerySamplesRepository(session)


# --- Montagem da arvore ------------------------------------------------------------------------


_SessionInfo = dict[str, object]
"""Campos denormalizados (`db_user`/`program`/`machine`/`wait_event_native`/`wait_resource`/
`blocked_object`/`transaction_begin_time`) de uma sessao, coletados a partir das arestas em que
ela aparece — ver `_collect_session_info`."""


def _collect_session_info(events: list[BlockingIncident]) -> dict[str, _SessionInfo]:
    """Junta os campos denormalizados `blocker_*`/`blocked_*` de cada incidente numa unica
    entrada POR SESSAO — uma sessao no meio de uma cadeia aparece como bloqueadora numa aresta e
    como bloqueada em outra; o lado bloqueado tem mais detalhe (`wait_resource`/`blocked_object`,
    so faz sentido pra quem esta de fato esperando por um recurso), entao ele tem prioridade
    quando os dois lados discordarem (nao deveriam, mesmo conjunto aberto, mas por seguranca)."""
    info: dict[str, _SessionInfo] = {}
    for e in events:
        blocker = info.setdefault(e.blocker_session_id, {})
        blocker.setdefault("db_user", e.blocker_db_user)
        blocker.setdefault("program", e.blocker_program)
        blocker.setdefault("machine", e.blocker_machine)
        blocker.setdefault("wait_event_native", e.blocker_wait_event_native)
        blocker.setdefault("transaction_id", e.blocker_transaction_id)
        blocker.setdefault("database_name", e.blocker_database_name)
        blocker.setdefault("transaction_begin_time", e.blocker_transaction_begin_time)

        blocked = info.setdefault(e.blocked_session_id, {})
        blocked["db_user"] = e.blocked_db_user or blocked.get("db_user")
        blocked["program"] = e.blocked_program or blocked.get("program")
        blocked["machine"] = e.blocked_machine or blocked.get("machine")
        blocked["wait_event_native"] = e.blocked_wait_event_native or blocked.get(
            "wait_event_native"
        )
        blocked["wait_resource"] = e.blocked_wait_resource or blocked.get("wait_resource")
        blocked["blocked_object"] = e.blocked_object or blocked.get("blocked_object")
        blocked["transaction_id"] = e.blocked_transaction_id or blocked.get("transaction_id")
        blocked["database_name"] = e.blocked_database_name or blocked.get("database_name")
        blocked["transaction_begin_time"] = e.blocked_transaction_begin_time or blocked.get(
            "transaction_begin_time"
        )
    return info


def _build_blocking_tree(
    events: list[BlockingIncident], enrichment: dict[str, Row]
) -> list[BlockingTreeNodeResponse]:
    """Monta a arvore completa de bloqueio a partir dos incidentes EM ANDAMENTO (uma linha de
    `blocking_incidents` por aresta) — encadeia quantos niveis existirem quando um bloqueador
    tambem aparece como bloqueado em outra linha do mesmo conjunto aberto (SQL Server: sempre 1
    bloqueador por linha; PostgreSQL: ja reduzido pro bloqueador direto pelo coletor, ver
    `app.collectors.postgresql.adapter._reduce_to_direct_edges`)."""
    children_by_blocker: dict[str, list[BlockingIncident]] = defaultdict(list)
    blocked_ids: set[str] = set()
    blocker_sql_hash_by_session: dict[str, str | None] = {}
    for e in events:
        children_by_blocker[e.blocker_session_id].append(e)
        blocked_ids.add(e.blocked_session_id)
        blocker_sql_hash_by_session[e.blocker_session_id] = e.blocker_sql_hash

    session_info = _collect_session_info(events)

    def node_for(
        session_id: str,
        sql_hash: str | None,
        waited_ms: int | None,
        started_at: datetime | None,
    ) -> BlockingTreeNodeResponse:
        query_info = enrichment.get(session_id)
        info = session_info.get(session_id, {})
        children = [
            node_for(c.blocked_session_id, c.blocked_sql_hash, c.wait_time_ms, c.started_at)
            for c in children_by_blocker.get(session_id, [])
        ]
        # "Caused" (impacto total dessa sessao na cadeia): soma, recursivamente, o tempo que
        # cada descendente esperou por causa dela — quanto mais fundo/largo o bloqueio que ela
        # origina, maior o numero, igual a semantica do DPA.
        caused_ms = sum((c.waited_ms or 0) + c.caused_ms for c in children)
        return BlockingTreeNodeResponse(
            session_id=session_id,
            sql_hash=sql_hash,
            sql_text_masked=query_info.sql_text_masked if query_info else None,
            db_user=info.get("db_user"),
            program=info.get("program"),
            machine=info.get("machine"),
            wait_event_native=info.get("wait_event_native"),
            wait_resource=info.get("wait_resource"),
            blocked_object=info.get("blocked_object"),
            transaction_id=info.get("transaction_id"),
            database_name=info.get("database_name"),
            transaction_begin_time=info.get("transaction_begin_time"),
            started_at=started_at,
            waited_ms=waited_ms,
            caused_ms=caused_ms,
            children=children,
        )

    roots = [sid for sid in children_by_blocker if sid not in blocked_ids]
    # Raiz nunca aparece como bloqueada (senao nao seria raiz) — `waited_ms=None`, nao `0`: "0ms
    # de espera" seria enganoso pra quem nunca esperou nada (o "idle blocker" do print do DPA).
    # `started_at` da raiz e o mais antigo entre os incidentes que ela origina (ela mesma nunca
    # tem uma aresta "de entrada" — nao e bloqueada por ninguem, senao nao seria raiz).
    return [
        node_for(
            r,
            blocker_sql_hash_by_session[r],
            None,
            min((c.started_at for c in children_by_blocker[r]), default=None),
        )
        for r in roots
    ]


# --- Endpoints -------------------------------------------------------------------------------


@router.get("/blocking/{instance_id}", response_model=list[BlockingTreeNodeResponse])
async def list_blocking_events(
    instance_id: UUID,
    blocking_repo: Annotated[
        BlockingIncidentsRepository, Depends(get_blocking_incidents_repository)
    ],
    query_repo: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
) -> list[BlockingTreeNodeResponse]:
    """Arvore de bloqueio — so os incidentes AINDA EM ANDAMENTO (`ended_at IS NULL`). Sem janela
    de tempo: "em andamento" e um estado atual, nao um recorte de periodo (histórico/incidentes
    encerrados: `GET /blocking/{instance_id}/incidents`)."""
    settings = get_settings()
    events = await blocking_repo.list_open(instance_id, settings.api_read_max_rows)
    session_ids = {e.blocker_session_id for e in events} | {e.blocked_session_id for e in events}
    enrichment = await query_repo.latest_by_session(
        instance_id,
        list(session_ids),
        datetime.now(UTC) - timedelta(seconds=settings.api_read_default_lookback_seconds),
    )
    return _build_blocking_tree(events, enrichment)


@router.get("/blocking/{instance_id}/incidents", response_model=list[BlockingIncidentResponse])
async def list_blocking_incidents(
    instance_id: UUID,
    blocking_repo: Annotated[
        BlockingIncidentsRepository, Depends(get_blocking_incidents_repository)
    ],
    open_only: Annotated[bool, Query()] = False,
    database_name: Annotated[str | None, Query()] = None,
) -> list[BlockingIncidentResponse]:
    """Lista PLANA (nao arvore) de incidentes de bloqueio, mais recentes primeiro — para
    investigacao/historico. `open_only=true` mostra so os em andamento (mesmo conjunto da
    arvore, sem nesting); por padrao inclui os ja encerrados, retidos ate
    `settings.problem_events_retention_days`. `database_name` casa contra qualquer lado."""
    settings = get_settings()
    rows = await blocking_repo.list_incidents(
        instance_id,
        settings.api_read_max_rows,
        open_only=open_only,
        database_name=database_name,
    )
    return [BlockingIncidentResponse.model_validate(row) for row in rows]


@router.get(
    "/blocking/{instance_id}/session/{session_id}",
    response_model=SessionBlockingDetailResponse,
)
async def get_session_blocking_detail(
    instance_id: UUID,
    session_id: str,
    blocking_repo: Annotated[
        BlockingIncidentsRepository, Depends(get_blocking_incidents_repository)
    ],
    query_repo: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
) -> SessionBlockingDetailResponse:
    """Folha do drill-down de bloqueio — resolve incidentes envolvendo `session_id` em andamento
    OU ja encerrados (dentro da janela de retencao), nao mais exigindo que a sessao apareca no
    conjunto aberto agora (`started_at`/`ended_at` de cada aresta mostram se ja foi resolvida).
    404 so quando a sessao nunca apareceu em nenhum incidente retido."""
    settings = get_settings()
    relevant = await blocking_repo.find_involving_session(
        instance_id, session_id, settings.api_read_max_rows
    )
    if not relevant:
        raise HTTPException(
            status_code=404,
            detail="Sessão não encontrada em nenhum incidente de bloqueio retido.",
        )

    info = _collect_session_info(relevant).get(session_id, {})
    enrichment = await query_repo.latest_by_session(
        instance_id,
        [session_id],
        datetime.now(UTC) - timedelta(seconds=settings.api_read_default_lookback_seconds),
    )
    sample = enrichment.get(session_id)

    return SessionBlockingDetailResponse(
        session_id=session_id,
        db_user=info.get("db_user"),
        program=info.get("program"),
        machine=info.get("machine"),
        wait_event_native=info.get("wait_event_native"),
        wait_resource=info.get("wait_resource"),
        blocked_object=info.get("blocked_object"),
        transaction_id=info.get("transaction_id"),
        database_name=info.get("database_name"),
        sql_hash=sample.sql_hash if sample else None,
        sql_text_masked=sample.sql_text_masked if sample else None,
        sql_text_full_masked=sample.sql_text_full_masked if sample else None,
        blocking=[
            BlockingEdgeResponse(
                session_id=e.blocked_session_id,
                db_user=e.blocked_db_user,
                program=e.blocked_program,
                machine=e.blocked_machine,
                wait_time_ms=e.wait_time_ms,
                transaction_begin_time=e.blocked_transaction_begin_time,
                started_at=e.started_at,
                ended_at=e.ended_at,
            )
            for e in relevant
            if e.blocker_session_id == session_id
        ],
        blocked_by=[
            BlockingEdgeResponse(
                session_id=e.blocker_session_id,
                db_user=e.blocker_db_user,
                program=e.blocker_program,
                machine=e.blocker_machine,
                wait_time_ms=e.wait_time_ms,
                transaction_begin_time=e.blocker_transaction_begin_time,
                started_at=e.started_at,
                ended_at=e.ended_at,
            )
            for e in relevant
            if e.blocked_session_id == session_id
        ],
    )


@router.get("/waits/{instance_id}", response_model=list[WaitSummaryEntryResponse])
async def list_wait_summary(
    instance_id: UUID,
    repository: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[WaitSummaryEntryResponse]:
    """Resumo de wait por categoria canonica de uma instancia, agregado em tempo real sobre
    `query_samples`."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    rows = await repository.wait_summary(instance_id, resolved_since, resolved_until)
    return [WaitSummaryEntryResponse(**row._mapping) for row in rows]
