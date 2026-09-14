"""Endpoints de waits e bloqueios.

Bloqueio: hypertable bruta `blocking_events` (`Docs/database-schema.md` §3), alimentado por
`pg_blocking_pids()` (PostgreSQL, reduzido pro bloqueador direto — `pg_blocking_pids` devolve o
fecho transitivo, ver `app.collectors.postgresql.adapter._reduce_to_direct_edges`) ou
`sys.dm_exec_requests.blocking_session_id` (SQL Server) — `Docs/collector-queries.md` §2. Wait:
hypertable bruta `query_samples`, agregada por categoria canonica
(`app.normalizers.wait_taxonomy.WaitCategory`) em tempo real — nenhum endpoint aqui consulta os
continuous aggregates (`query_stats_hourly`), vazios e nunca atualizados.

`GET /blocking/{instance_id}` devolve uma arvore recursiva (`BlockingTreeNodeResponse`) — cada
sessao bloqueadora raiz com seus bloqueados aninhados em `children`, encadeando as arestas
DIRETAS de um mesmo snapshot (Etapa 9: arvore completa, nao so 1 nivel). Identidade/wait
nativo/recurso bloqueado vem denormalizado no proprio `blocking_events` (capturado no momento da
coleta, ver `app.normalizers.models.NormalizedBlockingEvent`) — so o texto mascarado do comando
continua vindo de um join de leitura contra `query_samples`
(`QuerySamplesRepository.latest_by_session`), que nao guarda o dado no evento (so o hash).
`GET /blocking/{instance_id}/session/{session_id}` e a folha do drill-down (tela de detalhe).
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel, Field
from sqlalchemy import Row
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_db_session, resolve_time_window
from app.core.config import get_settings
from app.repositories.blocking_events_repository import BlockingEventsRepository
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
    waited_ms: int | None
    caused_ms: int
    children: list[BlockingTreeNodeResponse] = Field(default_factory=list)


BlockingTreeNodeResponse.model_rebuild()  # necessario para tipo recursivo com forward ref


class BlockingEdgeResponse(BaseModel):
    """Uma aresta de bloqueio envolvendo a sessao consultada em `SessionBlockingDetailResponse`
    — ou uma sessao que ela bloqueia (`blocking`), ou uma que a bloqueia (`blocked_by`)."""

    session_id: str
    db_user: str | None
    program: str | None
    machine: str | None
    wait_time_ms: int


class SessionBlockingDetailResponse(BaseModel):
    """Folha do drill-down de bloqueio: identidade, comando (quando houver amostra recente em
    `query_samples` — sessao ociosa sem nenhuma amostra fica com `sql_text_masked=None`, nao
    inventa dado) e as arestas de bloqueio que envolvem essa sessao no MESMO snapshot da arvore
    (`GET /blocking/{instance_id}`)."""

    session_id: str
    db_user: str | None
    program: str | None
    machine: str | None
    wait_event_native: str | None
    wait_resource: str | None
    blocked_object: str | None
    sql_hash: str | None
    sql_text_masked: str | None
    sql_text_full_masked: str | None
    blocking: list[BlockingEdgeResponse]
    blocked_by: list[BlockingEdgeResponse]


class WaitSummaryEntryResponse(BaseModel):
    """Resumo de wait por categoria canonica para uma instancia num periodo."""

    wait_type: str  # WaitCategory
    total_wait_time_ms: int


# --- Dependency providers -------------------------------------------------------------------


def get_blocking_events_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> BlockingEventsRepository:
    return BlockingEventsRepository(session)


def get_query_samples_repository(
    session: Annotated[AsyncSession, Depends(get_db_session)],
) -> QuerySamplesRepository:
    return QuerySamplesRepository(session)


# --- Montagem da arvore ------------------------------------------------------------------------


_SessionInfo = dict[str, "str | None"]
"""Campos denormalizados (`db_user`/`program`/`machine`/`wait_event_native`/`wait_resource`/
`blocked_object`) de uma sessao, coletados a partir das arestas em que ela aparece — ver
`_collect_session_info`."""


def _collect_session_info(events: list[Row]) -> dict[str, _SessionInfo]:
    """Junta os campos denormalizados `blocker_*`/`blocked_*` de cada evento numa unica entrada
    POR SESSAO — uma sessao no meio de uma cadeia aparece como bloqueadora numa aresta e como
    bloqueada em outra; o lado bloqueado tem mais detalhe (`wait_resource`/`blocked_object`, so
    faz sentido pra quem esta de fato esperando por um recurso), entao ele tem prioridade quando
    os dois lados discordarem (nao deveriam, mesmo snapshot, mas por seguranca)."""
    info: dict[str, _SessionInfo] = {}
    for e in events:
        blocker = info.setdefault(e.blocker_session_id, {})
        blocker.setdefault("db_user", e.blocker_db_user)
        blocker.setdefault("program", e.blocker_program)
        blocker.setdefault("machine", e.blocker_machine)
        blocker.setdefault("wait_event_native", e.blocker_wait_event_native)

        blocked = info.setdefault(e.blocked_session_id, {})
        blocked["db_user"] = e.blocked_db_user or blocked.get("db_user")
        blocked["program"] = e.blocked_program or blocked.get("program")
        blocked["machine"] = e.blocked_machine or blocked.get("machine")
        blocked["wait_event_native"] = e.blocked_wait_event_native or blocked.get(
            "wait_event_native"
        )
        blocked["wait_resource"] = e.blocked_wait_resource or blocked.get("wait_resource")
        blocked["blocked_object"] = e.blocked_object or blocked.get("blocked_object")
    return info


def _build_blocking_tree(
    events: list[Row], enrichment: dict[str, Row]
) -> list[BlockingTreeNodeResponse]:
    """Monta a arvore completa de bloqueio a partir de eventos diretos (uma linha de
    `blocking_events` por aresta) — encadeia quantos niveis existirem quando um bloqueador
    tambem aparece como bloqueado em outra linha do mesmo snapshot (SQL Server: sempre 1
    bloqueador por linha; PostgreSQL: ja reduzido pro bloqueador direto pelo coletor, ver
    `app.collectors.postgresql.adapter._reduce_to_direct_edges`)."""
    children_by_blocker: dict[str, list[Row]] = defaultdict(list)
    blocked_ids: set[str] = set()
    blocker_sql_hash_by_session: dict[str, str | None] = {}
    for e in events:
        children_by_blocker[e.blocker_session_id].append(e)
        blocked_ids.add(e.blocked_session_id)
        blocker_sql_hash_by_session[e.blocker_session_id] = e.blocker_sql_hash

    session_info = _collect_session_info(events)

    def node_for(
        session_id: str, sql_hash: str | None, waited_ms: int | None
    ) -> BlockingTreeNodeResponse:
        query_info = enrichment.get(session_id)
        info = session_info.get(session_id, {})
        children = [
            node_for(c.blocked_session_id, c.blocked_sql_hash, c.wait_time_ms)
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
            waited_ms=waited_ms,
            caused_ms=caused_ms,
            children=children,
        )

    roots = [sid for sid in children_by_blocker if sid not in blocked_ids]
    # Raiz nunca aparece como bloqueada (senao nao seria raiz) — `waited_ms=None`, nao `0`: "0ms
    # de espera" seria enganoso pra quem nunca esperou nada (o "idle blocker" do print do DPA).
    return [node_for(r, blocker_sql_hash_by_session[r], None) for r in roots]


# --- Endpoints -------------------------------------------------------------------------------


@router.get("/blocking/{instance_id}", response_model=list[BlockingTreeNodeResponse])
async def list_blocking_events(
    instance_id: UUID,
    blocking_repo: Annotated[BlockingEventsRepository, Depends(get_blocking_events_repository)],
    query_repo: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> list[BlockingTreeNodeResponse]:
    """Arvore de bloqueio do ultimo ciclo que registrou bloqueio dentro da janela (nao uma
    janela deslizante inteira — bloqueios ja resolvidos nao aparecem junto dos atuais). `to`
    (opcional, aditivo) permite delimitar um dia calendario especifico no passado (`DayFilter`
    do frontend); sem ele, `resolve_time_window` resolve pra "agora", o que na pratica nunca
    exclui nada (nenhum evento e gravado com `time` no futuro)."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    events = await blocking_repo.list_latest_snapshot(
        instance_id, resolved_since, settings.api_read_max_rows, until=resolved_until
    )
    session_ids = {e.blocker_session_id for e in events} | {e.blocked_session_id for e in events}
    enrichment = await query_repo.latest_by_session(instance_id, list(session_ids), resolved_since)
    return _build_blocking_tree(events, enrichment)


@router.get(
    "/blocking/{instance_id}/session/{session_id}",
    response_model=SessionBlockingDetailResponse,
)
async def get_session_blocking_detail(
    instance_id: UUID,
    session_id: str,
    blocking_repo: Annotated[BlockingEventsRepository, Depends(get_blocking_events_repository)],
    query_repo: Annotated[QuerySamplesRepository, Depends(get_query_samples_repository)],
    since: Annotated[datetime | None, Query(alias="from")] = None,
    until: Annotated[datetime | None, Query(alias="to")] = None,
) -> SessionBlockingDetailResponse:
    """Folha do drill-down de bloqueio — mesma janela/snapshot de `GET /blocking/{instance_id}`
    (`from`/`to` devem vir com os mesmos valores ja resolvidos pela tela de arvore, pra ancorar
    no mesmo ciclo: `session_id` nao e uma identidade estavel no tempo, o motor reaproveita o
    numero). 404 quando a sessao nao aparece em nenhuma aresta desse snapshot."""
    settings = get_settings()
    resolved_since, resolved_until = resolve_time_window(
        since, until, timedelta(seconds=settings.api_read_default_lookback_seconds)
    )
    events = await blocking_repo.list_latest_snapshot(
        instance_id, resolved_since, settings.api_read_max_rows, until=resolved_until
    )
    relevant = [e for e in events if session_id in (e.blocker_session_id, e.blocked_session_id)]
    if not relevant:
        raise HTTPException(
            status_code=404,
            detail="Sessao nao encontrada no ultimo snapshot de bloqueio dessa janela.",
        )

    info = _collect_session_info(events).get(session_id, {})
    enrichment = await query_repo.latest_by_session(instance_id, [session_id], resolved_since)
    sample = enrichment.get(session_id)

    return SessionBlockingDetailResponse(
        session_id=session_id,
        db_user=info.get("db_user"),
        program=info.get("program"),
        machine=info.get("machine"),
        wait_event_native=info.get("wait_event_native"),
        wait_resource=info.get("wait_resource"),
        blocked_object=info.get("blocked_object"),
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
