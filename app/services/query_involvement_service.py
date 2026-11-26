"""Envolvimento de uma query (`sql_hash`) em eventos — seção "Envolvimento em eventos" do detalhe
de query (nível 3 do drill-down).

A tela de detalhe é por QUERY, mas bloqueios, transações e deadlocks são registrados por SESSÃO.
Este serviço faz a ponte: dado um `sql_hash` e um período, lista em quais sessões essa query
apareceu em cada tipo de evento, para a UI linkar direto às telas de detalhe já existentes
(`/blocking/{sessionId}`, `/transactions/{transactionIdentifier}`, `/deadlocks/{deadlockId}`).

Força de cada ligação (exposta em `match` para a UI nunca apresentar heurística como fato):

- Bloqueio — `exact`: `blocking_incidents.blocker_sql_hash`/`blocked_sql_hash` é a mesma chave.
- Transação — `exact` quando a transação é referenciada por um desses incidentes de bloqueio
  (`blocker_transaction_id`/`blocked_transaction_id`); `probable` quando só a sessão executou a
  query dentro da janela da transação (`session_id` é reaproveitado pelo motor). Nos dois casos
  vale o MESMO critério da tela de Transações (`problem_transaction_conditions`: transação
  problemática — `flag_reason` preenchido — que sobrepõe o período), então toda transação listada
  aqui também aparece na tela de Transações para o mesmo período.
- Deadlock — `exact` quando o processo tem `sql_hashes` gravado na coleta; `probable` para
  deadlocks antigos, com os hashes recalculados a partir de `raw_xml_masked` (só texto do frame e
  `inputbuf`, sem a fatia por offset).
- Linked server — `machine` das amostras da query resolvida contra o host das outras instâncias
  cadastradas (mesmo critério da tela de Bloqueios).

Nenhum vínculo encontrado NÃO prova que a query nunca se envolveu num evento — só que nada foi
observado nas amostras/eventos coletados (a UI diz isso explicitamente).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel

from app.models.blocking_incident import BlockingIncident
from app.models.deadlock_event import DeadlockEvent
from app.normalizers.deadlock_sql_hashes import deadlock_sql_hashes_by_process_from_xml
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository
from app.repositories.deadlock_events_repository import DeadlockEventsRepository
from app.repositories.instances_repository import InstancesRepository
from app.repositories.query_samples_repository import QuerySamplesRepository
from app.repositories.transaction_events_repository import TransactionEventsRepository

MatchKind = Literal["exact", "probable"]

# Teto por seção — a tela mostra uma lista navegável, não um log completo; `truncated` avisa a UI.
SECTION_LIMIT = 200
# Quantos deadlocks do período são inspecionados (o casamento por hash é feito em Python, sobre
# o `detail` JSONB). Deadlock é evento raro; o teto só protege contra período muito longo.
DEADLOCK_SCAN_LIMIT = 1000


class InvolvementBlocking(BaseModel):
    incident_id: UUID
    role: Literal["blocker", "blocked"]
    session_id: str
    other_session_id: str
    database_name: str | None
    machine: str | None
    transaction_id: str | None
    started_at: datetime
    last_seen_at: datetime
    ended_at: datetime | None
    wait_time_ms: int
    blocked_object: str | None
    linked_instance_id: UUID | None = None
    linked_instance_name: str | None = None


class InvolvementTransaction(BaseModel):
    transaction_identifier: str
    session_id: str | None
    database_name: str | None
    begin_time: datetime | None
    closed_at: datetime | None
    flag_reason: str | None
    match: MatchKind
    # Amostras da query dentro da transação (só para `probable` — prova do vínculo heurístico).
    sample_count: int | None = None


class InvolvementDeadlock(BaseModel):
    deadlock_id: UUID
    time: datetime
    session_id: str | None
    is_victim: bool
    database_name: str | None
    resource_description: str | None
    match: MatchKind


class InvolvementLinkedSource(BaseModel):
    machine: str
    instance_id: UUID
    instance_name: str
    sample_count: int
    first_seen_at: datetime
    last_seen_at: datetime


class QueryInvolvementResponse(BaseModel):
    sql_hash: str
    blocking: list[InvolvementBlocking]
    transactions: list[InvolvementTransaction]
    deadlocks: list[InvolvementDeadlock]
    linked_sources: list[InvolvementLinkedSource]
    blocking_truncated: bool = False
    transactions_truncated: bool = False
    deadlocks_truncated: bool = False


def _blocking_items(
    incidents: list[BlockingIncident],
    sql_hash: str,
    instance_names: dict[UUID, str],
) -> list[InvolvementBlocking]:
    items: list[InvolvementBlocking] = []
    for inc in incidents:
        # Uma mesma query pode estar dos DOIS lados (duas execuções da mesma query, uma
        # bloqueando a outra) — vira duas linhas, uma por papel.
        sides: list[Literal["blocker", "blocked"]] = []
        if inc.blocker_sql_hash == sql_hash:
            sides.append("blocker")
        if inc.blocked_sql_hash == sql_hash:
            sides.append("blocked")
        for role in sides:
            is_blocker = role == "blocker"
            linked_id = (
                inc.blocker_linked_instance_id if is_blocker else inc.blocked_linked_instance_id
            )
            items.append(
                InvolvementBlocking(
                    incident_id=inc.id,
                    role=role,
                    session_id=inc.blocker_session_id if is_blocker else inc.blocked_session_id,
                    other_session_id=(
                        inc.blocked_session_id if is_blocker else inc.blocker_session_id
                    ),
                    database_name=(
                        inc.blocker_database_name if is_blocker else inc.blocked_database_name
                    ),
                    machine=inc.blocker_machine if is_blocker else inc.blocked_machine,
                    transaction_id=(
                        inc.blocker_transaction_id if is_blocker else inc.blocked_transaction_id
                    ),
                    started_at=inc.started_at,
                    last_seen_at=inc.last_seen_at,
                    ended_at=inc.ended_at,
                    wait_time_ms=inc.wait_time_ms,
                    blocked_object=inc.blocked_object,
                    linked_instance_id=linked_id,
                    linked_instance_name=instance_names.get(linked_id) if linked_id else None,
                )
            )
    return items


def match_deadlock(event: DeadlockEvent, sql_hash: str) -> list[InvolvementDeadlock]:
    """Um item por processo do deadlock cujo conjunto de hashes contém `sql_hash`. Usa
    `detail.processes[].sql_hashes` (coleta nova, `exact`); quando nenhum processo tem o campo
    (deadlock coletado antes dele existir), recalcula a partir de `raw_xml_masked` (`probable`)."""
    detail: dict[str, Any] = event.detail or {}
    processes: list[dict[str, Any]] = list(detail.get("processes") or [])
    has_stored_hashes = any("sql_hashes" in p for p in processes)

    historical: dict[str, list[str]] = {}
    if not has_stored_hashes:
        historical = deadlock_sql_hashes_by_process_from_xml(event.raw_xml_masked)

    items: list[InvolvementDeadlock] = []
    for process in processes:
        if has_stored_hashes:
            hashes = process.get("sql_hashes") or []
            match: MatchKind = "exact"
        else:
            hashes = historical.get(process.get("id") or "", [])
            match = "probable"
        if sql_hash not in hashes:
            continue
        items.append(
            InvolvementDeadlock(
                deadlock_id=event.id,
                time=event.time,
                session_id=process.get("session_id"),
                is_victim=bool(process.get("is_victim")),
                database_name=process.get("database_name") or event.database_name,
                resource_description=event.resource_description,
                match=match,
            )
        )
    return items


async def build_query_involvement(
    *,
    instance_id: UUID,
    sql_hash: str,
    since: datetime,
    until: datetime,
    blocking_repository: BlockingIncidentsRepository,
    transactions_repository: TransactionEventsRepository,
    deadlocks_repository: DeadlockEventsRepository,
    query_samples_repository: QuerySamplesRepository,
    instances_repository: InstancesRepository,
) -> QueryInvolvementResponse:
    # Consultas em sequência — todos os repositórios compartilham a mesma sessão/conexão asyncpg
    # (nunca `asyncio.gather` na mesma conexão, ver CLAUDE.md do backend).
    incidents = await blocking_repository.find_by_sql_hash(
        instance_id, sql_hash, since, until, SECTION_LIMIT + 1
    )
    blocking_truncated = len(incidents) > SECTION_LIMIT
    incidents = incidents[:SECTION_LIMIT]

    linked_ids = {
        lid
        for inc in incidents
        for lid in (inc.blocker_linked_instance_id, inc.blocked_linked_instance_id)
        if lid is not None
    }
    instance_names = await instances_repository.get_names_by_ids(list(linked_ids))
    blocking = _blocking_items(incidents, sql_hash, instance_names)

    # Transações: primeiro as referenciadas pelos incidentes (exatas), depois as heurísticas.
    tx_ids_from_blocking = list(
        dict.fromkeys(b.transaction_id for b in blocking if b.transaction_id)
    )
    exact_txs = await transactions_repository.find_by_identifiers(
        instance_id, tx_ids_from_blocking, since=since, until=until
    )
    transactions: list[InvolvementTransaction] = [
        InvolvementTransaction(
            transaction_identifier=tx.transaction_identifier,
            session_id=tx.session_id,
            database_name=tx.database_name,
            begin_time=tx.begin_time,
            closed_at=tx.closed_at,
            flag_reason=tx.flag_reason,
            match="exact",
        )
        for tx in exact_txs
    ]
    seen_tx = {t.transaction_identifier for t in transactions}
    probable = await transactions_repository.find_overlapping_sql_hash(
        instance_id, sql_hash, since, until, SECTION_LIMIT + 1
    )
    transactions_truncated = len(probable) > SECTION_LIMIT
    for tx, sample_count, _last in probable[:SECTION_LIMIT]:
        if tx.transaction_identifier in seen_tx:
            continue
        seen_tx.add(tx.transaction_identifier)
        transactions.append(
            InvolvementTransaction(
                transaction_identifier=tx.transaction_identifier,
                session_id=tx.session_id,
                database_name=tx.database_name,
                begin_time=tx.begin_time,
                closed_at=tx.closed_at,
                flag_reason=tx.flag_reason,
                match="probable",
                sample_count=sample_count,
            )
        )

    events = await deadlocks_repository.list_for_instance(
        instance_id, DEADLOCK_SCAN_LIMIT, since=since, until=until
    )
    deadlocks: list[InvolvementDeadlock] = []
    for event in events:
        if event.pending:
            continue
        deadlocks.extend(match_deadlock(event, sql_hash))
    deadlocks_truncated = len(deadlocks) > SECTION_LIMIT or len(events) >= DEADLOCK_SCAN_LIMIT
    deadlocks = deadlocks[:SECTION_LIMIT]

    machine_rows = await query_samples_repository.machines_for_sql_hash(
        instance_id, sql_hash, since, until, SECTION_LIMIT
    )
    linked_sources: list[InvolvementLinkedSource] = []
    if machine_rows:
        host_map = await instances_repository.host_map_excluding(instance_id)
        for row in machine_rows:
            m = (row.machine or "").lower()
            found = host_map.get(m) or host_map.get(m.split(".")[0])
            if found is None:
                continue
            linked_sources.append(
                InvolvementLinkedSource(
                    machine=row.machine,
                    instance_id=found[0],
                    instance_name=found[1],
                    sample_count=int(row.sample_count),
                    first_seen_at=row.first_seen_at,
                    last_seen_at=row.last_seen_at,
                )
            )

    return QueryInvolvementResponse(
        sql_hash=sql_hash,
        blocking=blocking,
        transactions=transactions,
        deadlocks=deadlocks,
        linked_sources=linked_sources,
        blocking_truncated=blocking_truncated,
        transactions_truncated=transactions_truncated,
        deadlocks_truncated=deadlocks_truncated,
    )
