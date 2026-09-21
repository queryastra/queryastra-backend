"""Testes de integracao de `GET /api/v1/waits-blocking/*` contra o banco real `ab_xql_pm_dev`.
Semeia via `BlockingIncidentsRepository`/`QuerySamplesRepository` (fixture `db_session`) e chama o
endpoint via `client` (fixture de `tests/conftest.py`)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedBlockingEvent, NormalizedQuerySample
from app.normalizers.wait_taxonomy import WaitCategory
from app.repositories.blocking_incidents_repository import BlockingIncidentsRepository
from app.repositories.query_samples_repository import QuerySamplesRepository


async def test_list_blocking_events_builds_tree_with_enrichment(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Identidade/wait/recurso vem denormalizado do proprio incidente (`blocker_*`/`blocked_*`)
    — so o texto mascarado do comando continua vindo do join contra `query_samples`."""
    now = datetime.now(UTC)

    query_repo = QuerySamplesRepository(db_session)
    await query_repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-blocker",
                sql_text_masked="UPDATE t SET x = ?",
                session_id="blocker-1",
                db_user="blocker_user",
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-blocked",
                sql_text_masked="SELECT ? FROM t",
                session_id="blocked-1",
                db_user="blocked_user",
            ),
        ]
    )

    def make_event(wait_time_ms: int) -> NormalizedBlockingEvent:
        return NormalizedBlockingEvent(
            time=now,
            instance_id=test_instance.id,
            blocker_session_id="blocker-1",
            blocked_session_id="blocked-1",
            blocker_sql_hash="hash-blocker",
            blocked_sql_hash="hash-blocked",
            wait_time_ms=wait_time_ms,
            blocked_db_user="blocked_user",
            blocked_program="psql",
            blocked_machine="10.0.0.5",
            blocked_wait_event_native="relation",
            blocked_wait_resource="relation/AccessShareLock",
            blocked_object="t",
        )

    blocking_repo = BlockingIncidentsRepository(db_session)
    # 2 ciclos com um gap de 1234ms entre eles: `waited_ms`/`caused_ms` exibidos vem do intervalo
    # OBSERVADO (`last_seen_at - started_at`), nao do `wait_time_ms` bruto de nenhum dos dois
    # ciclos (que pode ser qualquer valor — o DMV reinicia essa contagem a qualquer momento) —
    # ver `_observed_wait_ms`.
    await blocking_repo.upsert_batch(test_instance.id, [make_event(50)], now)
    await blocking_repo.upsert_batch(
        test_instance.id, [make_event(80)], now + timedelta(milliseconds=1234)
    )

    response = await client.get(f"/api/v1/waits-blocking/blocking/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    root = body[0]
    assert root["session_id"] == "blocker-1"
    assert root["sql_hash"] == "hash-blocker"
    assert root["waited_ms"] is None  # bloqueador puro, nunca aparece como bloqueado
    assert root["caused_ms"] == 1234
    assert len(root["children"]) == 1
    child = root["children"][0]
    assert child["session_id"] == "blocked-1"
    assert child["sql_text_masked"] == "SELECT ? FROM t"
    assert child["db_user"] == "blocked_user"
    assert child["program"] == "psql"
    assert child["machine"] == "10.0.0.5"
    assert child["wait_event_native"] == "relation"
    assert child["wait_resource"] == "relation/AccessShareLock"
    assert child["blocked_object"] == "t"
    assert child["waited_ms"] == 1234
    assert child["caused_ms"] == 0


async def test_list_blocking_events_builds_multi_level_tree(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Cadeia A -> B -> C de 3 niveis: B aparece bloqueado por A E bloqueando C no mesmo
    conjunto aberto — a arvore encadeia as 2 arestas diretas. `caused_ms` de A soma o proprio
    bloqueio de B (500) mais tudo que B, por sua vez, causou (o `waited_ms` de C, 200) — reflete
    o impacto TOTAL da cadeia, nao so o filho direto. `waited_ms` de cada aresta vem do intervalo
    OBSERVADO entre ciclos (`_observed_wait_ms`), entao A->B e B->C comecam em momentos
    diferentes pra teminarem com durações distintas (500ms/200ms)."""
    now = datetime.now(UTC)
    a_blocks_b = NormalizedBlockingEvent(
        time=now,
        instance_id=test_instance.id,
        blocker_session_id="A",
        blocked_session_id="B",
        wait_time_ms=1,
    )
    b_blocks_c = NormalizedBlockingEvent(
        time=now,
        instance_id=test_instance.id,
        blocker_session_id="B",
        blocked_session_id="C",
        wait_time_ms=1,
    )
    blocking_repo = BlockingIncidentsRepository(db_session)
    # A->B comeca no ciclo 1 (t0); B->C só aparece no ciclo 2 (t0+300ms); ciclo 3 (t0+500ms)
    # observa os dois de novo — A->B acumula 500ms de intervalo observado, B->C só 200ms.
    await blocking_repo.upsert_batch(test_instance.id, [a_blocks_b], now)
    await blocking_repo.upsert_batch(
        test_instance.id, [a_blocks_b, b_blocks_c], now + timedelta(milliseconds=300)
    )
    await blocking_repo.upsert_batch(
        test_instance.id, [a_blocks_b, b_blocks_c], now + timedelta(milliseconds=500)
    )

    response = await client.get(f"/api/v1/waits-blocking/blocking/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    root = body[0]
    assert root["session_id"] == "A"
    assert root["waited_ms"] is None
    assert root["caused_ms"] == 700  # 500 (B) + 200 (C, causado transitivamente por A via B)
    assert len(root["children"]) == 1
    node_b = root["children"][0]
    assert node_b["session_id"] == "B"
    assert node_b["waited_ms"] == 500
    assert node_b["caused_ms"] == 200
    assert len(node_b["children"]) == 1
    node_c = node_b["children"][0]
    assert node_c["session_id"] == "C"
    assert node_c["waited_ms"] == 200
    assert node_c["caused_ms"] == 0
    assert node_c["children"] == []


async def test_list_blocking_incidents_includes_machine_and_command(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Item 22: o historico plano (`GET /blocking/{id}/incidents`) precisa mostrar os mesmos
    campos que a arvore ja mostra — `blocker_machine`/`blocked_machine` ja existiam no model mas
    nao eram expostos, e `blocker_sql_text_masked`/`blocked_sql_text_masked` sao resolvidos por
    `sql_hash` (nao por sessao) contra `query_samples`."""
    now = datetime.now(UTC)

    query_repo = QuerySamplesRepository(db_session)
    await query_repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-blocker-22",
                sql_text_masked="UPDATE t SET x = ?",
                session_id="blocker-22",
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-blocked-22",
                sql_text_masked="SELECT ? FROM t",
                session_id="blocked-22",
            ),
        ]
    )

    blocking_repo = BlockingIncidentsRepository(db_session)
    await blocking_repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="blocker-22",
                blocked_session_id="blocked-22",
                blocker_sql_hash="hash-blocker-22",
                blocked_sql_hash="hash-blocked-22",
                wait_time_ms=999,
                blocker_machine="WKS-01",
                blocked_machine="WKS-02",
                blocked_wait_resource="relation/AccessShareLock",
                blocked_object="t",
            )
        ],
        now,
    )

    response = await client.get(f"/api/v1/waits-blocking/blocking/{test_instance.id}/incidents")

    assert response.status_code == 200
    [incident] = response.json()
    assert incident["blocker_machine"] == "WKS-01"
    assert incident["blocked_machine"] == "WKS-02"
    assert incident["blocked_wait_resource"] == "relation/AccessShareLock"
    assert incident["blocked_object"] == "t"
    assert incident["blocker_sql_text_masked"] == "UPDATE t SET x = ?"
    assert incident["blocked_sql_text_masked"] == "SELECT ? FROM t"


async def test_list_blocking_events_empty_when_no_events(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/waits-blocking/blocking/{test_instance.id}")

    assert response.status_code == 200
    assert response.json() == []


async def test_list_blocking_events_excludes_ended_incidents(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """A arvore ao vivo (`GET /blocking/{id}`) so mostra incidentes EM ANDAMENTO — um incidente
    ja encerrado (segundo ciclo sem ele no batch) some da arvore, mas continua acessivel via
    `GET /blocking/{id}/incidents` (lista plana/historico)."""
    now = datetime.now(UTC)
    blocking_repo = BlockingIncidentsRepository(db_session)
    await blocking_repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="ended-blocker",
                blocked_session_id="ended-blocked",
                wait_time_ms=300,
            )
        ],
        now,
    )
    # Proximo ciclo: o incidente ja foi resolvido (nao aparece mais no batch).
    await blocking_repo.upsert_batch(test_instance.id, [], datetime.now(UTC))

    tree_response = await client.get(f"/api/v1/waits-blocking/blocking/{test_instance.id}")
    assert tree_response.status_code == 200
    assert tree_response.json() == []

    incidents_response = await client.get(
        f"/api/v1/waits-blocking/blocking/{test_instance.id}/incidents"
    )
    assert incidents_response.status_code == 200
    incidents = incidents_response.json()
    assert len(incidents) == 1
    assert incidents[0]["blocker_session_id"] == "ended-blocker"
    assert incidents[0]["ended_at"] is not None

    open_only_response = await client.get(
        f"/api/v1/waits-blocking/blocking/{test_instance.id}/incidents",
        params={"open_only": "true"},
    )
    assert open_only_response.status_code == 200
    assert open_only_response.json() == []


async def test_get_session_blocking_detail_returns_identity_command_and_edges(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    now = datetime.now(UTC)

    query_repo = QuerySamplesRepository(db_session)
    await query_repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-blocked",
                sql_text_masked="SELECT ? FROM t",
                sql_text_full_masked="SELECT ? FROM t",
                session_id="blocked-1",
            ),
        ]
    )

    def make_event(wait_time_ms: int) -> NormalizedBlockingEvent:
        return NormalizedBlockingEvent(
            time=now,
            instance_id=test_instance.id,
            blocker_session_id="blocker-1",
            blocked_session_id="blocked-1",
            wait_time_ms=wait_time_ms,
            blocked_db_user="blocked_user",
            blocked_program="psql",
            blocked_machine="10.0.0.5",
            blocked_wait_event_native="relation",
            blocked_wait_resource="relation/AccessShareLock",
            blocked_object="t",
        )

    blocking_repo = BlockingIncidentsRepository(db_session)
    # 2 ciclos, gap de 999ms — `wait_time_ms` exibido vem do intervalo observado, nao do valor
    # bruto de nenhum dos dois ciclos (ver `_observed_wait_ms`).
    await blocking_repo.upsert_batch(test_instance.id, [make_event(10)], now)
    await blocking_repo.upsert_batch(
        test_instance.id, [make_event(20)], now + timedelta(milliseconds=999)
    )

    response = await client.get(
        f"/api/v1/waits-blocking/blocking/{test_instance.id}/session/blocked-1"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["session_id"] == "blocked-1"
    assert body["db_user"] == "blocked_user"
    assert body["program"] == "psql"
    assert body["machine"] == "10.0.0.5"
    assert body["wait_event_native"] == "relation"
    assert body["wait_resource"] == "relation/AccessShareLock"
    assert body["blocked_object"] == "t"
    assert body["sql_text_masked"] == "SELECT ? FROM t"
    assert body["sql_text_full_masked"] == "SELECT ? FROM t"
    assert body["blocking"] == []
    assert len(body["blocked_by"]) == 1
    assert body["blocked_by"][0]["session_id"] == "blocker-1"
    assert body["blocked_by"][0]["wait_time_ms"] == 999
    assert body["blocked_by"][0]["ended_at"] is None


async def test_get_session_blocking_detail_resolves_after_incident_ends(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """A pedido explicito do usuario: o detalhe de sessao continua resolvendo depois do
    bloqueio ja ter sido encerrado — mostrando `ended_at` preenchido, nao 404."""
    now = datetime.now(UTC)
    blocking_repo = BlockingIncidentsRepository(db_session)
    await blocking_repo.upsert_batch(
        test_instance.id,
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="blocker-2",
                blocked_session_id="blocked-2",
                wait_time_ms=42,
            )
        ],
        now,
    )
    await blocking_repo.upsert_batch(test_instance.id, [], datetime.now(UTC))

    response = await client.get(
        f"/api/v1/waits-blocking/blocking/{test_instance.id}/session/blocked-2"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body["blocked_by"]) == 1
    assert body["blocked_by"][0]["ended_at"] is not None


async def test_get_session_blocking_detail_404_when_session_not_in_snapshot(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(
        f"/api/v1/waits-blocking/blocking/{test_instance.id}/session/does-not-exist"
    )

    assert response.status_code == 404


async def test_list_wait_summary_groups_total_wait_time_by_category(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-1",
                sql_text_masked="SELECT ?",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-2",
                sql_text_masked="SELECT ?",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/waits-blocking/waits/{test_instance.id}",
        params={"from": (now - timedelta(minutes=5)).isoformat(), "to": now.isoformat()},
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["wait_type"] == WaitCategory.LOCK.value
    assert body[0]["total_wait_time_ms"] == 150
