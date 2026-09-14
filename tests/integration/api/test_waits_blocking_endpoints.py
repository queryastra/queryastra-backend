"""Testes de integracao de `GET /api/v1/waits-blocking/*` contra o banco real `ab_xql_pm_dev`.
Semeia via `BlockingEventsRepository`/`QuerySamplesRepository` (fixture `db_session`) e chama o
endpoint via `client` (fixture de `tests/conftest.py`)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedBlockingEvent, NormalizedQuerySample
from app.normalizers.wait_taxonomy import WaitCategory
from app.repositories.blocking_events_repository import BlockingEventsRepository
from app.repositories.query_samples_repository import QuerySamplesRepository


async def test_list_blocking_events_builds_tree_with_enrichment(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Identidade/wait/recurso vem denormalizado do proprio evento (`blocker_*`/`blocked_*`) —
    so o texto mascarado do comando continua vindo do join contra `query_samples`."""
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

    blocking_repo = BlockingEventsRepository(db_session)
    await blocking_repo.insert_batch(
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="blocker-1",
                blocked_session_id="blocked-1",
                blocker_sql_hash="hash-blocker",
                blocked_sql_hash="hash-blocked",
                wait_time_ms=1234,
                blocked_db_user="blocked_user",
                blocked_program="psql",
                blocked_machine="10.0.0.5",
                blocked_wait_event_native="relation",
                blocked_wait_resource="relation/AccessShareLock",
                blocked_object="t",
            )
        ]
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
    """Cadeia A -> B -> C de 3 niveis: B aparece bloqueado por A E bloqueando C na mesma linha
    do tempo — a arvore encadeia as 2 arestas diretas num unico snapshot. `caused_ms` de A soma o
    proprio bloqueio de B (500) mais tudo que B, por sua vez, causou (o `waited_ms` de C, 200) —
    reflete o impacto TOTAL da cadeia, nao so o filho direto."""
    now = datetime.now(UTC)
    blocking_repo = BlockingEventsRepository(db_session)
    await blocking_repo.insert_batch(
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="A",
                blocked_session_id="B",
                wait_time_ms=500,
            ),
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="B",
                blocked_session_id="C",
                wait_time_ms=200,
            ),
        ]
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


async def test_list_blocking_events_empty_when_no_events(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/waits-blocking/blocking/{test_instance.id}")

    assert response.status_code == 200
    assert response.json() == []


async def test_list_blocking_events_respects_to_upper_bound(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """`to` (opcional, aditivo — usado pelo `DayFilter` do frontend pra delimitar um dia
    calendario especifico) exclui um snapshot mais recente fora da janela pedida."""
    now = datetime.now(UTC)
    within_window_time = now - timedelta(hours=2)

    blocking_repo = BlockingEventsRepository(db_session)
    await blocking_repo.insert_batch(
        [
            NormalizedBlockingEvent(
                time=within_window_time,
                instance_id=test_instance.id,
                blocker_session_id="within-window-blocker",
                blocked_session_id="within-window-blocked",
                wait_time_ms=300,
            )
        ]
    )
    await blocking_repo.insert_batch(
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="after-window-blocker",
                blocked_session_id="after-window-blocked",
                wait_time_ms=100,
            )
        ]
    )

    response = await client.get(
        f"/api/v1/waits-blocking/blocking/{test_instance.id}",
        params={
            "from": (now - timedelta(hours=6)).isoformat(),
            "to": (now - timedelta(hours=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["session_id"] == "within-window-blocker"


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

    blocking_repo = BlockingEventsRepository(db_session)
    await blocking_repo.insert_batch(
        [
            NormalizedBlockingEvent(
                time=now,
                instance_id=test_instance.id,
                blocker_session_id="blocker-1",
                blocked_session_id="blocked-1",
                wait_time_ms=999,
                blocked_db_user="blocked_user",
                blocked_program="psql",
                blocked_machine="10.0.0.5",
                blocked_wait_event_native="relation",
                blocked_wait_resource="relation/AccessShareLock",
                blocked_object="t",
            )
        ]
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
