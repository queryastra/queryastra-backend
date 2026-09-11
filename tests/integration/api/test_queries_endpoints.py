"""Testes de integracao de `GET /api/v1/queries/*` contra o banco real `ab_xql_pm_dev`. Semeia
via `QuerySamplesRepository.insert_batch` (fixture `db_session`) e chama o endpoint via `client`
(fixture de `tests/conftest.py`)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedQuerySample
from app.normalizers.wait_taxonomy import WaitCategory
from app.repositories.query_samples_repository import QuerySamplesRepository


async def test_list_active_queries_returns_masked_samples_in_window(
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
                sql_hash="hash-active",
                sql_text_masked="SELECT ? FROM accounts",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=42,
                session_id="42",
                db_user="app_user",
            )
        ]
    )

    response = await client.get(f"/api/v1/queries/active/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["sql_hash"] == "hash-active"
    assert body[0]["sql_text_masked"] == "SELECT ? FROM accounts"
    assert body[0]["instance_id"] == str(test_instance.id)
    assert body[0]["db_user"] == "app_user"


async def test_top_sql_returns_empty_list_when_no_samples(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/queries/top-sql/{test_instance.id}")

    assert response.status_code == 200
    assert response.json() == []


async def test_top_sql_aggregates_execution_count_and_wait_time(
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
                sql_hash="hash-top",
                sql_name="q1",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.IO,
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                # Note: intencionalmente no PASSADO (nao `now + ...`) — o endpoint usa a janela
                # padrao (`until` = agora no momento da requisicao, que e alguns milissegundos
                # depois deste `now`), entao um timestamp no futuro relativo a este `now` correria
                # o risco de cair fora de `time <= until` e ser excluido.
                time=now - timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-top",
                sql_name="q1",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.IO,
                wait_time_ms=100,
            ),
        ]
    )

    response = await client.get(f"/api/v1/queries/top-sql/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["sql_hash"] == "hash-top"
    assert body[0]["execution_count"] == 2
    assert body[0]["total_wait_time_ms"] == 200
    assert body[0]["predominant_wait_type"] == WaitCategory.IO.value
    assert body[0]["instance_id"] == str(test_instance.id)


async def test_list_databases_aggregates_by_database_name(
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
                sql_hash="hash-db-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-db-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(f"/api/v1/queries/databases/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    by_database = {row["database_name"]: row for row in body}
    assert set(by_database) == {"db_a", "db_b"}
    assert by_database["db_a"]["execution_count"] == 1
    assert by_database["db_a"]["total_wait_time_ms"] == 100
    assert by_database["db_b"]["total_wait_time_ms"] == 50


async def test_top_sql_filters_by_database_name_query_param(
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
                sql_hash="hash-shared",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-shared",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=200,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/top-sql/{test_instance.id}", params={"database_name": "db_a"}
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["sql_hash"] == "hash-shared"
    assert body[0]["database_name"] == "db_a"
    assert body[0]["execution_count"] == 1
    assert body[0]["total_wait_time_ms"] == 100


async def test_get_query_detail_returns_raw_samples(
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
                sql_hash="hash-detail",
                sql_name="query-detail",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                session_id="1",
                wait_time_ms=10,
            ),
            NormalizedQuerySample(
                # No PASSADO (nao `now + ...`) — o endpoint usa a janela padrao (`until` = agora
                # no momento da requisicao, alguns milissegundos depois deste `now`); ver
                # comentario identico em `test_top_sql_aggregates_execution_count_and_wait_time`.
                time=now - timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-detail",
                sql_name="query-detail",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                session_id="2",
                wait_time_ms=20,
            ),
        ]
    )

    response = await client.get(f"/api/v1/queries/detail/{test_instance.id}/hash-detail")

    assert response.status_code == 200
    body = response.json()
    assert body["instance_id"] == str(test_instance.id)
    assert body["sql_hash"] == "hash-detail"
    assert body["sql_name"] == "query-detail"
    assert body["database_name"] == "db_a"
    assert len(body["samples"]) == 2
    session_ids = {s["session_id"] for s in body["samples"]}
    assert session_ids == {"1", "2"}


async def test_get_query_detail_returns_404_when_no_samples(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/queries/detail/{test_instance.id}/hash-does-not-exist")

    assert response.status_code == 404
