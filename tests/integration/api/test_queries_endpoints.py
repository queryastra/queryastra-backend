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
