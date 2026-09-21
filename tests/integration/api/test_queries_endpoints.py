"""Testes de integracao de `GET /api/v1/queries/*` contra o banco real `ab_xql_pm_dev`. Semeia
via `QuerySamplesRepository.insert_batch` (fixture `db_session`) e chama o endpoint via `client`
(fixture de `tests/conftest.py`)."""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from typing import Any
from uuid import uuid4

import pymssql
import pytest
from httpx import AsyncClient
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.v1.queries import get_sql_insight_client
from app.main import app
from app.masking.sql_masking import mask_sql_literals
from app.models.instance import Instance
from app.normalizers.models import NormalizedPlanSnapshot, NormalizedQuerySample
from app.normalizers.wait_taxonomy import WaitCategory
from app.repositories.query_plan_snapshots_repository import QueryPlanSnapshotsRepository
from app.repositories.query_samples_repository import QuerySamplesRepository
from app.services.sql_insight_client import SqlInsightAnalysis, SqlInsightRule


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
    # Decomposicao por wait nativo (nenhum informado nas 2 amostras -> agrupa em "desconhecido").
    assert body[0]["wait_breakdown"] == [
        {"wait_event_native": "desconhecido", "total_wait_time_ms": 200}
    ]


async def test_top_sql_by_wait_type_filters_to_the_requested_native_wait(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Drill-down a partir da folha da dimensao "Waits" (clicar num tipo de espera no
    `WaitTypeBarChart`) — so queries com amostra do `wait_event_native` pedido aparecem."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-lck",
                sql_name="q-lck",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.LOCK,
                wait_event_native="LCK_M_S",
                wait_time_ms=150,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-io",
                sql_name="q-io",
                sql_text_masked="SELECT ? FROM other",
                wait_type=WaitCategory.IO,
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/top-sql-by-wait-type/{test_instance.id}",
        params={"wait_event_native": "LCK_M_S"},
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["sql_hash"] == "hash-lck"
    assert body[0]["total_wait_time_ms"] == 150
    assert body[0]["predominant_wait_type"] == WaitCategory.LOCK.value
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
                wait_event_native="IO done",
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
    # Decomposicao por wait nativo (nao o bucket canonico de 9 categorias).
    assert by_database["db_a"]["wait_breakdown"] == [
        {"wait_event_native": "IO done", "total_wait_time_ms": 100}
    ]
    assert by_database["db_b"]["wait_breakdown"] == [
        {"wait_event_native": "desconhecido", "total_wait_time_ms": 50}
    ]


async def test_list_database_names_returns_distinct_names_without_aggregation(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """`GET /database-names/{id}` só devolve os nomes distintos — sem estatística nenhuma, bem
    mais barato que `GET /databases/{id}` pra instâncias com muito volume histórico."""
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
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-db-a-2",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-db-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-no-db",
                sql_text_masked="SELECT ? FROM t",
                database_name=None,
            ),
        ]
    )

    response = await client.get(f"/api/v1/queries/database-names/{test_instance.id}")

    assert response.status_code == 200
    assert response.json() == ["db_a", "db_b"]


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


async def test_top_sql_filters_by_db_user_query_param(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Nivel 2 da dimensao "Usuarios" — mesmo padrao de `database_name`, filtrando por
    `db_user`."""
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
                db_user="app_erp",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-shared",
                sql_text_masked="SELECT ? FROM t",
                db_user="batch_conciliacao",
                wait_time_ms=200,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/top-sql/{test_instance.id}", params={"db_user": "app_erp"}
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["sql_hash"] == "hash-shared"
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


async def test_list_databases_by_day_groups_by_utc_day(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    day1 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    day2 = datetime(2026, 9, 2, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=day1,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=day2,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/databases-by-day/{test_instance.id}",
        params={
            "from": (day1 - timedelta(days=1)).isoformat(),
            "to": (day2 + timedelta(days=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    by_day = {entry["bucket_start"][:10]: entry for entry in body}
    assert by_day["2026-09-01"]["label"] == "db_a"
    assert by_day["2026-09-01"]["total_wait_time_ms"] == 100
    assert by_day["2026-09-02"]["total_wait_time_ms"] == 50


async def test_list_top_sql_by_day_groups_by_utc_day(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    day1 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=day1,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/top-sql-by-day/{test_instance.id}",
        params={
            "from": (day1 - timedelta(days=1)).isoformat(),
            "to": (day1 + timedelta(days=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["label"] == "hash-a"
    assert body[0]["total_wait_time_ms"] == 100


async def test_list_databases_by_hour_groups_by_utc_hour(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    hour10 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    hour11 = datetime(2026, 9, 1, 11, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=hour10,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=hour11,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=20,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/databases-by-hour/{test_instance.id}",
        params={
            "from": (hour10 - timedelta(hours=1)).isoformat(),
            "to": (hour11 + timedelta(hours=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    by_hour = {entry["bucket_start"][:13]: entry for entry in body}
    assert by_hour["2026-09-01T10"]["label"] == "db_a"
    assert by_hour["2026-09-01T10"]["total_wait_time_ms"] == 100
    assert by_hour["2026-09-01T11"]["label"] == "db_b"


async def test_list_top_sql_by_hour_groups_by_utc_hour(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    hour10 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=hour10,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/top-sql-by-hour/{test_instance.id}",
        params={
            "from": (hour10 - timedelta(hours=1)).isoformat(),
            "to": (hour10 + timedelta(hours=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["label"] == "hash-a"
    assert body[0]["total_wait_time_ms"] == 100


async def test_list_wait_types_aggregates_by_wait_event_native(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(f"/api/v1/queries/wait-types/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    by_type = {row["wait_event_native"]: row for row in body}
    assert set(by_type) == {"ASYNC_NETWORK_IO", "PAGEIOLATCH_SH"}
    assert by_type["ASYNC_NETWORK_IO"]["total_wait_time_ms"] == 100
    assert by_type["PAGEIOLATCH_SH"]["total_wait_time_ms"] == 50


async def test_list_wait_types_by_day_groups_by_utc_day(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    day1 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=day1,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=100,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/wait-types-by-day/{test_instance.id}",
        params={
            "from": (day1 - timedelta(days=1)).isoformat(),
            "to": (day1 + timedelta(days=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["label"] == "ASYNC_NETWORK_IO"
    assert body[0]["total_wait_time_ms"] == 100
    assert body[0]["bucket_start"].endswith("Z") or "+" in body[0]["bucket_start"]


async def test_list_wait_types_by_hour_groups_by_utc_hour(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    hour10 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=hour10,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=100,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/wait-types-by-hour/{test_instance.id}",
        params={
            "from": (hour10 - timedelta(hours=1)).isoformat(),
            "to": (hour10 + timedelta(hours=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["label"] == "ASYNC_NETWORK_IO"
    assert body[0]["total_wait_time_ms"] == 100


async def test_list_db_users_aggregates_by_db_user(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """Dimensao "Usuarios" (irma de "Bancos de dados"/"SQL"/"Waits") — mesmo formato de
    `list_databases`, agrupado por `db_user`."""
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-user-a",
                sql_text_masked="SELECT ? FROM t",
                db_user="app_erp",
                wait_event_native="IO done",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-user-b",
                sql_text_masked="SELECT ? FROM t",
                db_user="batch_conciliacao",
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(f"/api/v1/queries/users/{test_instance.id}")

    assert response.status_code == 200
    body = response.json()
    by_user = {row["db_user"]: row for row in body}
    assert set(by_user) == {"app_erp", "batch_conciliacao"}
    assert by_user["app_erp"]["execution_count"] == 1
    assert by_user["app_erp"]["total_wait_time_ms"] == 100
    assert by_user["app_erp"]["wait_breakdown"] == [
        {"wait_event_native": "IO done", "total_wait_time_ms": 100}
    ]
    assert by_user["batch_conciliacao"]["wait_breakdown"] == [
        {"wait_event_native": "desconhecido", "total_wait_time_ms": 50}
    ]


async def test_list_db_users_by_day_groups_by_utc_day(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    day1 = datetime(2026, 9, 10, 14, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=day1,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-user-a",
                sql_text_masked="SELECT ?",
                db_user="app_erp",
                wait_time_ms=100,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/users-by-day/{test_instance.id}",
        params={
            "from": (day1 - timedelta(days=1)).isoformat(),
            "to": (day1 + timedelta(days=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["label"] == "app_erp"
    assert body[0]["total_wait_time_ms"] == 100


async def test_list_db_users_by_hour_groups_by_utc_hour(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    hour10 = datetime(2026, 9, 10, 10, 30, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=hour10,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-user-a",
                sql_text_masked="SELECT ?",
                db_user="app_erp",
                wait_time_ms=100,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/users-by-hour/{test_instance.id}",
        params={
            "from": (hour10 - timedelta(hours=1)).isoformat(),
            "to": (hour10 + timedelta(hours=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["label"] == "app_erp"
    assert body[0]["total_wait_time_ms"] == 100


# --- Etapa 11 -- /detail/{instance_id}/{sql_hash}/stats, /sql-insight, /execution-plan ---------


async def test_get_query_stats_by_bucket_counts_executions_per_day(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    day1 = datetime(2026, 9, 1, 10, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=day1,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-stats",
                sql_text_masked="SELECT ? FROM t",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=day1 + timedelta(minutes=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-stats",
                sql_text_masked="SELECT ? FROM t",
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/detail/{test_instance.id}/hash-stats/stats",
        params={
            "from": (day1 - timedelta(days=1)).isoformat(),
            "to": (day1 + timedelta(days=1)).isoformat(),
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["execution_count"] == 2
    assert body[0]["total_wait_time_ms"] == 150
    assert body[0]["avg_wait_time_ms"] == 75.0


async def test_get_query_detail_includes_wait_breakdown(
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
                sql_hash="hash-wb",
                sql_text_masked="SELECT ? FROM t",
                wait_event_native="Lock",
                wait_time_ms=100,
            ),
        ]
    )

    response = await client.get(f"/api/v1/queries/detail/{test_instance.id}/hash-wb")

    assert response.status_code == 200
    body = response.json()
    assert body["wait_breakdown"] == [{"wait_event_native": "Lock", "total_wait_time_ms": 100}]


async def test_get_query_sql_insight_analysis_returns_rules(
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
                sql_hash="hash-insight",
                sql_text_masked="SELECT * FROM t",
            ),
        ]
    )

    class _FakeSqlInsightClient:
        async def check(self, sql_text_masked: str) -> SqlInsightAnalysis:
            assert sql_text_masked == "SELECT * FROM t"
            return SqlInsightAnalysis(
                available=True,
                rules=[SqlInsightRule(rule_id="R001", description="Evite SELECT *", details=None)],
            )

    app.dependency_overrides[get_sql_insight_client] = lambda: _FakeSqlInsightClient()
    try:
        response = await client.get(
            f"/api/v1/queries/detail/{test_instance.id}/hash-insight/sql-insight"
        )
    finally:
        app.dependency_overrides.pop(get_sql_insight_client, None)

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["rules"] == [{"rule_id": "R001", "description": "Evite SELECT *", "details": None}]


async def test_get_query_sql_insight_analysis_returns_404_when_no_samples(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(
        f"/api/v1/queries/detail/{test_instance.id}/hash-does-not-exist/sql-insight"
    )
    assert response.status_code == 404


async def test_get_query_execution_plan_unavailable_for_postgresql_without_connecting(
    client: AsyncClient, test_instance: Instance
) -> None:
    """`test_instance` e PostgreSQL (fixture padrao) — o endpoint devolve `available=False` com
    o motivo certo sem sequer tentar conectar (nenhum monkeypatch de rede necessario)."""
    response = await client.get(
        f"/api/v1/queries/detail/{test_instance.id}/qualquer-hash/execution-plan"
    )

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is False
    assert body["plan_xml"] is None
    assert "PostgreSQL" in body["reason"]


async def test_get_query_execution_plan_returns_404_when_instance_not_found(
    client: AsyncClient,
) -> None:
    response = await client.get(f"/api/v1/queries/detail/{uuid4()}/qualquer-hash/execution-plan")
    assert response.status_code == 404


def _masked_hash(statement_text: str) -> str:
    return hashlib.sha256(mask_sql_literals(statement_text).encode("utf-8")).hexdigest()


class _FakeExecutionPlanCursor:
    def __init__(
        self,
        search_rows: list[dict[str, Any]],
        plan_xml_by_handle: dict,
        active_rows: list[dict[str, Any]] | None = None,
        actual_plan_xml_by_session: dict | None = None,
    ) -> None:
        self._search_rows = search_rows
        self._plan_xml_by_handle = plan_xml_by_handle
        self._active_rows = active_rows or []
        self._actual_plan_xml_by_session = actual_plan_xml_by_session or {}
        self._pending_rows: list[dict[str, Any]] = []
        self._pending_one: dict[str, Any] | None = None

    def execute(self, query: str, params: tuple | None = None) -> None:
        if "dm_exec_query_stats" in query:
            self._pending_rows = self._search_rows
        elif "dm_exec_query_plan" in query:
            assert params is not None
            xml = self._plan_xml_by_handle.get(params[0])
            self._pending_one = {"query_plan": xml} if xml is not None else None
        elif "dm_exec_requests" in query:
            # `fetch_actual_execution_plan` (tentado primeiro pelo service) - sem sessao ativa
            # por padrao, o service cai pro caminho cacheado ja testado abaixo.
            self._pending_rows = self._active_rows
        elif "dm_exec_query_statistics_xml" in query:
            assert params is not None
            xml = self._actual_plan_xml_by_session.get(params[0])
            self._pending_one = {"query_plan": xml} if xml is not None else None
        else:  # pragma: no cover - defensivo
            raise AssertionError(f"execute inesperado: {query}")

    def fetchall(self) -> list[dict[str, Any]]:
        return self._pending_rows

    def fetchone(self) -> dict[str, Any] | None:
        return self._pending_one

    def close(self) -> None:
        pass


class _FakeExecutionPlanConnection:
    def __init__(
        self,
        search_rows: list[dict[str, Any]],
        plan_xml_by_handle: dict,
        active_rows: list[dict[str, Any]] | None = None,
        actual_plan_xml_by_session: dict | None = None,
    ) -> None:
        self._search_rows = search_rows
        self._plan_xml_by_handle = plan_xml_by_handle
        self._active_rows = active_rows
        self._actual_plan_xml_by_session = actual_plan_xml_by_session

    def cursor(self, as_dict: bool = False) -> _FakeExecutionPlanCursor:
        assert as_dict is True
        return _FakeExecutionPlanCursor(
            self._search_rows,
            self._plan_xml_by_handle,
            self._active_rows,
            self._actual_plan_xml_by_session,
        )

    def close(self) -> None:
        pass


async def test_get_query_execution_plan_returns_xml_for_sqlserver_instance(
    client: AsyncClient, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Instancia SQL Server real (`db_type="sqlserver"`, `auth_method="integrated"` — sem
    credencial pra resolver), `pymssql.connect` monkeypatchado pra devolver um plano cujo
    statement mascarado+hasheado bate com o `sql_hash` pedido."""
    instance = Instance(
        name=f"test-instance-sqlserver-{uuid4()}",
        db_type="sqlserver",
        host="127.0.0.1",
        port=1433,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
    )
    db_session.add(instance)
    await db_session.commit()
    await db_session.refresh(instance)

    statement = "SELECT * FROM t WHERE id = 42"
    sql_hash = _masked_hash(statement)
    fake_conn = _FakeExecutionPlanConnection(
        search_rows=[{"plan_handle": b"handle-a", "statement_text": statement}],
        plan_xml_by_handle={b"handle-a": "<ShowPlanXML>...</ShowPlanXML>"},
    )
    monkeypatch.setattr(pymssql, "connect", lambda **kwargs: fake_conn)

    try:
        response = await client.get(
            f"/api/v1/queries/detail/{instance.id}/{sql_hash}/execution-plan"
        )
    finally:
        await db_session.delete(instance)
        await db_session.commit()

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["plan_xml"] == "<ShowPlanXML>...</ShowPlanXML>"
    assert body["reason"] is None
    assert body["source"] == "cached"


async def test_get_query_execution_plan_prefers_actual_over_cached_when_session_active(
    client: AsyncClient, db_session: AsyncSession, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Quando a sessao que rodou `sql_hash` AINDA esta ativa, o service devolve o plano REAL
    (`source="actual"`) sem sequer consultar o cache/plan_handle."""
    instance = Instance(
        name=f"test-instance-sqlserver-{uuid4()}",
        db_type="sqlserver",
        host="127.0.0.1",
        port=1433,
        criticality="standard",
        polling_interval_seconds=10,
        auth_method="integrated",
    )
    db_session.add(instance)
    await db_session.commit()
    await db_session.refresh(instance)

    statement = "SELECT * FROM t WHERE id = 42"
    sql_hash = _masked_hash(statement)
    fake_conn = _FakeExecutionPlanConnection(
        search_rows=[],
        plan_xml_by_handle={},
        active_rows=[{"session_id": 52, "statement_text": statement}],
        actual_plan_xml_by_session={52: "<ShowPlanXML actual='1'>...</ShowPlanXML>"},
    )
    monkeypatch.setattr(pymssql, "connect", lambda **kwargs: fake_conn)

    try:
        response = await client.get(
            f"/api/v1/queries/detail/{instance.id}/{sql_hash}/execution-plan"
        )
    finally:
        await db_session.delete(instance)
        await db_session.commit()

    assert response.status_code == 200
    body = response.json()
    assert body["available"] is True
    assert body["plan_xml"] == "<ShowPlanXML actual='1'>...</ShowPlanXML>"
    assert body["source"] == "actual"


# --- /detail/{instance_id}/{sql_hash}/plan-history (historico de plano de execucao) -------------


async def test_get_query_plan_history_returns_empty_list_when_never_scanned(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(
        f"/api/v1/queries/detail/{test_instance.id}/hash-sem-historico/plan-history"
    )

    assert response.status_code == 200
    assert response.json() == []


async def test_get_query_plan_history_returns_snapshots_most_recent_first(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QueryPlanSnapshotsRepository(db_session)
    await repo.insert_if_changed(
        [
            NormalizedPlanSnapshot(
                instance_id=test_instance.id,
                sql_hash="hash-com-historico",
                plan_hash="plan-hash-old",
                plan_xml_masked="<ShowPlanXML>old</ShowPlanXML>",
                last_execution_time=datetime.now(UTC) - timedelta(days=1),
            )
        ]
    )
    await repo.insert_if_changed(
        [
            NormalizedPlanSnapshot(
                instance_id=test_instance.id,
                sql_hash="hash-com-historico",
                plan_hash="plan-hash-new",
                plan_xml_masked="<ShowPlanXML>new</ShowPlanXML>",
                last_execution_time=datetime.now(UTC),
            )
        ]
    )

    response = await client.get(
        f"/api/v1/queries/detail/{test_instance.id}/hash-com-historico/plan-history"
    )

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert body[0]["plan_hash"] == "plan-hash-new"
    assert body[0]["plan_xml"] == "<ShowPlanXML>new</ShowPlanXML>"
    assert body[0]["source"] == "cached"


async def test_timeslice_ten_minutes_by_database(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """"Fatia de Tempo" — dimensao "database", bucket sub-hora "ten_minutes"."""
    repo = QuerySamplesRepository(db_session)
    hour_start = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=hour_start,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=hour_start + timedelta(minutes=20),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/timeslice/{test_instance.id}",
        params={
            "from": hour_start.isoformat(),
            "to": (hour_start + timedelta(hours=1)).isoformat(),
            "dimension": "database",
            "bucket": "ten_minutes",
        },
    )

    assert response.status_code == 200
    body = response.json()
    by_label = {row["label"]: row for row in body}
    assert by_label["db_a"]["total_wait_time_ms"] == 100
    assert by_label["db_b"]["total_wait_time_ms"] == 50
    # 2 buckets de 10min distintos (minuto 0 e minuto 20 nao caem no mesmo bucket).
    assert by_label["db_a"]["bucket_start"] != by_label["db_b"]["bucket_start"]


async def test_timeslice_requires_from_and_to(
    client: AsyncClient, test_instance: Instance
) -> None:
    response = await client.get(f"/api/v1/queries/timeslice/{test_instance.id}")
    assert response.status_code == 422


async def test_timeslice_sql_dimension_accepts_database_name_filter(
    client: AsyncClient, db_session: AsyncSession, test_instance: Instance
) -> None:
    """dimension="sql" com `database_name` opcional — usado pela "Fatia de Tempo" dentro do
    nivel 2 do drill-down (`/instances/{id}/queries/{databaseName}`), nao so na dimensao "SQL"
    da instancia inteira."""
    repo = QuerySamplesRepository(db_session)
    hour_start = datetime(2026, 9, 1, 10, 0, 0, tzinfo=UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=hour_start,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=hour_start,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=50,
            ),
        ]
    )

    response = await client.get(
        f"/api/v1/queries/timeslice/{test_instance.id}",
        params={
            "from": hour_start.isoformat(),
            "to": (hour_start + timedelta(hours=1)).isoformat(),
            "dimension": "sql",
            "bucket": "ten_minutes",
            "database_name": "db_a",
        },
    )

    assert response.status_code == 200
    body = response.json()
    assert {row["label"]: row["total_wait_time_ms"] for row in body} == {"hash-a": 100}
