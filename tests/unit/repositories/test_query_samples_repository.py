"""Testes de `QuerySamplesRepository.insert_batch` contra o banco real `ab_xql_pm_dev`.

Caso principal: uma regra em `sql_exclusions` (`match_type='program_name'`,
`match_value='meu_programa'`) descarta so as amostras daquele programa — o resto do batch e
inserido normalmente (`Docs/rules/03-sql-privacy.md`).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.instance import Instance
from app.normalizers.models import NormalizedQuerySample
from app.normalizers.wait_taxonomy import WaitCategory
from app.repositories._tables import query_samples_table, sql_exclusions_table
from app.repositories.query_samples_repository import QuerySamplesRepository


async def test_insert_batch_excludes_matching_program_name(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    await db_session.execute(
        sa.insert(sql_exclusions_table),
        [
            {
                "id": uuid4(),
                "instance_id": test_instance.id,
                "match_type": "program_name",
                "match_value": "meu_programa",
                "reason": "teste",
                "created_at": datetime.now(UTC),
            }
        ],
    )
    await db_session.commit()

    excluded_sample = NormalizedQuerySample(
        time=datetime.now(UTC),
        instance_id=test_instance.id,
        engine="postgresql",
        sql_hash="hash-excluded",
        sql_text_masked="SELECT ?",
        program="meu_programa",
    )
    kept_sample = NormalizedQuerySample(
        time=datetime.now(UTC),
        instance_id=test_instance.id,
        engine="postgresql",
        sql_hash="hash-kept",
        sql_text_masked="SELECT ?",
        program="outro_programa",
    )

    repo = QuerySamplesRepository(db_session)
    await repo.insert_batch([excluded_sample, kept_sample])

    result = await db_session.execute(
        sa.select(query_samples_table.c.sql_hash).where(
            query_samples_table.c.instance_id == test_instance.id
        )
    )
    hashes = {row.sql_hash for row in result.fetchall()}
    assert hashes == {"hash-kept"}


async def test_insert_batch_without_exclusions_inserts_all(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    sample = NormalizedQuerySample(
        time=datetime.now(UTC),
        instance_id=test_instance.id,
        engine="sqlserver",
        sql_hash="hash-no-exclusion",
        sql_text_masked="SELECT ?",
    )
    repo = QuerySamplesRepository(db_session)
    await repo.insert_batch([sample])

    result = await db_session.execute(
        sa.select(query_samples_table.c.sql_hash).where(
            query_samples_table.c.instance_id == test_instance.id
        )
    )
    hashes = {row.sql_hash for row in result.fetchall()}
    assert hashes == {"hash-no-exclusion"}


async def test_insert_batch_noop_for_empty_list(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    await repo.insert_batch([])


async def test_list_active_filters_by_window(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-in-window",
                sql_text_masked="SELECT ?",
            ),
            NormalizedQuerySample(
                time=now - timedelta(hours=2),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-out-of-window",
                sql_text_masked="SELECT ?",
            ),
        ]
    )

    rows = await repo.list_active(
        test_instance.id,
        since=now - timedelta(minutes=5),
        until=now + timedelta(minutes=5),
        limit=100,
    )

    hashes = {row.sql_hash for row in rows}
    assert hashes == {"hash-in-window"}


async def test_top_sql_aggregates_by_hash_with_predominant_wait_type(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_name="query-a",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_name="query-a",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=200,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=2),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_name="query-a",
                sql_text_masked="SELECT ? FROM t",
                wait_type=WaitCategory.IO,
                wait_time_ms=50,
            ),
        ]
    )

    rows = await repo.top_sql(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    assert len(rows) == 1
    row = rows[0]
    assert row.sql_hash == "hash-a"
    assert row.execution_count == 3
    assert row.total_wait_time_ms == 350
    assert row.predominant_wait_type == WaitCategory.LOCK.value


async def test_wait_summary_groups_by_wait_type(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ?",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ?",
                wait_type=WaitCategory.LOCK,
                wait_time_ms=50,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-c",
                sql_text_masked="SELECT ?",
                wait_type=WaitCategory.IO,
                wait_time_ms=30,
            ),
        ]
    )

    rows = await repo.wait_summary(
        test_instance.id, since=now - timedelta(minutes=1), until=now + timedelta(minutes=1)
    )

    summary = {row.wait_type: row.total_wait_time_ms for row in rows}
    assert summary == {WaitCategory.LOCK.value: 150, WaitCategory.IO.value: 30}


async def test_top_sql_by_database_groups_by_database_name(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-a",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=50,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-c",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=30,
            ),
        ]
    )

    rows = await repo.top_sql_by_database(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_database = {row.database_name: row for row in rows}
    assert set(by_database) == {"db_a", "db_b"}
    assert by_database["db_a"].execution_count == 2
    assert by_database["db_a"].total_wait_time_ms == 150
    assert by_database["db_b"].execution_count == 1
    assert by_database["db_b"].total_wait_time_ms == 30


async def test_top_sql_by_database_groups_null_database_name_as_desconhecido(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-no-db",
                sql_text_masked="SELECT ?",
                database_name=None,
                wait_time_ms=10,
            )
        ]
    )

    rows = await repo.top_sql_by_database(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    assert len(rows) == 1
    assert rows[0].database_name == "desconhecido"
    assert rows[0].execution_count == 1


async def test_wait_breakdown_by_database_groups_by_native_wait_type(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    """`wait_breakdown_by_database` decompoe o wait time de cada banco pelo tipo de espera NATIVO
    (`wait_event_native`) — granularidade maior que o bucket canonico de 9 categorias usado em
    `wait_summary()` (ex.: `PAGEIOLATCH_SH` continua `PAGEIOLATCH_SH`, nao vira so `IO`)."""
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
                database_name="db_a",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-b",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_event_native="PAGEIOLATCH_SH",
                wait_time_ms=50,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-c",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_event_native="ASYNC_NETWORK_IO",
                wait_time_ms=30,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="sqlserver",
                sql_hash="hash-d",
                sql_text_masked="SELECT ?",
                database_name="db_b",
                wait_event_native=None,
                wait_time_ms=10,
            ),
        ]
    )

    rows = await repo.wait_breakdown_by_database(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    by_key = {(row.database_name, row.wait_event_native): row.total_wait_time_ms for row in rows}
    assert by_key[("db_a", "PAGEIOLATCH_SH")] == 150
    assert by_key[("db_a", "ASYNC_NETWORK_IO")] == 30
    assert by_key[("db_b", "desconhecido")] == 10


async def test_top_sql_filters_by_database_name(
    db_session: AsyncSession, test_instance: Instance
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
                sql_name="query-shared",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                wait_time_ms=100,
            ),
            NormalizedQuerySample(
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-shared",
                sql_name="query-shared",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_b",
                wait_time_ms=200,
            ),
        ]
    )

    unfiltered = await repo.top_sql(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )
    assert len(unfiltered) == 1
    assert unfiltered[0].sql_hash == "hash-shared"
    # Sem filtro: agrega os dois bancos juntos sob o mesmo sql_hash (comportamento historico).
    assert unfiltered[0].execution_count == 2
    assert unfiltered[0].total_wait_time_ms == 300

    filtered = await repo.top_sql(
        test_instance.id,
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
        database_name="db_a",
    )
    assert len(filtered) == 1
    assert filtered[0].sql_hash == "hash-shared"
    assert filtered[0].database_name == "db_a"
    assert filtered[0].execution_count == 1
    assert filtered[0].total_wait_time_ms == 100


async def test_query_detail_returns_raw_samples_for_hash(
    db_session: AsyncSession, test_instance: Instance
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
                time=now + timedelta(seconds=1),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-detail",
                sql_name="query-detail",
                sql_text_masked="SELECT ? FROM t",
                database_name="db_a",
                session_id="2",
                wait_time_ms=20,
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-other",
                sql_text_masked="SELECT ? FROM other",
                database_name="db_a",
            ),
        ]
    )

    rows = await repo.query_detail(
        test_instance.id,
        "hash-detail",
        since=now - timedelta(minutes=1),
        until=now + timedelta(minutes=1),
        limit=10,
    )

    assert len(rows) == 2
    session_ids = {row.session_id for row in rows}
    assert session_ids == {"1", "2"}
    # Ordenado por time desc — o mais recente (session_id="2") vem primeiro.
    assert rows[0].session_id == "2"


async def test_latest_by_session_returns_most_recent_row(
    db_session: AsyncSession, test_instance: Instance
) -> None:
    repo = QuerySamplesRepository(db_session)
    now = datetime.now(UTC)
    await repo.insert_batch(
        [
            NormalizedQuerySample(
                time=now - timedelta(seconds=10),
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-old",
                sql_text_masked="SELECT ? FROM old",
                session_id="session-1",
                db_user="old_user",
            ),
            NormalizedQuerySample(
                time=now,
                instance_id=test_instance.id,
                engine="postgresql",
                sql_hash="hash-new",
                sql_text_masked="SELECT ? FROM new",
                session_id="session-1",
                db_user="new_user",
            ),
        ]
    )

    result = await repo.latest_by_session(
        test_instance.id, ["session-1"], since=now - timedelta(minutes=1)
    )

    assert result["session-1"].sql_hash == "hash-new"
    assert result["session-1"].db_user == "new_user"
